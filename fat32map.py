import argparse
import struct
from array import array
from ctypes import c_uint32
from io import BytesIO
from math import floor
from os import path

from parser.mbr_partition_table import MbrPartitionTable
from parser.vfat import Vfat

MBR_SECTOR_SIZE = 512
DIR_ENTRY_SIZE = 32


class DirectoryEntry:
    FORMAT = '<11sB7xBHHHHI'
    ATTR_DIRECTORY = 0x10

    def __init__(self, bytes):
        data = struct.unpack(DirectoryEntry.FORMAT, bytes)
        self.name = data[0]
        self.attrs = data[1]
        self.creation_time_tenth_milis = data[2]
        self.first_data_cluster = (data[3] << 2) + data[6]
        self.write_time = data[4]
        self.write_date = data[5]
        self.file_size = data[7]

    @property
    def is_free(self):
        return self.name[0] == 0x00

    @property
    def filename(self):
        return str(self.name)

    @property
    def is_directory(self):
        return True if self.attrs & DirectoryEntry.ATTR_DIRECTORY > 0 else False


class Fat32Partition(Vfat):
    FAT_ENTRY_FORMAT = 'I'
    EOC_START = c_uint32(0x0FFFFFF8)

    def __init__(self, _io):
        super().__init__(_io)
        self.lba_start = None
        self.fat = None

    @staticmethod
    def is_eoc(fat_chain_element: int) -> bool:
        return fat_chain_element >= Fat32Partition.EOC_START.value

    @property
    def raw_file_allocation_table(self) -> array:
        """
        Returns first non-empty (if exists, else last one) file allocation table.
        It does not compare them in any way.
        """
        if self.fat is None:
            current_fat_bytes = None
            for fat in self.fats:
                current_fat_bytes = bytearray(fat)
                if len(current_fat_bytes) == current_fat_bytes.count(0):
                    continue
            self.fat = array(self.FAT_ENTRY_FORMAT, current_fat_bytes)
        return self.fat

    @property
    def directory_section_offset(self):
        return self.boot_sector.bpb.num_reserved_ls + (self.boot_sector.ls_per_fat * self.boot_sector.bpb.num_fats)

    def find_first_cluster(self, cluster: int) -> int:
        first_cluster = cluster
        found = True
        fat = self.raw_file_allocation_table
        while found:
            found = False
            for i in range(len(fat)):
                if fat[i] == first_cluster:
                    first_cluster = i
                    found = True
                    break
        return first_cluster

    def sector_to_cluster(self, relative_sector: int) -> int:
        clusters_offset = self.directory_section_offset
        cluster_num = floor((relative_sector - clusters_offset) / self.boot_sector.bpb.ls_per_clus)
        return cluster_num + 2

    def cluster_to_byte(self, cluster: int) -> int:
        return self.boot_sector.bpb.ls_per_clus * cluster * self.boot_sector.bpb.bytes_per_ls

    def next_file_in_directory(self, first_dir_cluster, partition_lba):
        io = self._io._io
        offset, end_of_dir = 0, False

        fat = self.raw_file_allocation_table
        partition_byte_offset = partition_lba * MBR_SECTOR_SIZE
        directory_section_byte_offset = self.directory_section_offset * self.boot_sector.bpb.bytes_per_ls
        current_dir_byte_offset = partition_byte_offset + self.cluster_to_byte(
            first_dir_cluster) + directory_section_byte_offset
        cluster_byte_size = self.boot_sector.bpb.ls_per_clus * self.boot_sector.bpb.bytes_per_ls
        while not end_of_dir:
            io.seek(current_dir_byte_offset + offset)
            entry = DirectoryEntry(io.read(DIR_ENTRY_SIZE))
            if not entry.is_free:
                yield entry
            offset += DIR_ENTRY_SIZE
            # Traverse the next cluster from FAT
            if current_dir_byte_offset + offset > current_dir_byte_offset + cluster_byte_size:
                next_clus = fat[first_dir_cluster]
                if self.is_eoc(next_clus):
                    end_of_dir = True
                current_dir_byte_offset = partition_byte_offset + self.cluster_to_byte(
                    next_clus) + directory_section_byte_offset
                offset = 0


def get_fat32_partition(image_io: BytesIO, sector: int) -> (Fat32Partition, int):
    mbr_section = MbrPartitionTable.from_io(image_io)
    partitions = filter(lambda partition: partition.lba_start != 0, mbr_section.partitions)

    for partition in partitions:
        image_io.seek(partition.lba_start * MBR_SECTOR_SIZE)
        vfat_partition = Fat32Partition.from_io(image_io)

        partition_sector_start = partition.lba_start
        partition_sector_end = partition_sector_start + partition.num_sectors
        if partition_sector_start <= sector <= partition_sector_end and vfat_partition.boot_sector.is_fat32:
            return vfat_partition, partition.lba_start
    return None, 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Map section to specific file in FAT32 volume')
    parser.add_argument('section', type=int,
                        help='section which will be mapped to file')
    parser.add_argument('image_path', metavar='image', type=str,
                        help='path to raw image file')

    args = parser.parse_args()
    absolute_image_path = path.abspath(args.image_path)
    absolute_sector = args.section

    image_io = open(absolute_image_path, 'rb')
    fat32_partition, partition_lba = get_fat32_partition(image_io, absolute_sector)

    if fat32_partition is None:
        print("Not in FAT32 partition")
        exit()

    print("Partition LBA", partition_lba)

    boot_sector = fat32_partition.boot_sector
    fat = fat32_partition.raw_file_allocation_table

    print("Bytes per logical sector:", boot_sector.bpb.bytes_per_ls)
    print("Sectors per cluster:", boot_sector.bpb.ls_per_clus)

    print("Sectors per FAT:", boot_sector.ls_per_fat)
    print("Number of FATs:", boot_sector.bpb.num_fats)
    print("First 32 entries from FAT:", fat[:32])

    print("Root dir cluster:", fat32_partition.boot_sector.ebpb_fat32.root_dir_start_clus)
    print("Cluster offset in sectors:", fat32_partition.directory_section_offset)

    cluster_num = fat32_partition.sector_to_cluster(absolute_sector - partition_lba)

    if cluster_num < 0:
        print("Not in data region")
        exit()

    print("Cluster number:", cluster_num)

    print("First cluster of the found file:", fat32_partition.find_first_cluster(cluster_num))

    entries = [entry.name for entry in fat32_partition.next_file_in_directory(0, partition_lba)]
    print(entries)
