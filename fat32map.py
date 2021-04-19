import argparse
from array import array
from ctypes import c_uint32
from io import BytesIO
from math import floor
from os import path

from parser.mbr_partition_table import MbrPartitionTable
from parser.vfat import Vfat

MBR_SECTOR_SIZE = 512


class Fat32Partition(Vfat):
    FAT_ENTRY_FORMAT = 'L'
    # FAT_ENTRY_FORMAT = 'I'
    EOC_START = c_uint32(0x0FFFFFF8)

    def __init__(self, _io):
        super().__init__(_io)

    @staticmethod
    def is_eoc(fat_chain_element: int) -> bool:
        return fat_chain_element >= Fat32Partition.EOC_START.value

    @property
    def raw_file_allocation_table(self) -> array:
        """
        Returns first non-empty (if exists, else last one) file allocation table.
        It does not compare them in any way.
        """
        current_fat_bytes = None
        for fat in self.fats:
            current_fat_bytes = bytearray(fat)
            if len(current_fat_bytes) == current_fat_bytes.count(0):
                continue
        return array(self.FAT_ENTRY_FORMAT, current_fat_bytes)


def get_fat32_partition(image_io: BytesIO, sector: int) -> (Fat32Partition, int):
    mbr_section = MbrPartitionTable.from_io(image_io)
    partitions = filter(lambda partition: partition.lba_start != 0, mbr_section.partitions)

    for partition in partitions:
        image_io.seek(partition.lba_start * MBR_SECTOR_SIZE)
        vfat_partition = Fat32Partition.from_io(image_io)

        part_sector_size = vfat_partition.boot_sector.bpb.bytes_per_ls
        partition_sector_start = partition.lba_start * MBR_SECTOR_SIZE
        partition_sector_end = partition_sector_start + partition.num_sectors * part_sector_size
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

    boot_sector = fat32_partition.boot_sector
    fat = fat32_partition.raw_file_allocation_table

    print("Bytes per logical sector:", boot_sector.bpb.bytes_per_ls)
    print("Sectors per cluster:", boot_sector.bpb.ls_per_clus)

    print("Sectors per FAT:", boot_sector.ls_per_fat)
    print("Number of FATs:", boot_sector.bpb.num_fats)
    print("First 32 entries from FAT:", fat[:32])

    print("Root dir cluster:", fat32_partition.boot_sector.ebpb_fat32.root_dir_start_clus)
    print("Cluster offset in sectors:", fat32_partition.boot_sector.bpb.num_reserved_ls + (
            fat32_partition.boot_sector.ls_per_fat * fat32_partition.boot_sector.bpb.num_fats))

    relative_sector = absolute_sector - partition_lba
    clusters_offset = boot_sector.bpb.num_reserved_ls + (boot_sector.ls_per_fat * boot_sector.bpb.num_fats)
    cluster_num = floor((relative_sector - clusters_offset) / boot_sector.bpb.ls_per_clus)
    cluster_num = cluster_num + 2
    if cluster_num < 0:
        print("Not in data region")
        exit()

    print("Cluster number:", cluster_num)

    first_cluster = cluster_num
    found = True
    while found:
        found = False
        for i in range(len(fat)):
            if fat[i] == first_cluster:
                first_cluster = i
                found = True
                break

    print("First cluster of the found file:", first_cluster)
