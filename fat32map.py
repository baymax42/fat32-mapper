import argparse
import queue
import struct
from array import array
from ctypes import c_uint32
from io import BytesIO
from math import floor
from os import path
from typing import Union

from parser.mbr_partition_table import MbrPartitionTable
from parser.vfat import Vfat

MBR_SECTOR_SIZE = 512
DIR_ENTRY_SIZE = 32

class LFNEntry:

    SEQUENCE_NUMBER_MASK = 0x40    
    
    def __init__(self):
        self.first_byte = 0x0
        self.name_chars = None
        self.checksum = 0      
    
    @staticmethod
    def from_bytes(bytes):
        entry = LFNEntry()
        entry.first_byte = bytes[0]
        entry.name_chars = bytes[1:11] + bytes[14:26] + bytes[28:32]
        entry.checksum = bytes[13]
        
        return entry
        
    @property
    def is_deleted(self):
        return self.first_byte == 0xe5  
        
    @property    
    def is_final(self):
        return (self.first_byte & self.SEQUENCE_NUMBER_MASK) == self.SEQUENCE_NUMBER_MASK
           
    @property
    def seq_number(self):
        if self.is_deleted:
            return -1  
        if self.is_final:
            return self.first_byte ^ self.SEQUENCE_NUMBER_MASK
        
        return self.first_byte   
        
    @property
    def name(self):
        return self.name_chars.rstrip(b'\xff').decode("utf-16")   
    

class DirectoryEntry:
    FORMAT = '<11sB7xBHHHHI'
    ATTR_DIRECTORY = 0x10
    ATTR_VOLUME_ID = 0x08
    ATTR_LONG_DIR = 0xF
    

    def __init__(self, **kwargs):
        self._raw_name = kwargs.get('raw_name', '')
        self.first_data_cluster = kwargs.get('first_clus')
        self.attrs = 0
        self.creation_time_tenth_milis = 0
        self.write_time = 0
        self.write_date = 0
        self.file_size = 0
        
        self.lfn_entries = []

    @property
    def name(self):
        if len(self.lfn_entries) == 0:
            if self._raw_name[0] == 0xe5:
                decoded_name = '?' + self._raw_name[1:].decode('ascii')
            else:
                decoded_name = self._raw_name.decode('ascii')
            extension = decoded_name[8:].rstrip()
            return f'{decoded_name[0:8].rstrip()}' + (f'.{extension}' if len(extension) > 0 else '')
            
        else:
            name = ''
            for entry in self.lfn_entries[::-1]:
                name = name + entry.name
            return name
     

    @staticmethod
    def from_bytes(bytes):
        data = struct.unpack(DirectoryEntry.FORMAT, bytes)
        dir_entry = DirectoryEntry(raw_name=data[0], first_clus=(data[3] << 2) + data[6])
        dir_entry.attrs = data[1]
        dir_entry.creation_time_tenth_milis = data[2]
        dir_entry.write_time = data[4]
        dir_entry.write_date = data[5]
        dir_entry.file_size = data[7]
        return dir_entry

    @staticmethod
    def root_directory(cluster):
        return DirectoryEntry(
            raw_name=b"ROOTDIR",
            first_clus=cluster,
        )

    @property
    def is_free(self):
        return self._raw_name[0] == 0x00

    @property
    def _if_filename_decodable(self):
        try:
            self._raw_name.decode('ascii')
        except UnicodeDecodeError:
            return False
        return True

    @property
    def is_directory(self):
        combined_attrs = (DirectoryEntry.ATTR_DIRECTORY | DirectoryEntry.ATTR_VOLUME_ID)
        if self.attrs & DirectoryEntry.ATTR_LONG_DIR == DirectoryEntry.ATTR_LONG_DIR:
            return False
        return (self.attrs & combined_attrs == DirectoryEntry.ATTR_DIRECTORY) and self._if_filename_decodable
        
    @property
    def is_lfn_dir(self):
        return (self.attrs & DirectoryEntry.ATTR_LONG_DIR) == DirectoryEntry.ATTR_LONG_DIR

    def __str__(self):
        return f'name: {self.name}\n' \
               f'attrs: {self.attrs}\n' \
               f'creation_time_tenth_milis: {self.creation_time_tenth_milis}\n' \
               f'first_data_cluster: {self.first_data_cluster}\n' \
               f'write_time: {self.write_time}\n' \
               f'write_date: {self.write_date}\n' \
               f'file_size: {self.file_size}'


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
    def raw_file_allocation_table(self) -> list:
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
            self.fat = list(map(
                lambda fat_entry: fat_entry & 0x0FFFFFFF,
                array(self.FAT_ENTRY_FORMAT, current_fat_bytes)
            ))
        return self.fat

    @property
    def directory_section_offset(self):
        return self.boot_sector.bpb.num_reserved_ls + (self.boot_sector.ls_per_fat * self.boot_sector.bpb.num_fats)

    def _find_first_cluster_in_fat(self, cluster: int) -> int:
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

    def _cluster_to_byte(self, cluster: int) -> int:
        # First two clusters are not used in data section, pad it properly
        return self.boot_sector.bpb.ls_per_clus * (cluster - 2) * self.boot_sector.bpb.bytes_per_ls

    def next_file_in_directory(self, first_dir_cluster, partition_lba):
        io = self._io._io
        offset, end_of_dir = 0, False
        next_clus = first_dir_cluster

        fat = self.raw_file_allocation_table
        partition_byte_offset = partition_lba * MBR_SECTOR_SIZE
        directory_section_byte_offset = self.directory_section_offset * self.boot_sector.bpb.bytes_per_ls
        current_dir_byte_offset = partition_byte_offset + self._cluster_to_byte(
            first_dir_cluster) + directory_section_byte_offset
        cluster_byte_size = self.boot_sector.bpb.ls_per_clus * self.boot_sector.bpb.bytes_per_ls
        
        lfn_entries = []
        while not end_of_dir:
            io.seek(current_dir_byte_offset + offset)
            bytes = io.read(DIR_ENTRY_SIZE)
            entry = DirectoryEntry.from_bytes(bytes)
            
            if entry.is_lfn_dir:
                lfn_entries.append(LFNEntry.from_bytes(bytes))                
            else:
                entry.lfn_entries = lfn_entries
                lfn_entries = []
                if not entry.is_free:
                    yield entry
                    
            offset += DIR_ENTRY_SIZE
            # Traverse the next cluster from FAT
            if offset >= cluster_byte_size:
                next_clus = fat[next_clus]
                if self.is_eoc(next_clus) or next_clus == 0:
                    end_of_dir = True
                current_dir_byte_offset = partition_byte_offset + self._cluster_to_byte(
                    next_clus) + directory_section_byte_offset
                offset = 0

    def _check_if_root_dir_clus(self, cluster):
        return cluster == self.boot_sector.ebpb_fat32.root_dir_start_clus

    def find_file_by_cluster(self, cluster: int, partition_lba: int) -> Union[DirectoryEntry, None]:
        first_cluster = self._find_first_cluster_in_fat(cluster)

        if first_cluster == self.boot_sector.ebpb_fat32.root_dir_start_clus:
            print("\tDetected root dir")
            return DirectoryEntry.root_directory(first_cluster)

        directories_clus = queue.Queue()
        root_dir_clus = self.boot_sector.ebpb_fat32.root_dir_start_clus
        directories_clus.put(root_dir_clus)
        seen_directories = {0, root_dir_clus}
        while not directories_clus.empty():
            current_dir_clus = directories_clus.get()
            print("Searching in cluster:", current_dir_clus)
            for file_entry in self.next_file_in_directory(current_dir_clus, partition_lba):
                if file_entry.first_data_cluster == first_cluster:
                    return file_entry
                elif file_entry.is_directory and file_entry.first_data_cluster not in seen_directories:
                    print("\tFound new directory", file_entry.name)
                    seen_directories.add(file_entry.first_data_cluster)
                    directories_clus.put(file_entry.first_data_cluster)
        return None


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
        print("Not in FAT32 partition!")
        exit()

    print("Partition LBA", partition_lba)

    boot_sector = fat32_partition.boot_sector
    fat = fat32_partition.raw_file_allocation_table

    print("Bytes per logical sector:", boot_sector.bpb.bytes_per_ls)
    print("Sectors per cluster:", boot_sector.bpb.ls_per_clus)

    print("Sectors per FAT:", boot_sector.ls_per_fat)
    print("Number of FATs:", boot_sector.bpb.num_fats)

    print("Root dir cluster:", fat32_partition.boot_sector.ebpb_fat32.root_dir_start_clus)
    print("Cluster offset in sectors:", fat32_partition.directory_section_offset)

    cluster_num = fat32_partition.sector_to_cluster(absolute_sector - partition_lba)
    if cluster_num < 0:
        print("Not in data region!")
        exit()

    print("=" * 45)

    result = fat32_partition.find_file_by_cluster(cluster_num, partition_lba)
    print("=" * 45)
    print(result if result is not None else 'No file bound with FAT has been found')
