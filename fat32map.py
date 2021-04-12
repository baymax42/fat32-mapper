import argparse
from array import array
from ctypes import c_uint32
from io import BytesIO
from os import path

from parser.mbr_partition_table import MbrPartitionTable
from parser.vfat import Vfat

MBR_SECTOR_SIZE = 512


class Fat32Partition(Vfat):
    FAT_ENTRY_FORMAT = 'L'
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


def get_fat32_partitions(image_io: BytesIO) -> [Fat32Partition]:
    mbr_section = MbrPartitionTable.from_io(image_io)
    partitions = filter(lambda partition: partition.lba_start != 0, mbr_section.partitions)
    partition_offsets = map(lambda partition: partition.lba_start * MBR_SECTOR_SIZE, partitions)

    fat32_partitions = []
    for offset in partition_offsets:
        image_io.seek(offset)
        vfat_partition = Fat32Partition.from_io(image_io)
        if vfat_partition.boot_sector.is_fat32:
            fat32_partitions.append(vfat_partition)
    return fat32_partitions


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Map section to specific file in FAT32 volume')
    parser.add_argument('section', type=int,
                        help='section which will be mapped to file')
    parser.add_argument('image_path', metavar='image', type=str,
                        help='path to raw image file')

    args = parser.parse_args()
    absolute_image_path = path.abspath(args.image_path)
    image_io = open(absolute_image_path, 'rb')
    fat32_partitions = get_fat32_partitions(image_io)
    for part in fat32_partitions:
        print("OEMName:", part.boot_sector.oem_name)
        print("Hidden sectors:", part.boot_sector.bpb.num_hidden_sectors)
        print("FAT size:", part.boot_sector.size_fat)
        print("First 32 entries from FAT:", part.raw_file_allocation_table[:32])
        """
        Kaitai's Vfat root_dir is broken because it relies on self.boot_sector.size_root_dir (which can be zero on FAT32)
        and self._root.boot_sector.bpb.max_root_dir_rec (which also can be zero).
        
        Needs to be re-done.
        """
