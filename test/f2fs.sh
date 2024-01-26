#!/bin/bash
####################################################
##### Mount F2FS on a zone namespace SSD device#####
####################################################

ZNS_NAME="nvme2n2"
ZNS_PATH="/dev/"${ZNS_NAME}
REGULAR_PATH="/dev/nvme5n1p2"
ZNS_FS_PATH="/data/public/hjl/bbtree/f2fs"

# change to mq deadline scheduler
echo mq-deadline >/sys/block/${ZNS_NAME}/queue/scheduler

# mkfs
mkfs.f2fs -f -m -c ${ZNS_PATH} ${REGULAR_PATH}

# mount
mount -t f2fs ${REGULAR_PATH} ${ZNS_FS_PATH}
chown hjl:hjl ${ZNS_FS_PATH}
# umount ${ZNS_FS_PATH}

##
0xf4 nand_bytes_written 100 950765
0xf5 host_bytes_written 100 859895
##
