#include "btier.h"

u64 round_to_blksize(u64 size)
{
	u64 roundsize;
	roundsize = (size >> BLK_SHIFT) << BLK_SHIFT;
	return roundsize;
}

u64 calc_bitlist_size(u64 devicesize)
{
	u64 bitlistsize;
	u64 round;
	u64 rdevsize;

	rdevsize = round_to_blksize(devicesize);
	bitlistsize = (rdevsize >> BLK_SHIFT);
	round = (bitlistsize >> BLK_SHIFT) << BLK_SHIFT;
	if (round < bitlistsize)
		bitlistsize = round + BLKSIZE;
	return bitlistsize;
}

#ifdef __KERNEL__
u64 btier_div(u64 x, u32 y)
{
	u64 res = x;
	do_div(res, y);
	return res;
}
#else
u64 btier_div(u64 x, u32 y) { return x / y; }
#endif
u64 calc_blocklist_size(u64 total_device_size, u64 total_bitlist_size)
{
	u64 blocklistsize;
	u64 round;
	u64 netdevsize;
	u32 blocks;
	u64 blksize = BLKSIZE;

	netdevsize = total_device_size - total_bitlist_size;
	blocks = btier_div(blksize, sizeof(struct physical_blockinfo));
	blocks++;
	blocklistsize = btier_div(netdevsize, blocks);
	round = btier_div(blocklistsize, sizeof(struct physical_blockinfo));
	round *= sizeof(struct physical_blockinfo);
	if (round < blocklistsize)
		blocklistsize = round + sizeof(struct physical_blockinfo);
	round = (blocklistsize >> BLK_SHIFT) << BLK_SHIFT;
	if (round < blocklistsize)
		blocklistsize = round + BLKSIZE;
	return blocklistsize;
}
