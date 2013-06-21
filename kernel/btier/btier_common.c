#include "btier.h"

u64 __udivmoddi4(u64 num, u64 den, u64 * rem_p)
{
	u64 quot = 0, qbit = 1;

	if (den == 0) {
		return 1 / ((unsigned)den);	/* Intentional divide by zero, without
						   triggering a compiler warning which
						   would abort the build */
	}

/* Left-justify denominator and count shift */
	while ((int64_t) den >= 0) {
		den <<= 1;
		qbit <<= 1;
	}

	while (qbit) {
		if (den <= num) {
			num -= den;
			quot += qbit;
		}
		den >>= 1;
		qbit >>= 1;
	}

	if (rem_p)
		*rem_p = num;

	return quot;
}

u64 __udivdi3(u64 num, u64 den)
{
	return __udivmoddi4(num, den, NULL);
}

u64 __umoddi3(u64 num, u64 den)
{
	u64 v;

	(void)__udivmoddi4(num, den, &v);
	return v;
}

u64 round_to_blksize(u64 size)
{
	u64 roundsize;
	roundsize = size / BLKSIZE;
	roundsize *= BLKSIZE;
	return roundsize;
}

u64 calc_bitlist_size(u64 devicesize)
{
	u64 bitlistsize;
	u64 startofbitlist;
	u64 round;
	u64 rdevsize;

	rdevsize = round_to_blksize(devicesize);
	startofbitlist = TIER_HEADERSIZE;
	bitlistsize = (rdevsize / BLKSIZE);
	round = bitlistsize / BLKSIZE;
	round *= BLKSIZE;
	if (round < bitlistsize)
		bitlistsize = round + BLKSIZE;
	return bitlistsize;
}

u64 calc_blocklist_size(u64 total_device_size, u64 total_bitlist_size)
{
	u64 blocklistsize;
	u64 round;
	u64 netdevsize;
	u64 blocks;
	u64 binfosize = sizeof(struct blockinfo);

	netdevsize = total_device_size - total_bitlist_size;
	blocks = BLKSIZE / binfosize;
	blocks++;
	blocklistsize = netdevsize / blocks;
	round = blocklistsize / binfosize;
	round *= sizeof(struct blockinfo);
	if (round < blocklistsize)
		blocklistsize = round + sizeof(struct blockinfo);
	round = blocklistsize / BLKSIZE;
	round *= BLKSIZE;
	if (round < blocklistsize)
		blocklistsize = round + BLKSIZE;
	return blocklistsize;
}
