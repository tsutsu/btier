#define _LARGEFILE64_SOURCE
#define _XOPEN_SOURCE 500
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include "../kernel/btier/btier_common.c"

#define die_ioctlerr(f...) { fprintf(stderr,(f)); flock(fd, LOCK_UN); exit(-1); }
#define die_syserr() { fprintf(stderr,"Fatal system error : %s",strerror(errno)); exit(-2); }

int errno;

struct backing_device {
	int tier_dta_file;
	u64 bitlistsize;
	u64 devicesize;
	u64 startofdata;
	u64 startofbitlist;
	u64 startofblocklist;
	char *datafile;
};

struct option_info {
	int backup;
	int restore;
	int sectorsize;
	u64 blocklistsize;
	struct backing_device **backdev;
	char *device;
	int backdev_count;
	u64 total_device_size;
	u64 bitlistsize_total;
};

void *s_malloc(size_t size)
{
	void *retval;
	retval = malloc(size);
	if (!retval)
		die_syserr();
	return retval;
}

void *s_realloc(void *ptr, size_t size)
{
	void *retval;
	retval = realloc(ptr, size);

	if (!retval)
		die_syserr();
	return retval;
}

void *as_sprintf(const char *fmt, ...)
{
	/* Guess we need no more than 100 bytes. */
	int n, size = 100;
	void *p;
	va_list ap;
	p = s_malloc(size);
	while (1) {
		/* Try to print in the allocated space. */
		va_start(ap, fmt);
		n = vsnprintf(p, size, fmt, ap);
		va_end(ap);
		/* If that worked, return the string. */
		if (n > -1 && n < size)
			return p;
		/* Else try again with more space. */
		if (n > -1)	/* glibc 2.1 */
			size = n + 1;	/* precisely what is needed */
		else		/* glibc 2.0 */
			size *= 2;	/* twice the old size */
		p = s_realloc(p, size);
	}
}

int s_pwrite(int fd, const void *buf, size_t len, u64 off)
{
	size_t total;
	ssize_t thistime;
	u64 thisoff;

	thisoff = off;
	for (total = 0; total < len;) {
		thistime = pwrite(fd, buf + total, len - total, thisoff);
		if (thistime < 0) {
			if (EINTR == errno || EAGAIN == errno)
				continue;
			return thistime;	/* always an error for writes */
		}
		total += thistime;
		thisoff += total;
	}
	return total;
}

int s_pread(int fd, void *buf, size_t len, off_t off)
{
	int total;
	int thistime;
	off_t thisoffset;

	thisoffset = off;
	for (total = 0; total < len;) {
		thistime = pread(fd, buf + total, len - total, thisoffset);
		if (thistime < 0) {
			if (EINTR == errno || EAGAIN == errno)
				continue;
			return -1;
		} else if (thistime == 0) {
			/* EOF, but we didn't read the minimum.  return what we've read
			 * so far and next read (if there is one) will return 0. */
			return total;
		}
		total += thistime;
		thisoffset += total;
	}
	return total;
}

static struct option_info mkoptions;

void backup_device_magic(int fd, u64 total_device_size,
			 unsigned int devicenr, u64 blocklistsize,
			 struct backing_device *bdev)
{
	struct devicemagic magic;
	int res, sfd;
	char *sp = "/tmp/magic_dev";
	char *fsp;
	int mode = O_CREAT | O_TRUNC | O_RDWR;

	fsp = as_sprintf("%s%i", sp, devicenr);
	sfd = open(fsp, mode, 0600);
	free(fsp);
	if (sfd < 0)
		die_syserr();
	memset(&magic, 0, sizeof(struct devicemagic));
	res = s_pread(fd, &magic, sizeof(magic), 0);
	if (res != sizeof(magic))
		die_syserr();
	res = s_pwrite(sfd, &magic, sizeof(magic), 0);
	if (res != sizeof(magic))
		die_syserr();
}

void restore_device_magic(int fd, u64 total_device_size,
			  unsigned int devicenr, u64 blocklistsize,
			  struct backing_device *bdev)
{
	struct devicemagic magic;
	int res, sfd;
	char *sp = "/tmp/magic_dev";
	char *fsp;
	int mode = O_RDONLY;

	fsp = as_sprintf("%s%i", sp, devicenr);
	sfd = open(fsp, mode, 0600);
	free(fsp);
	if (sfd < 0)
		die_syserr();
	memset(&magic, 0, sizeof(struct devicemagic));
	res = s_pread(sfd, &magic, sizeof(magic), 0);
	if (res != sizeof(magic))
		die_syserr();
	res = s_pwrite(fd, &magic, sizeof(magic), 0);
	if (res != sizeof(magic))
		die_syserr();
}

void backup_list(int fd, u64 size, u64 soffset, char *type, int device)
{
	char *block;
	u64 end_offset = soffset + size;
	u64 start_offset = soffset;
	int res;
	char *sp = "/tmp/";
	char *fsp;
	int mode = O_CREAT | O_TRUNC | O_RDWR;
	int sfd;

	fsp = as_sprintf("%s%s%i", sp, type, device);
	sfd = open(fsp, mode, 0600);
	free(fsp);
	if (sfd < 0)
		die_syserr();
	block = s_malloc(BLKSIZE);
	memset(block, 0, BLKSIZE);
	while (end_offset > start_offset) {
		res = s_pread(fd, block, BLKSIZE, start_offset);
		if (res != BLKSIZE) {
			free(block);
			die_syserr();
		}
		res = s_pwrite(sfd, block, BLKSIZE, start_offset - soffset);
		if (res != BLKSIZE) {
			free(block);
			die_syserr();
		}
		start_offset += BLKSIZE;
	}
	free(block);
	close(sfd);
}

void restore_list(int fd, u64 size, u64 soffset, char *type, int device)
{
	char *block;
	u64 end_offset = soffset + size;
	u64 start_offset = soffset;
	int res;
	char *sp = "/tmp/";
	char *fsp;
	int mode = O_RDONLY;
	int sfd;

	fsp = as_sprintf("%s%s%i", sp, type, device);
	sfd = open(fsp, mode, 0600);
	if (sfd < 0)
		die_syserr();
	block = s_malloc(BLKSIZE);
	memset(block, 0, BLKSIZE);
	while (end_offset > start_offset) {
		res = s_pread(sfd, block, BLKSIZE, start_offset - soffset);
		if (res != BLKSIZE)
			die_syserr();
		res = s_pwrite(fd, block, BLKSIZE, start_offset);
		if (res != BLKSIZE)
			die_syserr();
		start_offset += BLKSIZE;
	}
	free(block);
	free(fsp);
	close(sfd);
}

int tier_set_fd(int fd, char *datafile, int devicenr)
{
	int res;
	int ffd;
	int mode = O_RDWR | O_NOATIME;
	u64 bitlistsize;
	u64 devsize;
	u64 round;
	struct stat stbuf;
	struct devicemagic tier_magic;
	u64 soffset = 0;
	int header_size = TIER_HEADERSIZE;

	ffd = open(datafile, mode, 0600);
	if (ffd < 0)
		return -1;
	if (-1 == fstat(ffd, &stbuf)) {
		fprintf(stderr, "Failed to stat %s\n", datafile);
		close(ffd);
		return -1;
	}
	if (S_ISBLK(stbuf.st_mode)) {
		devsize = lseek64(ffd, 0, SEEK_END);
		if (-1 == devsize) {
			fprintf(stderr, "Error while opening %llu : %s\n",
				devsize, strerror(errno));
			close(ffd);
			return -1;
		}
	} else {
		devsize = (u64) stbuf.st_size;
	}
	devsize = round_to_blksize(devsize);
	if (devsize < 1048576) {
		fprintf(stderr, "Blockdevice %s with size 0x%llx is to small\n",
			datafile, devsize);
		close(ffd);
		return -1;
	}

	bitlistsize = calc_bitlist_size(devsize);
	soffset = devsize - bitlistsize;
	if (mkoptions.backup) {
		printf
		    ("Backup bitlist of device     : %s\n     offset                    : 0x%llx (%llu)\n     device size               : 0x%llx (%llu)\n     bitlist size              : 0x%llx (%llu)\n\n",
		     datafile, soffset, soffset, devsize, devsize, bitlistsize,
		     bitlistsize);
		backup_list(ffd, bitlistsize, soffset, "bitlist", devicenr);
	}
	if (mkoptions.restore) {
		printf
		    ("Restore bitlist of device     : %s\n     offset                    : 0x%llx (%llu)\n     device size               : 0x%llx (%llu)\n     bitlist size              : 0x%llx (%llu)\n\n",
		     datafile, soffset, soffset, devsize, devsize, bitlistsize,
		     bitlistsize);
		restore_list(ffd, bitlistsize, soffset, "bitlist", devicenr);
	}
	mkoptions.backdev[devicenr]->tier_dta_file = ffd;
	mkoptions.backdev[devicenr]->bitlistsize = bitlistsize;
	mkoptions.backdev[devicenr]->devicesize = devsize;
	return 0;
}

void usage(char *name)
{
	printf
	    ("%s -f datadev[:datadev:datadev] -b(ackup) -r(estore) -h(help)]\n",
	     name);
	exit(-1);
}

void parse_datafile(char *optarg)
{
	char *cur = NULL;
	cur = strtok(optarg, ":");
	if (!cur) {
		mkoptions.backdev[0] = s_malloc(sizeof(struct backing_device));
		mkoptions.backdev[mkoptions.backdev_count]->datafile = optarg;
		mkoptions.backdev_count++;
		return;
	}
	while (cur) {
		mkoptions.backdev[mkoptions.backdev_count] =
		    s_malloc(sizeof(struct backing_device));
		mkoptions.backdev[mkoptions.backdev_count]->datafile =
		    as_sprintf("%s", cur);
		mkoptions.backdev_count++;
		cur = strtok(NULL, ":");
	}
	mkoptions.backdev_count--;
}

int get_opts(int argc, char *argv[])
{

	int c, ret = 0;

	while ((c = getopt(argc, argv, "brf:")) != -1)
		switch (c) {
		case 'b':
			mkoptions.backup = 1;
			break;
		case 'r':
			mkoptions.restore = 1;
			break;
		case 'f':
			if (optopt == 'f')
				printf
				    ("Option -%c requires a lessfs configuration file as argument.\n",
				     optopt);
			else
				parse_datafile(optarg);
			break;
		case 'h':
			usage(argv[0]);
			break;
		default:
			abort();
		}
	printf("\n");
	return ret;
}

int main(int argc, char *argv[])
{
	int ret = -1, dtaexists;
	mkoptions.backup = 0;
	mkoptions.restore = 0;
	mkoptions.backdev_count = 0;
	mkoptions.sectorsize = 0;
	mkoptions.total_device_size = 0;
	mkoptions.bitlistsize_total = 0;
	struct stat stdta;
	struct stat device;
	int mode = O_RDWR | O_NOATIME;
	int fd, ffd;
	int dev;
	int count;
	u64 round;
	u64 devsize;
	int header_size = TIER_HEADERSIZE;
	u64 soffset;

	mkoptions.backdev =
	    s_malloc(sizeof(struct backing_device *) * MAX_BACKING_DEV);

	if (argc < 3)
		usage(argv[0]);
	if (0 != get_opts(argc, argv))
		exit(-1);

	for (count = 0; count <= mkoptions.backdev_count; count++) {
		dtaexists = stat(mkoptions.backdev[count]->datafile, &stdta);
		if (S_ISBLK(stdta.st_mode)) {
			if ((ffd =
			     open(mkoptions.backdev[count]->datafile, mode,
				  0600)) < 0) {
				if (ffd < 0) {
					fprintf(stderr,
						"Failed to open file %s\n",
						mkoptions.backdev[count]->
						datafile);
					exit(-1);
				}
			}
			devsize = lseek64(ffd, 0, SEEK_END);
			if (-1 == devsize) {
				fprintf(stderr,
					"Error while opening %llu : %s\n",
					devsize, strerror(errno));
				return -1;
			}
			close(ffd);
		} else
			devsize = (u64) stdta.st_size;
		printf("Device size (raw)              : 0x%llx (%llu)\n",
		       devsize, devsize);
		devsize = round_to_blksize(devsize);
		printf("Device size (rnd)              : 0x%llx (%llu)\n",
		       devsize, devsize);
		mkoptions.total_device_size += devsize - TIER_DEVICE_PLAYGROUND;
		if (-1 == dtaexists) {
			fprintf(stderr, "Failed to stat backend device %s\n",
				mkoptions.backdev[count]->datafile);
			exit(-1);
		}
		if (0 != (tier_set_fd(fd, mkoptions.backdev[count]->datafile,
				      count)))
			die_syserr();
		mkoptions.bitlistsize_total +=
		    mkoptions.backdev[count]->bitlistsize;
	}

	mkoptions.blocklistsize =
	    calc_blocklist_size(mkoptions.total_device_size,
				mkoptions.bitlistsize_total);
	mkoptions.total_device_size =
	    round_to_blksize(mkoptions.total_device_size -
			     mkoptions.bitlistsize_total -
			     mkoptions.blocklistsize -
			     (mkoptions.backdev_count * header_size));
	printf("Total device size              : 0x%llx (%llu)\n",
	       mkoptions.total_device_size, mkoptions.total_device_size);
	soffset =
	    mkoptions.backdev[0]->devicesize -
	    mkoptions.backdev[0]->bitlistsize - mkoptions.blocklistsize;
	if (mkoptions.backup) {
		printf
		    ("Backup blocklist of device   : %s\n     list size                 : 0x%llx (%llu)\n     starting from offset      : 0x%llx (%llu)\n\n",
		     mkoptions.backdev[0]->datafile, mkoptions.blocklistsize,
		     mkoptions.blocklistsize, soffset, soffset);
		backup_list(mkoptions.backdev[0]->tier_dta_file,
			    mkoptions.blocklistsize, soffset, "blocklist", 0);
	}
	if (mkoptions.restore) {
		printf
		    ("Restore blocklist of device   : %s\n     list size                 : 0x%llx (%llu)\n     starting from offset      : 0x%llx (%llu)\n\n",
		     mkoptions.backdev[0]->datafile, mkoptions.blocklistsize,
		     mkoptions.blocklistsize, soffset, soffset);
		restore_list(mkoptions.backdev[0]->tier_dta_file,
			     mkoptions.blocklistsize, soffset, "blocklist", 0);
	}
	for (count = 0; count <= mkoptions.backdev_count; count++) {
		mkoptions.backdev[count]->startofdata = header_size;
		mkoptions.backdev[count]->startofbitlist =
		    mkoptions.backdev[count]->devicesize -
		    mkoptions.backdev[count]->bitlistsize;
		mkoptions.backdev[count]->startofblocklist =
		    mkoptions.backdev[0]->devicesize -
		    mkoptions.backdev[0]->bitlistsize - mkoptions.blocklistsize;
		if (mkoptions.backup) {
			printf
			    ("backup_device_magic device      : %u\n     size                      : 0x%llx (%llu)\n",
			     count, mkoptions.total_device_size,
			     mkoptions.total_device_size);
			backup_device_magic(mkoptions.backdev[count]->
					    tier_dta_file,
					    mkoptions.total_device_size, count,
					    mkoptions.blocklistsize,
					    mkoptions.backdev[count]);
		}
		if (mkoptions.restore) {
			printf
			    ("restore_device_magic device      : %u\n     size                      : 0x%llx (%llu)\n",
			     count, mkoptions.total_device_size,
			     mkoptions.total_device_size);
			restore_device_magic(mkoptions.backdev[count]->
					     tier_dta_file,
					     mkoptions.total_device_size, count,
					     mkoptions.blocklistsize,
					     mkoptions.backdev[count]);
		}
	}
end_exit:
	exit(ret);
}
