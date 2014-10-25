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
	int use_bio;
};

struct option_info {
	int create;
	int sync;
	int sectorsize;
	u64 blocklistsize;
	struct backing_device **backdev;
	char *device;
	int backdev_count;
	u64 total_device_size;
	u64 bitlistsize_total;
	int use_bio;
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

void write_device_magic(int fd, u64 total_device_size,
			unsigned int devicenr, u64 blocklistsize,
			struct backing_device *bdev)
{
	char *block;
	struct devicemagic magic;
	int res;

	block = s_malloc(BLKSIZE);
	memset(block, 0, BLKSIZE);
	memset(&magic, 0, sizeof(struct devicemagic));
	magic.magic = TIER_DEVICE_BIT_MAGIC;
	magic.device = devicenr;
	magic.total_device_size = total_device_size;
	magic.clean = CLEAN;
	magic.devicesize = bdev->devicesize;
	magic.bitlistsize = bdev->bitlistsize;
	magic.blocklistsize = blocklistsize;
	magic.startofblocklist = bdev->startofblocklist;
	magic.startofbitlist = bdev->startofbitlist;
	magic.use_bio = mkoptions.use_bio;
	if (strlen(bdev->datafile) > 1024)
		exit(-ENAMETOOLONG);
	memcpy(&magic.fullpathname, bdev->datafile, strlen(bdev->datafile));
	memcpy(block, &magic, sizeof(struct devicemagic));
	res = s_pwrite(fd, block, BLKSIZE, 0);
	if (res != BLKSIZE)
		die_syserr();
	free(block);
}

void clear_list(int fd, u64 size, u64 soffset)
{
	char *block;
	u64 end_offset = soffset + size;
	u64 start_offset = soffset;
	int res;

	block = s_malloc(BLKSIZE);
	memset(block, 0, BLKSIZE);
	while (end_offset > start_offset) {
		res = s_pwrite(fd, block, BLKSIZE, start_offset);
		if (res != BLKSIZE)
			die_syserr();
		start_offset += BLKSIZE;
	}
	free(block);
}

int tier_set_fd(int fd, char *datafile, int devicenr)
{
	struct fd_s fds;
	int res;
	int ffd;
	int mode;
	u64 bitlistsize;
	u64 devsize;
	u64 round;
	struct stat stbuf;
	struct devicemagic tier_magic;
	u64 soffset = 0;
	int header_size = TIER_HEADERSIZE;

	mode = O_RDWR | O_NOATIME;
	if (mkoptions.sync)
		mode |= O_SYNC;
	fds.use_bio = mkoptions.use_bio;
	fds.fd = open(datafile, mode, 0600);
	if (fds.fd < 0)
		return -1;
	ffd = fds.fd;
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
	if (mkoptions.create) {
		soffset = devsize - bitlistsize;
		printf
		    ("Clearing bitlist of device     : %s\n     offset                    : 0x%llx (%llu)\n     device size               : 0x%llx (%llu)\n     bitlist size              : 0x%llx (%llu)\n\n",
		     datafile, soffset, soffset, devsize, devsize, bitlistsize,
		     bitlistsize);
		clear_list(ffd, bitlistsize, soffset);
	} else {
		res = s_pread(ffd, &tier_magic, sizeof(tier_magic), 0);
		if (res != sizeof(tier_magic))
			die_syserr();
		if (tier_magic.magic != TIER_DEVICE_BIT_MAGIC) {
			fprintf(stderr,
				"Datastore %s has invalid magic, not a tier device\n",
				datafile);
			return -1;
		}
	}
	mkoptions.backdev[devicenr]->tier_dta_file = ffd;
	mkoptions.backdev[devicenr]->bitlistsize = bitlistsize;
	mkoptions.backdev[devicenr]->devicesize = devsize;

	res = ioctl(fd, TIER_SET_FD, &fds);
	if (res < 0) {
		int rc = 1;
		fprintf(stderr,
			"ioctl TIER_SET_FDX failed on /dev/tiercontrol\n");
		close(fds.fd);
		return rc;
	}
	return 0;
}

int tier_setup(int op, int fd, int devicenr)
{
	int ffd, i;
	char *pass;
	char *filename;
	u64 fsize;
	int ret = 0;
	int rc;

	switch (op) {
	case TIER_SET_FD:
		if (0 !=
		    tier_set_fd(fd, mkoptions.backdev[devicenr]->datafile,
				devicenr)) {
			rc = 1;
			return rc;
		}
		break;
	case TIER_SET_SECTORSIZE:
		if (ioctl(fd, TIER_SET_SECTORSIZE, mkoptions.sectorsize) < 0) {
			rc = 1;
			fprintf(stderr,
				"ioctl TIER_SET_SECTORSIZE failed on /dev/tiercontrol\n");
			return rc;
		}
		break;
	case TIER_REGISTER:
		if (ioctl(fd, TIER_REGISTER, 0) < 0) {
			rc = 1;
			fprintf(stderr,
				"ioctl TIER_REGISTER failed on /dev/tiercontrol\n");
			return rc;
		}
		break;
	case TIER_DEREGISTER:
		if (ioctl(fd, TIER_DEREGISTER, (unsigned long)mkoptions.device)
		    < 0) {
			if (errno == EBUSY) {
				fprintf(stderr,
					"Failed to deregister active device %s\n",
					mkoptions.device);
				fprintf(stderr,
					"Is the device still mounted or in use by multipathd?\n");
			} else
				fprintf(stderr,
					"ioctl TIER_DEREGISTER failed on /dev/tiercontrol\n");
			rc = 1;
			return rc;
		}
		break;
	case TIER_INIT:
		if (ioctl(fd, TIER_INIT, 0) < 0) {
			rc = 1;
			fprintf(stderr,
				"ioctl TIER_INIT failed on /dev/tiercontrol\n");
			return rc;
		}
		break;
	default:
		fprintf(stderr, "OP =%02x\n", op);
		abort();
	}
	return 0;
}

void usage(char *name)
{
	printf
	    ("Create : %s -f datadev[:datadev:datadev] [-z sectorsize(512..4096) -c(create) -s(sync) -h(help) -B(bio) -V(vfs)]\n",
	     name);
	printf
	    ("         datadevX can either be a path to a file or a blockdevice. No more then 16 devices are supported.\n");
	printf
	    ("         specify the fastest storage first. E.g. -f /dev/ssd:/dev/sas:/dev/sata.\n");
	printf("Detach : %s -d /dev/tier_device_name\n", name);
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
        int has_devices = 0;

	while ((c = getopt(argc, argv, "VBcd:bhcsf:m:z:")) != -1)
		switch (c) {
		case 'B':
			mkoptions.use_bio = USE_BIO;
			break;
		case 'V':
			mkoptions.use_bio = USE_VFS;
			break;
		case 'c':
			mkoptions.create = 1;
			break;
		case 'f':
			if (optopt == 'f')
				printf
				    ("Option -%c requires a device or file as argument.\n",
				     optopt);
			else {
				parse_datafile(optarg);
                                has_devices = 1;
			}

			break;
		case 'd':
			if (optopt == 'd')
				printf
				    ("Option -%c requires a device as argument.\n",
				     optopt);
			else {
				mkoptions.device = optarg;
				has_devices = 1;
			}
			break;
		case 's':
			mkoptions.sync = 1;
			break;
		case 'z':
			if (optopt == 'z')
				printf
				    ("Option -%c requires sector size as argument.\n",
				     optopt);
			else {
				sscanf(optarg, "%i", &mkoptions.sectorsize);
				if (mkoptions.sectorsize > 4096)
					mkoptions.sectorsize = 0;
				if (mkoptions.sectorsize < 0)
					mkoptions.sectorsize = 0;
				if (mkoptions.sectorsize > 0) {
					ret = mkoptions.sectorsize / 512;
					ret *= 512;
					if (ret != mkoptions.sectorsize) {
						ret = -1;
						printf
						    ("The sectorsize has to be a multiple of 512 bytes\n");
					} else
						ret = 0;
				}
			}
			break;
		case 'h':
			usage(argv[0]);
			break;
		default:
			abort();
		}
        if (!has_devices) {
            printf("btier_setup requires at least one device to be specified with -f\n");
            ret = -1;
        }
	printf("\n");
	return ret;
}

int main(int argc, char *argv[])
{
	int ret = -1, dtaexists;
	mkoptions.create = 0;
	mkoptions.sync = 0;
	mkoptions.backdev_count = 0;
	mkoptions.sectorsize = 0;
	mkoptions.total_device_size = 0;
	mkoptions.bitlistsize_total = 0;
	mkoptions.use_bio = 0;
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

	if ((fd = open("/dev/tiercontrol", mode)) < 0) {
		fprintf(stderr,
			"Failed to open /dev/tiercontrol, is tier.ko loaded?\n");
		exit(-1);
	}

	if (-1 == flock(fd, LOCK_EX)) {
		fprintf(stderr, "Failed to lock /dev/tiercontrol\n");
		exit(-1);
	}

	if (mkoptions.device) {
		if (-1 == stat(mkoptions.device, &device)) {
			fprintf(stderr, "No such device : %s\n",
				mkoptions.device);
			exit(-1);
		}
		if (!S_ISBLK(device.st_mode)) {
			fprintf(stderr, "Not a blockdevice : %s\n",
				mkoptions.device);
			exit(-1);
		}
		ret = tier_setup(TIER_DEREGISTER, fd, 0);
		if (0 != ret)
			die_ioctlerr("ioctl TIER_DEREGISTER failed\n");
		exit(ret);
	}

	ret = tier_setup(TIER_INIT, fd, 0);
	if (0 != ret)
		die_ioctlerr("ioctl TIER_INIT failed\n");
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
		ret = tier_setup(TIER_SET_FD, fd, count);
		if (0 != ret)
			die_ioctlerr("ioctl TIER_SET_FD failed\n");
		mkoptions.bitlistsize_total +=
		    mkoptions.backdev[count]->bitlistsize;
	}

	if (0 != mkoptions.sectorsize) {
		printf("Sector size = %u\n", mkoptions.sectorsize);
		ret = tier_setup(TIER_SET_SECTORSIZE, fd, 0);
		if (0 != ret)
			die_ioctlerr("ioctl TIER_SET_SECTORSIZE failed\n");
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
	if (mkoptions.create) {
		soffset =
		    mkoptions.backdev[0]->devicesize -
		    mkoptions.backdev[0]->bitlistsize - mkoptions.blocklistsize;
		printf
		    ("Clearing blocklist of device   : %s\n     list size                 : 0x%llx (%llu)\n     starting from offset      : 0x%llx (%llu)\n\n",
		     mkoptions.backdev[0]->datafile, mkoptions.blocklistsize,
		     mkoptions.blocklistsize, soffset, soffset);
		clear_list(mkoptions.backdev[0]->tier_dta_file,
			   mkoptions.blocklistsize, soffset);
	}
	for (count = 0; count <= mkoptions.backdev_count; count++) {
		mkoptions.backdev[count]->startofdata = header_size;
		mkoptions.backdev[count]->startofbitlist =
		    mkoptions.backdev[count]->devicesize -
		    mkoptions.backdev[count]->bitlistsize;
		mkoptions.backdev[count]->startofblocklist =
		    mkoptions.backdev[0]->devicesize -
		    mkoptions.backdev[0]->bitlistsize - mkoptions.blocklistsize;
		if (mkoptions.create) {
			printf
			    ("write_device_magic device      : %u\n     size                      : 0x%llx (%llu)\n",
			     count, mkoptions.total_device_size,
			     mkoptions.total_device_size);
			write_device_magic(mkoptions.backdev[count]->
					   tier_dta_file,
					   mkoptions.total_device_size, count,
					   mkoptions.blocklistsize,
					   mkoptions.backdev[count]);
		}
	}
	if ((mkoptions.create) && (mkoptions.use_bio == 0))
		mkoptions.use_bio = USE_VFS;
	ret = tier_setup(TIER_REGISTER, fd, 0);
	if (0 != ret)
		die_ioctlerr("ioctl TIER_REGISTER failed\n");

end_exit:
	flock(fd, LOCK_UN);
	close(fd);
	exit(ret);
}
