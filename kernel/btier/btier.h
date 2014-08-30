#ifdef __KERNEL__
#define pr_fmt(fmt) "btier: " fmt
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/spinlock.h>
#include <linux/mutex.h>
#include <linux/file.h>
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/fs.h>
#include <linux/errno.h>
#include <linux/types.h>
#include <linux/vmalloc.h>
#include <linux/genhd.h>
#include <linux/blkdev.h>
#include <linux/hdreg.h>
#include <linux/crypto.h>
#include <linux/err.h>
#include <linux/scatterlist.h>
#include <linux/workqueue.h>
#include <linux/rbtree.h>
#include <linux/miscdevice.h>
#include <linux/delay.h>
#include <linux/falloc.h>
#include <linux/kthread.h>
#include <linux/version.h>
#include <linux/sysfs.h>
#include <linux/device.h>
#include <linux/socket.h>
#include <linux/in.h>
#include <linux/net.h>
#include <linux/inet.h>
#include <asm/div64.h>
#else
typedef unsigned long long u64;
typedef unsigned long u32;
#include <time.h>
#endif

/* Enable MAX_PERFORMANCE will stop maintaining counters for
   internal statistics, which can be used to trace deadlocks and
   other useful stuff. In most cases it makes sense to keep the
   counters. Only very fast PCI-e SSD's may benefit from enabling
   MAX_PERFORMANCE.

#define MAX_PERFORMANCE

*/

#define BLKSIZE 1048576		/*Moving smaller blocks then 4M around
				   will lead to fragmentation */
#define BLKBITS 20		/*Adjust when changing BLKSIZE */
#define PAGE_SHIFT 12		/*4k page size */
#define TIER_NAME_SIZE     64	/* Max lenght of the filenames */
#define TIER_SET_FD        0xFE00
#define TIER_SET_DEVSZ     0xFE03
#define TIER_REGISTER      0xFE04
#define TIER_DEREGISTER    0xFE05
#define TIER_INIT          0xFE07
#define TIER_BARRIER       0xFE08
#define TIER_CACHESIZE     0xFE09
#define TIER_SET_SECTORSIZE  0xFE0A
#define TIER_HEADERSIZE    1048576
#define TIER_DEVICE_BIT_MAGIC  0xabe
#define TIER_DEVICE_BLOCK_MAGIC  0xafdf

#define WD 1			/* Write disk */
#define WC 2			/* Write cache */
#define WA 3			/* All: Cache and disk */

#define BTIER_MAX_DEVS 26
#define BTIER_MAX_INFLIGHT 256

#define TIGER_HASH_LEN 24

#define RANDOM 0x01
#define SEQUENTIAL 0x02
#define KERNEL_SECTORSIZE 512
#define MAX_BACKING_DEV 24
/* Tier reserves 2 MB per device for playing data migration games. */
#define TIER_DEVICE_PLAYGROUND BLKSIZE*2

#define NORMAL_IO 1
#define MIGRATION_IO 2

#define CLEAN 1
#define DIRTY 2
#define MASTER 0
#define SLAVE 1
#define EST 1
#define DIS 2

#define ALLOCATED 0xff
#define UNALLOCATED 0x00
#define MAXPAGESHOW 20

#define USE_BIO 2
#define USE_VFS 1

#define TIERREAD 1
#define TIERWRITE 2
#define FSMODE 1		/* vfs datasync mode 0 or 1 */

#define TIERMAXAGE 86400	/* When a chunk has not been used TIERMAXAGE it
				   will migrate to a slower (higher) tier */
#define TIERHITCOLLECTTIME 43200	/* Every block has TIERHITCOLLECTTIME to collect hits before
					   being migrated when it has less hits than average */
#define MIGRATE_INTERVAL 14400	/* Check every 4 hours */

/* MAX_STAT_COUNT 10000000 will allow devices up to 
 * 1.7 Zettabyte before statistics can overflow.
 * Max size of unsigned long long = 18446744073709551615
 * With a 1 MB chunksize this we have 1073741824 blocks per PB
 * So with 10000000 hits per block this is 
 * 1073741824*10000000=10737418240000000 hits per PB
 * 18446744073709551615/10737418240000000=1717 PB before counters can overflow.
 */
#define MAX_STAT_COUNT 10000000	/* We count max 10 million hits, hits are reset upon migration */
#define MAX_STAT_DECAY 500000	/* Loose 5% hits per walk when we have reached the max */
#ifndef MAX_PERFORMANCE
enum states {
	IDLE = 0,
	BIOREAD = 1,
	VFSREAD = 2,
	VFSWRITE = 4,
	BIOWRITE = 8,
	WAITAIOPENDING = 16,
	PRESYNC = 32,
	PREBINFO = 64,
	PREALLOCBLOCK = 128,
	DISCARD = 512
};
#endif

struct data_policy {
	unsigned int max_age;
	unsigned int hit_collecttime;
	unsigned int sequential_landing;
	int migration_disabled;
	u64 migration_interval;
};

struct blockinfo {
	unsigned int device;
	u64 offset;
	time_t lastused;
	unsigned int readcount;
	unsigned int writecount;
} __attribute__ ((packed));

struct devicemagic {
	unsigned int magic;
	unsigned int device;
	unsigned int clean;
	u64 blocknr_journal;
	struct blockinfo binfo_journal_new;
	struct blockinfo binfo_journal_old;
	unsigned int average_reads;
	unsigned int average_writes;
	u64 total_reads;
	u64 total_writes;
	time_t average_age;
	u64 devicesize;
	u64 total_device_size;	/* Only valid for tier 0 */
	u64 total_bitlist_size;	/* Only valid for tier 0 */
	u64 bitlistsize;
	u64 blocklistsize;
	u64 startofbitlist;
	u64 startofblocklist;
	char fullpathname[1025];
	struct data_policy dtapolicy;
	char uuid[24];
	unsigned int writethrough;
	unsigned int use_bio;
} __attribute__ ((packed));

struct fd_s {
	int fd;
	int use_bio;
};

#ifdef __KERNEL__

struct bio_task {
	atomic_t pending;
	struct bio *parent_bio;
	struct tier_device *dev;
	int vfs;
        int in_one;
};

typedef struct {
	struct file *fp;
	mm_segment_t fs;
} file_info_t;

struct backing_device {
	struct file *fds;
	u64 bitlistsize;
	u64 devicesize;
	u64 startofdata;
	u64 endofdata;
	u64 startofbitlist;
	u64 startofblocklist;
	u64 bitbufoffset;
	u64 free_offset;
	u64 usedoffset;
	unsigned int dirty;
	struct devicemagic *devmagic;
	struct kobject *ex_kobj;
	struct blockinfo **blocklist;
	u8 *bitlist;
	unsigned int ra_pages;
	struct block_device *bdev;
};

struct tier_stats {
	u64 seq_reads;
	u64 rand_reads;
	u64 seq_writes;
	u64 rand_writes;
};

struct migrate_direct {
	u64 blocknr;
	int newdevice;
	atomic_t direct;
};

struct tier_device {
	struct list_head list;
	int major_num;
	int tier_device_number;
	int active;
	int attached_devices;
	int (*ioctl) (struct tier_device *, int cmd, u64 arg);
	u64 nsectors;
	unsigned int logical_block_size;
	struct backing_device **backdev;
	struct block_device *tier_device;
	struct mutex tier_ctl_mutex;
	u64 size;
	u64 blocklistsize;
	spinlock_t lock;
	spinlock_t statlock;
	spinlock_t usage_lock;
	spinlock_t dbg_lock;
	struct gendisk *gd;
	struct workqueue_struct *migration_queue;	/* Data migration */
	struct workqueue_struct *aio_queue;	/* Async IO */
	struct task_struct *tier_thread;
	struct bio_list tier_bio_list;
	struct request_queue *rqueue;
	char *devname;
	char *managername;
	char *aioname;
	atomic_t migrate;
	atomic_t aio_pending;
	atomic_t wqlock;
	atomic_t commit;
	atomic_t curfd;
	int debug_state;
	unsigned int commit_interval;
	int barrier;
	int stop;
/*Holds the type of IO random or sequential*/
	int iotype;
/*Last blocknr written or read*/
	u64 lastblocknr;
	u64 resumeblockwalk;
/*Incremented if current blocknr == lastblocknr -1 or +1 */
	unsigned int insequence;
	u64 cacheentries;
	struct mutex qlock;
	wait_queue_head_t tier_event;
	wait_queue_head_t migrate_event;
	wait_queue_head_t aio_event;
	struct timer_list migrate_timer;
	struct tier_stats stats;
	struct migrate_direct mgdirect;
	int migrate_verbose;
	int ptsync;
	int discard_to_devices;
	int discard;
	int writethrough;
/* Where do we initially store sequential IO */
	int inerror;
/* The blocknr that the user can retrieve info for via sysfs*/
	u64 user_selected_blockinfo;
	int user_selected_ispaged;
	unsigned int users;
	int use_bio;
	char zero_buf[PAGE_SIZE];
};

typedef struct {
	struct work_struct work;
	struct tier_device *device;
} tier_worker_t;

typedef struct {
	struct work_struct work;
	struct tier_device *dev;
	u64 offset;
	unsigned int device;
	void *data;
	unsigned int size;
	struct page *bv_page;
} aio_work_t;

void free_bitlists(struct tier_device *);
void resize_tier(struct tier_device *);
int load_bitlists(struct tier_device *);
void *as_sprintf(const char *, ...);
u64 allocated_on_device(struct tier_device *, int);
void btier_clear_statistics(struct tier_device *dev);
struct blockinfo *get_blockinfo(struct tier_device *, u64, int);
int migrate_direct(struct tier_device *, u64, int);
char *tiger_hash(char *, unsigned int);
void btier_lock(struct tier_device *);
void btier_unlock(struct tier_device *);
#endif
