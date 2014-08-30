static loff_t tier_get_size(struct file *);
static int tier_file_write(struct tier_device *, unsigned int,
			   void *, size_t, loff_t);
static int tier_sync(struct tier_device *);
static int tier_file_read(struct tier_device *, unsigned int, void *, const int,
			  loff_t);
static int tier_read_page(unsigned int, struct bio_vec *, u64, struct bio_task *);
static int write_blocklist(struct tier_device *, u64, struct blockinfo *, int);
struct file *get_dev_file(struct tier_device *, unsigned int);
static void sync_device(struct tier_device *, int);
static int migrate_up_ifneeded(struct tier_device *, struct blockinfo *, u64);
static int migrate_down_ifneeded(struct tier_device *, struct blockinfo *, u64);
static void free_blocklist(struct tier_device *);
static void reset_counters_on_migration(struct tier_device *,
					struct blockinfo *);
static void tiererror(struct tier_device *, char *);
static int get_chunksize(struct block_device *);
#if LINUX_VERSION_CODE <= KERNEL_VERSION(3,0,0)
#define vzalloc xzalloc
void *xzalloc(size_t size)
{
	void *retval;

	retval = vmalloc(size);
	if (retval)
		memset(retval, 0, size);
	return retval;
}
#endif
