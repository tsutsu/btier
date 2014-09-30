#ifndef _BTIER_MAIN_H_
#define _BTIER_MAIN_H_

static loff_t tier_get_size(struct file *);
static int tier_file_write(struct tier_device *, unsigned int,
			   void *, size_t, loff_t);
static int tier_file_read(struct tier_device *, unsigned int, void *, const int,
			  loff_t);
struct file *get_dev_file(struct tier_device *, unsigned int);
static void sync_device(struct tier_device *, int);
static int migrate_up_ifneeded(struct tier_device *, struct blockinfo *, u64);
static int migrate_down_ifneeded(struct tier_device *, struct blockinfo *, u64);
static void free_blocklist(struct tier_device *);
static void reset_counters_on_migration(struct tier_device *,
					struct blockinfo *);

#endif /* _BTIER_MAIN_H_ */
