#include "btier.h"

extern struct list_head device_list;
extern struct mutex tier_devices_mutex;

/* tier sysfs attributes */
ssize_t tier_attr_show(struct device *dev, char *page,
		       ssize_t(*callback) (struct tier_device *, char *))
{
	struct tier_device *l, *lo = NULL;

	mutex_lock(&tier_devices_mutex);
	list_for_each_entry(l, &device_list, list)
	    if (disk_to_dev(l->gd) == dev) {
		lo = l;
		break;
	}
	mutex_unlock(&tier_devices_mutex);

	return lo ? callback(lo, page) : -EIO;
}

ssize_t tier_attr_store(struct device * dev, const char *page, size_t s,
			ssize_t(*callback) (struct tier_device *,
					    const char *, size_t))
{
	struct tier_device *l, *lo = NULL;

	mutex_lock(&tier_devices_mutex);
	list_for_each_entry(l, &device_list, list)
	    if (disk_to_dev(l->gd) == dev) {
		lo = l;
		break;
	}
	mutex_unlock(&tier_devices_mutex);

	return lo ? callback(lo, page, s) : -EIO;
}

static char *as_strarrcat(const char **strarr, ssize_t count)
{
	int totallen = 0;
	int i;
	char *retstr = NULL, *curpos;

	for (i = 0; i < count; i++) {
		totallen += strlen(strarr[i]);
	}

	curpos = retstr = kzalloc(totallen + 1, GFP_KERNEL);
	for (i = 0; i < count; i++) {
		strcpy(curpos, strarr[i]);
		curpos += strlen(strarr[i]);
	}

	return retstr;
}

#define TIER_ATTR_RO(_name) \
static ssize_t tier_attr_##_name##_show(struct tier_device *, char *);  \
static ssize_t tier_attr_do_show_##_name(struct device *d,              \
                                struct device_attribute *attr, char *b) \
{                                                                       \
        return tier_attr_show(d, b, tier_attr_##_name##_show);          \
}                                                                       \
static struct device_attribute tier_attr_##_name =                      \
        __ATTR(_name, S_IRUGO, tier_attr_do_show_##_name, NULL);

static ssize_t tier_attr_attacheddevices_show(struct tier_device *dev,
					      char *buf)
{
	return sprintf(buf, "%u\n", dev->attached_devices);
}

#define TIER_ATTR_WO(_name) \
static ssize_t tier_attr_##_name##_store(struct tier_device *, const char *, size_t);  \
static ssize_t tier_attr_do_store_##_name(struct device *d,                      \
                                struct device_attribute *attr, const char *b, size_t s)\
{                                                                                \
        return tier_attr_store(d, b, s, tier_attr_##_name##_store);              \
}                                                                                \
static struct device_attribute tier_attr_##_name =                               \
        __ATTR(_name, S_IWUSR, NULL, tier_attr_do_store_##_name);

#define TIER_ATTR_RW(_name) \
static ssize_t tier_attr_##_name##_store(struct tier_device *, const char *, size_t);  \
static ssize_t tier_attr_do_store_##_name(struct device *d,                      \
                                struct device_attribute *attr, const char *b, size_t s)\
{                                                                                \
        return tier_attr_store(d, b, s, tier_attr_##_name##_store);              \
}                                                                                \
static ssize_t tier_attr_do_show_##_name(struct device *d,              \
                                struct device_attribute *attr, char *b) \
{                                                                       \
        return tier_attr_show(d, b, tier_attr_##_name##_show);          \
}                                                                       \
static struct device_attribute tier_attr_##_name =                               \
        __ATTR(_name, (S_IRWXU ^ S_IXUSR) | S_IRGRP| S_IROTH,  tier_attr_do_show_##_name, tier_attr_do_store_##_name);

static ssize_t tier_attr_migration_enable_store(struct tier_device *dev,
						const char *buf, size_t s)
{
	struct data_policy *dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
	if ('0' != buf[0] && '1' != buf[0])
		return s;
	if ('1' == buf[0]) {
		if (dtapolicy->migration_disabled) {
			dtapolicy->migration_disabled = 0;
			dev->resumeblockwalk = 0;
			if (0 == atomic_read(&dev->migrate)) {
				atomic_set(&dev->migrate, 1);
				wake_up(&dev->migrate_event);
			}
			pr_info("migration is enabled for %s\n", dev->devname);
		}
	} else {
		if (!dtapolicy->migration_disabled
		    && 0 == atomic_read(&dev->migrate)) {
			dtapolicy->migration_disabled = 1;
			del_timer_sync(&dev->migrate_timer);
			pr_info("migration is disabled for %s\n", dev->devname);
		}
		dtapolicy->migration_disabled = 1;
	}
	return s;
}

static ssize_t tier_attr_barriers_store(struct tier_device *dev,
					const char *buf, size_t s)
{
	if ('0' != buf[0] && '1' != buf[0])
		return s;
	if ('0' == buf[0]) {
		if (dev->barrier) {
			dev->barrier = 0;
			pr_info("barriers are disabled\n");
		}
	} else {
		if (!dev->barrier) {
			dev->barrier = 1;
			pr_info("barriers are enabled\n");
		}
	}
	return s;
}

static ssize_t tier_attr_clear_statistics_store(struct tier_device *dev,
						const char *buf, size_t s)
{
	if (buf[0] != '1')
		return -ENOMSG;
	btier_clear_statistics(dev);
	return s;
}

static ssize_t tier_attr_ptsync_store(struct tier_device *dev,
				      const char *buf, size_t s)
{
	if ('0' != buf[0] && '1' != buf[0])
		return s;
	if ('0' == buf[0]) {
		if (dev->ptsync) {
			dev->ptsync = 0;
			pr_info("pass-through sync is disabled\n");
		}
	} else {
		if (!dev->ptsync) {
			dev->ptsync = 1;
			pr_info("pass-through sync is enabled\n");
		}
	}
	return s;
}

static ssize_t tier_attr_discard_to_devices_store(struct tier_device *dev,
						  const char *buf, size_t s)
{
	if ('0' != buf[0] && '1' != buf[0])
		return s;
#if LINUX_VERSION_CODE < KERNEL_VERSION(3,0,0)
	return -EOPNOTSUPP;
#endif
	if ('0' == buf[0]) {
		if (dev->discard_to_devices) {
			dev->discard_to_devices = 0;
			pr_info("discard_to_devices is disabled\n");
		}
	} else {
		if (!dev->ptsync) {
			dev->discard_to_devices = 1;
			pr_info("discard_to_devices is enabled\n");
		}
	}
	return s;
}

static ssize_t tier_attr_writethrough_store(struct tier_device *dev,
					    const char *buf, size_t s)
{
	if ('0' != buf[0] && '1' != buf[0])
		return s;
	btier_lock(dev);
	if ('0' == buf[0]) {
		if (dev->writethrough) {
			dev->writethrough = 0;
			pr_info("writethrough is disabled\n");
		}
	} else {
		if (!dev->writethrough) {
			dev->writethrough = 1;
			pr_info("writethrough is enabled\n");
		}
	}
	btier_unlock(dev);
	return s;
}

static ssize_t tier_attr_resize_store(struct tier_device *dev,
				      const char *buf, size_t s)
{
	if ('1' != buf[0])
		return s;
	mutex_lock(&dev->qlock);
	free_bitlists(dev);
	resize_tier(dev);
	load_bitlists(dev);
	mutex_unlock(&dev->qlock);
	return s;
}

/* return the input NULL terminated */
static char *null_term_buf(const char *buf, size_t s)
{
	char *cpybuf;

	cpybuf = kzalloc(s + 1, GFP_KERNEL);
	if (NULL == cpybuf)
		return NULL;
	memcpy(cpybuf, buf, s);

	return cpybuf;
}

static ssize_t tier_attr_show_blockinfo_store(struct tier_device *dev,
					      const char *buf, size_t s)
{
	int res;
	char *cpybuf;
	u64 maxblocks = dev->size / BLKSIZE;
	u64 selected;

	cpybuf = null_term_buf(buf, s);
	if (!cpybuf)
		return -ENOMEM;
	res = sscanf(cpybuf, "%llu", &selected);
	if (strstr(cpybuf, " paged"))
		dev->user_selected_ispaged = 1;
	else
		dev->user_selected_ispaged = 0;
	kfree(cpybuf);
	if (res != 1)
		return -ENOMSG;
	if (maxblocks > selected)
		dev->user_selected_blockinfo = selected;
	else
		return -EOVERFLOW;
	return s;
}

static ssize_t tier_attr_sequential_landing_store(struct tier_device *dev,
						  const char *buf, size_t s)
{
	int landdev;
	int res;
	char *cpybuf;

	cpybuf = null_term_buf(buf, s);
	if (!cpybuf)
		return -ENOMEM;
	res = sscanf(cpybuf, "%i", &landdev);
	if (res != 1)
		return -ENOMSG;
	if (landdev >= dev->attached_devices)
		return -ENOMSG;
	if (landdev < 0)
		return -ENOMSG;
	dev->backdev[0]->devmagic->dtapolicy.sequential_landing = landdev;
	kfree(cpybuf);
	return s;
}

static ssize_t tier_attr_migrate_block_store(struct tier_device *dev,
					     const char *buf, size_t s)
{
	u64 blocknr;
	int device;
	int res = 0;
	size_t m = s;
	char *cpybuf;
	u64 maxblocks = dev->size / BLKSIZE;

	cpybuf = null_term_buf(buf, s);
	if (!cpybuf)
		return -ENOMEM;
	s = -ENOMSG;
	res = sscanf(cpybuf, "%llu/%u", &blocknr, &device);
	if (res != 2)
		goto end_error;
	if (device >= dev->attached_devices)
		goto end_error;
	if (device < 0)
		goto end_error;
	if (blocknr < maxblocks) {
		res = migrate_direct(dev, blocknr, device);
		if (res < 0)
			s = res;
		else
			s = m;
	}
end_error:
	kfree(cpybuf);
	return s;
}

static ssize_t tier_attr_migrate_verbose_store(struct tier_device *dev,
					       const char *buf, size_t s)
{
	if ('0' != buf[0] && '1' != buf[0])
		return s;
	if ('0' == buf[0]) {
		if (dev->migrate_verbose) {
			dev->migrate_verbose = 0;
			pr_info("migrate_verbose is disabled\n");
		}
	} else {
		if (!dev->ptsync) {
			dev->migrate_verbose = 1;
			pr_info("migrate_verbose is enabled\n");
		}
	}
	return s;
}

static ssize_t tier_attr_migration_policy_store(struct tier_device *dev,
						const char *buf, size_t s)
{
	int devicenr, res;
	unsigned int max_age;
	unsigned int hit_collecttime;

	char *a, *p, *cur;
	char *devicename;
	char *cpybuf;

	cpybuf = null_term_buf(buf, s);
	if (!cpybuf)
		return -ENOMEM;

	p = strchr(cpybuf, ' ');
	if (!p)
		goto end_error;
	a = kzalloc(p - cpybuf + 1, GFP_KERNEL);
	memcpy(a, cpybuf, p - cpybuf);
	res = sscanf(a, "%u", &devicenr);
	if (res != 1)
		goto end_error;
	if (devicenr < 0 || devicenr >= dev->attached_devices)
		goto end_error;
	kfree(a);
	a = p;

	while (a[0] == ' ')
		a++;
	p = strchr(a, ' ');
	if (!p)
		goto end_error;
	devicename = kzalloc(p - cpybuf + 1, GFP_KERNEL);
	memcpy(devicename, a, p - a);
	if (0 !=
	    strcmp(devicename,
		   dev->backdev[devicenr]->fds->f_dentry->d_name.name)) {
		kfree(devicename);
		goto end_error;
	}
	kfree(devicename);

	a = p;
	while (a[0] == ' ')
		a++;
	p = strchr(a, ' ');
	cur = kzalloc(p - a, GFP_KERNEL);
	memcpy(cur, a, p - a + 1);
	res = sscanf(cur, "%u", &max_age);
	kfree(cur);
	if (res != 1)
		goto end_error;

	a = p;
	while (a[0] == ' ')
		a++;
	res = sscanf(a, "%u", &hit_collecttime);
	if (res != 1)
		goto end_error;
	mutex_lock(&dev->qlock);
	dev->backdev[devicenr]->devmagic->dtapolicy.max_age = max_age;
	dev->backdev[devicenr]->devmagic->dtapolicy.hit_collecttime =
	    hit_collecttime;
	mutex_unlock(&dev->qlock);
	kfree(cpybuf);
	return s;

end_error:
	kfree(cpybuf);
	return -ENOMSG;
}

static ssize_t tier_attr_migration_interval_store(struct tier_device *dev,
						  const char *buf, size_t s)
{
	int res;
	u64 interval;
	char *cpybuf;
	struct data_policy *dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
	cpybuf = null_term_buf(buf, s);
	if (!cpybuf)
		return -ENOMEM;
	res = sscanf(cpybuf, "%llu", &interval);
	if (res == 1) {
		if (interval <= 0)
			return -ENOMSG;
		mutex_lock(&dev->qlock);
		dtapolicy->migration_interval = interval;
		mod_timer(&dev->migrate_timer,
			  jiffies +
			  msecs_to_jiffies(dtapolicy->migration_interval *
					   1000));
		mutex_unlock(&dev->qlock);
	} else
		s = -ENOMSG;
	kfree(cpybuf);
	return s;
}

static ssize_t tier_attr_migration_enable_show(struct tier_device *dev,
					       char *buf)
{
	struct data_policy *dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
	return sprintf(buf, "%i\n", !dtapolicy->migration_disabled);
}

char *uuid_hash(char *data, int hashlen)
{
	int n, pos;
	char *ahash = NULL;

	ahash = kzalloc(hashlen * 2, GFP_KERNEL);
	if (!ahash)
		return NULL;
	pos = 0;
	for (n = 0; n < hashlen; n++) {
		if (pos == 3) {
			sprintf(&ahash[n * 2], "-");
			pos = 0;
		} else {
			pos++;
			sprintf(&ahash[n * 2], "%02X", data[n]);
		}
	}
	return ahash;
}

static ssize_t tier_attr_uuid_show(struct tier_device *dev, char *buf)
{
	int i, n, res = 0;
	char *thash;
	int hashlen = 24;
	const char *name;
	char *xbuf;
	char *asc;

	xbuf = kzalloc(hashlen, GFP_KERNEL);
	if (!xbuf)
		return res;
	for (i = 0; i < dev->attached_devices; i++) {
		name = dev->backdev[i]->fds->f_dentry->d_name.name;
		thash = tiger_hash((char *)name, strlen(name));
		if (!thash)
			goto end_error;
		for (n = 0; n < hashlen; n++) {
			xbuf[n] ^= thash[n];
		}
		kfree(thash);
	}
	asc = uuid_hash(xbuf, hashlen);
	if (asc) {
		memcpy(buf, asc, 30);
		kfree(asc);
		buf[31] = '\n';
		res = 32;
	}
end_error:
	kfree(xbuf);
	return res;
}

static ssize_t tier_attr_show_blockinfo_show(struct tier_device *dev, char *buf)
{
	struct blockinfo *binfo;
	int res = 0;
	int len;
	int i = 0;
	u64 maxblocks = dev->size >> BLKBITS;
	u64 blocknr = dev->user_selected_blockinfo;

	for (i = 0; i < MAXPAGESHOW; i++) {
		binfo = get_blockinfo(dev, blocknr, 0);
		if (!binfo)
			return res;
		len = sprintf(buf + res, "%i,%llu,%lu,%u,%u\n",
			      binfo->device - 1, binfo->offset,
			      binfo->lastused, binfo->readcount,
			      binfo->writecount);
		res += len;
		kfree(binfo);
		if (!dev->user_selected_ispaged)
			break;
		blocknr++;
		if (blocknr >= maxblocks)
			break;
	}
	return res;
}

static ssize_t tier_attr_size_in_blocks_show(struct tier_device *dev, char *buf)
{
	return sprintf(buf, "%llu\n", dev->size / BLKSIZE);
}

static ssize_t tier_attr_barriers_show(struct tier_device *dev, char *buf)
{
	return sprintf(buf, "%i\n", dev->barrier);
}

static ssize_t tier_attr_ptsync_show(struct tier_device *dev, char *buf)
{
	return sprintf(buf, "%i\n", dev->ptsync);
}

static ssize_t tier_attr_discard_to_devices_show(struct tier_device *dev,
						 char *buf)
{
	return sprintf(buf, "%i\n", dev->discard_to_devices);
}

static ssize_t tier_attr_writethrough_show(struct tier_device *dev, char *buf)
{
	return sprintf(buf, "%i\n", dev->writethrough);
}

static ssize_t tier_attr_resize_show(struct tier_device *dev, char *buf)
{
	return sprintf(buf, "0\n");
}

static ssize_t tier_attr_sequential_landing_show(struct tier_device *dev,
						 char *buf)
{
	return sprintf(buf, "%i\n",
		       dev->backdev[0]->devmagic->dtapolicy.sequential_landing);
}

static ssize_t tier_attr_migrate_verbose_show(struct tier_device *dev,
					      char *buf)
{
	return sprintf(buf, "%i\n", dev->migrate_verbose);
}

static ssize_t tier_attr_migration_policy_show(struct tier_device *dev,
					       char *buf)
{
	char *msg = NULL;
	char *msg2;
	int i;
	int res;

	for (i = 0; i < dev->attached_devices; i++) {
		if (!msg) {
			msg2 =
			    as_sprintf
			    ("%7s %20s %15s %15s\n%7u %20s %15u %15u\n", "tier",
			     "device", "max_age", "hit_collecttime", i,
			     dev->backdev[i]->fds->f_dentry->d_name.name,
			     dev->backdev[i]->devmagic->dtapolicy.max_age,
			     dev->backdev[i]->devmagic->
			     dtapolicy.hit_collecttime);
		} else {
			msg2 =
			    as_sprintf("%s%7u %20s %15u %15u\n", msg,
				       i,
				       dev->backdev[i]->fds->f_dentry->d_name.
				       name,
				       dev->backdev[i]->devmagic->
				       dtapolicy.max_age,
				       dev->backdev[i]->devmagic->
				       dtapolicy.hit_collecttime);
		}
		kfree(msg);
		msg = msg2;
	}
	res = sprintf(buf, "%s\n", msg);
	kfree(msg);
	return res;
}

static ssize_t tier_attr_migration_interval_show(struct tier_device *dev,
						 char *buf)
{
	struct data_policy *dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
	return sprintf(buf, "%llu\n", dtapolicy->migration_interval);
}

static ssize_t tier_attr_numwrites_show(struct tier_device *dev, char *buf)
{
	return sprintf(buf, "sequential %llu random %llu\n",
		       dev->stats.seq_writes, dev->stats.rand_writes);
}

static ssize_t tier_attr_numreads_show(struct tier_device *dev, char *buf)
{
	return sprintf(buf, "sequential %llu random %llu\n",
		       dev->stats.seq_reads, dev->stats.rand_reads);
}

static ssize_t tier_attr_device_usage_show(struct tier_device *dev, char *buf)
{
	unsigned int i = 0;
	int res = 0;
	u64 allocated;
	unsigned int lcount = dev->attached_devices + 1;
	u64 devblocks;

	const char **lines = NULL;
	char *line;
	char *msg;
	lines = kzalloc(lcount * sizeof(char *), GFP_KERNEL);
	if (!lines)
		return -ENOMEM;

	line =
	    as_sprintf("%7s %20s %15s %15s %15s %15s %15s %15s\n", "TIER",
		       "DEVICE", "SIZE MB", "ALLOCATED MB", "AVERAGE READS",
		       "AVERAGE WRITES", "TOTAL_READS", "TOTAL_WRITES");
	if (!line) {
		kfree(lines);
		return -ENOMEM;
	}
	lines[0] = line;
	for (i = 0; i < dev->attached_devices; i++) {
		allocated = allocated_on_device(dev, i);
		if (dev->inerror)
			goto end_error;
		allocated >>= BLKBITS;
		devblocks =
		    (dev->backdev[i]->endofdata -
		     dev->backdev[i]->startofdata) >> BLKBITS;
		dev->backdev[i]->devmagic->average_reads =
		    dev->backdev[i]->devmagic->total_reads / devblocks;
		dev->backdev[i]->devmagic->average_writes =
		    dev->backdev[i]->devmagic->total_writes / devblocks;
		line =
		    as_sprintf
		    ("%7u %20s %15llu %15llu %15u %15u %15llu %15llu\n", i,
		     dev->backdev[i]->fds->f_dentry->d_name.name, devblocks,
		     allocated, dev->backdev[i]->devmagic->average_reads,
		     dev->backdev[i]->devmagic->average_writes,
		     dev->backdev[i]->devmagic->total_reads,
		     dev->backdev[i]->devmagic->total_writes);
		lines[i + 1] = line;
	}
	msg = as_strarrcat(lines, i + 1);
	if (!msg) {
		res = -ENOMEM;
		goto end_error;
	}
	while (i) {
		kfree((char *)lines[--i]);
	}
	res = snprintf(buf, 1023, "%s\n", msg);
	kfree(msg);
end_error:
	kfree(lines);
	kfree(line);
	return res;
}

TIER_ATTR_RW(sequential_landing);
TIER_ATTR_RW(migrate_verbose);
TIER_ATTR_RW(ptsync);
TIER_ATTR_RW(discard_to_devices);
TIER_ATTR_RW(writethrough);
TIER_ATTR_RW(barriers);
TIER_ATTR_RW(migration_interval);
TIER_ATTR_RW(migration_enable);
TIER_ATTR_RW(migration_policy);
TIER_ATTR_RW(resize);
TIER_ATTR_RO(size_in_blocks);
TIER_ATTR_RO(attacheddevices);
TIER_ATTR_RO(numreads);
TIER_ATTR_RO(numwrites);
TIER_ATTR_RO(device_usage);
TIER_ATTR_RO(uuid);
TIER_ATTR_RW(show_blockinfo);
TIER_ATTR_WO(clear_statistics);
TIER_ATTR_WO(migrate_block);

struct attribute *tier_attrs[] = {
	&tier_attr_sequential_landing.attr,
	&tier_attr_migrate_verbose.attr,
	&tier_attr_ptsync.attr,
	&tier_attr_discard_to_devices.attr,
	&tier_attr_writethrough.attr,
	&tier_attr_barriers.attr,
	&tier_attr_migration_interval.attr,
	&tier_attr_migration_enable.attr,
	&tier_attr_migration_policy.attr,
	&tier_attr_attacheddevices.attr,
	&tier_attr_numreads.attr,
	&tier_attr_numwrites.attr,
	&tier_attr_device_usage.attr,
	&tier_attr_resize.attr,
	&tier_attr_clear_statistics.attr,
	&tier_attr_size_in_blocks.attr,
	&tier_attr_show_blockinfo.attr,
	&tier_attr_uuid.attr,
	&tier_attr_migrate_block.attr,
	NULL,
};
