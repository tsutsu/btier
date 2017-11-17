/*
 * Btier sysfs attributes and store/show functions.
 *
 * Copyright (C) 2014 Mark Ruijter, <mruijter@gmail.com>
 *
 * Btier2 changes, Copyright (C) 2014 Jianjian Huo, <samuel.huo@gmail.com>
 *
 * Copyright (c) 2017 SoftNAS, LLC
 */

#include "btier.h"

#define sprintf_one_var(buf, var)                                              \
	sprintf(                                                               \
	    buf,                                                               \
	    __builtin_types_compatible_p(typeof(var), int)                     \
		? "%i\n"                                                       \
		: __builtin_types_compatible_p(typeof(var), unsigned)          \
		      ? "%u\n"                                                 \
		      : __builtin_types_compatible_p(typeof(var), long)        \
			    ? "%li\n"                                          \
			    : __builtin_types_compatible_p(typeof(var),        \
							   unsigned long)      \
				  ? "%lu\n"                                    \
				  : __builtin_types_compatible_p(typeof(var),  \
								 int64_t)      \
					? "%lli\n"                             \
					: __builtin_types_compatible_p(        \
					      typeof(var), uint64_t)           \
					      ? "%llu\n"                       \
					      : __builtin_types_compatible_p(  \
						    typeof(var), const char *) \
						    ? "%s\n"                   \
						    : "%i\n",                  \
	    var)

extern struct list_head device_list;
extern struct mutex tier_devices_mutex;

/* tier sysfs attributes */
ssize_t tier_attr_show(struct device *dev, char *page,
		       ssize_t (*callback)(struct tier_device *, char *))
{
	struct tier_device *l, *lo = NULL;

	mutex_lock(&tier_devices_mutex);
	list_for_each_entry(l, &device_list,
			    list) if (disk_to_dev(l->gd) == dev)
	{
		lo = l;
		break;
	}
	mutex_unlock(&tier_devices_mutex);

	return lo ? callback(lo, page) : -EIO;
}

ssize_t tier_attr_store(struct device *dev, const char *page, size_t s,
			ssize_t (*callback)(struct tier_device *, const char *,
					    size_t))
{
	struct tier_device *l, *lo = NULL;

	mutex_lock(&tier_devices_mutex);
	list_for_each_entry(l, &device_list,
			    list) if (disk_to_dev(l->gd) == dev)
	{
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

#define TIER_ATTR_RO(_name)                                                    \
	static ssize_t tier_attr_##_name##_show(struct tier_device *, char *); \
	static ssize_t tier_attr_do_show_##_name(                              \
	    struct device *d, struct device_attribute *attr, char *b)          \
	{                                                                      \
		return tier_attr_show(d, b, tier_attr_##_name##_show);         \
	}                                                                      \
	static struct device_attribute tier_attr_##_name =                     \
	    __ATTR(_name, S_IRUGO, tier_attr_do_show_##_name, NULL);

static ssize_t tier_attr_attacheddevices_show(struct tier_device *dev,
					      char *buf)
{
	return sprintf(buf, "%u\n", dev->attached_devices);
}

#define TIER_ATTR_WO(_name)                                                    \
	static ssize_t tier_attr_##_name##_store(struct tier_device *,         \
						 const char *, size_t);        \
	static ssize_t tier_attr_do_store_##_name(                             \
	    struct device *d, struct device_attribute *attr, const char *b,    \
	    size_t s)                                                          \
	{                                                                      \
		return tier_attr_store(d, b, s, tier_attr_##_name##_store);    \
	}                                                                      \
	static struct device_attribute tier_attr_##_name =                     \
	    __ATTR(_name, S_IWUSR, NULL, tier_attr_do_store_##_name);

#define TIER_ATTR_RW(_name)                                                    \
	static ssize_t tier_attr_##_name##_store(struct tier_device *,         \
						 const char *, size_t);        \
	static ssize_t tier_attr_do_store_##_name(                             \
	    struct device *d, struct device_attribute *attr, const char *b,    \
	    size_t s)                                                          \
	{                                                                      \
		return tier_attr_store(d, b, s, tier_attr_##_name##_store);    \
	}                                                                      \
	static ssize_t tier_attr_do_show_##_name(                              \
	    struct device *d, struct device_attribute *attr, char *b)          \
	{                                                                      \
		return tier_attr_show(d, b, tier_attr_##_name##_show);         \
	}                                                                      \
	static struct device_attribute tier_attr_##_name =                     \
	    __ATTR(_name, (S_IRWXU ^ S_IXUSR) | S_IRGRP | S_IROTH,             \
		   tier_attr_do_show_##_name, tier_attr_do_store_##_name);

static ssize_t tier_attr_migration_enable_store(struct tier_device *dev,
						const char *buf, size_t s)
{
	struct data_policy *dtapolicy;

	if ('0' != buf[0] && '1' != buf[0])
		return s;
	spin_lock(&dev->backdev[0]->magic_lock);
	dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
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
		if (!dtapolicy->migration_disabled &&
		    0 == atomic_read(&dev->migrate)) {
			dtapolicy->migration_disabled = 1;
			if (timer_pending(&dev->migrate_timer))
				del_timer_sync(&dev->migrate_timer);
			pr_info("migration is disabled for %s\n", dev->devname);
		}
		dtapolicy->migration_disabled = 1;
	}
	spin_unlock(&dev->backdev[0]->magic_lock);
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

static ssize_t tier_attr_discard_to_devices_store(struct tier_device *dev,
						  const char *buf, size_t s)
{
	if ('0' != buf[0] && '1' != buf[0])
		return s;

	if ('0' == buf[0]) {
		if (dev->discard_to_devices) {
			dev->discard_to_devices = 0;
			pr_info("discard_to_devices is disabled\n");
		}
	} else {
		if (!dev->discard_to_devices) {
			dev->discard_to_devices = 1;
			pr_info("discard_to_devices is enabled\n");
		}
	}

	return s;
}

static ssize_t tier_attr_discard_store(struct tier_device *dev, const char *buf,
				       size_t s)
{
	if ('0' != buf[0] && '1' != buf[0])
		return s;

	if ('0' == buf[0]) {
		if (dev->discard) {
			dev->discard = 0;
			pr_info("discard is disabled\n");
			queue_flag_clear_unlocked(QUEUE_FLAG_DISCARD,
						  dev->rqueue);
		}
	} else {
		if (!dev->discard) {
			dev->discard = 1;
			pr_info("discard is enabled\n");
			queue_flag_set_unlocked(QUEUE_FLAG_DISCARD,
						dev->rqueue);
		}
	}
	return s;
}

static ssize_t tier_attr_resize_store(struct tier_device *dev, const char *buf,
				      size_t s)
{
	if ('1' != buf[0])
		return s;
	resize_tier(dev);
	return s;
}

/* return the input NULL terminated */
static char *null_term_buf(const char *buf, size_t s)
{
	char *cpybuf;

	cpybuf = kzalloc(s + 1, GFP_KERNEL);
	if (!cpybuf)
		return NULL;
	memcpy(cpybuf, buf, s);

	return cpybuf;
}

static ssize_t tier_attr_show_blockinfo_store(struct tier_device *dev,
					      const char *buf, size_t s)
{
	int res;
	char *cpybuf;
	u64 maxblocks = dev->size >> BLK_SHIFT;
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
	struct backing_device *backdev = dev->backdev[0];

	cpybuf = null_term_buf(buf, s);
	if (!cpybuf)
		return -ENOMEM;
	res = sscanf(cpybuf, "%i", &landdev);
	if (res != 1)
		goto end_error;
	if (landdev >= dev->attached_devices)
		goto end_error;
	if (landdev < 0)
		goto end_error;

	spin_lock(&backdev->magic_lock);
	backdev->devmagic->dtapolicy.sequential_landing = landdev;
	spin_unlock(&backdev->magic_lock);

	kfree(cpybuf);
	return s;

end_error:
	kfree(cpybuf);
	return -ENOMSG;
}

static ssize_t tier_attr_migrate_block_store(struct tier_device *dev,
					     const char *buf, size_t s)
{
	u64 blocknr;
	int device;
	int res = 0;
	size_t m = s;
	char *cpybuf;
	u64 maxblocks = dev->size >> BLK_SHIFT;

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
		if (!dev->migrate_verbose) {
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

	char *cur = NULL;
	char *a = NULL;
	char *p = NULL;
	char *devicename = NULL;
	char *cpybuf;

	cpybuf = null_term_buf(buf, s);
	if (!cpybuf)
		return -ENOMEM;

	p = strchr(cpybuf, ' ');
	if (!p)
		goto end_error;
	a = kzalloc(p - cpybuf + 1, GFP_KERNEL);
	if (!a)
		goto end_error;
	memcpy(a, cpybuf, p - cpybuf);
	res = sscanf(a, "%u", &devicenr);
	kfree(a);
	if (res != 1 || devicenr < 0 || devicenr >= dev->attached_devices)
		goto end_error;
	a = p;

	while (a[0] == ' ')
		a++;
	p = strchr(a, ' ');
	if (!p)
		goto end_error;
	devicename = kzalloc(p - cpybuf + 1, GFP_KERNEL);
	if (!devicename) {
		goto end_error;
	}
	memcpy(devicename, a, p - a);
	if (0 !=
	    strcmp(devicename,
		   dev->backdev[devicenr]->fds->f_path.dentry->d_name.name)) {
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
	spin_lock(&dev->backdev[devicenr]->magic_lock);
	dev->backdev[devicenr]->devmagic->dtapolicy.max_age = max_age;
	dev->backdev[devicenr]->devmagic->dtapolicy.hit_collecttime =
	    hit_collecttime;
	spin_unlock(&dev->backdev[devicenr]->magic_lock);
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
	struct data_policy *dtapolicy;
	int curstate;

	cpybuf = null_term_buf(buf, s);
	if (!cpybuf)
		return -ENOMEM;
	res = sscanf(cpybuf, "%llu", &interval);
	if (res == 1) {
		if (interval <= 0)
			return -ENOMSG;
		spin_lock(&dev->backdev[0]->magic_lock);
		dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
		curstate = dtapolicy->migration_disabled;
		dtapolicy->migration_disabled = 1;
		spin_unlock(&dev->backdev[0]->magic_lock);
		down_write(&dev->qlock);
		dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
		dtapolicy->migration_interval = interval;
		if (!dtapolicy->migration_disabled)
			mod_timer(&dev->migrate_timer,
				  jiffies + msecs_to_jiffies(interval * 1000));
		up_write(&dev->qlock);
		spin_lock(&dev->backdev[0]->magic_lock);
		dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
		dtapolicy->migration_disabled = curstate;
		spin_unlock(&dev->backdev[0]->magic_lock);
	} else
		s = -ENOMSG;
	kfree(cpybuf);
	return s;
}

static ssize_t tier_attr_migration_enable_show(struct tier_device *dev,
					       char *buf)
{
	struct data_policy *dtapolicy;
	int migration_disabled;

	spin_lock(&dev->backdev[0]->magic_lock);
	dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
	migration_disabled = dtapolicy->migration_disabled;
	spin_unlock(&dev->backdev[0]->magic_lock);
	return sprintf(buf, "%i\n", !migration_disabled);
}

static ssize_t tier_attr_internals_show(struct tier_device *dev, char *buf)
{
	char *iotype;
	char *iopending;
	char *qlock;
	char *aiowq;
	char *discard;
#ifndef MAX_PERFORMANCE
	char *debug_state;
#endif
	int res = 0;

	if (atomic_read(&dev->migrate) == MIGRATION_IO)
		iotype =
		    as_sprintf("iotype (normal or migration) : migration_io\n");
	else if (atomic_read(&dev->wqlock))
		iotype =
		    as_sprintf("iotype (normal or migration) : normal_io\n");
	else
		iotype =
		    as_sprintf("iotype (normal or migration) : no activity\n");
	iopending = as_sprintf("async random ios pending     : %i\n",
			       atomic_read(&dev->aio_pending));
	if (rwsem_is_locked(&dev->qlock))
		qlock = as_sprintf("main mutex                   : locked\n");
	else
		qlock = as_sprintf("main mutex                   : unlocked\n");
	if (waitqueue_active(&dev->aio_event))
		aiowq = as_sprintf("waiting on asynchrounous io  : True\n");
	else
		aiowq = as_sprintf("waiting on asynchrounous io  : False\n");
#ifndef MAX_PERFORMANCE
	spin_lock(&dev->dbg_lock);
	if (dev->debug_state & DISCARD)
		discard = as_sprintf("discard request is pending   : True\n");
	else
		discard = as_sprintf("discard request is pending   : False\n");
	debug_state =
	    as_sprintf("debug state                  : %i\n", dev->debug_state);
	spin_unlock(&dev->dbg_lock);
	res = sprintf(buf, "%s%s%s%s%s%s", iotype, iopending, qlock, aiowq,
		      discard, debug_state);
#else
	res = sprintf(buf, "%s%s%s%s", iotype, iopending, qlock, aiowq);
#endif
	kfree(iotype);
	kfree(iopending);
	kfree(qlock);
	kfree(aiowq);
#ifndef MAX_PERFORMANCE
	kfree(discard);
	kfree(debug_state);
#endif
	return res;
}

static ssize_t tier_attr_uuid_show(struct tier_device *dev, char *buf)
{
	int res = 0;

	spin_lock(&dev->backdev[0]->magic_lock);
	memcpy(buf, dev->backdev[0]->devmagic->uuid, TIGER_HASH_LEN);
	spin_unlock(&dev->backdev[0]->magic_lock);
	buf[TIGER_HASH_LEN] = '\n';
	res = TIGER_HASH_LEN + 1;
	return res;
}

static ssize_t tier_attr_show_blockinfo_show(struct tier_device *dev, char *buf)
{
	struct blockinfo *binfo;
	int res = 0;
	int len;
	int i = 0;
	u64 maxblocks = dev->size >> BLK_SHIFT;
	u64 blocknr = dev->user_selected_blockinfo;

	for (i = 0; i < MAXPAGESHOW; i++) {
		binfo = get_blockinfo(dev, blocknr, 0);
		if (!binfo)
			return res;
		len = sprintf(buf + res, "%i,%llu,%lu,%u,%u\n",
			      binfo->device - 1, binfo->offset, binfo->lastused,
			      binfo->readcount, binfo->writecount);
		res += len;
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
	return sprintf(buf, "%llu\n", dev->size >> BLK_SHIFT);
}

static ssize_t tier_attr_discard_to_devices_show(struct tier_device *dev,
						 char *buf)
{
	return sprintf(buf, "%i\n", dev->discard_to_devices);
}

static ssize_t tier_attr_discard_show(struct tier_device *dev, char *buf)
{
	return sprintf(buf, "%i\n", dev->discard);
}

static ssize_t tier_attr_resize_show(struct tier_device *dev, char *buf)
{
	return sprintf(buf, "0\n");
}

static ssize_t tier_attr_sequential_landing_show(struct tier_device *dev,
						 char *buf)
{
	struct backing_device *backdev = dev->backdev[0];
	int len;

	spin_lock(&backdev->magic_lock);
	len = sprintf(buf, "%i\n",
		      backdev->devmagic->dtapolicy.sequential_landing);
	spin_unlock(&backdev->magic_lock);

	return len;
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
		spin_lock(&dev->backdev[i]->magic_lock);
		if (!msg) {
			msg2 = as_sprintf(
			    "%7s %20s %15s %15s\n%7u %20s %15u %15u\n", "tier",
			    "device", "max_age", "hit_collecttime", i,
			    dev->backdev[i]->fds->f_path.dentry->d_name.name,
			    dev->backdev[i]->devmagic->dtapolicy.max_age,
			    dev->backdev[i]
				->devmagic->dtapolicy.hit_collecttime);
		} else {
			msg2 = as_sprintf(
			    "%s%7u %20s %15u %15u\n", msg, i,
			    dev->backdev[i]->fds->f_path.dentry->d_name.name,
			    dev->backdev[i]->devmagic->dtapolicy.max_age,
			    dev->backdev[i]
				->devmagic->dtapolicy.hit_collecttime);
		}
		spin_unlock(&dev->backdev[i]->magic_lock);
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
	struct data_policy *dtapolicy;
	u64 migration_interval;

	spin_lock(&dev->backdev[0]->magic_lock);
	dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
	migration_interval = dtapolicy->migration_interval;
	spin_unlock(&dev->backdev[0]->magic_lock);
	return sprintf(buf, "%llu\n", migration_interval);
}

static ssize_t tier_attr_numwrites_show(struct tier_device *dev, char *buf)
{
	int len;

	len = sprintf(buf, "sequential ");
	len +=
	    sprintf_one_var(buf + len, atomic64_read(&dev->stats.seq_writes));
	len += sprintf(buf + len, ", Random ");
	len +=
	    sprintf_one_var(buf + len, atomic64_read(&dev->stats.rand_writes));

	return len;
}

static ssize_t tier_attr_numreads_show(struct tier_device *dev, char *buf)
{
	int len;

	len = sprintf(buf, "sequential ");
	len += sprintf_one_var(buf + len, atomic64_read(&dev->stats.seq_reads));
	len += sprintf(buf + len, ", Random ");
	len +=
	    sprintf_one_var(buf + len, atomic64_read(&dev->stats.rand_reads));

	return len;
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

	line = as_sprintf("%7s %20s %15s %15s %15s %15s %15s %15s\n", "TIER",
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
		allocated >>= BLK_SHIFT;
		devblocks = (dev->backdev[i]->endofdata -
			     dev->backdev[i]->startofdata) >>
			    BLK_SHIFT;

		spin_lock(&dev->backdev[i]->magic_lock);
		dev->backdev[i]->devmagic->average_reads = btier_div(
		    dev->backdev[i]->devmagic->total_reads, devblocks);
		dev->backdev[i]->devmagic->average_writes = btier_div(
		    dev->backdev[i]->devmagic->total_writes, devblocks);
		line = as_sprintf(
		    "%7u %20s %15llu %15llu %15u %15u %15llu %15llu\n", i,
		    dev->backdev[i]->fds->f_path.dentry->d_name.name, devblocks,
		    allocated, dev->backdev[i]->devmagic->average_reads,
		    dev->backdev[i]->devmagic->average_writes,
		    dev->backdev[i]->devmagic->total_reads,
		    dev->backdev[i]->devmagic->total_writes);
		lines[i + 1] = line;
		spin_unlock(&dev->backdev[i]->magic_lock);
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
TIER_ATTR_RW(discard_to_devices);
TIER_ATTR_RW(discard);
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
TIER_ATTR_RO(internals);
TIER_ATTR_RW(show_blockinfo);
TIER_ATTR_WO(clear_statistics);
TIER_ATTR_WO(migrate_block);

struct attribute *tier_attrs[] = {
    &tier_attr_sequential_landing.attr,
    &tier_attr_migrate_verbose.attr,
    &tier_attr_discard_to_devices.attr,
    &tier_attr_discard.attr,
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
    &tier_attr_internals.attr,
    &tier_attr_migrate_block.attr,
    NULL,
};
