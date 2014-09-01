/*
 * tier : Tiered storage made easy.
 *        Tier allows to create a virtual blockdevice
 *        that consists of multiple physical devices.
 *        A common configuration would be to use
 *        SSD/SAS/SATA 
 *
 * Partly based up-on sbd and the loop driver.
 * Redistributable under the terms of the GNU GPL.
 * Author: Mark Ruijter, mruijter@gmail.com
 */
#include "btier.h"
#include "btier_main.h"
#include "btier_common.h"

#define TRUE 1
#define FALSE 0
#define TIER_VERSION "1.3.5"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Mark Ruijter");

LIST_HEAD(device_list);
DEFINE_MUTEX(tier_devices_mutex);

/*
 * The internal representation of our device.
 */
static struct tier_device *device = NULL;
static char *devicenames;
static struct mutex ioctl_mutex;
static DEFINE_SPINLOCK(uselock);

static int tier_device_count(void)
{
	struct list_head *pos;
	int count = 0;

	list_for_each(pos, &device_list) {
		count++;
	}
	return count;
}

char *tiger_hash(char *data, unsigned int dlen)
{
	struct scatterlist sg;
	struct hash_desc desc;
	char *thash;

	thash = kzalloc(32, GFP_KERNEL);
	if (!thash)
		return thash;
	/* ... set up the scatterlists ... */
	desc.tfm = crypto_alloc_hash("tgr192", 0, CRYPTO_ALG_ASYNC);
	if (IS_ERR(desc.tfm)) {
                pr_warn("unable to allocate crypto_hash\n");
		goto fail;
	}
	desc.flags = 0;
	sg_init_one(&sg, data, dlen);
	if (crypto_hash_digest(&desc, &sg, dlen, thash))
		goto fail;
	crypto_free_hash(desc.tfm);
	return thash;

fail:
	kfree(thash);
	return NULL;
}

/*
 * Open and close.
 */
static int tier_open(struct block_device *bdev, fmode_t mode)
{
	struct tier_device *dev;

	dev = bdev->bd_inode->i_bdev->bd_disk->private_data;
	spin_lock(&uselock);
	dev->users++;
	spin_unlock(&uselock);
	return 0;
}

void set_debug_info(struct tier_device *dev, int state)
{
#ifndef MAX_PERFORMANCE
	spin_lock(&dev->dbg_lock);
	dev->debug_state |= state;
	spin_unlock(&dev->dbg_lock);
#endif
}

void clear_debug_info(struct tier_device *dev, int state)
{
#ifndef MAX_PERFORMANCE
	spin_lock(&dev->dbg_lock);
	if (dev->debug_state & state)
		dev->debug_state ^= state;
	spin_unlock(&dev->dbg_lock);
#endif
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,12,0)
static void tier_release(struct gendisk *gd, fmode_t mode)
#else
static int tier_release(struct gendisk *gd, fmode_t mode)
#endif
{
	struct tier_device *dev;

	dev = gd->private_data;
	spin_lock(&uselock);
	dev->users--;
	spin_unlock(&uselock);
#if LINUX_VERSION_CODE < KERNEL_VERSION(3,12,0)
	return 0;
#endif
}

/*
 * The device operations structure.
 */
static struct block_device_operations tier_ops = {
	.open = tier_open,
	.release = tier_release,
	.owner = THIS_MODULE,
};

extern struct attribute *tier_attrs[];

static struct attribute_group tier_attribute_group = {
	.name = "tier",
	.attrs = tier_attrs,
};

static int tier_sysfs_init(struct tier_device *dev)
{
	return sysfs_create_group(&disk_to_dev(dev->gd)->kobj,
				  &tier_attribute_group);
}

void btier_lock(struct tier_device *dev)
{
	atomic_set(&dev->migrate, MIGRATION_IO);
	mutex_lock(&dev->qlock);
	if (0 != atomic_read(&dev->aio_pending))
		wait_event(dev->aio_event, 0 == atomic_read(&dev->aio_pending));
}

void btier_unlock(struct tier_device *dev)
{
	atomic_set(&dev->migrate, 0);
	mutex_unlock(&dev->qlock);
}

void btier_clear_statistics(struct tier_device *dev)
{
	u64 curblock;
	u64 blocks = dev->size >> BLKBITS;
	struct devicemagic *dmagic;
	int i;
	struct blockinfo *binfo = NULL;

	btier_lock(dev);

	for (curblock = 0; curblock < blocks; curblock++) {
		binfo = get_blockinfo(dev, curblock, 0);
		if (dev->inerror) {
			break;
		}
		if (binfo->device != 0) {
			binfo->readcount = 0;
			binfo->writecount = 0;
			(void)write_blocklist(dev, curblock, binfo, WC);
		}
	}
	for (i = 0; i < dev->attached_devices; i++) {
		dmagic = dev->backdev[i]->devmagic;
		dmagic->average_reads = 0;
		dmagic->average_writes = 0;
		dmagic->total_reads = 0;
		dmagic->total_writes = 0;
	}
	btier_unlock(dev);
}

static void aio_reader(struct work_struct *work)
{
	aio_work_t *rwork;
	struct tier_device *dev;
	int res;

	set_user_nice(current, -20);
	rwork = (aio_work_t *) work;
	dev = rwork->dev;
	res =
	    tier_file_read(dev, rwork->device,
			   rwork->data, rwork->size, rwork->offset);
	if (res < 0)
		tiererror(dev, "read failed");
	atomic_dec(&dev->aio_pending);
	wake_up(&dev->aio_event);
	kunmap(rwork->bv_page);
	kfree(work);
}

static int read_aio(struct tier_device *dev, int device, char *data, int size,
		    u64 offset, struct page *bv_page)
{
	int ret = 0;
	aio_work_t *rwork;
	rwork = kzalloc(sizeof(aio_work_t), GFP_NOFS);
	if (!rwork)
		return -ENOMEM;
	rwork->dev = dev;
	rwork->data = data;
	rwork->offset = offset;
	rwork->size = size;
	rwork->device = device;
	rwork->bv_page = bv_page;
	atomic_inc(&dev->aio_pending);
	INIT_WORK((struct work_struct *)rwork, aio_reader);
	if (!queue_work(dev->aio_queue, (struct work_struct *)rwork))
		ret = -EIO;
	return ret;
}

static void tier_sysfs_exit(struct tier_device *dev)
{
	sysfs_remove_group(&disk_to_dev(dev->gd)->kobj, &tier_attribute_group);
}

static struct devicemagic *read_device_magic(struct tier_device *dev,
					     int device)
{
	struct devicemagic *dmagic;

	dmagic = kzalloc(sizeof(struct devicemagic), GFP_KERNEL);
	if (!dmagic)
		return NULL;
	tier_file_read(dev, device, dmagic, sizeof(*dmagic), 0);
	return dmagic;
}

static void write_device_magic(struct tier_device *dev, int device)
{
	struct devicemagic *dmagic = dev->backdev[device]->devmagic;
	tier_file_write(dev, device, dmagic, sizeof(*dmagic), 0);
}

static void mark_device_clean(struct tier_device *dev, int device)
{
	struct backing_device *backdev = dev->backdev[device];
	backdev->devmagic->clean = CLEAN;
	backdev->devmagic->use_bio = dev->use_bio;
	memset(&backdev->devmagic->binfo_journal_new, 0,
	       sizeof(struct blockinfo));
	memset(&backdev->devmagic->binfo_journal_old, 0,
	       sizeof(struct blockinfo));
	write_device_magic(dev, device);
}

static int mark_offset_as_used(struct tier_device *dev, int device, u64 offset)
{
	u64 boffset;
	u8 allocated = ALLOCATED;
	struct backing_device *backdev = dev->backdev[device];
	int ret;

	boffset = offset >> BLKBITS;
	ret = tier_file_write(dev, device, &allocated, 1,
			      backdev->startofbitlist + boffset);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,0,0)
	ret =
	    vfs_fsync_range(backdev->fds, backdev->startofbitlist + boffset, 1,
			    FSMODE);
#else
	ret = vfs_fsync_range(backdev->fds, backdev->fds->f_path.dentry,
			      backdev->startofbitlist + boffset, 1, FSMODE);
#endif
	backdev->bitlist[boffset] = allocated;
	return ret;
}

static void clear_dev_list(struct tier_device *dev, struct blockinfo *binfo)
{
	u64 offset;
	u64 boffset;
	u8 unallocated = UNALLOCATED;
	struct backing_device *backdev = dev->backdev[binfo->device - 1];

	offset = binfo->offset - backdev->startofdata;
	boffset = offset >> BLKBITS;
	tier_file_write(dev, binfo->device - 1,
			&unallocated, 1, backdev->startofbitlist + boffset);
	if (backdev->free_offset > boffset)
		backdev->free_offset = boffset;
	if (backdev->bitlist)
		backdev->bitlist[boffset] = unallocated;
}

static int allocate_dev(struct tier_device *dev, u64 blocknr,
			struct blockinfo *binfo, int device)
{
	struct backing_device *backdev = dev->backdev[device];
	u8 *buffer = NULL;
	u64 cur = 0;
	u64 relative_offset = 0;
	int ret = 0;
	unsigned int buffercount;
	cur = backdev->free_offset >> 12;
	/* The bitlist may be loaded into memory or be NULL if not */
	while (0 == binfo->device && (cur * PAGE_SIZE) < backdev->bitlistsize) {
		buffer = &backdev->bitlist[cur * PAGE_SIZE];
		buffercount = 0;
		while (0 == binfo->device) {
			if (ALLOCATED != buffer[buffercount]) {
				binfo->offset =
				    (cur * PAGE_SIZE * BLKSIZE) +
				    (buffercount * BLKSIZE);
				relative_offset = binfo->offset;
				binfo->offset += backdev->startofdata;
				if (binfo->offset + BLKSIZE >
				    backdev->endofdata) {
					goto end_exit;
				} else {
					binfo->device = device + 1;
					ret = mark_offset_as_used(dev, device,
								  relative_offset);
					if (0 != ret)
						goto end_exit;
					backdev->free_offset =
					    relative_offset >> BLKBITS;
					backdev->usedoffset = binfo->offset;
				}
			}
			buffercount++;
			if (buffercount >= PAGE_SIZE)
				break;
		}
		cur++;
	}
end_exit:
	return ret;
}

static int allocate_block(struct tier_device *dev, u64 blocknr,
			  struct blockinfo *binfo)
{
	int device = 0;
	int count = 0;

/* Sequential writes will go to SAS or SATA */
	if (dev->iotype == SEQUENTIAL && dev->attached_devices > 1)
		device =
		    dev->backdev[0]->devmagic->dtapolicy.sequential_landing;
	while (1) {
		if (0 != allocate_dev(dev, blocknr, binfo, device))
			return -EIO;
		if (0 != binfo->device) {
			if (0 != write_blocklist(dev, blocknr, binfo, WA))
				return -EIO;
			break;
		}
		device++;
		count++;
		if (count >= dev->attached_devices) {
			pr_err
			    ("no free space found, this should never happen!!\n");
			return -ENOSPC;
		}
		if (device >= dev->attached_devices)
			device = 0;
	}
	return 0;
}

static int tier_file_write(struct tier_device *dev, unsigned int device,
			   void *buf, size_t len, loff_t pos)
{
	ssize_t bw;
	mm_segment_t old_fs = get_fs();
	struct backing_device *backdev = dev->backdev[device];

	set_fs(get_ds());
	set_debug_info(dev, VFSWRITE);
	bw = vfs_write(backdev->fds, buf, len, &pos);
	clear_debug_info(dev, VFSWRITE);
	backdev->dirty = 1;
	set_fs(old_fs);
	if (likely(bw == len))
		return 0;
	pr_err("Write error on device %s at offset %llu, length %li\n",
	       backdev->fds->f_dentry->d_name.name,
	       (unsigned long long)pos, len);
	if (bw >= 0)
		bw = -EIO;
	return bw;
}

static int read_tiered(void *data, unsigned int len,
                       u64 offset, struct bio_vec *bvec,
		       struct bio_task *bio_task)
{
	struct blockinfo *binfo = NULL;
	u64 blocknr;
	unsigned int block_offset;
	int res = 0;
	int size = 0;
	unsigned int done = 0;
	u64 curoff;
	unsigned int device;
        unsigned int chunksize=PAGE_SIZE;
	int keep = 0;
        struct tier_device *dev = bio_task->dev;

	if (dev->iotype == RANDOM)
		dev->stats.rand_reads++;
	else
		dev->stats.seq_reads++;
	if (len == 0)
		return -1;
	while (done < len) {
		curoff = offset + done;
		blocknr = curoff >> BLKBITS;
		block_offset = curoff - (blocknr << BLKBITS);

		binfo = get_blockinfo(dev, blocknr, TIERREAD);
		if (dev->inerror) {
			res = -EIO;
			break;
		}
		if (len - done + block_offset > BLKSIZE) {
			size = BLKSIZE - block_offset;
		} else
			size = len - done;
		if (0 == binfo->device) {
			memset(data + done, 0, size);
			res = 0;
			if (atomic_dec_and_test(&bio_task->pending)) {
				if (dev->inerror) {
					bio_endio(bio_task->parent_bio, -EIO);
				} else
					bio_endio(bio_task->parent_bio, 0);
			}
		} else {
			device = binfo->device - 1;
			if (dev->backdev[device]->bdev
			    && dev->use_bio == USE_BIO) {
                                if(done == 0) {
                                     chunksize = get_chunksize(dev->backdev[device]->bdev);
                                     if ( bio_task->parent_bio->bi_size <= chunksize &&
                                          block_offset + bio_task->parent_bio->bi_size <= BLKSIZE )
                                         bio_task->in_one = 1;
                                }
				res =
				    tier_read_page(device, bvec,
						   binfo->offset + block_offset,
						   bio_task);
			} else {
				bio_task->vfs = 1;
				if (dev->iotype == RANDOM) {
					keep = 1;
					res =
					    read_aio(dev, device,
						     data + done, size,
						     binfo->offset +
						     block_offset,
						     bvec->bv_page);
				} else {
					res =
					    tier_file_read(dev, device,
							   data + done, size,
							   binfo->offset +
							   block_offset);
				}
			}
		}
		done += size;
		if (res != 0 || bio_task->in_one)
			break;
	}
	if (!keep)
		kunmap(bvec->bv_page);
	return res;
}

static inline void bio_write_done(struct bio *bio, int err)
{
	struct bio_task *bio_task = bio->bi_private;
	struct tier_device *dev = bio_task->dev;
	if (err)
		tiererror(dev, "write error\n");
	if (atomic_dec_and_test(&bio_task->pending)) {
		if (dev->inerror) {
			bio_endio(bio_task->parent_bio, -EIO);
		} else
			bio_endio(bio_task->parent_bio, 0);
	}
	atomic_dec(&dev->aio_pending);
	wake_up(&dev->aio_event);
	bio_put(bio);
}

static inline void bio_read_done(struct bio *bio, int err)
{
	struct bio_task *bio_task = bio->bi_private;
	struct tier_device *dev = bio_task->dev;
	if (err)
		tiererror(dev, "read error\n");
	if (atomic_dec_and_test(&bio_task->pending)) {
		if (dev->inerror) {
			bio_endio(bio_task->parent_bio, -EIO);
		} else
			bio_endio(bio_task->parent_bio, 0);
	}
	atomic_dec(&dev->aio_pending);
	wake_up(&dev->aio_event);
	bio_put(bio);
}

static struct bio *prepare_bio_req(struct tier_device *dev, unsigned int device,
				   struct bio_vec *bvec, u64 offset,
				   struct bio_task *bio_task)
{
	struct block_device *bdev = dev->backdev[device]->bdev;
        struct bio *bio;
  
        if (bio_task->in_one) {
             bio = bio_clone(bio_task->parent_bio, GFP_NOIO);
             if (!bio)
                  tiererror(dev, "bio_clone failed\n");
        } else {
	     bio = bio_alloc(GFP_NOIO, 1);
             if (!bio)
                  tiererror(dev, "bio_clone failed\n");
	     bio->bi_bdev = bdev;
	     bio->bi_io_vec[0].bv_page = bvec->bv_page;
	     bio->bi_io_vec[0].bv_len = bvec->bv_len;
	     bio->bi_io_vec[0].bv_offset = bvec->bv_offset;
	     bio->bi_vcnt = 1;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,14,0)
	     bio->bi_iter.bi_size = bvec->bv_len;
#else
	     bio->bi_size = bvec->bv_len;
#endif
        }
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,14,0)
	bio->bi_iter.bi_sector = offset >> 9;
	bio->bi_iter.bi_idx = 0;
#else
	bio->bi_sector = offset >> 9;
	bio->bi_idx = 0;
#endif
	bio->bi_private = bio_task;
	bio->bi_bdev = bdev;
	return bio;
}

static int tier_write_page(unsigned int device,
                           struct bio_vec *bvec, u64 offset,
                           struct bio_task *bio_task)
{
        struct bio *bio;
        struct tier_device *dev = bio_task->dev;
        set_debug_info(dev, BIOWRITE);
        bio = prepare_bio_req(dev, device, bvec, offset, bio_task);
        if (!bio)
                tiererror(dev, "bio_alloc failed from tier_write_page\n");
        bio->bi_end_io = bio_write_done;
        bio->bi_rw = WRITE;
        atomic_inc(&dev->aio_pending);
        submit_bio(WRITE, bio);
        clear_debug_info(dev, BIOWRITE);
        return 0;
}

static int tier_read_page(unsigned int device,
			  struct bio_vec *bvec, u64 offset,
			  struct bio_task *bio_task)
{
	struct bio *bio;
        struct tier_device *dev = bio_task->dev;
	set_debug_info(dev, BIOREAD);
	bio = prepare_bio_req(dev, device, bvec, offset, bio_task);
	if (!bio)
		tiererror(dev, "bio_alloc failed from tier_write_page\n");
	bio->bi_end_io = bio_read_done;
	bio->bi_rw = READ;
	atomic_inc(&dev->aio_pending);
	submit_bio(READ, bio);
	clear_debug_info(dev, BIOREAD);
	return 0;
}

#if LINUX_VERSION_CODE <= KERNEL_VERSION(3,10,1)
struct submit_bio_ret {
	struct completion event;
	int error;
};

static void submit_bio_wait_endio(struct bio *bio, int error)
{
	struct submit_bio_ret *ret = bio->bi_private;

	ret->error = error;
	complete(&ret->event);
}

/**
 * submit_bio_wait - submit a bio, and wait until it completes
 * @rw: whether to %READ or %WRITE, or maybe to %READA (read ahead)
 * @bio: The &struct bio which describes the I/O
 *
 * Simple wrapper around submit_bio(). Returns 0 on success, or the error from
 * bio_endio() on failure.
 */
int submit_bio_wait(int rw, struct bio *bio)
{
	struct submit_bio_ret ret;

#if LINUX_VERSION_CODE <= KERNEL_VERSION(3,0,0)
	bio->bi_flags |= BIO_RW_BARRIER;
#else
	bio->bi_flags |= REQ_FLUSH;
#endif
	init_completion(&ret.event);
	bio->bi_private = &ret;
	bio->bi_end_io = submit_bio_wait_endio;
	submit_bio(rw, bio);
	wait_for_completion(&ret.event);
	return ret.error;
}
#endif

static int tier_bio_io(struct tier_device *dev, unsigned int device,
		       char *buffer, unsigned int size, u64 offset, int rw)
{
	struct bio *bio;
	struct block_device *bdev = dev->backdev[device]->bdev;
	int bvecs;
	int res;
	int bv;
	void *buf;
	struct page *page;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,14,0)
	struct bvec_iter iter;
	struct bio_vec bvec;
        unsigned bytes;
#else
	struct bio_vec *bvec;
#endif

	bvecs = size >> PAGE_SHIFT;
	bio = bio_alloc(GFP_NOIO, bvecs);
	if (!bio) {
		tiererror(dev, "bio_alloc failed from tier_bio_io\n");
		return -EIO;
	}
	bio->bi_bdev = bdev;
	bio->bi_rw = rw;
	bio->bi_vcnt = bvecs;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,14,0)
	bio->bi_iter.bi_sector = offset >> 9;
	bio->bi_iter.bi_size = size;
	bio->bi_iter.bi_idx = 0;
        bio->bi_iter.bi_bvec_done = 0;
#else
	bio->bi_sector = offset >> 9;
	bio->bi_size = size;
	bio->bi_idx = 0;
#endif
	for (bv = 0; bv < bvecs; bv++) {
                page = alloc_page(GFP_NOIO);
                if (!page) {
                        tiererror(dev, "tier_bio_io : alloc_page failed\n");
                        return -EIO;
                }
                if (rw == WRITE) {
                        buf = kmap(page);
                        memcpy(buf, &buffer[PAGE_SIZE * bv], PAGE_SIZE);
                        kunmap(page);
                }
		bio->bi_io_vec[bv].bv_len = PAGE_SIZE;
		bio->bi_io_vec[bv].bv_offset = 0;
		bio->bi_io_vec[bv].bv_page = page;
        }
        bio_get(bio);
	res = submit_bio_wait(rw, bio);

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,14,0)
        bio->bi_iter.bi_sector = offset >> 9;
        bio->bi_iter.bi_size = size;
        bio->bi_iter.bi_idx = 0;
        iter = bio->bi_iter;
        bytes = 0;
	bio_for_each_segment(bvec, bio, iter) {
                if (rw == READ) {
                        buf = kmap_atomic(bvec.bv_page);
			memcpy(&buffer[bytes], buf, bvec.bv_len);
                        kunmap_atomic(buf);
                }
                bytes += bvec.bv_len;
                __free_page(bvec.bv_page);
        }
#else
	bv = 0;
	bio->bi_idx = 0;
	bio_for_each_segment(bvec, bio, bv) {
		if (rw == READ) {
			buf = kmap(bvec->bv_page);
			memcpy(&buffer[PAGE_SIZE * bv], buf, bvec->bv_len);
			kunmap(bvec->bv_page);
		}
		__free_page(bvec->bv_page);
	}
#endif
	bio_put(bio);
	if (res) {
		tiererror(dev, "tier_bio_io : read/write failed\n");
		return -EIO;
	}
	return res;
}

static int get_chunksize(struct block_device *bdev)
{
        struct request_queue *q = bdev_get_queue(bdev);
        unsigned int chunksize;
        unsigned int max_hwsectors_kb;

        max_hwsectors_kb = queue_max_hw_sectors(q);
        chunksize = max_hwsectors_kb << 9;
        if ( chunksize < PAGE_SIZE )
            chunksize = PAGE_SIZE;
        if ( chunksize > BLKSIZE )
            chunksize = BLKSIZE;
        return chunksize;
}

static int tier_bio_io_paged(struct tier_device *dev, unsigned int device,
			     char *buffer, unsigned int size, u64 offset,
			     int rw)
{
	unsigned int done;
	struct block_device *bdev = dev->backdev[device]->bdev;
	int res = 0;
        unsigned int chunksize;

        chunksize = get_chunksize(bdev);
	for (done = 0; done < size; done += chunksize) {
                if (chunksize < (size - done)) 
		    res =
		        tier_bio_io(dev, device, buffer + done, chunksize,
		    		offset + done, rw);
                else
		    res =
		        tier_bio_io(dev, device, buffer + done, size - done,
		    		offset + done, rw);
		if (res)
			break;
	}
	return res;
}

static int write_tiered(void *data, unsigned int len,
			u64 offset, struct bio_vec *bvec,
			struct bio_task *bio_task)
{
	struct blockinfo *binfo;
	u64 blocknr;
	unsigned int block_offset;
	int res = 0;
	unsigned int size = 0;
	unsigned int done = 0;
	u64 curoff;
	unsigned int device;
        unsigned int chunksize=PAGE_SIZE;
        struct tier_device *dev = bio_task->dev;

	if (dev->iotype == RANDOM)
		dev->stats.rand_writes++;
	else
		dev->stats.seq_writes++;
	while (done < len) {
		curoff = offset + done;
		blocknr = curoff >> BLKBITS;
		block_offset = curoff - (blocknr << BLKBITS);
		set_debug_info(dev, PREBINFO);
		binfo = get_blockinfo(dev, blocknr, TIERWRITE);
		clear_debug_info(dev, PREBINFO);
		if (dev->inerror) {
			res = -EIO;
			break;
		}
		if (len - done + block_offset > BLKSIZE) {
			size = BLKSIZE - block_offset;
		} else
			size = len - done;
		if (0 == binfo->device) {
			set_debug_info(dev, PREALLOCBLOCK);
			res = allocate_block(dev, blocknr, binfo);
			clear_debug_info(dev, PREALLOCBLOCK);
			if (res != 0) {
				pr_crit("Failed to allocate_block\n");
				return res;
			}
		}
		device = binfo->device - 1;
		if (dev->backdev[device]->bdev && dev->use_bio == USE_BIO) {
                        if(done == 0) {
                             chunksize = get_chunksize(dev->backdev[device]->bdev);
                             if ( bio_task->parent_bio->bi_size <= chunksize &&
                                 block_offset + bio_task->parent_bio->bi_size <= BLKSIZE )
                                 bio_task->in_one = 1;
                        }
			res =
			    tier_write_page(device, bvec,
					    binfo->offset + block_offset,
					    bio_task);
		} else {
			bio_task->vfs = 1;
			res =
			    tier_file_write(dev, device,
					    data + done, size,
					    binfo->offset + block_offset);
		}
		done += size;
		if (res != 0 || bio_task->in_one)
			break;
	}
	return res;
}

/**
 * tier_file_read - helper for reading data
 */
static int tier_file_read(struct tier_device *dev, unsigned int device,
			  void *buf, const int len, loff_t pos)
{
	struct backing_device *backdev = dev->backdev[device];
	struct file *file;
	ssize_t bw;
	mm_segment_t old_fs = get_fs();

	file = backdev->fds;
	/* Disable readahead on random IO */
	if (dev->iotype == RANDOM)
		file->f_ra.ra_pages = 0;
	set_debug_info(dev, VFSREAD);
	set_fs(get_ds());
	bw = vfs_read(file, buf, len, &pos);
	set_fs(old_fs);
	clear_debug_info(dev, VFSREAD);
	file->f_ra.ra_pages = backdev->ra_pages;
	if (likely(bw == len))
		return 0;
	pr_err("Read error at byte offset %llu, length %i.\n",
	       (unsigned long long)pos, len);
	if (bw >= 0)
		bw = -EIO;
	return bw;
}

static int tier_sync(struct tier_device *dev)
{
	int ret = 0;
	int i;
	set_debug_info(dev, PRESYNC);
	for (i = 0; i < dev->attached_devices; i++) {
		if (dev->backdev[i]->dirty) {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,0,0)
			ret = vfs_fsync(dev->backdev[i]->fds, 0);
#else
			ret =
			    vfs_fsync(dev->backdev[i]->fds,
				      dev->backdev[i]->fds->f_path.dentry, 0);
#endif
			if (ret != 0)
				break;
			dev->backdev[i]->dirty = 0;
		}
	}
	clear_debug_info(dev, PRESYNC);
	return ret;
}

void *as_sprintf(const char *fmt, ...)
{
	/* Guess we need no more than 100 bytes. */
	int n, size = 100;
	void *p;
	va_list ap;
	p = kmalloc(size, GFP_ATOMIC);
	if (!p) {
		pr_err("as_sprintf : alloc failed\n");
		return NULL;
	}
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
		p = krealloc(p, size, GFP_ATOMIC);
	}
}

static void tiererror(struct tier_device *dev, char *msg)
{
	dev->inerror = 1;
	pr_crit("tiererror : %s\n", msg);
}

/* Delayed metadata update routine */
static void update_blocklist(struct tier_device *dev, u64 blocknr,
			     struct blockinfo *binfo)
{
	struct blockinfo *odinfo;
	int res;

	if (dev->inerror)
		return;
	odinfo = kzalloc(sizeof(struct blockinfo), GFP_NOFS);
	if (!odinfo) {
		tiererror(dev, "kzalloc failed");
		return;
	}
	res = tier_file_read(dev, 0,
			     odinfo, sizeof(*odinfo),
			     dev->backdev[0]->startofblocklist +
			     (blocknr * sizeof(*odinfo)));
	if (res != 0)
		tiererror(dev, "tier_file_read : returned an error");
	if (0 != memcmp(binfo, odinfo, sizeof(*odinfo))) {
		(void)write_blocklist(dev, blocknr, binfo, WD);
	}
	kfree(odinfo);
}

/* Check for corruption */
static int binfo_sanity(struct tier_device *dev, struct blockinfo *binfo)
{
	struct backing_device *backdev = dev->backdev[binfo->device - 1];
	if (binfo->device > dev->attached_devices) {
		pr_info
		    ("Metadata corruption detected : device %u, dev->attached_devices %u\n",
		     binfo->device, dev->attached_devices);
		tiererror(dev,
			  "get_blockinfo : binfo->device > dev->attached_devices");
		return 0;
	}

	if (binfo->offset > backdev->devicesize) {
		pr_info
		    ("Metadata corruption detected : device %u, offset %llu, devsize %llu\n",
		     binfo->device, binfo->offset, backdev->devicesize);
		tiererror(dev, "get_blockinfo : offset exceeds device size");
		return 0;
	}
	return 1;
}

/*  Read the metadata of the blocknr specified
 *  When a blocknr is not yet allocated binfo->device is 0
 *  otherwhise > 0 
 *  Metadata statistics are updated when called with 
 *  TIERREAD or TIERWRITE (updatemeta != 0 )
 */
struct blockinfo *get_blockinfo(struct tier_device *dev, u64 blocknr,
				int updatemeta)
{
/* The blocklist starts at the end of the bitlist on device1 */
	struct blockinfo *binfo;
	struct backing_device *backdev = dev->backdev[0];

	if (dev->inerror)
		return NULL;
/* random reads are multithreaded, so lock the blocklist cache up-on modification*/
	spin_lock_irq(&dev->statlock);
	binfo = backdev->blocklist[blocknr];
	if (0 != binfo->device) {
		if (!binfo_sanity(dev, binfo)) {
			binfo = NULL;
			goto err_ret;
		}
		backdev = dev->backdev[binfo->device - 1];
/* update accesstime and hitcount */
		if (updatemeta > 0) {
			if (updatemeta == TIERREAD) {
				if (binfo->readcount < MAX_STAT_COUNT) {
					binfo->readcount++;
					backdev->devmagic->total_reads++;
				}
			} else {
				if (binfo->writecount < MAX_STAT_COUNT) {
					binfo->writecount++;
					backdev->devmagic->total_writes++;
				}
			}
			binfo->lastused = get_seconds();
			(void)write_blocklist(dev, blocknr, binfo, WC);
		}
	}
err_ret:
	spin_unlock_irq(&dev->statlock);
	return binfo;
}

/* When write_blocklist is called with write_policy set to
 * WD(isk) the data is written to disk without updating the cache
 * WC(ache) only updates the cache. This is used for statistics only
 * since this data is not critical.
 * WA(ll) writes to all, cache and disk.
 */
static int write_blocklist(struct tier_device *dev, u64 blocknr,
			   struct blockinfo *binfo, int write_policy)
{
	int ret = 0;
	struct backing_device *backdev = dev->backdev[0];
	u64 blocklist_offset = backdev->startofblocklist;

	blocklist_offset += (blocknr * sizeof(struct blockinfo));
	binfo->lastused = get_seconds();
	if (write_policy != WD) {
		memcpy(backdev->blocklist[blocknr], binfo,
		       sizeof(struct blockinfo));
	}
	if (write_policy != WC) {
		ret =
		    tier_file_write(dev, 0, binfo,
				    sizeof(*binfo), blocklist_offset);
		if (ret != 0) {
			pr_crit("write_blocklist failed to write blockinfo\n");
			return ret;
		}
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,0,0)
		ret =
		    vfs_fsync_range(backdev->fds, blocklist_offset,
				    blocklist_offset + sizeof(*binfo), FSMODE);
#else
		ret = vfs_fsync_range(backdev->fds, backdev->fds->f_path.dentry,
				      blocklist_offset,
				      blocklist_offset + sizeof(*binfo),
				      FSMODE);
#endif
	}
	return ret;
}

static void sync_device(struct tier_device *dev, int device)
{
	struct backing_device *backdev = dev->backdev[device];
	if (backdev->dirty) {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,0,0)
		vfs_fsync(backdev->fds, 0);
#else
		vfs_fsync(backdev->fds, backdev->fds->f_path.dentry, 0);
#endif
		backdev->dirty = 0;
	}
}

static void write_blocklist_journal(struct tier_device *dev, u64 blocknr,
				    struct blockinfo *newdevice,
				    struct blockinfo *olddevice)
{
	struct backing_device *oldbackdev = dev->backdev[olddevice->device - 1];
	struct devicemagic *olddev_magic = oldbackdev->devmagic;
	memcpy(&olddev_magic->binfo_journal_old, olddevice,
	       sizeof(struct blockinfo));
	memcpy(&olddev_magic->binfo_journal_new, newdevice,
	       sizeof(struct blockinfo));
	olddev_magic->blocknr_journal = blocknr;
	tier_file_write(dev, olddevice->device - 1,
			oldbackdev->devmagic, sizeof(struct devicemagic), 0);
	sync_device(dev, olddevice->device - 1);
}

static void clean_blocklist_journal(struct tier_device *dev, int device)
{
	struct devicemagic *devmagic = dev->backdev[device]->devmagic;

	memset(&devmagic->binfo_journal_old, 0, sizeof(struct blockinfo));
	memset(&devmagic->binfo_journal_new, 0, sizeof(struct blockinfo));
	devmagic->blocknr_journal = 0;
	tier_file_write(dev, device, devmagic, sizeof(*devmagic), 0);
	sync_device(dev, device);
}

static void recover_journal(struct tier_device *dev, int device)
{
	u64 blocknr;
	struct backing_device *backdev = dev->backdev[device];
	struct devicemagic *devmagic = backdev->devmagic;

	tier_file_read(dev, device, devmagic, sizeof(*devmagic), 0);
	if (0 == devmagic->binfo_journal_old.device) {
		pr_info
		    ("recover_journal : journal is clean, no need to recover\n");
		return;
	}
	blocknr = devmagic->blocknr_journal;
	write_blocklist(dev, blocknr, &devmagic->binfo_journal_old, WD);
	if (0 != devmagic->binfo_journal_new.device)
		clear_dev_list(dev, &devmagic->binfo_journal_new);
	clean_blocklist_journal(dev, device);
	pr_info
	    ("recover_journal : recovered pending migration of blocknr %llu\n",
	     blocknr);
}

/*
 * Grab first pending buffer
 */
static struct bio *tier_get_bio(struct tier_device *dev)
{
	return bio_list_pop(&dev->tier_bio_list);
}

static void determine_iotype(struct tier_device *dev, u64 blocknr)
{
	int ioswitch = 0;
	if (blocknr >= dev->lastblocknr && blocknr <= dev->lastblocknr + 1) {
		ioswitch = 1;
	}
	if (ioswitch && dev->insequence < 10)
		dev->insequence++;
	else {
		if (dev->insequence > 0)
			dev->insequence--;
	}
	if (dev->insequence > 5) {
		dev->iotype = SEQUENTIAL;
	} else {
		dev->iotype = RANDOM;
	}
	dev->lastblocknr = blocknr;
}

static void discard_on_real_device(struct tier_device *dev,
				   struct blockinfo *binfo)
{
	struct block_device *bdev;
	sector_t sector, nr_sects, endsector;
	u64 endoffset;
	unsigned int sector_size;
	u64 devsectors;
	unsigned long flags = 0;
	struct request_queue *dq;
	struct backing_device *backdev = dev->backdev[binfo->device - 1];
	int ret;

	bdev = backdev->bdev;
	if (!bdev) {
		pr_debug("No bdev for device %u\n", binfo->device - 1);
		return;
	}

	if (!dev->discard_to_devices || !dev->discard)
		return;

/* Check if this device supports discard
 * return when it does not
 */
	dq = bdev_get_queue(bdev);
	if (blk_queue_discard(dq)) {
		sector_size = bdev_logical_block_size(bdev);
		devsectors = get_capacity(bdev->bd_disk);

		sector = binfo->offset / sector_size;
		if (sector * sector_size < binfo->offset)
			sector++;

		endoffset = binfo->offset + BLKSIZE;
		endsector = endoffset / sector_size;
		nr_sects = endsector - sector;
		ret =
		    blkdev_issue_discard(bdev, sector, nr_sects, GFP_NOFS,
					 flags);
		if (0 == ret)
			pr_debug
			    ("discarded : device %s : sector %llu, nrsects %llu,sectorsize %u\n",
			     backdev->devmagic->fullpathname,
			     (unsigned long long)sector,
			     (unsigned long long)nr_sects, sector_size);
	}
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,0,0)
/* Reset blockinfo for this block to unused and clear the
   bitlist for this block
*/
static void tier_discard(struct tier_device *dev, u64 offset, unsigned int size)
{
	struct blockinfo *binfo;
	u64 blocknr;
	u64 lastblocknr;
	u64 curoff;
	u64 start;

	if (!dev->discard)
		return;
	curoff = offset + size;
	lastblocknr = curoff >> BLKBITS;
	start = offset >> BLKBITS;
	/* Make sure we don't discard a block while a part of it is still inuse */
	if ((start << BLKBITS) < offset)
		start++;
	if ((start << BLKBITS) > (offset + size))
		return;

	for (blocknr = start; blocknr < lastblocknr; blocknr++) {
		binfo = get_blockinfo(dev, blocknr, 0);
		if (dev->inerror) {
			break;
		}
		if (binfo->device != 0) {
			pr_debug
			    ("really discard blocknr %llu at offset %llu size %u\n",
			     blocknr, offset, size);
			clear_dev_list(dev, binfo);
			reset_counters_on_migration(dev, binfo);
			discard_on_real_device(dev, binfo);
			memset(binfo, 0, sizeof(struct blockinfo));
			write_blocklist(dev, blocknr, binfo, WA);
		}
	}
}
#endif

static int tier_do_bio(struct bio_task *bio_task)
{
	loff_t offset;
	int ret = 0;
	u64 blocknr = 0;
	char *buffer;
        struct tier_device *dev = bio_task->dev;
	struct bio *bio = bio_task->parent_bio;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,14,0)
	struct bio_vec bvec;
	struct bvec_iter iter;
#else
	struct bio_vec *bvec;
	int i;
#endif

#if LINUX_VERSION_CODE <= KERNEL_VERSION(3,0,0)
	const u64 do_sync = (bio->bi_rw & WRITE_SYNC);
#else
	const u64 do_sync = (bio->bi_rw & REQ_SYNC);
#endif

	atomic_set(&dev->wqlock, NORMAL_IO);
	mutex_lock(&dev->qlock);

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,14,0)
	offset = ((loff_t) bio->bi_iter.bi_sector << 9);
#else
	offset = ((loff_t) bio->bi_sector << 9);
#endif
	blocknr = offset >> BLKBITS;

	if (bio_rw(bio) == WRITE) {
#if LINUX_VERSION_CODE <= KERNEL_VERSION(3,0,0)
		if (bio_rw_flagged(bio, BIO_RW_BARRIER)) {
#else
		if (bio->bi_rw & REQ_FLUSH) {
#endif
			if (dev->barrier) {
				ret = tier_sync(dev);
				if (unlikely(ret && ret != -EINVAL)) {
					ret = -EIO;
					goto out;
				}
			}
		}
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,0,0)
		if (bio->bi_rw & REQ_DISCARD) {
			set_debug_info(dev, DISCARD);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,14,0)
			pr_debug("Got a discard request offset %llu len %u\n",
				 offset, bio->bi_iter.bi_size);
			tier_discard(dev, offset, bio->bi_iter.bi_size);
#else
			pr_debug("Got a discard request offset %llu len %u\n",
				 offset, bio->bi_size);
			tier_discard(dev, offset, bio->bi_size);
#endif
			set_debug_info(dev, DISCARD);
		}
#endif
	}
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,14,0)
	bio_for_each_segment(bvec, bio, iter) {
		determine_iotype(dev, blocknr);
		atomic_inc(&bio_task->pending);
		if (bio_rw(bio) == WRITE) {
			buffer = kmap(bvec.bv_page);
			ret =
			    write_tiered(buffer + bvec.bv_offset,
					 bvec.bv_len, offset, &bvec, bio_task);
			kunmap(bvec.bv_page);
		} else {
			buffer = kmap(bvec.bv_page);
			ret = read_tiered(buffer + bvec.bv_offset,
					  bvec.bv_len, offset, &bvec, bio_task);
		}
		if (ret < 0)
			break;
		offset += bvec.bv_len;
		blocknr = offset >> BLKBITS;
                if (bio_task->in_one)
                    break;
	}
#else
	bio_for_each_segment(bvec, bio, i) {
		determine_iotype(dev, blocknr);
		atomic_inc(&bio_task->pending);
		if (bio_rw(bio) == WRITE) {
			buffer = kmap(bvec->bv_page);
			ret =
			    write_tiered(buffer + bvec->bv_offset,
					 bvec->bv_len, offset, bvec, bio_task);
			kunmap(bvec->bv_page);
		} else {
			buffer = kmap(bvec->bv_page);
			ret = read_tiered(buffer + bvec->bv_offset,
					  bvec->bv_len, offset, bvec, bio_task);
		}
		if (ret < 0)
			break;
		offset += bvec->bv_len;
		blocknr = offset >> BLKBITS;
                if (bio_task->in_one)
                    break;
	}
#endif
	if (bio_rw(bio) == WRITE) {
#if LINUX_VERSION_CODE <= KERNEL_VERSION(3,0,0)
		if (bio_rw_flagged(bio, BIO_RW_BARRIER)) {
#else
		if (bio->bi_rw & REQ_FUA) {
#endif
			if (dev->barrier) {
				ret = tier_sync(dev);
				if (unlikely(ret && ret != -EINVAL))
					ret = -EIO;
			}
		}
		if (do_sync && dev->ptsync) {
			ret = tier_sync(dev);
			if (unlikely(ret && ret != -EINVAL))
				ret = -EIO;
		}
	}

	if (atomic_dec_and_test(&bio_task->pending)) {
		if (dev->inerror) {
			bio_endio(bio_task->parent_bio, -EIO);
		} else
			bio_endio(bio_task->parent_bio, 0);
	}
out:
	atomic_set(&dev->wqlock, 0);
	mutex_unlock(&dev->qlock);
	return ret;
}

static inline void tier_handle_bio(struct bio_task *bio_task)
{
	int ret;
        struct tier_device *dev = bio_task->dev;
	ret = tier_do_bio(bio_task);
	if (ret != 0)
		dev->inerror = 1;
}

static inline void tier_wait_bio(struct bio_task *bio_task)
{
	struct tier_device *dev = bio_task->dev;
	if (0 != atomic_read(&dev->aio_pending)) {
		set_debug_info(dev, WAITAIOPENDING);
		wait_event(dev->aio_event, 0 == atomic_read(&dev->aio_pending));
		clear_debug_info(dev, WAITAIOPENDING);
	}
	if (bio_task->vfs) {
		if (dev->inerror)
			bio_endio(bio_task->parent_bio, -EIO);
		else
			bio_endio(bio_task->parent_bio, 0);
	}
}

static void reset_counters_on_migration(struct tier_device *dev,
					struct blockinfo *binfo)
{
	struct backing_device *backdev = dev->backdev[binfo->device - 1];
	struct devicemagic *devmagic = backdev->devmagic;
	u64 devblocks = backdev->devicesize >> BLKBITS;

	if (dev->migrate_verbose) {
		pr_info("block %u-%llu reads %u writes %u\n", binfo->device,
			binfo->offset, binfo->readcount, binfo->writecount);
		pr_info("devmagic->total_writes was %llu\n",
			backdev->devmagic->total_writes);
		pr_info("devmagic->total_reads was %llu\n",
			backdev->devmagic->total_reads);
	}
	devmagic->total_reads -= binfo->readcount;
	devmagic->total_writes -= binfo->writecount;
	devmagic->average_writes = devmagic->total_writes / devblocks;
	devmagic->average_reads = devmagic->total_reads / devblocks;
	if (dev->migrate_verbose) {
		pr_info("devmagic->total_writes is now %llu\n",
			devmagic->total_writes);
		pr_info("devmagic->total_reads is now %llu\n",
			devmagic->total_reads);
	}
	return;
}

/* When a block is migrated to a different tier
 * the readcount and writecount are reset to 0.
 * The block now has hit_collecttime seconds to
 * collect enough hits. After which it is compared
 * to the average hits that blocks have had on this
 * device. Should the block score less then average
 * hits - hysteresis then it will be migrated to an 
 * even lower tier.

 * Although reads and writes are counted seperately
 * for now they are threated equally.

 * We can in the future differentiate between SLC
 * and MLC SSD's and store chunks with high read and
 * low write frequency on MLC SSD. And chunks that
 * are often re-written on SLC SSD.
 */
static int copyblock(struct tier_device *dev, struct blockinfo *newdevice,
		     struct blockinfo *olddevice, u64 curblock)
{
	int devicenr = newdevice->device - 1;
	char *buffer;
	int res;

	newdevice->device = 0;
/* reset readcount and writecount up-on migration
   to another tier */
	newdevice->readcount = 0;
	newdevice->writecount = 0;
	newdevice->lastused = get_seconds();
	if (newdevice->device == olddevice->device) {
		pr_err
		    ("copyblock : refuse to migrate block to current device %u -> %u\n",
		     newdevice->device, olddevice->device);
		return 0;
	}
	allocate_dev(dev, curblock, newdevice, devicenr);
/* No space on the device to copy to is not an error */
	if (0 == newdevice->device)
		return 0;
	buffer = vzalloc(BLKSIZE);
	if (!buffer) {
		pr_err("copyblock: vzalloc failed, cancel operation\n");
		return 0;
	}

	if (dev->backdev[olddevice->device - 1]->bdev
	    && dev->use_bio == USE_BIO)
		res =
		    tier_bio_io_paged(dev, olddevice->device - 1, buffer,
				      BLKSIZE, olddevice->offset, READ);
	else
		res = tier_file_read(dev, olddevice->device - 1,
				     buffer, BLKSIZE, olddevice->offset);
	if (res != 0)
		goto end_error;

	if (dev->backdev[newdevice->device - 1]->bdev
	    && dev->use_bio == USE_BIO)
		res =
		    tier_bio_io_paged(dev, newdevice->device - 1, buffer,
				      BLKSIZE, newdevice->offset, WRITE);
	else
		res = tier_file_write(dev, newdevice->device - 1,
				      buffer, BLKSIZE, newdevice->offset);
	if (res != 0)
		goto end_error;

	write_blocklist_journal(dev, curblock, newdevice, olddevice);
	write_blocklist(dev, curblock, newdevice, WA);
	sync_device(dev, newdevice->device - 1);
	clean_blocklist_journal(dev, olddevice->device - 1);
	vfree(buffer);
	if (dev->migrate_verbose)
		pr_info
		    ("migrated blocknr %llu from device %u-%llu to device %u-%llu\n",
		     curblock, olddevice->device - 1, olddevice->offset,
		     newdevice->device - 1, newdevice->offset);
	return 1;

end_error:
	pr_err("copyblock: read failed, cancelling operation\n");
	vfree(buffer);
	return 0;
}

static int migrate_up_ifneeded(struct tier_device *dev, struct blockinfo *binfo,
			       u64 curblock)
{
	int res = 0;
	struct blockinfo *orgbinfo;
	u64 hitcount = 0;
	u64 avghitcount = 0;
	u64 avghitcountnexttier = 0;
	u64 hysteresis;
	struct devicemagic *dmagic;

	if (!binfo)
		return res;
	if (binfo->device <= 1)	/* already on tier0 */
		return res;

	orgbinfo = kzalloc(sizeof(struct blockinfo), GFP_NOFS);
	if (!orgbinfo) {
		tiererror(dev, "alloc failed");
		return -ENOMEM;
	}
	memcpy(orgbinfo, binfo, sizeof(struct blockinfo));

	hitcount = binfo->readcount + binfo->writecount;
	dmagic = dev->backdev[binfo->device - 1]->devmagic;
	avghitcount = dmagic->average_reads + dmagic->average_writes;
	if (hitcount > avghitcount + (avghitcount / dev->attached_devices)) {
		if (binfo->device > 1) {
			dmagic = dev->backdev[binfo->device - 2]->devmagic;
			avghitcountnexttier =
			    dmagic->average_reads + dmagic->average_writes;
/* Hard coded hysteresis, maybe change this later
 * so that it can be adjusted via sysfs
 * Migrate up when the chunk is used more frequently then 
 * the chunks of the higher tier - hysteresis 
 */
			hysteresis =
			    avghitcountnexttier / dev->attached_devices;
			if (hitcount > avghitcountnexttier - hysteresis)
				binfo->device--;
		}
	}
	if (orgbinfo->device != binfo->device) {
		res = copyblock(dev, binfo, orgbinfo, curblock);
		if (res) {
			reset_counters_on_migration(dev, orgbinfo);
			clear_dev_list(dev, orgbinfo);
			discard_on_real_device(dev, orgbinfo);
		} else {
			/* copyblock failed, restore the old settings */
			memcpy(binfo, orgbinfo, sizeof(struct blockinfo));
		}
	}
	kfree(orgbinfo);
	return res;
}

static int migrate_down_ifneeded(struct tier_device *dev,
				 struct blockinfo *binfo, u64 curblock)
{
	int res = 0;
	time_t curseconds = get_seconds();
	struct blockinfo *orgbinfo;
	u64 hitcount = 0;
	u64 avghitcount = 0;
	u64 hysteresis;
	struct backing_device *backdev = dev->backdev[binfo->device - 1];
	struct devicemagic *dmagic = backdev->devmagic;

	if (binfo->device == 0)
		return res;

	orgbinfo = kzalloc(sizeof(struct blockinfo), GFP_NOFS);
	if (!orgbinfo) {
		tiererror(dev, "alloc failed");
		return -ENOMEM;
	}
	memcpy(orgbinfo, binfo, sizeof(struct blockinfo));

	hitcount = binfo->readcount + binfo->writecount;
	avghitcount = dmagic->average_reads + dmagic->average_writes;
	/* Check if the block has been unused long enough that it may
	 * be moved to a lower tier
	 */
	hysteresis = avghitcount / dev->attached_devices;
	if (curseconds - binfo->lastused > backdev->devmagic->dtapolicy.max_age)
		binfo->device++;
	else if (hitcount < avghitcount - hysteresis
		 && curseconds - binfo->lastused >
		 backdev->devmagic->dtapolicy.hit_collecttime)
		if (binfo->device < dev->attached_devices - 1)
			binfo->device++;
	if (binfo->device > dev->attached_devices) {
		binfo->device = orgbinfo->device;
	} else if (orgbinfo->device != binfo->device) {
		res = copyblock(dev, binfo, orgbinfo, curblock);
		if (res) {
			reset_counters_on_migration(dev, orgbinfo);
			clear_dev_list(dev, orgbinfo);
			discard_on_real_device(dev, orgbinfo);
		} else {
			/* copyblock failed, restore the old settings */
			memcpy(binfo, orgbinfo, sizeof(struct blockinfo));
		}
	}
	kfree(orgbinfo);
	return res;
}

int migrate_direct(struct tier_device *dev, u64 blocknr, int device)
{
	if (NORMAL_IO == atomic_read(&dev->wqlock))
		return -EAGAIN;
	if (0 == atomic_add_unless(&dev->mgdirect.direct, 1, 1))
		return -EAGAIN;
	dev->mgdirect.blocknr = blocknr;
	dev->mgdirect.newdevice = device;
	wake_up(&dev->migrate_event);
	return 0;
}

int load_bitlists(struct tier_device *dev)
{
	int device;
	u64 cur;
	struct backing_device *backdev;
	int res = 0;

	for (device = 0; device < dev->attached_devices; device++) {
		backdev = dev->backdev[device];
		backdev->bitlist = vzalloc(backdev->bitlistsize);
		if (!backdev->bitlist) {
			pr_info
			    ("Failed to allocate memory to load bitlist %u in memory\n",
			     device);
			res = -ENOMEM;
			break;
		}
		for (cur = 0; cur < backdev->bitlistsize; cur += PAGE_SIZE) {
			tier_file_read(dev, device,
				       &backdev->bitlist[cur],
				       PAGE_SIZE,
				       backdev->startofbitlist + cur);
		}
	}
	return res;
}

void free_bitlists(struct tier_device *dev)
{
	int device;

	for (device = 0; device < dev->attached_devices; device++) {
		if (dev->backdev[device]->bitlist) {
			vfree(dev->backdev[device]->bitlist);
			dev->backdev[device]->bitlist = NULL;
		}
	}
}

static int load_blocklist(struct tier_device *dev)
{
	int alloc_failed = 0;
	u64 curblock;
	u64 blocks = dev->size >> BLKBITS;
	u64 listentries = dev->blocklistsize / sizeof(struct blockinfo);
	struct backing_device *backdev = dev->backdev[0];
	int res = 0;

	backdev->blocklist = vzalloc(sizeof(struct blockinfo *) * listentries);
	if (!dev->backdev[0]->blocklist)
		return -ENOMEM;
	for (curblock = 0; curblock < blocks; curblock++) {
		backdev->blocklist[curblock] =
		    kzalloc(sizeof(struct blockinfo), GFP_KERNEL);
		if (!backdev->blocklist[curblock]) {
			alloc_failed = 1;
			break;
		}
		res = tier_file_read(dev, 0,
				     backdev->blocklist[curblock],
				     sizeof(struct blockinfo),
				     backdev->startofblocklist +
				     (curblock * sizeof(struct blockinfo)));
		if (res != 0)
			tiererror(dev, "tier_file_read : returned an error");
	}
	if (alloc_failed) {
		res = -ENOMEM;
		free_blocklist(dev);
	}
	return res;
}

static void free_blocklist(struct tier_device *dev)
{
	u64 curblock;
	u64 blocks = dev->size >> BLKBITS;
	struct backing_device *backdev = dev->backdev[0];
	if (!backdev->blocklist)
		return;
	for (curblock = 0; curblock < blocks; curblock++) {
		if (backdev->blocklist[curblock]) {
			update_blocklist(dev, curblock,
					 backdev->blocklist[curblock]);
			kfree(backdev->blocklist[curblock]);
		}
	}
	vfree(backdev->blocklist);
	backdev->blocklist = NULL;
}

static void walk_blocklist(struct tier_device *dev)
{
	u64 blocks = dev->size >> BLKBITS;
	u64 curblock;
	struct blockinfo *binfo;
	int interrupted = 0;
	int res = 0;
	int mincount = 0;
	u64 devblocks;
	struct backing_device *backdev;
	struct data_policy *dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;

	if (dev->migrate_verbose)
		pr_info("walk_blocklist start from : %llu\n",
			dev->resumeblockwalk);
	for (curblock = dev->resumeblockwalk; curblock < blocks; curblock++) {
		if (dev->stop || dtapolicy->migration_disabled || dev->inerror) {
			pr_info("walk_block_list ends on stop or disabled\n");
			break;
		}
		binfo = get_blockinfo(dev, curblock, 0);
		if (dev->inerror) {
			pr_err("walk_block_list stops, device is inerror\n");
			break;
		}
		if (binfo->device != 0) {
			backdev = dev->backdev[binfo->device - 1];
			devblocks = backdev->devicesize >> BLKBITS;
			backdev->devmagic->average_reads =
			    backdev->devmagic->total_reads / devblocks;
			backdev->devmagic->average_writes =
			    backdev->devmagic->total_writes / devblocks;
			res = migrate_down_ifneeded(dev, binfo, curblock);
			if (!res)
				res = migrate_up_ifneeded(dev, binfo, curblock);
			if (!res) {
				if (binfo->readcount >= MAX_STAT_COUNT) {
					binfo->readcount -= MAX_STAT_DECAY;
					backdev->devmagic->total_reads -=
					    MAX_STAT_DECAY;
					(void)write_blocklist(dev, curblock,
							      binfo, WC);
				}
				if (binfo->writecount >= MAX_STAT_COUNT) {
					binfo->writecount -= MAX_STAT_DECAY;
					backdev->devmagic->total_writes -=
					    MAX_STAT_DECAY;
					(void)write_blocklist(dev, curblock,
							      binfo, WC);
				}
				update_blocklist(dev, curblock, binfo);
			}
		}
		if (NORMAL_IO == atomic_read(&dev->wqlock)) {
			mincount++;
			if (mincount > 5 || res) {
				dev->resumeblockwalk = curblock;
				interrupted = 1;
				if (dev->migrate_verbose)
					pr_info
					    ("walk_block_list interrupted by normal io\n");
				break;
			}
		}
	}
	if (dev->inerror)
		return;
	tier_sync(dev);
	if (!interrupted) {
		dev->resumeblockwalk = 0;
		dev->migrate_timer.expires =
		    jiffies +
		    msecs_to_jiffies(dtapolicy->migration_interval * 1000);
	} else {
		dev->migrate_timer.expires = jiffies + msecs_to_jiffies(3000);
	}
	if (!dev->stop && !dtapolicy->migration_disabled)
		add_timer(&dev->migrate_timer);
}

void do_migrate_direct(struct tier_device *dev)
{
	struct data_policy *dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
	u64 blocknr = dev->mgdirect.blocknr;
	int newdevice = dev->mgdirect.newdevice;
	int res;
	struct blockinfo *binfo, *orgbinfo;

	btier_lock(dev);
	if (!dtapolicy->migration_disabled) {
		dtapolicy->migration_disabled = 1;
		del_timer_sync(&dev->migrate_timer);
		pr_info
		    ("migration is disabled for %s due to user controlled data migration\n",
		     dev->devname);
	}
	if (dev->migrate_verbose)
		pr_info("sysfs request migrate blocknr %llu to %i\n", blocknr,
			newdevice);
	binfo = get_blockinfo(dev, blocknr, 0);
	if (!binfo)
		goto end_error;

	if (binfo->device == newdevice + 1) {
		res = -EEXIST;
		pr_err("Failed to migrate block %llu, already on device %i\n",
		       blocknr, newdevice);
		goto end_error;
	}
	orgbinfo = kzalloc(sizeof(struct blockinfo), GFP_NOFS);
	if (!orgbinfo) {
		tiererror(dev, "alloc failed");
		res = -ENOMEM;
		goto end_error;
	}
	memcpy(orgbinfo, binfo, sizeof(*binfo));
	binfo->device = newdevice + 1;

	res = copyblock(dev, binfo, orgbinfo, blocknr);
	if (res) {
		reset_counters_on_migration(dev, orgbinfo);
		clear_dev_list(dev, orgbinfo);
		discard_on_real_device(dev, orgbinfo);
	} else {
		pr_err("copyblock failed\n");
		memcpy(binfo, orgbinfo, sizeof(struct blockinfo));
	}
	kfree(orgbinfo);
end_error:
	btier_unlock(dev);
}

static void data_migrator(struct work_struct *work)
{
	struct tier_device *dev;
	tier_worker_t *mwork = (tier_worker_t *) work;
	struct data_policy *dtapolicy;

	dev = mwork->device;
	dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
	while (!dev->stop) {
		wait_event_interruptible(dev->migrate_event,
					 1 == atomic_read(&dev->migrate)
					 || dev->stop
					 || 1 ==
					 atomic_read(&dev->mgdirect.direct));
		if (dev->migrate_verbose)
			pr_info("data_migrator woke up\n");
		if (dev->stop)
			break;

		if (1 == atomic_read(&dev->mgdirect.direct)) {
			if (dev->migrate_verbose)
				pr_info("do_migrate_direct\n");
			do_migrate_direct(dev);
			atomic_set(&dev->mgdirect.direct, 0);
			continue;
		}

		if (NORMAL_IO == atomic_read(&dev->wqlock)) {
			if (dev->migrate_verbose)
				pr_info("NORMAL_IO pending: backoff\n");
			dev->migrate_timer.expires =
			    jiffies + msecs_to_jiffies(300);
			if (!dev->stop && !dtapolicy->migration_disabled)
				mod_timer_pinned(&dev->migrate_timer,
						 dev->migrate_timer.expires);
			atomic_set(&dev->migrate, 0);
			continue;
		}
		btier_lock(dev);
		tier_sync(dev);
		walk_blocklist(dev);
		btier_unlock(dev);
		if (dev->migrate_verbose)
			pr_info("data_migrator goes back to sleep\n");
	}
	kfree(work);
	pr_info("data_migrator halted\n");
}

static int tier_thread(void *data)
{
	struct tier_device *dev = data;
	struct bio_task **bio_task;
	int backlog;
	int i;

	set_user_nice(current, -20);
	bio_task =
	    kzalloc(BTIER_MAX_INFLIGHT * sizeof(struct bio_task *), GFP_KERNEL);
	if (!bio_task) {
		tiererror(dev, "tier_thread : alloc failed");
		return -ENOMEM;
	}
	for (i = 0; i < BTIER_MAX_INFLIGHT; i++) {
		bio_task[i] = kzalloc(sizeof(struct bio_task), GFP_KERNEL);
		if (!bio_task[i]) {
			tiererror(dev, "tier_thread : alloc failed");
			return -ENOMEM;
		}
		bio_task[i]->dev = dev;
		atomic_set(&bio_task[i]->pending, 0);
	}
	while (!kthread_should_stop() || !bio_list_empty(&dev->tier_bio_list)) {

		wait_event_interruptible(dev->tier_event,
					 !bio_list_empty(&dev->tier_bio_list) ||
					 kthread_should_stop());
		if (bio_list_empty(&dev->tier_bio_list))
			continue;
		backlog = 0;
		do {
			atomic_set(&bio_task[backlog]->pending, 1);
			spin_lock_irq(&dev->lock);
			bio_task[backlog]->parent_bio = tier_get_bio(dev);
			spin_unlock_irq(&dev->lock);
			BUG_ON(!bio_task[backlog]->parent_bio);
			tier_handle_bio(bio_task[backlog]);
			backlog++;
/* When reading sequential we stay on a single thread and a single filedescriptor */
		} while (!bio_list_empty(&dev->tier_bio_list)
			 && backlog < BTIER_MAX_INFLIGHT);
		if (dev->writethrough)
			tier_sync(dev);
		for (i = 0; i < backlog; i++) {
			tier_wait_bio(bio_task[i]);
                        bio_task[i]->in_one = 0;
			bio_task[i]->vfs = 0;
		}
	}
	for (i = 0; i < BTIER_MAX_INFLIGHT; i++) {
		kfree(bio_task[i]);
	}
	kfree(bio_task);
	pr_info("tier_thread worker halted\n");
	return 0;
}

static void tier_add_bio(struct tier_device *dev, struct bio *bio)
{
	bio_list_add(&dev->tier_bio_list, bio);
}

#if LINUX_VERSION_CODE < KERNEL_VERSION(3,2,0)
static int tier_make_request(struct request_queue *q, struct bio *old_bio)
#else
static void tier_make_request(struct request_queue *q, struct bio *old_bio)
#endif
{
	int cpu;
	struct tier_device *dev = q->queuedata;
	int rw = bio_rw(old_bio);

	if (rw == READA)
		rw = READ;

	BUG_ON(!dev || (rw != READ && rw != WRITE));
	spin_lock_irq(&dev->lock);
	if (!dev->active)
		goto out;
	cpu = part_stat_lock();
	part_stat_inc(cpu, &dev->gd->part0, ios[rw]);
	part_stat_add(cpu, &dev->gd->part0, sectors[rw], bio_sectors(old_bio));
	part_stat_unlock();
	tier_add_bio(dev, old_bio);
	wake_up(&dev->tier_event);
	spin_unlock_irq(&dev->lock);
	goto end_return;

out:
	spin_unlock_irq(&dev->lock);
	bio_io_error(old_bio);

end_return:
#if LINUX_VERSION_CODE < KERNEL_VERSION(3,2,0)
	return 0;
#else
	return;
#endif
}

static int init_devicenames(void)
{
	int i;
/* Allow max 26 devices to be configured */
	devicenames = kmalloc(sizeof(char) * BTIER_MAX_DEVS, GFP_KERNEL);
	if (!devicenames) {
		pr_err("init_devicenames : alloc failed\n");
		return -ENOMEM;
	}
	for (i = 0; i < BTIER_MAX_DEVS; i++) {
		/* sdtiera/b/c/d../z */
		devicenames[i] = 'a' + i;
	}
	return 0;
}

static void release_devicename(char *devicename)
{
	int pos;
	char d;

	if (!devicename)
		return;
	d = devicename[6];	/*sdtierN */
/* Restore the char in devicenames */
	pos = d - 'a';
	devicenames[pos] = d;
	kfree(devicename);
}

static char *reserve_devicename(unsigned int *devnr)
{
	char device;
	char *retname;
	int i;
	for (i = 0; i < BTIER_MAX_DEVS; i++) {
		device = devicenames[i];
		if (device != 0)
			break;
	}
	if (0 == device) {
		pr_err("Maximum number of devices exceeded\n");
		return NULL;
	}
	retname = as_sprintf("sdtier%c", device);
	*devnr = i;
	devicenames[i] = 0;
	return retname;
}

static void migrate_timer_expired(unsigned long q)
{
	struct tier_device *dev = (struct tier_device *)q;

	if (0 == atomic_read(&dev->migrate)) {
		atomic_set(&dev->migrate, 1);
		wake_up(&dev->migrate_event);
	}
}

static void tier_check(struct tier_device *dev, int devicenr)
{
	pr_info("device %s is not clean, check forced\n",
		dev->backdev[devicenr]->fds->f_dentry->d_name.name);
	recover_journal(dev, devicenr);
}

/* Zero out the bitlist starting at offset startofbitlist
   with size bitlistsize */
static void wipe_bitlist(struct tier_device *dev, int device,
			 u64 startofbitlist, u64 bitlistsize)
{
	char *buffer;
	u64 offset = 0;

	buffer = kzalloc(PAGE_SIZE, GFP_KERNEL);
	while (offset < bitlistsize) {
		tier_file_write(dev, device, buffer,
				PAGE_SIZE, startofbitlist + offset);
		offset += PAGE_SIZE;
	}
	if (offset < bitlistsize)
		tier_file_write(dev, device, buffer,
				bitlistsize - offset, startofbitlist + offset);
	kfree(buffer);
}

u64 allocated_on_device(struct tier_device *dev, int device)
{
	u_char *buffer = NULL;
	u64 offset = 0;
	int i;
	u64 allocated = 0;
	int hascache = 0;

	if (dev->backdev[device]->bitlist)
		hascache = 1;
	buffer = kzalloc(PAGE_SIZE, GFP_KERNEL);
	if (!buffer) {
		tiererror(dev, "allocated_on_device : alloc failed");
		return 0 - 1;
	}

	if (!hascache) {
		while (offset < dev->backdev[device]->bitlistsize) {
			tier_file_read(dev, device,
				       buffer, PAGE_SIZE,
				       dev->backdev[device]->startofbitlist +
				       offset);
			offset += PAGE_SIZE;
			for (i = 0; i < PAGE_SIZE; i++) {
				if (buffer[i] == 0xff)
					allocated += BLKSIZE;
			}
		}
		if (offset < dev->backdev[device]->bitlistsize) {
			tier_file_read(dev, device,
				       buffer,
				       dev->backdev[device]->bitlistsize -
				       offset,
				       dev->backdev[device]->startofbitlist +
				       offset);
		}
	} else {
		while (offset < dev->backdev[device]->bitlistsize) {
			memcpy(buffer, &dev->backdev[device]->bitlist[offset],
			       PAGE_SIZE);
			offset += PAGE_SIZE;
			for (i = 0; i < PAGE_SIZE; i++) {
				if (buffer[i] == 0xff)
					allocated += BLKSIZE;
			}
		}
		if (offset < dev->backdev[device]->bitlistsize) {
			memset(buffer, 0, PAGE_SIZE);
			memcpy(buffer, &dev->backdev[device]->bitlist[offset],
			       dev->backdev[device]->bitlistsize - offset);
		}
	}
	for (i = 0; i < dev->backdev[device]->bitlistsize - offset; i++) {
		if (i >= PAGE_SIZE) {
			pr_err
			    ("allocated_on_device : buffer overflow, should never happen\n");
			break;
		}
		if (buffer[i] == 0xff)
			allocated += BLKSIZE;
	}
	kfree(buffer);
	return allocated;
}

static void repair_bitlists(struct tier_device *dev)
{
	u64 blocknr;
	struct blockinfo *binfo;
	u64 relative_offset;
	unsigned int i;

	pr_info("repair_bitlists : clearing and rebuilding bitlists\n");
	for (i = 0; i < dev->attached_devices; i++) {
		wipe_bitlist(dev, i,
			     dev->backdev[i]->startofbitlist,
			     dev->backdev[i]->bitlistsize);
		dev->backdev[i]->free_offset = 0;
	}

	for (blocknr = 0; blocknr < dev->size >> BLKBITS; blocknr++) {
		binfo = get_blockinfo(dev, blocknr, 0);
		if (dev->inerror)
			return;
		if (0 != binfo->device) {
			if (binfo->device > dev->attached_devices) {
				pr_err
				    ("repair_bitlists : cleared corrupted blocklist entry for blocknr %llu\n",
				     blocknr);
				memset(binfo, 0, sizeof(struct blockinfo));
				continue;
			}
			if (BLKSIZE + binfo->offset >
			    dev->backdev[binfo->device - 1]->devicesize) {
				pr_err
				    ("repair_bitlists : cleared corrupted blocklist entry for blocknr %llu\n",
				     blocknr);
				memset(binfo, 0, sizeof(struct blockinfo));
				continue;
			}
			relative_offset =
			    binfo->offset - dev->backdev[binfo->device -
							 1]->startofdata;
			mark_offset_as_used(dev, binfo->device - 1,
					    relative_offset);
			dev->backdev[i]->free_offset =
			    relative_offset >> BLKBITS;
		}
	}
}

char *uuid_hash(char *data, int hashlen)
{
	int n;
	char *ahash = NULL;

	ahash = kzalloc(TIGER_HASH_LEN * 2, GFP_KERNEL);
	if (!ahash)
		return NULL;
	for (n = 0; n < hashlen; n++) {
		sprintf(&ahash[n * 2], "%02X", data[n]);
	}
	return ahash;
}

char *btier_uuid(struct tier_device *dev)
{
	int i, n;
	char *thash;
	int hashlen = TIGER_HASH_LEN;
	const char *name;
	char *xbuf;
	char *asc;

	xbuf = kzalloc(hashlen, GFP_KERNEL);
	if (!xbuf)
		return NULL;
	for (i = 0; i < dev->attached_devices; i++) {
		name = dev->backdev[i]->fds->f_dentry->d_name.name;
		thash = tiger_hash((char *)name, strlen(name));
		if (!thash) {
			/* When tiger is not supported, use a simple UUID construction */
			thash = kzalloc(TIGER_HASH_LEN, GFP_KERNEL);
			memcpy(thash,
			       dev->backdev[i]->fds->f_dentry->d_name.name,
			       hashlen);
		}
		for (n = 0; n < hashlen; n++) {
			xbuf[n] ^= thash[n];
		}
		kfree(thash);
	}
	asc = uuid_hash(xbuf, hashlen);
	kfree(xbuf);
	return asc;
}

/* This is called by bio_add_page().
 * q->max_hw_sectors and other global limits are already enforced there.
 *
 * We need to call down to our lower level device,
 * in case it has special restrictions.
 *
 * We also may need to enforce configured max-bio-bvecs limits.
 *
 * As long as the BIO is empty we have to allow at least one bvec,
 * regardless of size and offset, so no need to ask lower levels.
 */
int btier_merge_bvec(struct request_queue *q, struct bvec_merge_data *bvm, struct bio_vec *bvec)
{
        struct tier_device *dev = (struct tier_device *) q->queuedata;
	struct block_device *bdev;
        unsigned int max_hw_sectors = queue_max_hw_sectors(q);
        int limit = BLKSIZE;
        int i, backing_limit;
        struct request_queue *b;

	for (i = 0; i < dev->attached_devices; i++) {
            if (dev->backdev[i]->bdev) {
                bdev = dev->backdev[i]->bdev;
                b = bdev->bd_disk->queue;
                if (b->merge_bvec_fn) {
                        backing_limit = b->merge_bvec_fn(b, bvm, bvec);
                        limit = min(limit, backing_limit);
                }
                if ((limit >> 9) > max_hw_sectors) 
                        limit = max_hw_sectors << 9;
            }
        }
        return limit;
}

static int order_devices(struct tier_device *dev)
{
	int swap = 0;
	int i;
	int newnr;
	int clean = 1;
	struct backing_device *backdev;
	struct data_policy *dtapolicy;
	static struct devicemagic *devmagic;
	char *zhash, *uuid;
	const char *devicename;
	struct block_device *bdev = NULL;
	int res = -ENOMEM;

	zhash = kzalloc(TIGER_HASH_LEN, GFP_KERNEL);
	if (!zhash) {
		tiererror(dev, "order_devices : alloc failed");
		return res;
	}
	backdev = kzalloc(sizeof(*backdev), GFP_KERNEL);
	if (!backdev) {
		tiererror(dev, "order_devices : alloc failed");
		goto end_error;
	}

/* Allocate and load */
	for (i = 0; i < dev->attached_devices; i++) {
		dev->backdev[i]->devmagic = read_device_magic(dev, i);
		if (!dev->backdev[i]->devmagic) {
			tiererror(dev, "order_devices : alloc failed");
			goto end_error;
		}
		if (i != dev->backdev[i]->devmagic->device)
			swap = 1;
	}

/* Check and swap */
	if (swap) {
		for (i = 0; i < dev->attached_devices; i++) {
			devmagic = read_device_magic(dev, i);
			if (!devmagic) {
				tiererror(dev, "order_devices : alloc failed");
				goto end_error;
			}
			newnr = devmagic->device;
			if (i != newnr) {
				memcpy(backdev, dev->backdev[i],
				       sizeof(struct backing_device));
				memcpy(dev->backdev[i], dev->backdev[newnr],
				       sizeof(struct backing_device));
				memcpy(dev->backdev[newnr], backdev,
				       sizeof(struct backing_device));
			}
			kfree(devmagic);
		}
	}
/* Mark as inuse */
	for (i = 0; i < dev->attached_devices; i++) {
		if (CLEAN != dev->backdev[i]->devmagic->clean) {
			tier_check(dev, i);
			clean = 0;
		}
		uuid = btier_uuid(dev);
		if (0 ==
		    memcmp(dev->backdev[i]->devmagic->uuid, zhash,
			   TIGER_HASH_LEN))
			memcpy(dev->backdev[i]->devmagic->uuid, uuid,
			       TIGER_HASH_LEN);
		if (0 !=
		    memcmp(dev->backdev[i]->devmagic->uuid, uuid,
			   TIGER_HASH_LEN)) {
			tiererror(dev,
				  "order_devices : incorrect device assembly");
			res = -EIO;
			kfree(uuid);
			goto end_error;
		}
		kfree(uuid);
		dev->backdev[i]->devmagic->clean = DIRTY;
		write_device_magic(dev, i);
		dtapolicy = &dev->backdev[i]->devmagic->dtapolicy;
		devicename = dev->backdev[i]->fds->f_dentry->d_name.name;
		pr_info("device %s registered as tier %u\n", devicename, i);
		if (0 == dtapolicy->max_age)
			dtapolicy->max_age = TIERMAXAGE;
		if (0 == dtapolicy->hit_collecttime)
			dtapolicy->hit_collecttime = TIERHITCOLLECTTIME;
		bdev = lookup_bdev(dev->backdev[i]->devmagic->fullpathname);
		if (IS_ERR(bdev)) {
			dev->backdev[i]->bdev = NULL;
			pr_info("device %s is a file\n", devicename);
		} else {
			dev->backdev[i]->bdev = bdev;
			pr_info("device %s is a real device\n", devicename);
		}
	}
	dtapolicy = &dev->backdev[0]->devmagic->dtapolicy;
	if (dtapolicy->sequential_landing >= dev->attached_devices)
		dtapolicy->sequential_landing = 0;
	if (0 == dtapolicy->migration_interval)
		dtapolicy->migration_interval = MIGRATE_INTERVAL;
	if (!dev->writethrough) {
		dev->writethrough = dev->backdev[0]->devmagic->writethrough;
	} else {
		pr_info("write-through (sync) io selected\n");
		dev->backdev[0]->devmagic->writethrough = dev->writethrough;
	}
	if (dev->use_bio == 0)
		dev->use_bio = dev->backdev[0]->devmagic->use_bio;
	if (dev->use_bio == 0)
		dev->use_bio = USE_VFS;
	if (!clean)
		repair_bitlists(dev);
	kfree(backdev);
	kfree(zhash);
	return 0;

end_error:
	if (backdev)
		kfree(backdev);
	if (zhash)
		kfree(zhash);
	return res;
}

static void register_new_device_size(struct tier_device *dev)
{

	dev->nsectors = dev->size / dev->logical_block_size;
	dev->size = dev->nsectors * dev->logical_block_size;
	set_capacity(dev->gd, dev->nsectors * (dev->logical_block_size / 512));
	revalidate_disk(dev->gd);
	/* let user-space know about the new size */
	kobject_uevent(&disk_to_dev(dev->gd)->kobj, KOBJ_CHANGE);
}

static int tier_register(struct tier_device *dev)
{
	int devnr;
	int ret = 0;
	tier_worker_t *migrateworker;
	struct devicemagic *magic = dev->backdev[0]->devmagic;
	struct data_policy *dtapolicy = &magic->dtapolicy;

	dev->devname = reserve_devicename(&devnr);
	if (!dev->devname)
		return -1;
	dev->active = 1;
	dev->barrier = 1;
/* Barriers can not be used when we work in ram only */
	if (0 == dev->logical_block_size)
		dev->logical_block_size = 512;
	if (dev->logical_block_size != 512 &&
	    dev->logical_block_size != 1024 &&
	    dev->logical_block_size != 2048 && dev->logical_block_size != 4096)
		dev->logical_block_size = 512;
	if (dev->logical_block_size == 512)
		dev->nsectors = dev->size >> 9;
	if (dev->logical_block_size == 1024)
		dev->nsectors = dev->size >> 10;
	if (dev->logical_block_size == 2048)
		dev->nsectors = dev->size >> 11;
	if (dev->logical_block_size == 4096)
		dev->nsectors = dev->size >> 12;
	dev->size = dev->nsectors * dev->logical_block_size;
	pr_info("%s size : %llu\n", dev->devname, dev->size);
	spin_lock_init(&dev->lock);
	spin_lock_init(&dev->statlock);
	spin_lock_init(&dev->dbg_lock);
	bio_list_init(&dev->tier_bio_list);
	dev->rqueue = blk_alloc_queue(GFP_KERNEL);
	if (!dev->rqueue) {
		ret = -ENOMEM;
		goto out;
	}
	ret = load_blocklist(dev);
	if (0 != ret)
		goto out;
	ret = load_bitlists(dev);
	if (0 != ret)
		goto out;
	init_waitqueue_head(&dev->tier_event);
	init_waitqueue_head(&dev->migrate_event);
	init_waitqueue_head(&dev->aio_event);
	dev->cacheentries = 0;
	dev->migrate_verbose = 0;
	dev->stop = 0;
	dev->iotype = RANDOM;
	atomic_set(&dev->migrate, 0);
	atomic_set(&dev->commit, 0);
	atomic_set(&dev->wqlock, 0);
	atomic_set(&dev->aio_pending, 0);
	atomic_set(&dev->curfd, 2);
	atomic_set(&dev->mgdirect.direct, 0);
	/*
	 * Get a request queue.
	 */
	mutex_init(&dev->tier_ctl_mutex);
	mutex_init(&dev->qlock);
	/*
	 * set queue make_request_fn, and add limits based on lower level
	 * device
	 */
	blk_queue_make_request(dev->rqueue, tier_make_request);
	dev->rqueue->queuedata = (void *)dev;

	/* Tell the block layer that we are not a rotational device
	   and that we support discard aka trim.
	 */
	blk_queue_logical_block_size(dev->rqueue, dev->logical_block_size);
	blk_queue_io_opt(dev->rqueue, BLKSIZE);
        blk_queue_merge_bvec(dev->rqueue, btier_merge_bvec);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,0,0)
	if (dev->barrier)
		blk_queue_flush(dev->rqueue, REQ_FLUSH | REQ_FUA);
#endif
	/*
	 * Get registered.
	 */
	dev->major_num = register_blkdev(0, dev->devname);
	if (dev->major_num <= 0) {
		pr_warning("tier: unable to get major number\n");
		goto out;
	}
	/*
	 * And the gendisk structure.
	 */

	/* We support 256 (kernel default) partitions */
	dev->gd = alloc_disk(DISK_MAX_PARTS);
	if (!dev->gd)
		goto out_unregister;
	dev->gd->major = dev->major_num;
	dev->gd->first_minor = 0;
	dev->gd->fops = &tier_ops;
	dev->gd->private_data = dev;
	strcpy(dev->gd->disk_name, dev->devname);
	set_capacity(dev->gd, dev->nsectors * (dev->logical_block_size / 512));
	dev->gd->queue = dev->rqueue;
	dev->tier_thread = kthread_create(tier_thread, dev, dev->devname);
	if (IS_ERR(dev->tier_thread)) {
		pr_err("Failed to create kernel thread\n");
		ret = PTR_ERR(dev->tier_thread);
		goto out_unregister;
	}
	wake_up_process(dev->tier_thread);
	migrateworker = kzalloc(sizeof(tier_worker_t), GFP_KERNEL);
	if (!migrateworker) {
		pr_err("Failed to allocate memory for migrateworker\n");
		goto out_unregister;
	}
	migrateworker->device = dev;
	dev->managername = as_sprintf("%s-manager", dev->devname);
	dev->aioname = as_sprintf("%s-aio", dev->devname);
	dev->migration_queue = create_workqueue(dev->managername);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,36)
	dev->aio_queue = alloc_workqueue(dev->aioname, WQ_HIGHPRI, 64);
#else
	dev->aio_queue = create_workqueue(dev->aioname);
#endif
	INIT_WORK((struct work_struct *)migrateworker, data_migrator);
	queue_work(dev->migration_queue, (struct work_struct *)migrateworker);
	init_timer(&dev->migrate_timer);
	dev->migrate_timer.data = (unsigned long)dev;
	dev->migrate_timer.function = migrate_timer_expired;
	dev->migrate_timer.expires =
	    jiffies + msecs_to_jiffies(dtapolicy->migration_interval * 1000);
	add_timer(&dev->migrate_timer);
	add_disk(dev->gd);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,0,0)
	blk_queue_max_discard_sectors(dev->rqueue, get_capacity(dev->gd));
	dev->rqueue->limits.discard_granularity = BLKSIZE;
	dev->rqueue->limits.discard_alignment = BLKSIZE;
#endif
	tier_sysfs_init(dev);
	/* let user-space know about the new size */
	kobject_uevent(&disk_to_dev(dev->gd)->kobj, KOBJ_CHANGE);
#ifdef MAX_PERFORMANCE
	pr_info("MAX_PERFORMANCE IS ENABLED, no internal statistics\n");
#endif
	if (dev->use_bio == USE_VFS)
		pr_info("write mode = vfs to devices and files\n");
	else
		pr_info("write mode = bio to devices and vfs to files\n");
	return ret;

out_unregister:
	unregister_blkdev(dev->major_num, dev->devname);
out:
	return ret;
}

static loff_t tier_get_size(struct file *file)
{
	loff_t size;

	// Compute loopsize in bytes 
	size = i_size_read(file->f_mapping->host);
	// *
	// * Unfortunately, if we want to do I/O on the device,
	// * the number of 512-byte sectors has to fit into a sector_t.
	// *
	return size >> 9;
}

static int tier_set_fd(struct tier_device *dev, struct fd_s *fds,
		       struct backing_device *backdev)
{
	int error = -EBADF;
	struct file *file = NULL;

	file = fget(fds->fd);
	if (!file)
		goto out;
	if (!(file->f_mode & FMODE_WRITE)) {
		error = -EPERM;
		goto out;
	}
	backdev->fds = file;

	error = 0;
	/* btier disables readahead when it detects a random io pattern
	   it restores the original when the pattern becomes sequential */
	backdev->ra_pages = file->f_ra.ra_pages;
	if (file->f_flags & O_SYNC) {
		dev->writethrough = 1;
		/* Store this persistent on unload */
		file->f_flags ^= O_SYNC;
	}
out:
	return error;
}

/* Return the number of devices in nr
   and return the last tier_device */
static struct tier_device *device_nr(int *nr)
{
	struct list_head *pos;
	struct tier_device *ret = NULL;

	*nr = 0;
	list_for_each(pos, &device_list) {
		ret = list_entry(pos, struct tier_device, list);
		*nr += 1;
	}
	return ret;
}

static void tier_deregister(struct tier_device *dev)
{
	int i;
	if (dev->active) {
		dev->stop = 1;
		wake_up(&dev->migrate_event);
		destroy_workqueue(dev->migration_queue);
		kthread_stop(dev->tier_thread);
		wake_up(&dev->aio_event);
		destroy_workqueue(dev->aio_queue);
		tier_sysfs_exit(dev);
		mutex_destroy(&dev->tier_ctl_mutex);
		mutex_destroy(&dev->qlock);
		del_timer_sync(&dev->migrate_timer);
		del_gendisk(dev->gd);
		put_disk(dev->gd);
		blk_cleanup_queue(dev->rqueue);
		pr_info("deregister device %s\n", dev->devname);
		unregister_blkdev(dev->major_num, dev->devname);
		list_del(&dev->list);
		kfree(dev->managername);
		kfree(dev->aioname);
		release_devicename(dev->devname);
		tier_sync(dev);
		free_blocklist(dev);
		free_bitlists(dev);
		for (i = 0; i < dev->attached_devices; i++) {
			mark_device_clean(dev, i);
			filp_close(dev->backdev[i]->fds, NULL);
			if (dev->backdev[i]->bdev)
				bdput(dev->backdev[i]->bdev);
			kfree(dev->backdev[i]->devmagic);
			kfree(dev->backdev[i]);
		}
		kfree(dev->backdev);
		kfree(dev);
		dev = NULL;
	}
}

static int del_tier_device(char *devicename)
{
	struct tier_device *tier, *next;
	int res = 0;

	list_for_each_entry_safe(tier, next, &device_list, list) {
		if (tier->devname) {
			if (strstr(devicename, tier->devname)) {
				if (tier->users > 0)
					res = -EBUSY;
				else
					tier_deregister(tier);
			}
		}
	}
	return res;
}

static int determine_device_size(struct tier_device *dev)
{
	int i;
	struct backing_device *backdev;
	dev->size = dev->backdev[0]->devmagic->total_device_size;
	dev->backdev[0]->startofblocklist =
	    dev->backdev[0]->devmagic->startofblocklist;
	dev->blocklistsize = dev->backdev[0]->devmagic->blocklistsize;
	pr_info("dev->blocklistsize               : 0x%llx (%llu)\n",
		dev->blocklistsize, dev->blocklistsize);
	dev->backdev[0]->endofdata = dev->backdev[0]->startofblocklist - 1;
	for (i = 0; i < dev->attached_devices; i++) {
		backdev = dev->backdev[i];
		backdev->bitlistsize = backdev->devmagic->bitlistsize;
		backdev->startofdata = TIER_HEADERSIZE;
		backdev->startofbitlist = backdev->devmagic->startofbitlist;
		backdev->devicesize = backdev->devmagic->devicesize;
		if (i > 0) {
			backdev->endofdata = backdev->startofbitlist - 1;
		}
		pr_info("backdev->devicesize      : 0x%llx (%llu)\n",
			backdev->devicesize, backdev->devicesize);
		pr_info("backdev->startofdata     : 0x%llx\n",
			backdev->startofdata);
		pr_info("backdev->bitlistsize     : 0x%llx\n",
			backdev->bitlistsize);
		pr_info("backdev->startofbitlist  : 0x%llx\n",
			backdev->startofbitlist);
		pr_info("backdev->endofdata       : 0x%llx\n",
			backdev->endofdata);

	}
	pr_info("dev->backdev[0]->startofblocklist: 0x%llx\n",
		dev->backdev[0]->startofblocklist);
	return 0;
}

static u64 calc_new_devsize(struct tier_device *dev, int cdev, u64 curdevsize)
{
	int i;
	u64 devsize = 0;
	unsigned int header_size = TIER_HEADERSIZE;

	for (i = 0; i < dev->attached_devices; i++) {
		if (cdev == i) {
			devsize +=
			    curdevsize - TIER_DEVICE_PLAYGROUND - header_size;
			continue;
		}
		devsize += dev->backdev[i]->devicesize - TIER_DEVICE_PLAYGROUND;
	}
	return devsize;
}

static u64 new_total_bitlistsize(struct tier_device *dev, int cdev,
				 u64 curbitlistsize)
{
	int i;
	u64 bitlistsize = 0;

	for (i = 0; i < dev->attached_devices; i++) {
		if (cdev == i) {
			bitlistsize += curbitlistsize;
			continue;
		}
		bitlistsize += dev->backdev[i]->bitlistsize;
	}
	return bitlistsize;
}

/* Copy a list from one location to another
   Return : 0 on success -1 on error  */
static int copylist(struct tier_device *dev, int devicenr,
		    u64 ostart, u64 osize, u64 nstart)
{
	int res = 0;
	u64 offset;
	u64 newoffset = nstart;
	char *buffer;

	pr_info
	    ("copylist device %u, ostart 0x%llx (%llu) osize  0x%llx (%llu), nstart 0x%llx (%llu) end 0x%llx (%llu)\n",
	     devicenr, ostart, ostart, osize, osize, nstart, nstart,
	     nstart + osize, nstart + osize);
	buffer = kzalloc(PAGE_SIZE, GFP_NOFS);
	for (offset = ostart; offset < ostart + osize; offset += PAGE_SIZE) {
		res = tier_file_read(dev, devicenr, buffer, PAGE_SIZE, offset);
		if (res < 0)
			break;
		res =
		    tier_file_write(dev, devicenr,
				    buffer, PAGE_SIZE, newoffset);
		if (res < 0)
			break;
		newoffset += PAGE_SIZE;
	}
	if (offset - ostart < osize) {
		pr_info
		    ("copylist has failed, not expanding : offset %llu, ostart %llu, osize %llu\n",
		     offset, ostart, osize);
		res = -1;
	}
	kfree(buffer);
	return res;
}

/* migrate a bitlist from one location to another
   Afterwards changes the structures to point to the new bitlist
   so that the old bitlist location is no longer used
   Return : 0 on success, negative on error */
static int migrate_bitlist(struct tier_device *dev, int devicenr,
			   u64 newdevsize,
			   u64 newbitlistsize, u64 newstartofbitlist)
{
	int res = 0;

	pr_info("migrate_bitlist : device %u\n", devicenr);
	if (newstartofbitlist < dev->backdev[devicenr]->devicesize) {
		pr_info("Device size has not grown enough to expand\n");
		return -1;
	}
	wipe_bitlist(dev, devicenr, newstartofbitlist, newbitlistsize);
	res =
	    copylist(dev, devicenr, dev->backdev[devicenr]->startofbitlist,
		     dev->backdev[devicenr]->bitlistsize, newstartofbitlist);
	if (res != 0)
		return res;
	// Make sure the new bitlist is synced to disk before
	// we continue
	if (0 != (res = tier_sync(dev)))
		return res;
	return res;
}

/* When the blocklist needs to be expanded 
   we have to move blocks of data out of the way
   then expand the bitlist and migrate it from it's
   current location to the new location.
   Since the blocklist is growing tier device 0
   will shrink in usable size. Therefore the bitlist
   may shrink as well. However to reduce complexity
   we let it be for now. */
static int migrate_data_if_needed(struct tier_device *dev, u64 startofblocklist,
				  u64 blocklistsize, int changeddevice)
{
	int res = 0;
	int cbres = 0;
	u64 blocks = dev->size >> BLKBITS;
	u64 curblock;
	struct blockinfo *binfo;
	struct blockinfo *orgbinfo;

	pr_info("migrate_data_if_needed\n");
	binfo = kzalloc(sizeof(struct blockinfo), GFP_NOFS);
	if (!binfo) {
		tiererror(dev, "migrate_data_if_needed : alloc failed");
		return -ENOMEM;
	}
	for (curblock = 0; curblock < blocks; curblock++) {
		/* Do not update the blocks metadata */
		orgbinfo = get_blockinfo(dev, curblock, 0);
		if (dev->inerror) {
			res = -EIO;
			break;
		}
		// Migrating blocks from device 0 + 1;
		if (orgbinfo->device != 1) {
			continue;
		}
		cbres = 1;
		pr_info
		    ("migrate_data_if_needed : blocknr %llu from device %u\n",
		     curblock, orgbinfo->device - 1);
		if (orgbinfo->offset >= startofblocklist
		    && orgbinfo->offset <= startofblocklist + blocklistsize) {
			memcpy(binfo, orgbinfo, sizeof(struct blockinfo));
			// Move the block to the device that has grown
			binfo->device = changeddevice + 1;
			pr_info
			    ("Call copyblock blocknr %llu from device %u to device %u\n",
			     curblock, orgbinfo->device - 1, binfo->device - 1);
			cbres = copyblock(dev, binfo, orgbinfo, curblock);
			if (cbres) {
				reset_counters_on_migration(dev, orgbinfo);
				clear_dev_list(dev, orgbinfo);
			} else
				pr_info
				    ("Failed to migrate blocknr %llu from device %u to device %u\n",
				     curblock, orgbinfo->device - 1,
				     binfo->device - 1);
		}
		if (!cbres) {
			res = -1;
			break;
		}
	}
	kfree(binfo);
	pr_info("migrate_data_if_needed return %u\n", res);
	return res;
}

static int do_resize_tier(struct tier_device *dev, int devicenr,
			  u64 newdevsize,
			  u64 newblocklistsize,
			  u64 newbitlistsize, u64 curdevsize)
{
	int res = 0;
	u64 startofnewblocklist;
	u64 startofnewbitlist;

	pr_info("resize device %s devicenr %u from %llu to %llu\n",
		dev->backdev[devicenr]->fds->f_dentry->d_name.name,
		devicenr, dev->backdev[devicenr]->devicesize, curdevsize);
	startofnewbitlist = newdevsize - newbitlistsize;
	res =
	    migrate_bitlist(dev, devicenr, newdevsize, newbitlistsize,
			    startofnewbitlist);
	if (0 != res)
		return res;
	/* When device 0 has grown we move the bitlist of the device to
	   the end of the device and then move the blocklist to the end
	   This does not require data migration 

	   When another device has grown we may need to expand the blocklist
	   on device 0 as well. In that case we may need to migrate data
	   from device0 to another device to make room for the larger 
	   blocklist */
	if (devicenr == 0) {
		startofnewblocklist = startofnewbitlist - newblocklistsize;
		wipe_bitlist(dev, devicenr, startofnewblocklist,
			     newblocklistsize);
		res =
		    copylist(dev, devicenr,
			     dev->backdev[devicenr]->startofblocklist,
			     dev->blocklistsize, startofnewblocklist);
		if (0 != res)
			return res;
		if (0 != (res = tier_sync(dev)))
			return res;
		dev->backdev[devicenr]->startofblocklist = startofnewblocklist;
		dev->blocklistsize = newblocklistsize;
		dev->backdev[devicenr]->devmagic->blocklistsize =
		    newblocklistsize;
		dev->backdev[devicenr]->devmagic->startofblocklist =
		    startofnewblocklist;
	} else {
		startofnewblocklist =
		    dev->backdev[0]->startofbitlist - newblocklistsize;
		if (startofnewblocklist < dev->backdev[0]->startofblocklist) {
			res =
			    migrate_data_if_needed(dev, startofnewblocklist,
						   newblocklistsize, devicenr);
			if (0 != res)
				return res;
// This should be journalled. FIX FIX FIX
// The blocklist needs to be protected at all cost.
			res =
			    copylist(dev, 0, dev->backdev[0]->startofblocklist,
				     dev->blocklistsize, startofnewblocklist);
			if (0 != res)
				return res;
			wipe_bitlist(dev, 0,
				     startofnewblocklist + dev->blocklistsize,
				     newblocklistsize - dev->blocklistsize);
			if (0 != (res = tier_sync(dev)))
				return res;
			dev->backdev[0]->startofblocklist = startofnewblocklist;
			dev->blocklistsize = newblocklistsize;
			dev->backdev[0]->devmagic->blocklistsize =
			    newblocklistsize;
			dev->backdev[0]->devmagic->startofblocklist =
			    startofnewblocklist;
			dev->backdev[0]->endofdata =
			    dev->backdev[0]->startofblocklist - 1;
			write_device_magic(dev, 0);
		} else
			pr_info
			    ("startofnewblocklist %llu, old start %llu, no migration needed\n",
			     startofnewblocklist,
			     dev->backdev[0]->startofblocklist);
	}
	if (devicenr == 0)
		dev->backdev[devicenr]->endofdata = startofnewblocklist - 1;
	else
		dev->backdev[devicenr]->endofdata = startofnewbitlist - 1;
	dev->backdev[devicenr]->startofbitlist = startofnewbitlist;
	dev->backdev[devicenr]->bitlistsize = newbitlistsize;
	dev->backdev[devicenr]->devmagic->bitlistsize = newbitlistsize;
	dev->backdev[devicenr]->devmagic->startofbitlist = startofnewbitlist;
	dev->backdev[devicenr]->devmagic->devicesize = newdevsize;
	dev->backdev[devicenr]->devicesize = newdevsize;
	write_device_magic(dev, devicenr);
	res = tier_sync(dev);
	return res;
}

void resize_tier(struct tier_device *dev)
{
	int count;
	int res = 1;
	u64 curdevsize = 0;
	u64 newbitlistsize = 0;
	u64 newblocklistsize = 0;
	u64 newdevsize = 0;
	u64 newbitlistsize_total = 0;
	int found = 0;

	for (count = 0; count < dev->attached_devices; count++) {
		curdevsize =
		    KERNEL_SECTORSIZE * tier_get_size(dev->backdev[count]->fds);
		curdevsize = round_to_blksize(curdevsize);
		newbitlistsize = calc_bitlist_size(curdevsize);
		pr_info("curdevsize = %llu old = %llu\n", curdevsize,
			dev->backdev[count]->devicesize);
		if (dev->backdev[count]->devicesize == curdevsize)
			continue;
		if (curdevsize - dev->backdev[count]->devicesize <
		    newbitlistsize) {
			pr_info
			    ("Ignoring unusable small devicesize change for device %u\n",
			     count);
			continue;
		}
		newdevsize = calc_new_devsize(dev, count, curdevsize);
		newbitlistsize_total =
		    new_total_bitlistsize(dev, count, newbitlistsize);
		newblocklistsize =
		    calc_blocklist_size(newdevsize, newbitlistsize_total);
		// Make sure there is plenty of space
		if (curdevsize <
		    dev->backdev[count]->devicesize + newblocklistsize +
		    newbitlistsize + BLKSIZE) {
			pr_info
			    ("Ignoring unusable small devicesize change for device %u\n",
			     count);
			continue;
		}
		found++;
		pr_info("newblocklistsize=%llu\n", newblocklistsize);
		res =
		    do_resize_tier(dev, count, curdevsize, newblocklistsize,
				   newbitlistsize, curdevsize);
	}
	if (0 == found) {
		pr_info
		    ("Ignoring request to resize, no devices have changed in size\n");
	} else {
		if (res == 0) {
			free_blocklist(dev);
			pr_info("Device %s is resized from %llu to %llu\n",
				dev->devname, dev->size,
				newdevsize - newblocklistsize -
				newbitlistsize_total);
			dev->size =
			    newdevsize - newblocklistsize -
			    newbitlistsize_total;
			dev->backdev[0]->devmagic->total_device_size =
			    dev->size;
			register_new_device_size(dev);
			load_blocklist(dev);
		}
	}
	return;
}

static long tier_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
	struct tier_device *dev = NULL;
	struct tier_device *devnew = NULL;
	int current_device_nr = 0;
	int err = 0;
	char *dname;
	int devlen;
	struct fd_s fds;

	if (!capable(CAP_SYS_ADMIN))
		return -EACCES;

	mutex_lock(&ioctl_mutex);
	if (cmd != TIER_INIT)
		dev = device_nr(&current_device_nr);
	if (!dev && cmd != TIER_INIT) {
		err = -EBADSLT;
		goto end_error;
	}
	switch (cmd) {
	case TIER_INIT:
		err = -ENOMEM;
		if (sizeof(struct tier_device) > TIER_HEADERSIZE)
			break;
		devnew = kzalloc(sizeof(struct tier_device), GFP_KERNEL);
		if (!devnew)
			break;
		if (0 == tier_device_count()) {
			device = devnew;
		}
		list_add_tail(&devnew->list, &device_list);
		devnew->backdev =
		    kzalloc(sizeof(struct backing_device *) * MAX_BACKING_DEV,
			    GFP_KERNEL);
		if (!devnew->backdev) {
			kfree(devnew);
			break;
		}
		err = 0;
		break;
	case TIER_SET_FD:
		err = -EEXIST;
		if (dev->attached_devices > MAX_BACKING_DEV)
			break;
		if (0 != dev->tier_device_number)
			break;
		err = -EFAULT;
		dev->backdev[dev->attached_devices] =
		    kzalloc(sizeof(struct backing_device), GFP_KERNEL);
		if (copy_from_user
		    (&fds, (struct fd_s __user *)arg, sizeof(fds))) {
			err = -EFAULT;
			break;
		}
		err =
		    tier_set_fd(dev, &fds, dev->backdev[dev->attached_devices]);
		if (0 != fds.use_bio)
			dev->use_bio = fds.use_bio;
		dev->attached_devices++;
		break;
	case TIER_SET_SECTORSIZE:
		err = -EEXIST;
		if (0 != dev->tier_device_number)
			break;
		err = 0;
		dev->logical_block_size = arg;
		pr_info("sectorsize : %d\n", dev->logical_block_size);
		break;
	case TIER_REGISTER:
		err = -EEXIST;
		if (0 != dev->tier_device_number)
			break;
		if (0 == dev->attached_devices) {
			pr_err("Insufficient parameters entered");
		} else {
			dev->tier_device_number = current_device_nr;
			if (0 != (err = order_devices(dev)))
				break;
			if (0 == (err = determine_device_size(dev)))
				err = tier_register(dev);
		}
		break;
	case TIER_DEREGISTER:
		pr_info("TIER_DEREGISTER\n");
		err = -ENOMEM;
		devlen = 1 + strlen("/dev/sdtierX");
		dname = kzalloc(devlen, GFP_KERNEL);
		if (!dname)
			break;
		if (copy_from_user(dname, (char __user *)arg, devlen - 1)) {
			err = -EFAULT;
		} else {
			err = tier_device_count();
			err = del_tier_device(dname);
			if (0 == err)
				device = NULL;
		}
		kfree(dname);
		break;
	default:
		err = dev->ioctl ? dev->ioctl(dev, cmd, arg) : -EINVAL;
	}
end_error:
	mutex_unlock(&ioctl_mutex);
	return err;
}

static const struct file_operations _tier_ctl_fops = {
	.open = nonseekable_open,
	.unlocked_ioctl = tier_ioctl,
	.owner = THIS_MODULE,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,2,0)
	.llseek = noop_llseek
#else
	.llseek = no_llseek
#endif
};

static struct miscdevice _tier_misc = {
	.minor = MISC_DYNAMIC_MINOR,
	.name = "tiercontrol",
	.nodename = "tiercontrol",
	.fops = &_tier_ctl_fops
};

static int __init tier_init(void)
{
	int r;
	/* First register out control device */
	pr_info("version    : %s\n", TIER_VERSION);
	r = misc_register(&_tier_misc);
	if (r) {
		pr_err("misc_register failed for control device");
		return r;
	}
	/*
	 * Alloc our device names
	 */
	r = init_devicenames();
	mutex_init(&ioctl_mutex);
	return r;
}

static void __exit tier_exit(void)
{
	struct tier_device *tier, *next;

	list_for_each_entry_safe(tier, next, &device_list, list)
	    tier_deregister(tier);

	if (misc_deregister(&_tier_misc) < 0)
		pr_err("misc_deregister failed for tier control device");
	kfree(devicenames);
	mutex_destroy(&ioctl_mutex);
}

module_init(tier_init);
module_exit(tier_exit);
