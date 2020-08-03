/*
 * This file contains routine from fs/buffer.c that is modified to support
 * our own data block-mapping facilities.
 */

#include <linux/memcontrol.h>
#include <linux/blk_types.h>
#include <linux/page-flags.h>
#include <linux/atomic.h>
#include <linux/bio.h>
#include <linux/bitmap.h>
#include <linux/blkdev.h>
#include <linux/cleancache.h>
#include <linux/slab.h>
#include <linux/pagemap.h>
#include <linux/prefetch.h>
#include <linux/writeback.h>
#include "ext2.h"

/* A dup mapping structure corresponds to one page cache page */
struct dup_mapping {
	/* Sub-fractions of a page. There would be more than one fraction
	 * if the block size of the file system is smaller than the page size
	 * of the architecture. */
	struct map_io_entry {
		sector_t blocks[3];
		/* Number of EIOs for this fraction */
		atomic_t nr_ioerr;
		/* Number of on-going IOs for this fraction */
		atomic_t nr_ongoing_io;
		/* Points to this dup_mapping structure */
		struct dup_mapping *map;
	} fractions[MAX_BUF_PER_PAGE];
	/* Dirty fractions in this page */
	DECLARE_BITMAP(dirty, MAX_BUF_PER_PAGE);
	/* Mapped fractions (having corresponding bmap mappings in this page) */
	DECLARE_BITMAP(mapped, MAX_BUF_PER_PAGE);
	/* Uptodate fractions (content not stale) */
	DECLARE_BITMAP(uptodate, MAX_BUF_PER_PAGE);
	/* New-allocated fractions */
	DECLARE_BITMAP(new, MAX_BUF_PER_PAGE);
	/* File system block size */
	unsigned int blocksize;
	/* Number of duplicates for this page */
	unsigned int nr_dups;
	/* Number of on-going IOs */
	atomic_t ongoing;
	/* The corresponding page cache page */
	struct page *page;
	/* The corresponding block device the FS resides on */
	struct block_device *bdev;
};

#define page_dup_mapping(page)                                                 \
	({                                                                     \
		BUG_ON(!PagePrivate(page));                                    \
		((struct dup_mapping *)page_private(page));                    \
	})
#define page_has_dup_mapping(page) PagePrivate(page)

static inline void attach_page_dup_mapping(struct page *page,
					   struct dup_mapping *map)
{
	get_page(page);
	SetPagePrivate(page);
	set_page_private(page, (unsigned long)map);
}

static struct kmem_cache *dup_mapping_kmem;

static struct dup_mapping *create_page_dup_mapping(struct page *page,
						   struct inode *inode)
{
	struct ext2_sb_info *sbi = EXT2_SB(inode->i_sb);
	struct ext2_inode_info *ei = EXT2_I(inode);

	if (!page_has_dup_mapping(page)) {
		/* Create dup_mapping if the page didn't have one */
		struct dup_mapping *map;
		int j;

		map = (struct dup_mapping *)kmem_cache_alloc(dup_mapping_kmem,
							     GFP_NOFS);
		memset(map, 0, sizeof(*map));
		for (j = 0; j < ARRAY_SIZE(map->fractions); j++) {
			map->fractions[j].map = map;
		}
		map->blocksize = inode->i_sb->s_blocksize;
		if (ext2_inode_has_dup_run(ei))
			map->nr_dups = sbi->s_dupinode_dup_cnt;
		else
			map->nr_dups = 1;
		map->page = page;
		map->bdev = inode->i_sb->s_bdev;
		/* Attach the newly-allocated dup_mapping to the page */
		attach_page_dup_mapping(page, map);
	}
	return page_dup_mapping(page);
}

static void try_to_release_dup_mapping(struct page *page)
{
	if (page_has_dup_mapping(page)) {
		struct dup_mapping *map = page_dup_mapping(page);
		kmem_cache_free(dup_mapping_kmem, map);
		ClearPagePrivate(page);
		set_page_private(page, 0);
		put_page(page);
	}
}

static void ext2_mark_dup_mapping_dirty(struct dup_mapping *map,
					unsigned int block)
{
	struct page *page = map->page;
	struct address_space *mapping = NULL;

	if (!test_and_set_bit(block, map->dirty)) {
		lock_page_memcg(page);
		if (!TestSetPageDirty(page)) {
			mapping = page_mapping(page);
			if (mapping)
				__set_page_dirty(page, mapping, 0);
		}
		unlock_page_memcg(page);
		if (mapping)
			__mark_inode_dirty(mapping->host, I_DIRTY_PAGES);
	}
}

/*
 * Size is a power-of-two in the range 512..PAGE_SIZE,
 * and the case we care about most is PAGE_SIZE.
 *
 * So this *could* possibly be written with those
 * constraints in mind (relevant mostly if some
 * architecture has a slow bit-scan instruction)
 */
static inline int block_size_bits(unsigned int blocksize)
{
	return ilog2(blocksize);
}

/* This is our common BIO IO end callback */
static void end_bio_common(struct bio *bio)
{
	struct map_io_entry *io_entry = bio->bi_private;
	blk_status_t status = bio->bi_status;
	bio_put(bio);
	if (status)
		atomic_inc(&io_entry->nr_ioerr);
	BUG_ON(atomic_read(&io_entry->nr_ongoing_io) <= 0);
	/* Wake up any potential waiters if all IO finishes */
	if (atomic_dec_and_test(&io_entry->nr_ongoing_io))
		wake_up_var(&io_entry->nr_ongoing_io);
}

static void end_bio_after_all_write(struct dup_mapping *map)
{
	struct page *page = map->page;
	int i;

	for (i = 0; i < MAX_BUF_PER_PAGE; i++) {
		/* If all write to duplications fails, clear the up-to-date bit of the fraction and set the page error.
		 * Otherwise, set the fraction up-to-date */
		if (atomic_read(&map->fractions[i].nr_ioerr) == map->nr_dups) {
			clear_bit(i, map->uptodate);
			SetPageError(map->page);
		} else {
			set_bit(i, map->uptodate);
		}
	}

	/* The page is no longer under writeback */
	end_page_writeback(page);
}

static void end_bio_wr_io_sync(struct bio *bio)
{
	struct map_io_entry *io_entry = bio->bi_private;
	struct dup_mapping *map = io_entry->map;
	bool wrback_finish;

	/* Common BIO END IO callback */
	end_bio_common(bio);
	wrback_finish = atomic_dec_and_test(&map->ongoing);
	if (!wrback_finish)
		return;

	/* All IO on the dup_mapping finishes */
	end_bio_after_all_write(map);
}

static void end_bio_after_all_read(struct dup_mapping *map)
{
	struct page *page = map->page;
	int i;

	for (i = 0; i < MAX_BUF_PER_PAGE; i++) {
		/* If all read to duplications fails, clear the up-to-date bit of the fraction and set the page error.
		 * Otherwise, set the fraction up-to-date */
		if (atomic_read(&map->fractions[i].nr_ioerr) == map->nr_dups) {
			clear_bit(i, map->uptodate);
			SetPageError(map->page);
		} else {
			set_bit(i, map->uptodate);
		}
	}

	/* If all fractions are okay, set the page up-to-date as well */
	if (!PageError(page))
		SetPageUptodate(page);
	unlock_page(page);
}

static void end_bio_rd_io_sync(struct bio *bio)
{
	struct map_io_entry *io_entry = bio->bi_private;
	struct dup_mapping *map = io_entry->map;
	bool wrback_finish;

	/* Common BIO END IO callback */
	end_bio_common(bio);
	wrback_finish = atomic_dec_and_test(&map->ongoing);
	if (!wrback_finish)
		return;

	/* All IO on the dup_mapping finishes */
	end_bio_after_all_read(map);
}

static void wait_ongoing_io(struct dup_mapping *map, int i)
{
	wait_var_event(&map->fractions[i].nr_ongoing_io,
		       !atomic_read(&map->fractions[i].nr_ongoing_io));
}

/*
 * This allows us to do IO even on the odd last sectors
 * of a device, even if the block size is some multiple
 * of the physical sector size.
 *
 * We'll just truncate the bio to the size of the device,
 * and clear the end of the buffer head manually.
 *
 * Truly out-of-range accesses will turn into actual IO
 * errors, this only handles the "we need to be able to
 * do IO at the final sector" case.
 */
static void __guard_bio_eod(int op, struct bio *bio)
{
	sector_t maxsector;
	struct bio_vec *bvec = bio_last_bvec_all(bio);
	unsigned truncated_bytes;
	struct hd_struct *part;

	part = disk_get_part(bio->bi_disk, bio->bi_partno);
	if (part)
		maxsector = part_nr_sects_read(part);
	else
		maxsector = get_capacity(bio->bi_disk);

	if (!maxsector)
		return;

	/*
	 * If the *whole* IO is past the end of the device,
	 * let it through, and the IO layer will turn it into
	 * an EIO.
	 */
	if (unlikely(bio->bi_iter.bi_sector >= maxsector))
		return;

	maxsector -= bio->bi_iter.bi_sector;
	if (likely((bio->bi_iter.bi_size >> 9) <= maxsector))
		return;

	/* Uhhuh. We've got a bio that straddles the device size! */
	truncated_bytes = bio->bi_iter.bi_size - (maxsector << 9);

	/*
	 * The bio contains more than one segment which spans EOD, just return
	 * and let IO layer turn it into an EIO
	 */
	if (truncated_bytes > bvec->bv_len)
		return;

	/* Truncate the bio.. */
	bio->bi_iter.bi_size -= truncated_bytes;
	bvec->bv_len -= truncated_bytes;

	/* ..and clear the end of the buffer for reads */
	if (op == REQ_OP_READ) {
		zero_user(bvec->bv_page, bvec->bv_offset + bvec->bv_len,
				truncated_bytes);
	}
}

static int ext2_submit_fraction(int op, int op_flags, struct dup_mapping *map,
				unsigned int block_inc, enum rw_hint write_hint,
				struct writeback_control *wbc,
				bio_end_io_t handler)
{
	struct bio *bios[MAX_BUF_PER_PAGE];
	int j;

	/* We are going to submit the fraction, and send each duplication to the media */

	atomic_set(&map->fractions[block_inc].nr_ioerr, 0);

	atomic_set(&map->fractions[block_inc].nr_ongoing_io, map->nr_dups);

	/*
	 * from here on down, it's all bio -- do the initial mapping,
	 * submit_bio -> generic_make_request may further map this bio around
	 */
	for (j = 0; j < map->nr_dups; j++) {
		BUG_ON(!map->fractions[block_inc].blocks[j]);

		bios[j] = bio_alloc(GFP_NOIO, 1);

		bios[j]->bi_iter.bi_sector =
			map->fractions[block_inc].blocks[j] *
			(map->blocksize >> 9);
		bio_set_dev(bios[j], map->bdev);
		bios[j]->bi_write_hint = write_hint;

		bio_add_page(bios[j], map->page, map->blocksize,
			     block_inc * map->blocksize);
		BUG_ON(bios[j]->bi_iter.bi_size != map->blocksize);

		bios[j]->bi_end_io = handler;
		bios[j]->bi_private = &map->fractions[block_inc];

		/* Take care of bh's that straddle the end of the device */
		__guard_bio_eod(op, bios[j]);

		bio_set_op_attrs(bios[j], op, op_flags);

		if (wbc) {
			wbc_init_bio(wbc, bios[j]);
			wbc_account_io(wbc, map->page, map->blocksize);
		}

		submit_bio(bios[j]);
	}
	return 0;
}

/*
 * NOTE! All mapped/uptodate combinations are valid:
 *
 *	Mapped	Uptodate	Meaning
 *
 *	No	No		"unknown" - must do get_block()
 *	No	Yes		"hole" - zero-filled
 *	Yes	No		"allocated" - allocated on disk, not read in
 *	Yes	Yes		"valid" - allocated and up-to-date in memory.
 *
 * "Dirty" is valid only with the last case (mapped+uptodate).
 */

/*
 * While block_write_full_page is writing back the dirty buffers under
 * the page lock, whoever dirtied the buffers may decide to clean them
 * again at any time.  We handle that by only looking at the buffer
 * state inside lock_buffer().
 *
 * If block_write_full_page() is called for regular writeback
 * (wbc->sync_mode == WB_SYNC_NONE) then it will redirty a page which has a
 * locked buffer.   This only can happen if someone has written the buffer
 * directly, with submit_bh().  At the address_space level PageWriteback
 * prevents this contention from occurring.
 *
 * If block_write_full_page() is called with wbc->sync_mode ==
 * WB_SYNC_ALL, the writes are posted using REQ_SYNC; this
 * causes the writes to be flagged as synchronous writes.
 */
int __ext2_block_write_full_page(struct inode *inode, struct page *page,
				 struct writeback_control *wbc)
{
	int err;
	unsigned int i;
	sector_t start_block;
	sector_t last_block;
	struct dup_mapping *map;
	unsigned int blocksize, bbits;
	int write_flags = wbc_to_write_flags(wbc);

	map = create_page_dup_mapping(page, inode);

	blocksize = map->blocksize;
	bbits = block_size_bits(blocksize);

	start_block = (sector_t)page->index << (PAGE_SHIFT - bbits);
	last_block = (i_size_read(inode) - 1) >> bbits;

	for (i = 0; i < PAGE_SIZE / blocksize; i++) {
		/* For any fractions outside the EOF, we clear the dirty bit */
		if (start_block + i > last_block) {
			clear_bit(i, map->dirty);
		}
	}

	for (i = 0; i < PAGE_SIZE / blocksize; i++) {
		if (start_block + i > last_block) {
			/*
			 * mapped buffers outside i_size will occur, because
			 * this page can be outside i_size when there is a
			 * truncate in progress.
			 */
			/*
			 * The buffer was zeroed by block_write_full_page()
			 */

			set_bit(i, map->uptodate);
		} else if (!test_bit(i, map->mapped) &&
			   test_bit(i, map->dirty)) {
			/* Bring in bmap mappings if the fraction isn't mapped. We also do allocations here */
			struct ext2_bmpt_map_args args;
			int j;
			args.iblk = start_block + i;
			args.flags = 0;
			args.len = 1;
			ext2_bmpt_irec_clear(&args.irec);
			err = ext2_bmpt_map_blocks(
				inode, 0, &args, EXT2_BMPT_GET_BLOCK_ALLOCATE);
			if (err)
				goto recover;

			if (args.flags & EXT2_BMPT_MAP_NEW) {
				for (j = 0; j < map->nr_dups; j++) {
					if (args.irec.b_blocks[j])
						clean_bdev_aliases(
							inode->i_sb->s_bdev,
							args.irec.b_blocks[j],
							1);
				}
			}
			if (args.flags & EXT2_BMPT_MAP_MAPPED) {
				for (j = 0; j < map->nr_dups; j++) {
					map->fractions[i].blocks[j] =
						args.irec.b_blocks[j];
				}
				set_bit(i, map->mapped);
			}
		}
	}

	/* Count the on-going IOs.*/
	for (i = 0; i < PAGE_SIZE / blocksize; i++) {
		/* Holes and non-dirty fractions won't count */
		if (test_bit(i, map->mapped) && test_bit(i, map->dirty))
			atomic_add(map->nr_dups, &map->ongoing);
	}

	/*
	 * The page is protected by PageWriteback(), so we can
	 * drop the refcounts early.
	 */
	BUG_ON(PageWriteback(page));
	set_page_writeback(page);
	ClearPageError(page);

	for (i = 0; i < PAGE_SIZE / blocksize; i++) {
		if (!test_bit(i, map->mapped))
			continue;
		if (test_and_clear_bit(i, map->dirty)) {
			/* Submit the dirty and non-hole fraction to the media */
			ext2_submit_fraction(REQ_OP_WRITE, write_flags, map, i,
					     inode->i_write_hint, wbc,
					     end_bio_wr_io_sync);
		}
	}

	unlock_page(page);
	err = 0;
done:
	return err;

recover:
	/*
	 * ENOSPC, or some other error.  We may already have added some
	 * blocks to the file, so we need to write these out to avoid
	 * exposing stale data.
	 * The page is currently locked and not marked for writeback
	 */
	for (i = 0; i < PAGE_SIZE / blocksize; i++) {
		if (test_bit(i, map->mapped) && test_bit(i, map->dirty))
			atomic_add(map->nr_dups, &map->ongoing);
	}

	SetPageError(page);
	BUG_ON(PageWriteback(page));
	mapping_set_error(page->mapping, err);
	set_page_writeback(page);

	for (i = 0; i < PAGE_SIZE / blocksize; i++) {
		if (!test_bit(i, map->mapped))
			continue;
		if (test_and_clear_bit(i, map->dirty)) {
			ext2_submit_fraction(REQ_OP_WRITE, write_flags, map, i,
					     inode->i_write_hint, wbc,
					     end_bio_wr_io_sync);
		}
	}

	unlock_page(page);
	goto done;
}

/*
 * Generic "read page" function for block devices that have the normal
 * get_block functionality. This is most of the block device filesystems.
 * Reads the page asynchronously --- the unlock_buffer() and
 * set/clear_buffer_uptodate() functions propagate buffer state into the
 * page struct once IO has completed.
 */
int ext2_block_read_full_page(struct page *page)
{
	struct inode *inode = page->mapping->host;
	sector_t iblock, lblock;
	struct dup_mapping *map;
	unsigned int blocksize, bbits;
	int nr, i;
	int fully_mapped = 1;

	map = create_page_dup_mapping(page, inode);
	blocksize = map->blocksize;
	bbits = block_size_bits(blocksize);

	/* I-node data block */
	iblock = (sector_t)page->index << (PAGE_SHIFT - bbits);
	lblock = (i_size_read(inode) + blocksize - 1) >> bbits;
	nr = 0;

	for (i = 0; i < PAGE_SIZE / blocksize; i++) {
		/* No need to read fractions that are already up-to-date */
		if (test_bit(i, map->uptodate))
			continue;

		/* If the fraction isn't mapped yet, get its mapping */
		if (!test_bit(i, map->mapped)) {
			struct ext2_bmpt_map_args args;
			int j, err;

			fully_mapped = 0;

			args.iblk = iblock + i;
			args.flags = 0;
			args.len = 1;
			ext2_bmpt_irec_clear(&args.irec);
			err = ext2_bmpt_map_blocks(inode, 0, &args, 0);
			if (err)
				SetPageError(page);

			if (args.flags & EXT2_BMPT_MAP_MAPPED) {
				for (j = 0; j < map->nr_dups; j++) {
					map->fractions[i].blocks[j] =
						args.irec.b_blocks[j];
				}
				set_bit(i, map->mapped);
				nr += map->nr_dups;
			} else {
				/* If it is a hole, or mapping error is encountered, zero the fraction */
				zero_user(page, i * blocksize, blocksize);
				/* And if it is a hole instead of mapping error encountered, set the fraction up-to-date */
				if (!err)
					set_bit(i, map->uptodate);
				continue;
			}
		}
	}

	if (fully_mapped)
		SetPageMappedToDisk(page);

	if (!nr) {
		/*
		 * All buffers are uptodate - we can set the page uptodate
		 * as well. But not if get_block() returned an error.
		 */
		if (!PageError(page))
			SetPageUptodate(page);
		unlock_page(page);
		return 0;
	}
	atomic_set(&map->ongoing, nr);

	/*
	 * Stage 3: start the IO.  Check for uptodateness
	 * inside the buffer lock in case another process reading
	 * the underlying blockdev brought it uptodate (the sct fix).
	 */
	for (i = 0; i < PAGE_SIZE / blocksize; i++) {
		/* Already up-to-date mappings need not to be read again */
		if (test_bit(i, map->uptodate))
			continue;
		ext2_submit_fraction(REQ_OP_READ, 0, map, i, 0, NULL,
				     end_bio_rd_io_sync);
	}
	return 0;
}

/*
 * The generic ->writepage function
 */
int ext2_block_write_full_page(struct page *page, struct writeback_control *wbc)
{
	struct inode *const inode = page->mapping->host;
	loff_t i_size = i_size_read(inode);
	const pgoff_t end_index = i_size >> PAGE_SHIFT;
	unsigned offset;

	/* Is the page fully inside i_size? */
	if (page->index < end_index)
		return __ext2_block_write_full_page(inode, page, wbc);

	/* Is the page fully outside i_size? (truncate in progress) */
	offset = i_size & (PAGE_SIZE - 1);
	if (page->index >= end_index + 1 || !offset) {
		/*
		 * The page may have dirty, unmapped buffers.  For example,
		 * they may have been added in ext3_writepage().  Make them
		 * freeable here, so the page does not leak.
		 */
		ext2_invalidatepage(page, 0, PAGE_SIZE);
		unlock_page(page);
		return 0; /* don't care */
	}

	/*
	 * The page straddles i_size.  It must be zeroed out on each and every
	 * writepage invocation because it may be mmapped.  "A file is mapped
	 * in multiples of the page size.  For a file that is not a multiple of
	 * the  page size, the remaining memory is zeroed when mapped, and
	 * writes to that region are not written out to the file."
	 */
	zero_user_segment(page, offset, PAGE_SIZE);
	return __ext2_block_write_full_page(inode, page, wbc);
}

static void ext2_page_zero_new_fraction(struct page *page, unsigned from, unsigned to)
{
	unsigned int block_start, block_end;
	unsigned int blocksize, bbits;
	struct dup_mapping *map;
	unsigned int i = 0;

	BUG_ON(!PageLocked(page));
	if (!page_has_dup_mapping(page))
		return;

	map = page_dup_mapping(page);
	/* FS Block size */
	blocksize = map->blocksize;
	bbits = block_size_bits(blocksize);

	for (block_start = 0; block_start < PAGE_SIZE;
	     i++, block_start = block_end) {
		block_end = block_start + blocksize;
		if (test_bit(i, map->new)) {
			/* For any newly-allocated FS blocks, zero the corresponding fraction if it is in the range of [from, to) */
			if (block_end > from && block_start < to) {
				if (!PageUptodate(page)) {
					unsigned start, size;

					start = max(from, block_start);
					size = min(to, block_end) - start;

					zero_user(page, start, size);
					set_bit(i, map->uptodate);
				}

				/* Clear the new flag of the fraction, and mark the fraction dirty */
				clear_bit(i, map->new);
				ext2_mark_dup_mapping_dirty(map, i);
			}
		}
	}
}

int __ext2_block_write_begin_int(struct page *page, loff_t pos, unsigned len)
{
	unsigned int from = pos & (PAGE_SIZE - 1);
	unsigned int to = from + len;
	struct inode *inode = page->mapping->host;
	unsigned int block_start, block_end;
	sector_t block;
	int err = 0, i = 0;
	unsigned int blocksize, bbits;
	struct dup_mapping *map;
	struct map_io_entry *ios[MAX_BUF_PER_PAGE];
	int nr_wait = 0;

	BUG_ON(!PageLocked(page));
	BUG_ON(from > PAGE_SIZE);
	BUG_ON(to > PAGE_SIZE);
	BUG_ON(from > to);

	map = create_page_dup_mapping(page, inode);
	blocksize = map->blocksize;
	bbits = block_size_bits(blocksize);

	/* I-node data block */
	block = (sector_t)page->index << (PAGE_SHIFT - bbits);

	for (block_start = 0; block_start < PAGE_SIZE;
	     i++, block_start = block_end) {
		block_end = block_start + blocksize;

		if (block_end <= from || block_start >= to) {
			/* For any out-of-range [from, to) fractions, always set them up-to-date */
			if (PageUptodate(page)) {
				if (!test_bit(i, map->uptodate))
					set_bit(i, map->uptodate);
			}
			continue;
		}

		/* Clear the new flag of the fraction */
		clear_bit(i, map->new);
		if (!test_bit(i, map->mapped)) {
			/* Bring in the bmap mapping if the fraction isn't mapped yet. We also do allocations here */
			struct ext2_bmpt_map_args args;

			args.iblk = block + i;
			args.flags = 0;
			args.len = 1;
			ext2_bmpt_irec_clear(&args.irec);
			err = ext2_bmpt_map_blocks(
				inode, 0, &args, EXT2_BMPT_GET_BLOCK_ALLOCATE);
			if (err)
				break;

			set_bit(i, map->new);
			if (args.flags & EXT2_BMPT_MAP_NEW) {
				clean_bdev_aliases(map->bdev, block + i, 1);
				if (PageUptodate(page)) {
					set_bit(i, map->uptodate);
					clear_bit(i, map->new);
					ext2_mark_dup_mapping_dirty(map, i);
					continue;
				}
				if (block_end > to || block_start < from)
					zero_user_segments(page, to, block_end,
							   block_start, from);
				continue;
			}

			if (args.flags & EXT2_BMPT_MAP_MAPPED) {
				int j;
				for (j = 0; j < map->nr_dups; j++) {
					map->fractions[i].blocks[j] =
						args.irec.b_blocks[j];
				}
				set_bit(i, map->mapped);
			}
		}

		if (PageUptodate(page)) {
			if (!test_bit(i, map->uptodate))
				set_bit(i, map->uptodate);
			continue;
		}
		if (!test_bit(i, map->uptodate) &&
		    (block_start < from || block_end > to)) {
			/* For any blocks that require Read-modify-write pattern, such as unaligned write,
			 * read the content from media first */
			ext2_submit_fraction(REQ_OP_READ, 0, map, i, 0, NULL,
					     end_bio_common);
			ios[nr_wait++] = &map->fractions[i];
		}
	}

	/*
	 * If we issued read requests - let them complete.
	 */
	while (nr_wait--) {
		i = ios[nr_wait] - map->fractions;
		wait_ongoing_io(map, i);
		if (atomic_read(&map->fractions[i].nr_ioerr) == map->nr_dups)
			err = -EIO;
		else
			set_bit(i, map->uptodate);
	}

	if (unlikely(err))
		ext2_page_zero_new_fraction(page, from, to);
	return err;
}

int __ext2_block_write_begin(struct page *page, loff_t pos, unsigned len)
{
	return __ext2_block_write_begin_int(page, pos, len);
}

static int __ext2_block_commit_write(struct inode *inode, struct page *page,
				     unsigned from, unsigned to)
{
	int partial = 0;
	unsigned int blocksize, bbits;
	unsigned int block_start, block_end;
	unsigned int i = 0;
	struct dup_mapping *map;

	map = page_dup_mapping(page);
	blocksize = map->blocksize;
	bbits = block_size_bits(blocksize);

	block_start = 0;

	for (block_start = 0; block_start < PAGE_SIZE;
	     i++, block_start = block_end) {
		block_end = block_start + blocksize;
		if (block_end <= from || block_start >= to) {
			if (!test_bit(i, map->uptodate))
				partial = 1;
		} else {
			set_bit(i, map->uptodate);
			ext2_mark_dup_mapping_dirty(map, i);
		}
		clear_bit(i, map->new);
	}

	/*
	 * If this is a partial write which happened to make all buffers
	 * uptodate then we can optimize away a bogus readpage() for
	 * the next read(). Here we 'discover' whether the page went
	 * uptodate as a result of this (potentially partial) write.
	 */
	if (!partial)
		SetPageUptodate(page);
	return 0;
}

/*
 * block_write_begin takes care of the basic task of block allocation and
 * bringing partial write blocks uptodate first.
 *
 * The filesystem needs to handle block truncation upon failure.
 */
int ext2_block_write_begin(struct address_space *mapping, loff_t pos,
			   unsigned len, unsigned flags, struct page **pagep)
{
	pgoff_t index = pos >> PAGE_SHIFT;
	struct page *page;
	int status;

	page = grab_cache_page_write_begin(mapping, index, flags);
	if (!page)
		return -ENOMEM;

	status = __ext2_block_write_begin(page, pos, len);
	if (unlikely(status)) {
		unlock_page(page);
		put_page(page);
		page = NULL;
	}

	*pagep = page;
	return status;
}

int ext2_block_write_end(struct file *file, struct address_space *mapping,
			 loff_t pos, unsigned len, unsigned copied,
			 struct page *page, void *fsdata)
{
	struct inode *inode = mapping->host;
	unsigned start;

	start = pos & (PAGE_SIZE - 1);

	if (unlikely(copied < len)) {
		/*
		 * The buffers that were written will now be uptodate, so we
		 * don't have to worry about a readpage reading them and
		 * overwriting a partial write. However if we have encountered
		 * a short write and only partially written into a buffer, it
		 * will not be marked uptodate, so a readpage might come in and
		 * destroy our partial write.
		 *
		 * Do the simplest thing, and just treat any short write to a
		 * non uptodate page as a zero-length write, and force the
		 * caller to redo the whole thing.
		 */
		if (!PageUptodate(page))
			copied = 0;

		ext2_page_zero_new_fraction(page, start + copied, start + len);
	}
	flush_dcache_page(page);

	/* This could be a short (even 0-length) commit */
	__ext2_block_commit_write(inode, page, start, start + copied);

	return copied;
}

int ext2_generic_write_end(struct file *file, struct address_space *mapping,
			   loff_t pos, unsigned len, unsigned copied,
			   struct page *page, void *fsdata)
{
	struct inode *inode = mapping->host;
	loff_t old_size = inode->i_size;
	bool i_size_changed = false;

	copied = ext2_block_write_end(file, mapping, pos, len, copied, page,
				      fsdata);

	/*
	 * No need to use i_size_read() here, the i_size cannot change under us
	 * because we hold i_rwsem.
	 *
	 * But it's important to update i_size while still holding page lock:
	 * page writeout could otherwise come in and zero beyond i_size.
	 */
	if (pos + copied > inode->i_size) {
		i_size_write(inode, pos + copied);
		i_size_changed = true;
	}

	unlock_page(page);
	put_page(page);

	if (old_size < pos)
		pagecache_isize_extended(inode, old_size, pos);
	/*
	 * Don't mark the inode dirty under page lock. First, it unnecessarily
	 * makes the holding time of page lock longer. Second, it forces lock
	 * ordering of page lock and transaction start for journaling
	 * filesystems.
	 */
	if (i_size_changed)
		mark_inode_dirty(inode);
	return copied;
}

/* Set all fractions plus the corresponding page cache page dirty */
static int __set_page_dirty_dup_mapping(struct page *page)
{
	struct address_space *mapping = page_mapping(page);
	int newly_dirty;

	if (unlikely(!mapping))
		return !TestSetPageDirty(page);

	/*
	 * Lock out page->mem_cgroup migration to keep PageDirty
	 * synchronized with per-memcg dirty page counters.
	 */
	lock_page_memcg(page);
	newly_dirty = !TestSetPageDirty(page);
	if (newly_dirty) {
		int i;
		struct dup_mapping *map = page_dup_mapping(page);
		unsigned int blocksize = map->blocksize;

		/* All fraction have their dirty bits dirty */
		for (i = 0; i < PAGE_SIZE / blocksize; i++)
			set_bit(i, map->dirty);
		/* Tag the radix tree/XARRAY of the page cache page dirty as well */
		__set_page_dirty(page, mapping, 0);
	}
	unlock_page_memcg(page);

	/* We also need to mark the page's owning I-node dirty if the page changes from non-dirty to dirty */
	if (newly_dirty)
		__mark_inode_dirty(mapping->host, I_DIRTY_PAGES);
	return newly_dirty;
}

int ext2_set_page_dirty(struct page *page)
{
	WARN_ON_ONCE(!PageLocked(page) && !PageDirty(page));
	BUG_ON(!page_has_dup_mapping(page));
	return __set_page_dirty_dup_mapping(page);
}

/* Invalidate some fractions, or the whole page */
void ext2_invalidatepage(struct page *page, unsigned int offset,
			 unsigned int len)
{
	BUG_ON(!PageLocked(page));
	if (!page_has_dup_mapping(page))
		return;

	if (offset == 0 && len == PAGE_SIZE) {
		/* The whole page is invalidated */
		cancel_dirty_page(page);
		/* Release its dup_mapping as well */
		try_to_release_dup_mapping(page);
	} else {
		struct dup_mapping *map = page_dup_mapping(page);
		unsigned int blocksize = map->blocksize;
		unsigned int bbits = block_size_bits(blocksize);
		unsigned int start = offset >> (PAGE_SIZE - bbits);
		unsigned int stop = (offset + len) >> (PAGE_SIZE - bbits);
		unsigned int i;

		for (i = start; i < stop; i++) {
			clear_bit(i, map->dirty);
			clear_bit(i, map->mapped);
		}
	}
}

int ext2_truncate_page(struct address_space *mapping, loff_t from)
{
	pgoff_t index = from >> PAGE_SHIFT;
	unsigned int offset = from & (PAGE_SIZE - 1);
	struct inode *inode = mapping->host;
	sector_t iblock;
	int err = 0;
	unsigned int blocksize, bbits, length, i;
	struct dup_mapping *map;
	struct page *page;

	/* FS blocksize */
	blocksize = i_blocksize(inode);
	/* FS blocksize shifts */
	bbits = inode->i_blkbits;
	/* The FS block of the starting offset within page */
	i = offset >> bbits;
	/* The starting offset within an FS block (will be different than @offset if FS blocksize < page size */
	length = offset & (blocksize - 1);

	/* Do nothing if from is aligned to block boundary */
	if (!length)
		return 0;

	/* We need to Read-modify-write an FS block within a page if the starting offset is not aligned to FS block */

	/* The length to be operated */
	length = blocksize - length;
	/* The I-node data block number */
	iblock = ((sector_t)index << (PAGE_SHIFT - bbits)) + i;

	/* Get the corresponding page cache page, or if we don't have, create one */
	page = grab_cache_page(mapping, index);
	if (!page) {
		err = -ENOMEM;
		goto out;
	}

	/* Create the dup_mapping, if the page doesn't have one */
	map = create_page_dup_mapping(page, inode);
	if (!test_bit(i, map->mapped)) {
		struct ext2_bmpt_map_args args;
		int j;

		WARN_ON(map->blocksize != blocksize);

		/* Bring in the bmap mapping if possible */
		args.iblk = iblock + i;
		args.flags = 0;
		args.len = 1;
		ext2_bmpt_irec_clear(&args.irec);
		err = ext2_bmpt_map_blocks(inode, 0, &args, 0);
		if (err)
			goto unlock;

		if (!(args.flags & EXT2_BMPT_MAP_MAPPED))
			goto unlock;

		for (j = 0; j < map->nr_dups; j++) {
			map->fractions[i].blocks[j] = args.irec.b_blocks[j];
		}
		set_bit(i, map->mapped);
	}

	/* If the page content is already up to date, set the starting fraction not aligned to the FS block up to data as well */
	if (PageUptodate(page))
		set_bit(i, map->uptodate);

	/* If the content within the fraction is not uptodate, read in the FS block */
	if (!test_bit(i, map->uptodate)) {
		ext2_submit_fraction(REQ_OP_READ, 0, map, i, 0, NULL,
				     end_bio_common);
		wait_ongoing_io(map, i);
		if (atomic_read(&map->fractions[i].nr_ioerr) == map->nr_dups) {
			/* If all IO on the FS blocks duplications fails, we bail out */
			err = -EIO;
			goto unlock;
		}
		/* Set the fraction up-to-date */
		set_bit(i, map->uptodate);
	}

	/* Fill the offset starting from @from with zero */
	zero_user(page, offset, length);
	/* Mark the fraction to be dirty */
	ext2_mark_dup_mapping_dirty(map, i);
	err = 0;

unlock:
	unlock_page(page);
	put_page(page);
out:
	return err;
}

int ext2_releasepage(struct page *page, gfp_t gfp_mask)
{
	if (PageWriteback(page) || PageDirty(page))
		return 0;
	try_to_release_dup_mapping(page);
	return 1;
}

/*
 * Check if the page contains any non up-to-date fractions.
 * Return 1 if the page is fully up-to-date (i.e. all fractions are uptodate).
 * Return 0 if the page isn't fully up-to-date (i.e. some or all fractions are not up-to-date)
 */
int ext2_is_partially_uptodate(struct page *page, unsigned long from,
			       unsigned long count)
{
	unsigned int block_start, block_end;
	unsigned int blocksize, bbits;
	unsigned int to;
	struct dup_mapping *map;
	int ret = 1;

	if (!page_has_dup_mapping(page))
		return 0;

	map = page_dup_mapping(page);
	blocksize = map->blocksize;
	bbits = block_size_bits(blocksize);
	to = min_t(unsigned int, PAGE_SIZE - from, count);
	to = from + to;
	if (from < blocksize && to > PAGE_SIZE - blocksize)
		return 0;

	block_start = 0;
	do {
		block_end = block_start + blocksize;
		if (block_end > from && block_start < to) {
			unsigned int i = block_start >> (PAGE_SIZE - bbits);
			if (!test_bit(i, map->uptodate)) {
				ret = 0;
				break;
			}
			if (block_end >= to)
				break;
		}
		block_start = block_end;
	} while (block_start < to);

	return ret;
}

int __init dup_mapping_kmem_init(void)
{
	dup_mapping_kmem =
		kmem_cache_create("ext2_dup_mapping_kmem",
				  sizeof(struct dup_mapping), 0, 0, NULL);
	if (dup_mapping_kmem == NULL)
		return -ENOMEM;
	return 0;
}

void dup_mapping_kmem_exit(void)
{
	kmem_cache_destroy(dup_mapping_kmem);
	dup_mapping_kmem = NULL;
}
