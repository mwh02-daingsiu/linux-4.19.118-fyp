#include <linux/buffer_head.h>
#include <linux/pagemap.h>
#include <linux/blk_types.h>
#include <linux/slab.h>
#include "dup_buf.h"

static struct kmem_cache *dup_buf_kmem;

struct dup_buf *dup_buf_read(struct super_block *sb, sector_t *data_blocknrs,
			     int nr_data_blocks)
{
	struct dup_buf *dbuf;
	int i;

	dbuf = kmem_cache_alloc(dup_buf_kmem, GFP_NOFS | GFP_KERNEL);
	if (dbuf == NULL)
		return NULL;
#ifdef DUP_BUF_DEBUG
	pr_debug("[%s] allocating %px\n", __func__, dbuf);
#endif
	memset(dbuf, 0, sizeof(*dbuf));

	dbuf->nr_data_blocks = nr_data_blocks;
	for (i = 0; i < nr_data_blocks; i++) {
		struct buffer_head *bh = sb_getblk(sb, data_blocknrs[i]);
		BUG_ON(!data_blocknrs[i]);
		if (bh == NULL) {
			while (i--)
				brelse(dbuf->bhs[i]);
			goto enomem;
		}
		dbuf->bhs[i] = bh;
		dbuf->blocknrs[i] = data_blocknrs[i];
	}
	dbuf->main_data_block = 0;
	for (i = 0; i < nr_data_blocks; i++)
		lock_buffer(dbuf->bhs[i]);
	for (i = 0; i < nr_data_blocks; i++) {
		int err = bh_submit_read(dbuf->bhs[i]);
		if (!err) {
			dbuf->main_data_block = i;
			break;
		}
		/* re-lock the buffer if it fails */
		lock_buffer(dbuf->bhs[i]);
	}
	if (i == nr_data_blocks) {
		while (i--)
			unlock_buffer(dbuf->bhs[i]);
		goto out;
	}

	for (i = 0; i < dbuf->main_data_block; i++) {
		memcpy(dbuf->bhs[i]->b_data,
		       dbuf->bhs[dbuf->main_data_block]->b_data,
		       sb->s_blocksize);
		set_buffer_uptodate(dbuf->bhs[i]);
		unlock_buffer(dbuf->bhs[i]);
	}
	for (i = dbuf->main_data_block + 1; i < nr_data_blocks; i++) {
		memcpy(dbuf->bhs[i]->b_data,
		       dbuf->bhs[dbuf->main_data_block]->b_data,
		       sb->s_blocksize);
		set_buffer_uptodate(dbuf->bhs[i]);
		unlock_buffer(dbuf->bhs[i]);
	}

out:
	dbuf->data = dbuf->bhs[dbuf->main_data_block]->b_data;
	dbuf->size = sb->s_blocksize;
	return dbuf;
enomem:
	kmem_cache_free(dup_buf_kmem, dbuf);
	return NULL;
}

struct dup_buf *dup_buf_get(struct super_block *sb, sector_t *data_blocknrs,
			    int nr_data_blocks, int main_data_block)
{
	struct dup_buf *dbuf;
	int i;

	dbuf = kmem_cache_alloc(dup_buf_kmem, GFP_NOFS | GFP_KERNEL);
	if (dbuf == NULL)
		return NULL;
#ifdef DUP_BUF_DEBUG
	pr_debug("[%s] allocating %px\n", __func__, dbuf);
#endif
	memset(dbuf, 0, sizeof(*dbuf));

	dbuf->nr_data_blocks = nr_data_blocks;
	for (i = 0; i < nr_data_blocks; i++) {
		struct buffer_head *bh = sb_getblk(sb, data_blocknrs[i]);
		BUG_ON(!data_blocknrs[i]);
		if (bh == NULL) {
			while (i--)
				brelse(dbuf->bhs[i]);
			goto errout;
		}
		dbuf->bhs[i] = bh;
		dbuf->blocknrs[i] = data_blocknrs[i];
	}
	dbuf->main_data_block = main_data_block;

	dbuf->data = dbuf->bhs[dbuf->main_data_block]->b_data;
	dbuf->size = sb->s_blocksize;
	return dbuf;
errout:
	kmem_cache_free(dup_buf_kmem, dbuf);
	return NULL;
}

void dup_buf_start_write(struct dup_buf *dbuf)
{
}

void dup_buf_finish_write(struct dup_buf *dbuf)
{
	int i;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		struct buffer_head *bh = dbuf->bhs[i];

		if (i == dbuf->main_data_block)
			continue;

		memcpy(bh->b_data, dbuf->data, dbuf->size);
	}
}

void dup_buf_lock(struct dup_buf *dbuf)
{
	int i;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		lock_buffer(dbuf->bhs[i]);
	}
}
void dup_buf_unlock(struct dup_buf *dbuf)
{
	int i;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		unlock_buffer(dbuf->bhs[i]);
	}
}

void set_dup_buf_uptodate(struct dup_buf *dbuf)
{
	int i;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		set_buffer_uptodate(dbuf->bhs[i]);
	}
}

void clear_dup_buf_uptodate(struct dup_buf *dbuf)
{
	int i;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		clear_buffer_uptodate(dbuf->bhs[i]);
	}
}
void clear_dup_buf_dirty(struct dup_buf *dbuf)
{
	int i;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		clear_buffer_dirty(dbuf->bhs[i]);
	}
}

void mark_dup_buf_dirty(struct dup_buf *dbuf)
{
	int i;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		mark_buffer_dirty(dbuf->bhs[i]);
	}
}

void mark_dup_buf_dirty_inode(struct dup_buf *dbuf, struct inode *inode)
{
	int i;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		mark_buffer_dirty_inode(dbuf->bhs[i], inode);
	}
}

int dup_buf_sync(struct dup_buf *dbuf)
{
	int i;
	int eio = 0;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		write_dirty_buffer(dbuf->bhs[i], REQ_SYNC);
	}
	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		wait_on_buffer(dbuf->bhs[i]);
		if (buffer_write_io_error(dbuf->bhs[i]))
			eio++;
	}

	return eio;
}

void dup_buf_release(struct dup_buf *dbuf)
{
	int i;
	if (dbuf == NULL)
		return;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		brelse(dbuf->bhs[i]);
		dbuf->bhs[i] = NULL;
	}
#ifdef DUP_BUF_DEBUG
	pr_debug("[%s] freeing %px\n", __func__, dbuf);
#endif
	kmem_cache_free(dup_buf_kmem, dbuf);
}
void dup_buf_forget(struct dup_buf *dbuf)
{
	int i;
	if (dbuf == NULL)
		return;

	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		bforget(dbuf->bhs[i]);
		dbuf->bhs[i] = NULL;
	}
#ifdef DUP_BUF_DEBUG
	pr_debug("[%s] freeing %px\n", __func__, dbuf);
#endif
	kmem_cache_free(dup_buf_kmem, dbuf);
}

int __init dup_buf_kmem_init(void)
{
	dup_buf_kmem = kmem_cache_create("ext2_dup_buf_kmem",
					 sizeof(struct dup_buf), 0, 0, NULL);
	if (dup_buf_kmem == NULL)
		return -ENOMEM;
	return 0;
}

void dup_buf_kmem_exit(void)
{
	kmem_cache_destroy(dup_buf_kmem);
	dup_buf_kmem = NULL;
}