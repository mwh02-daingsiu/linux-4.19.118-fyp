#ifndef DUP_BUF_H_
#define DUP_BUF_H_

#include <linux/buffer_head.h>
#include <linux/err.h>
#include <linux/fs.h>
#include <linux/pagemap.h>

struct dup_buf {
	void *data;
	size_t size;

	int main_data_block;

	int nr_data_blocks;
	sector_t blocknrs[4];
	struct buffer_head *bhs[4];
};

/* Only use this after a read attempt */
static inline int dup_buf_all_eio(struct dup_buf *dbuf)
{
	return !buffer_uptodate(dbuf->bhs[dbuf->main_data_block]);
}

static inline int dup_buf_all_write_eio(struct dup_buf *dbuf)
{
	int i;
	for (i = 0; i < dbuf->nr_data_blocks; i++) {
		if (!buffer_write_io_error(dbuf->bhs[i]))
			break;
	}
	return i == dbuf->nr_data_blocks;
}

extern struct dup_buf *dup_buf_read(struct super_block *sb, sector_t *data_blocknrs,
			     int nr_data_blocks);

extern struct dup_buf *dup_buf_get(struct super_block *sb, sector_t *data_blocknrs,
			    int nr_data_blocks, int main_data_block);

extern void dup_buf_start_write(struct dup_buf *dbuf);
extern void dup_buf_finish_write(struct dup_buf *dbuf);

extern void dup_buf_lock(struct dup_buf *dbuf);
extern void dup_buf_unlock(struct dup_buf *dbuf);

extern void set_dup_buf_uptodate(struct dup_buf *dbuf);
extern void clear_dup_buf_uptodate(struct dup_buf *dbuf);
extern void clear_dup_buf_dirty(struct dup_buf *dbuf);

extern void mark_dup_buf_dirty(struct dup_buf *dbuf);
extern void mark_dup_buf_dirty_inode(struct dup_buf *dbuf, struct inode *inode);

extern int dup_buf_sync(struct dup_buf *dbuf);

extern void dup_buf_release(struct dup_buf *dbuf);
extern void dup_buf_forget(struct dup_buf *dbuf);

extern int __init dup_buf_kmem_init(void);
extern void dup_buf_kmem_exit(void);

#endif /* DUP_BUF_H_ */