#include <linux/time.h>
#include <linux/pagemap.h>
#include <linux/quotaops.h>
#include <linux/iomap.h>
#include <linux/uio.h>
#include "ext2.h"

struct ext2_bmpt_path {
	struct inode *inode;
	struct ext2_bmpthdr *hdr;
	struct ext2_bmptihdr ihdr;

	struct dup_buf *dbufs[EXT2_BMPT_MAXLEVELS];
	struct ext2_bmptrec *recs[EXT2_BMPT_MAXLEVELS];
	int p[EXT2_BMPT_MAXLEVELS];
};

static inline int ext2_bmpt_numaddrs_bits(struct super_block *sb)
{
	return EXT2_BMPT_ADDR_PER_BLOCK_BITS(sb);
}

static inline int ext2_bmpt_min_numlevels(struct super_block *sb,
					  ext2_lblk_t iblk)
{
	int i = 0;
	while (iblk) {
		i++;
		iblk >>= ext2_bmpt_numaddrs_bits(sb);
	}
	return i;
}

static inline int ext2_bmpt_offsets(struct super_block *sb, int level,
				    ext2_lblk_t iblk)
{
	int nbits = ext2_bmpt_numaddrs_bits(sb);
	return ((unsigned long)iblk >> (level * nbits)) & ((1 << nbits) - 1);
}

static inline ext2_lblk_t ext2_bmpt_boundary(struct super_block *sb, int level,
					     ext2_lblk_t iblk)
{
	int nbits = ext2_bmpt_numaddrs_bits(sb);
	unsigned int mask = (1 << (level + 1) * nbits) - 1;

	return (unsigned long)iblk | mask;
}

static inline ext2_lblk_t ext2_bmpt_combine_offset(struct super_block *sb,
						   int level,
						   ext2_lblk_t prefix,
						   unsigned int offset)
{
	int nbits = ext2_bmpt_numaddrs_bits(sb);
	unsigned int mask = (1 << (level + 1) * nbits) - 1;

	return ((unsigned long)prefix & ~mask) | (offset << level * nbits);
}

static inline int ext2_bmpt_rec_is_null(struct ext2_bmptrec *rec)
{
	return !rec->b_blocks[0];
}

static inline void ext2_bmpt_rec_clear(struct ext2_bmptrec *rec)
{
	int i;
	for (i = 0; i < EXT2_BMPT_N_DUPS; i++)
		rec->b_blocks[i] = 0;
	rec->b_flags = 0;
}

static inline uint32_t ext2_bmpt_ag_start_bg(struct super_block *sb, int alt)
{
	uint32_t nags;

	nags = EXT2_SB(sb)->s_groups_count / EXT2_BMPT_N_DUPS;
	return nags * alt;
}

static inline ext2_fsblk_t ext2_bmpt_ag_start_fsblk(struct super_block *sb,
						    int alt)
{
	return ext2_group_first_block_no(sb, ext2_bmpt_ag_start_bg(sb, alt));
}

static inline struct ext2_bmpthdr *ext2_bmpt_run(struct inode *inode, int run)
{
	struct ext2_bmpthdr *bhdr =
		(struct ext2_bmpthdr *)EXT2_I(inode)->i_data;
	BUG_ON(run > 1);
	return &bhdr[run];
}

static inline void ext2_bmpt_rec2irec(struct ext2_bmptrec *rec,
				      struct ext2_bmptirec *irec)
{
	memcpy(irec->b_blocks, rec->b_blocks, sizeof(rec->b_blocks));
	le32_to_cpu_array(irec->b_blocks, ARRAY_SIZE(irec->b_blocks));
	irec->b_flags = le32_to_cpu(rec->b_flags);
}

static inline void ext2_bmpt_irec2rec(struct ext2_bmptirec *irec,
				      struct ext2_bmptrec *rec)
{
	memcpy(rec->b_blocks, irec->b_blocks, sizeof(irec->b_blocks));
	cpu_to_le32_array(rec->b_blocks, ARRAY_SIZE(rec->b_blocks));
	rec->b_flags = cpu_to_le32(irec->b_flags);
}

static inline void ext2_bmpt_hdr2ihdr(struct ext2_bmpthdr *hdr,
				      struct ext2_bmptihdr *ihdr)
{
	ihdr->h_magic = le32_to_cpu(hdr->h_magic);
	ihdr->h_levels = le32_to_cpu(hdr->h_levels);
	ihdr->h_flags = le32_to_cpu(hdr->h_flags);
	ext2_bmpt_rec2irec(&hdr->h_root, &ihdr->h_root);
}

static inline void ext2_bmpt_ihdr2hdr(struct ext2_bmptihdr *ihdr,
				      struct ext2_bmpthdr *hdr)
{
	hdr->h_magic = cpu_to_le32(ihdr->h_magic);
	hdr->h_levels = cpu_to_le32(ihdr->h_levels);
	hdr->h_flags = cpu_to_le32(ihdr->h_flags);
	ext2_bmpt_irec2rec(&ihdr->h_root, &hdr->h_root);
}

static int ext2_bmpt_rec_counts(struct super_block *sb,
				struct ext2_bmptihdr *ihdr,
				struct dup_buf *dbuf)
{
	struct ext2_bmptrec *rec = (struct ext2_bmptrec *)dbuf->data;
	int n = EXT2_BMPT_ADDR_PER_BLOCK(sb);
	int i, ret = 0;

	for (i = 0; i < n; i++)
		ret += !ext2_bmpt_rec_is_null(&rec[i]);

	return ret;
}

static ext2_fsblk_t ext2_bmpt_find_near(struct ext2_bmpt_path *path,
					struct dup_buf *dbuf, ssize_t pos,
					int alt)
{
	struct ext2_bmptrec *curr, *start;
	struct ext2_bmptrec *p;
	ext2_fsblk_t bg_start;
	ext2_fsblk_t colour;

	if (dbuf) {
		start = (struct ext2_bmptrec *)dbuf->data;
		curr = start + pos;

		/* Try to find previous block */
		for (p = curr - 1; p >= start; p--) {
			struct ext2_bmptirec irec;

			ext2_bmpt_rec2irec(p, &irec);
			if (irec.b_blocks[alt])
				return irec.b_blocks[alt];
		}

		/* No such thing, so let's try location of indirect block */
		if (dbuf->blocknrs[alt])
			return dbuf->blocknrs[alt];
	}

	/*
	 * It is going to be referred from inode itself? OK, just put it into
	 * the same cylinder group then.
	 */
	if (!alt)
		bg_start = ext2_group_first_block_no(
			path->inode->i_sb, EXT2_I(path->inode)->i_block_group);
	else
		bg_start = ext2_bmpt_ag_start_fsblk(path->inode->i_sb, alt);
	colour = (current->pid % 16) *
		 (EXT2_BLOCKS_PER_GROUP(path->inode->i_sb) / 16);
	return bg_start + colour;
}

static inline ext2_fsblk_t ext2_bmpt_find_goal(struct ext2_bmpt_path *path,
					       ext2_lblk_t iblk,
					       struct dup_buf *dbuf,
					       ssize_t pos, int alt)
{
	struct ext2_block_alloc_info *block_i;

	block_i = EXT2_I(path->inode)->i_block_alloc_info;

	/*
	 * try the heuristic for sequential allocation,
	 * failing that at least try to get decent locality.
	 */
	if (block_i && (iblk == block_i->last_alloc_logical_block + 1) &&
	    (block_i->last_alloc_physical_block != 0)) {
		return block_i->last_alloc_physical_block + 1;
	}

	return ext2_bmpt_find_near(path, dbuf, pos, alt);
}

static unsigned long ext2_bmpt_blks_to_allocate(struct ext2_bmpt_path *path,
						struct dup_buf *dbuf,
						ext2_lblk_t iblk,
						unsigned long numblks)
{
	unsigned long naddrs = EXT2_BMPT_ADDR_PER_BLOCK(path->inode->i_sb);
	unsigned long boundary;
	unsigned long count = 0;
	struct ext2_bmptrec *rec;

	boundary = naddrs - 1 - (iblk & (naddrs - 1));

	/*
	 * Simple case, [t,d]Indirect block(s) has not allocated yet
	 * then it's clear blocks on that path have not allocated
	 */
	if (dbuf == NULL) {
		/* right now don't hanel cross boundary allocation */
		if (numblks < boundary + 1)
			count += numblks;
		else
			count += boundary + 1;
		return count;
	}

	rec = (struct ext2_bmptrec *)dbuf->data;

	count++;
	while (count < numblks && count <= boundary &&
	       ext2_bmpt_rec_is_null(&rec[count])) {
		count++;
	}
	return count;
}

void ext2_bmpt_free_blocks(struct inode *inode, struct ext2_bmptirec *irec)
{
	int i;
	for (i = 0; i < EXT2_BMPT_N_DUPS; i++)
		if (irec->b_blocks[i])
			ext2_free_blocks(inode, irec->b_blocks[i], 1);
}

struct dup_buf *ext2_bmpt_dbuf_read(struct super_block *sb,
				    struct ext2_bmptirec *irec)
{
	sector_t pblks[EXT2_BMPT_N_DUPS];
	int n = EXT2_BMPT_N_DUPS, i, j;
	for (i = 0, j = 0; i < n; i++)
		if (irec->b_blocks[i])
			pblks[j++] = irec->b_blocks[i];
	return dup_buf_read(sb, pblks, j);
}

struct dup_buf *ext2_bmpt_dbuf_get(struct super_block *sb,
				   struct ext2_bmptirec *irec,
				   int main_data_block)
{
	sector_t pblks[EXT2_BMPT_N_DUPS];
	int n = EXT2_BMPT_N_DUPS, i, j;
	for (i = 0, j = 0; i < n; i++)
		if (irec->b_blocks[i])
			pblks[j++] = irec->b_blocks[i];
	return dup_buf_get(sb, pblks, j, main_data_block);
}

static int ext2_bmpt_get_path(struct ext2_bmpt_path *path, ext2_lblk_t iblk,
			      int from_depth, int *null_depth)
{
	int ret = 0;
	int i;
	struct ext2_bmptirec irec;
	int n = EXT2_BMPT_MAXLEVELS - from_depth;

	BUG_ON(from_depth >= EXT2_BMPT_MAXLEVELS);

	if (null_depth)
		*null_depth = from_depth;
	memset(&path->dbufs[from_depth], 0, n * sizeof(path->dbufs[0]));
	memset(&path->p[from_depth], 0, n * sizeof(path->p[0]));
	memset(&path->recs[from_depth], 0, n * sizeof(path->recs[0]));

#ifdef BMPT_DEBUG
	pr_debug("iblk: %ld, from_depth: %d, n: %d, levels: %d\n", iblk,
		 from_depth, n, path->ihdr.h_levels);
#endif

	if (path->ihdr.h_levels <
	    ext2_bmpt_min_numlevels(path->inode->i_sb, iblk))
		return 0;

	if (from_depth) {
		ext2_bmpt_rec2irec(
			&path->recs[from_depth - 1][path->p[from_depth - 1]],
			&irec);
	} else {
		irec = path->ihdr.h_root;
	}

	for (i = from_depth; i < path->ihdr.h_levels; i++) {
		int level = path->ihdr.h_levels - i - 1;
		struct dup_buf *dbuf;

		if (ext2_bmpt_irec_is_null(&irec))
			break;

		dbuf = ext2_bmpt_dbuf_read(path->inode->i_sb, &irec);
		if (dbuf == NULL) {
			ret = -ENOMEM;
			goto cleanup;
		}
		if (dup_buf_all_eio(dbuf)) {
			dup_buf_forget(dbuf);
			ret = -EIO;
			goto cleanup;
		}
		path->dbufs[i] = dbuf;
		path->recs[i] = (struct ext2_bmptrec *)path->dbufs[i]->data;
		path->p[i] = ext2_bmpt_offsets(path->inode->i_sb, level, iblk);
		ext2_bmpt_irec_clear(&irec);
		ext2_bmpt_rec2irec(&path->recs[i][path->p[i]], &irec);
#ifdef BMPT_DEBUG
		pr_debug(
			"[%s] path: %px, i: %d, path->dbufs[i]->block[0]: %llu, path->p[i]: %d, irec: <%u><%u><%u>\n",
			__func__, path, i, path->dbufs[i]->blocknrs[0],
			path->p[i], irec.b_blocks[0], irec.b_blocks[1],
			irec.b_blocks[2]);
#endif
	}
	if (null_depth)
		*null_depth = i;

cleanup:
	if (ret) {
		for (i = from_depth; i < path->ihdr.h_levels; i++) {
			if (path->dbufs[i])
				dup_buf_release(path->dbufs[i]);
		}
		memset(&path->dbufs[from_depth], 0, n * sizeof(path->dbufs[0]));
		memset(&path->p[from_depth], 0, n * sizeof(path->p[0]));
		memset(&path->recs[from_depth], 0, n * sizeof(path->recs[0]));
	}
	return ret;
}

static int ext2_bmpt_fill_path(struct inode *inode, int run, ext2_lblk_t iblk,
			       struct ext2_bmpt_path *path, int *null_depth)
{
	int ret = 0;

	memset(path, 0, sizeof(*path));
	path->inode = inode;
	path->hdr = ext2_bmpt_run(inode, run);
	ext2_bmpt_hdr2ihdr(path->hdr, &path->ihdr);
	if (path->ihdr.h_magic != EXT2_BMPT_HDR_MAGIC)
		return 0;

	ret = ext2_bmpt_get_path(path, iblk, 0, null_depth);
	if (ret)
		memset(path, 0, sizeof(*path));
	return ret;
}

static void ext2_bmpt_free_from_depth(struct ext2_bmpt_path *path,
				      int from_depth, int forget)
{
	int i;

	for (i = from_depth; i < ARRAY_SIZE(path->dbufs); i++) {
		if (!forget)
			dup_buf_release(path->dbufs[i]);
		else
			dup_buf_forget(path->dbufs[i]);
		path->dbufs[i] = NULL;
		path->recs[i] = NULL;
		path->p[i] = 0;
	}
}

static void ext2_bmpt_free_all_path(struct ext2_bmpt_path *path)
{
	ext2_bmpt_free_from_depth(path, 0, 0);
	memset(path, 0, sizeof(*path));
}

static int ext2_bmpt_unmap_ind(struct ext2_bmpt_path *path, int level,
			       unsigned long start_iblk, unsigned long end_iblk)
{
	int err = 0;
	int depth = path->ihdr.h_levels - level - 1;
	unsigned long steps =
		1 << EXT2_BMPT_ADDR_PER_BLOCK_BITS(path->inode->i_sb) * level;
	unsigned long curr_iblk = start_iblk;

	dup_buf_lock(path->dbufs[depth]);
	dup_buf_start_write(path->dbufs[depth]);
	for (; path->p[depth] < EXT2_BMPT_ADDR_PER_BLOCK(path->inode->i_sb);
	     path->p[depth]++, curr_iblk += steps) {
		int delete_curr = 0;
		struct ext2_bmptrec *rec = &path->recs[depth][path->p[depth]];

		if (curr_iblk < start_iblk || curr_iblk > end_iblk ||
		    ext2_bmpt_rec_is_null(rec))
			continue;

#ifdef BMPT_DEBUG
		pr_debug(
			"[%s] level: %d, start_iblk: %lu, end_iblk: %lu, curr_iblk: %lu\n",
			__func__, level, start_iblk, end_iblk, curr_iblk);
#endif

		if (level) {
			if (!path->dbufs[depth + 1]) {
				int t;
				err = ext2_bmpt_get_path(path, curr_iblk,
							 depth + 1, &t);
				if (err)
					break;
			}

			err = ext2_bmpt_unmap_ind(path, level - 1, curr_iblk,
						  curr_iblk + steps - 1);
			if (err)
				break;

			if (!ext2_bmpt_rec_counts(path->inode->i_sb,
						  &path->ihdr,
						  path->dbufs[depth + 1]))
				delete_curr = 1;
		} else {
			delete_curr = 1;
		}

		if (delete_curr) {
			struct ext2_bmptirec irec;
			ext2_bmpt_rec2irec(rec, &irec);

			ext2_bmpt_free_from_depth(path, depth + 1, 1);

			ext2_bmpt_rec_clear(rec);
			ext2_bmpt_free_blocks(path->inode, &irec);
		} else {
			set_dup_buf_uptodate(path->dbufs[depth + 1]);
			mark_dup_buf_dirty_inode(path->dbufs[depth + 1],
						 path->inode);
			ext2_bmpt_free_from_depth(path, depth + 1, 0);
		}
	}
	dup_buf_finish_write(path->dbufs[depth]);
	dup_buf_unlock(path->dbufs[depth]);

	return err;
}

static void ext2_bmpt_increase_indirection(struct ext2_bmpt_path *path,
					   int numlevels_add, int growblks,
					   struct ext2_bmptirec *irecs,
					   struct dup_buf **dbufs)
{
	int i;
	int new_nlevels = path->ihdr.h_levels + numlevels_add;

	for (i = 0; i < growblks; i++) {
		struct dup_buf *dbuf = dbufs[i];
		struct ext2_bmptrec *p = (struct ext2_bmptrec *)dbuf->data;

		dup_buf_lock(dbuf);
		dup_buf_start_write(dbuf);
		memset(dbuf->data, 0, dbuf->size);
		if (i == growblks - 1)
			ext2_bmpt_irec2rec(&path->ihdr.h_root, p);
		else
			ext2_bmpt_irec2rec(&irecs[i + 1], p);
		dup_buf_finish_write(dbuf);
		set_dup_buf_uptodate(dbuf);
		mark_dup_buf_dirty_inode(dbuf, path->inode);
		dup_buf_unlock(dbuf);
	}

	path->ihdr.h_levels = new_nlevels;
	path->ihdr.h_root = irecs[0];
	ext2_bmpt_ihdr2hdr(&path->ihdr, path->hdr);
	mark_inode_dirty(path->inode);
}

static void ext2_bmpt_unmap_blocks_internal(struct ext2_bmpt_path *path,
					    unsigned long start_iblk,
					    unsigned long end_iblk)
{
	int err;

	if (end_iblk < start_iblk)
		return;

	if (!path->ihdr.h_levels) {
		struct ext2_bmptirec irec = path->ihdr.h_root;

		if (start_iblk || ext2_bmpt_irec_is_null(&path->ihdr.h_root))
			return;

		ext2_bmpt_irec_clear(&path->ihdr.h_root);
		ext2_bmpt_ihdr2hdr(&path->ihdr, path->hdr);
		mark_inode_dirty_sync(path->inode);

		ext2_bmpt_free_blocks(path->inode, &irec);
		return;
	}

	err = ext2_bmpt_unmap_ind(path, path->ihdr.h_levels - 1, start_iblk,
				  end_iblk);
	if (err)
		return;
	if (!ext2_bmpt_rec_counts(path->inode->i_sb, &path->ihdr,
				  path->dbufs[0])) {
		struct ext2_bmptirec irec = path->ihdr.h_root;

		ext2_bmpt_free_from_depth(path, 0, 1);

		ext2_bmpt_irec_clear(&path->ihdr.h_root);
		path->ihdr.h_levels = 0;
		ext2_bmpt_ihdr2hdr(&path->ihdr, path->hdr);

		ext2_bmpt_free_blocks(path->inode, &irec);
	} else {
		set_dup_buf_uptodate(path->dbufs[0]);
		mark_dup_buf_dirty_inode(path->dbufs[0], path->inode);
		ext2_bmpt_free_from_depth(path, 0, 0);
	}

	path->inode->i_ctime = current_time(path->inode);
	mark_inode_dirty(path->inode);
}

static void ext2_bmpt_unmap_blocks_locked(struct inode *inode, int run,
					  unsigned long start_iblk,
					  unsigned long end_iblk)
{
	int null_depth;
	struct ext2_bmpt_path path;

	if (end_iblk < start_iblk)
		return;

	if (ext2_bmpt_fill_path(inode, run, start_iblk, &path, &null_depth))
		return;
	if (!null_depth)
		goto cleanup;
	ext2_bmpt_unmap_blocks_internal(&path, start_iblk, end_iblk);
cleanup:
	ext2_bmpt_free_all_path(&path);
}

void ext2_bmpt_unmap_blocks(struct inode *inode, int run,
			    unsigned long start_iblk, unsigned long end_iblk)
{
	struct ext2_inode_info *ei = EXT2_I(inode);

	mutex_lock(&ei->truncate_mutex);
	ext2_bmpt_unmap_blocks_locked(inode, 0, start_iblk, end_iblk);
	ext2_discard_reservation(inode);
	mutex_unlock(&ei->truncate_mutex);
}

static void ext2_bmpt_setup_branch(struct ext2_bmpt_path *path,
				   ext2_lblk_t iblk, int from_depth,
				   struct ext2_bmptirec *irecs,
				   struct dup_buf **dbufs)
{
	int i;
	int nindlvls = path->ihdr.h_levels - from_depth;

	for (i = 0; i < nindlvls; i++) {
		int level = path->ihdr.h_levels - (from_depth + i) - 1;
		struct ext2_bmptrec *recs =
			(struct ext2_bmptrec *)dbufs[i]->data;
		int p = ext2_bmpt_offsets(path->inode->i_sb, level, iblk);

		dup_buf_lock(dbufs[i]);
		dup_buf_start_write(dbufs[i]);
		memset(dbufs[i]->data, 0, dbufs[i]->size);
		if (level)
			ext2_bmpt_irec2rec(&irecs[i + 1], &recs[p]);
		dup_buf_finish_write(dbufs[i]);
		set_dup_buf_uptodate(dbufs[i]);
		mark_dup_buf_dirty_inode(dbufs[i], path->inode);
		dup_buf_unlock(dbufs[i]);
	}
}

static int ext2_bmpt_alloc_dupblocks(struct ext2_bmpt_path *path,
				     struct ext2_bmptirec *goal,
				     struct ext2_bmptirec *new_blocks,
				     int n_dups, unsigned long nrecs)
{
	unsigned long nblks = 0;
	int ret = 0;
	int i;

	/*
	 * To build a branch, we should allocate blocks for
	 * the indirect blocks(if not allocated yet), and at least
	 * the first direct block of this branch.  That's the
	 * minimum number of blocks need to allocate(required)
	 */
	memset(new_blocks, 0, nrecs * sizeof(struct ext2_bmptirec));

#if 0
	for (i = 0; i < n_dups; i++) {
		int target = nrecs;
		int index = 0;

		do {
			ext2_fsblk_t current_block;
			unsigned long count;

			count = target;
			current_block = ext2_new_blocks(
				path->inode, goal->b_blocks[i], &count, &ret);
			if (ret)
				goto failed_out;

			target -= count;
			while (count) {
				new_blocks[index++].b_blocks[i] =
					current_block++;
				nblks++;
				count--;
			}
		} while (target);
	}
#else
	for (i = 0; i < nrecs; i++) {
		uint32_t blks[EXT2_BMPT_N_DUPS];
		unsigned long count = 1;
		int j;

		for (j = 0; j < n_dups; j++) {
			ext2_fsblk_t current_block = ext2_new_blocks(
				path->inode, goal->b_blocks[j], &count, &ret);
			if (ret) {
				while (j--)
					ext2_free_blocks(path->inode, blks[j],
							 1);
				goto failed_out;
			}
			blks[j] = current_block;
		}
		for (j = 0; j < n_dups; j++)
			new_blocks[i].b_blocks[j] = blks[j];
	}
#endif

	path->inode->i_ctime = current_time(path->inode);
	mark_inode_dirty(path->inode);
	return ret;

failed_out:
	for (i = 0; i < nrecs; i++) {
		int j;
		for (j = 0; j < n_dups; j++) {
			if (new_blocks[i].b_blocks[j])
				ext2_free_blocks(path->inode,
						 new_blocks[i].b_blocks[j], 1);
			new_blocks[i].b_blocks[j] = 0;
		}
	}
	if (nblks) {
		path->inode->i_ctime = current_time(path->inode);
		mark_inode_dirty(path->inode);
	}
	return ret;
}

static int ext2_bmpt_alloc_blocks(struct ext2_bmpt_path *path,
				  ext2_fsblk_t goal,
				  struct ext2_bmptirec *new_blocks,
				  unsigned long *numblks)
{
	struct ext2_block_alloc_info *block_i;
	ext2_fsblk_t block;
	int err;
	int i;

	block_i = EXT2_I(path->inode)->i_block_alloc_info;

	block = ext2_new_blocks(path->inode, goal, numblks, &err);
	if (err)
		goto failed_out;
	new_blocks->b_blocks[0] = block;
	for (i = 1; i < EXT2_BMPT_N_DUPS; i++)
		new_blocks->b_blocks[i] = 0;
	new_blocks->b_flags = 0;
#ifdef BMPT_DEBUG
	pr_debug("[%s] alloc goal: %lu, new_blocks: %lu:%lu\n", __func__, goal,
		 block, *numblks);
#endif

	/*
	 * update the most recently allocated logical & physical block
	 * in i_block_alloc_info, to assist find the proper goal block for next
	 * allocation
	 */
	if (block_i) {
		block_i->last_alloc_logical_block = block + *numblks - 1;
		block_i->last_alloc_physical_block =
			new_blocks->b_blocks[0] + *numblks - 1;
	}

	path->inode->i_ctime = current_time(path->inode);
	mark_inode_dirty(path->inode);

	return 0;

failed_out:
	return err;
}

static int ext2_bmpt_linear_map_blocks(struct ext2_bmpt_path *path,
				       struct ext2_bmpt_map_args *map,
				       unsigned int flags)
{
	int can_alloc = flags & EXT2_BMPT_GET_BLOCK_ALLOCATE;
	struct ext2_bmptirec dblkirec;
	struct ext2_sb_info *sbi = EXT2_SB(path->inode->i_sb);

	if (!ext2_bmpt_irec_is_null(&path->ihdr.h_root)) {
		map->irec = path->ihdr.h_root;
		map->len = 1;
		map->flags = EXT2_BMPT_MAP_MAPPED;
		return 0;
	} else if (!can_alloc) {
		ext2_bmpt_irec_clear(&map->irec);
		map->len = 1;
		map->flags = 0;
		return 0;
	}

	if (!(path->ihdr.h_flags & EXT2_BMPT_HDR_FLAGS_DUP)) {
		unsigned long numblks = 1;
		ext2_fsblk_t goal;
		int ret;

		goal = ext2_bmpt_find_goal(path, map->iblk, NULL, 0, 0);
		ret = ext2_bmpt_alloc_blocks(path, goal, &dblkirec, &numblks);
		if (ret)
			return ret;
	} else {
		struct ext2_bmptirec goal;
		int i, ret;

		ext2_bmpt_irec_clear(&goal);
		for (i = 0; i < sbi->s_dupinode_dup_cnt; i++) {
			goal.b_blocks[i] = ext2_bmpt_find_goal(path, map->iblk,
							       NULL, 0, i);
		}

		ret = ext2_bmpt_alloc_dupblocks(path, &goal, &dblkirec,
						sbi->s_dupinode_dup_cnt, 1);
		if (ret)
			return ret;
	}
	path->ihdr.h_root = dblkirec;
	path->inode->i_ctime = current_time(path->inode);
	ext2_bmpt_ihdr2hdr(&path->ihdr, path->hdr);
	mark_inode_dirty(path->inode);

	map->irec = path->ihdr.h_root;
	map->len = 1;
	map->flags = EXT2_BMPT_MAP_NEW | EXT2_BMPT_MAP_MAPPED;
	return 0;
}

static int ext2_bmpt_map_blocks_locked(struct inode *inode, int run,
				       struct ext2_bmpt_map_args *map,
				       unsigned int flags)
{
	int i;
	int ret = 0;
	int null_depth,
		min_level = ext2_bmpt_min_numlevels(inode->i_sb, map->iblk);
	unsigned long need_indblks = 0, need_growblks = 0;
	unsigned long nblks_allocate = 0;
	int can_alloc = flags & EXT2_BMPT_GET_BLOCK_ALLOCATE;
	struct ext2_bmpt_path path;
	struct dup_buf *dbufs[EXT2_BMPT_MAXLEVELS << 1] = { NULL },
						     **dbufsp = dbufs;
	struct ext2_bmptirec irecs[EXT2_BMPT_MAXLEVELS << 1], *irecsp = irecs;
	struct ext2_bmptirec dbirec;
	struct ext2_sb_info *sbi = EXT2_SB(inode->i_sb);
	unsigned long numblks = map->len;
	int l0depth = -1;

	ext2_bmpt_irec_clear(&dbirec);
	memset(irecs, 0, sizeof(irecs));

	if (can_alloc && S_ISREG(inode->i_mode) &&
	    (!EXT2_I(inode)->i_block_alloc_info))
		ext2_init_block_alloc_info(inode);

	ret = ext2_bmpt_fill_path(inode, run, map->iblk, &path, &null_depth);
	if (ret)
		return ret;
	if (!path.ihdr.h_levels && !map->iblk)
		return ext2_bmpt_linear_map_blocks(&path, map, flags);
	BUG_ON(null_depth > path.ihdr.h_levels);

	if (!null_depth) {
		if (!can_alloc) {
			map->flags = 0;
			map->len = 0;
			ext2_bmpt_irec_clear(&map->irec);
			return 0;
		}

		if (!ext2_bmpt_irec_is_null(&path.ihdr.h_root))
			need_growblks = min_level - path.ihdr.h_levels;
		else
			need_growblks = 1;
		need_indblks = min_level - 1;
		nblks_allocate = need_growblks + need_indblks;
		l0depth = min_level - 1;
	} else {
		if (null_depth != path.ihdr.h_levels) {
			need_indblks = path.ihdr.h_levels - null_depth;
			nblks_allocate = need_indblks;
		}
		l0depth = path.ihdr.h_levels - 1;
	}

	if (!can_alloc &&
	    (need_indblks ||
	     ext2_bmpt_rec_is_null(&path.recs[l0depth][path.p[l0depth]]))) {
		map->flags = 0;
		map->len = 0;
		ext2_bmpt_irec_clear(&map->irec);
		ret = 0;
		goto cleanup;
	}

	if (nblks_allocate) {
		int i;
		struct ext2_bmptirec goal;

		ext2_bmpt_irec_clear(&goal);
		for (i = 0; i < EXT2_BMPT_N_DUPS; i++) {
			goal.b_blocks[i] = ext2_bmpt_find_goal(&path, map->iblk,
							       NULL, 0, i);
		}

		ret = ext2_bmpt_alloc_dupblocks(
			&path, &goal, irecs, EXT2_BMPT_N_DUPS, nblks_allocate);
		if (ret)
			goto cleanup;
	} else if (!ext2_bmpt_rec_is_null(
			   &path.recs[l0depth][path.p[l0depth]])) {
		unsigned long naddrs = EXT2_BMPT_ADDR_PER_BLOCK(inode->i_sb);
		unsigned long boundary;
		unsigned long p = path.p[l0depth];
		unsigned long count = 1;
		struct ext2_bmptrec *rec = path.recs[l0depth];

		boundary = naddrs - 1 - (map->iblk & (naddrs - 1));

		ext2_bmpt_rec2irec(&rec[p], &map->irec);
		if (!(path.ihdr.h_flags & path.ihdr.h_flags &
		      EXT2_BMPT_HDR_FLAGS_DUP)) {
			while (count < numblks && count <= boundary &&
			       !ext2_bmpt_rec_is_null(&rec[p + count])) {
				struct ext2_bmptirec t;
				ext2_bmpt_rec2irec(&rec[p + count], &t);
				if (t.b_blocks[0] !=
				    map->irec.b_blocks[0] + count)
					break;
				count++;
			}
		}
		numblks = count;
		map->flags = 0;
		if (count > boundary)
			map->flags = EXT2_BMPT_MAP_BOUNDARY;
		goto get_mapping;
	}

	if (!(path.ihdr.h_flags & EXT2_BMPT_HDR_FLAGS_DUP)) {
		ext2_fsblk_t goal;

		goal = ext2_bmpt_find_goal(&path, map->iblk, NULL, 0, 0);
		numblks = ext2_bmpt_blks_to_allocate(&path, path.dbufs[l0depth],
						     map->iblk, numblks);
		ret = ext2_bmpt_alloc_blocks(&path, goal, &dbirec, &numblks);
		if (ret)
			goto cleanup;
	} else {
		int i;
		struct ext2_bmptirec goal;
		ext2_bmpt_irec_clear(&goal);

		for (i = 0; i < sbi->s_dupinode_dup_cnt; i++) {
			goal.b_blocks[i] = ext2_bmpt_find_goal(&path, map->iblk,
							       NULL, 0, i);
		}

		ret = ext2_bmpt_alloc_dupblocks(&path, &goal, &dbirec,
						sbi->s_dupinode_dup_cnt, 1);
		if (ret)
			goto cleanup;

		numblks = 1;
	}

	for (i = 0; i < nblks_allocate; i++) {
		dbufs[i] = ext2_bmpt_dbuf_get(inode->i_sb, &irecs[i], 0);
		if (dbufs[i] == NULL) {
			ret = -ENOMEM;
			goto cleanup;
		}
#ifdef BMPT_DEBUG
		pr_debug("dbufs[%d] = %px\n", i, dbufs[i]);
#endif
	}

	/* From here on we never fails */

	if (need_growblks) {
		/* The path must be empty in this case */
		ext2_bmpt_increase_indirection(&path,
					       min_level - path.ihdr.h_levels,
					       need_growblks, irecsp, dbufsp);
		null_depth = 1;
		path.dbufs[0] = dbufsp[0];
		path.recs[0] = (struct ext2_bmptrec *)dbufsp[0]->data;
		path.p[0] = ext2_bmpt_offsets(
			inode->i_sb, path.ihdr.h_levels - 1, map->iblk);
		dbufsp[0] = NULL;
		for (i = 1; i < need_growblks; i++) {
			dup_buf_release(dbufsp[i]);
			dbufsp[i] = NULL;
		}

		irecsp += need_growblks;
		dbufsp += need_growblks;
	}

	if (need_indblks) {
		int ind_parent = null_depth - 1;

		BUG_ON(need_indblks != path.ihdr.h_levels - null_depth);

		ext2_bmpt_setup_branch(&path, map->iblk, null_depth, irecsp,
				       dbufsp);
		dup_buf_lock(path.dbufs[ind_parent]);
		dup_buf_start_write(path.dbufs[ind_parent]);
		ext2_bmpt_irec2rec(irecsp,
				   &path.recs[ind_parent][path.p[ind_parent]]);
		dup_buf_finish_write(path.dbufs[ind_parent]);
		set_dup_buf_uptodate(path.dbufs[ind_parent]);
		mark_dup_buf_dirty_inode(path.dbufs[ind_parent], inode);
		dup_buf_unlock(path.dbufs[ind_parent]);

		for (i = 0; i < need_indblks; i++) {
			BUG_ON(!atomic_read(
				&dbufsp[i]
					 ->bhs[dbufsp[i]->main_data_block]
					 ->b_count));
			path.dbufs[null_depth + i] = dbufsp[i];
			path.recs[null_depth + i] =
				(struct ext2_bmptrec *)dbufsp[i]->data;
			path.p[null_depth + i] = ext2_bmpt_offsets(
				inode->i_sb,
				path.ihdr.h_levels - 1 - (null_depth + i),
				map->iblk);
			dbufsp[i] = NULL;
		}
	}

	dup_buf_lock(path.dbufs[l0depth]);
	dup_buf_start_write(path.dbufs[l0depth]);
	for (i = 0; i < numblks; i++) {
		struct ext2_bmptirec t = dbirec;
		int j, n = sbi->s_dupinode_dup_cnt;
		if (!(path.ihdr.h_flags & EXT2_BMPT_HDR_FLAGS_DUP))
			n = 1;
		for (j = 0; j < n; j++) {
			if (t.b_blocks[j])
				t.b_blocks[j] += i;
		}
		ext2_bmpt_irec2rec(&t,
				   &path.recs[l0depth][path.p[l0depth] + i]);
	}
	dup_buf_finish_write(path.dbufs[l0depth]);
	set_dup_buf_uptodate(path.dbufs[l0depth]);
	mark_dup_buf_dirty_inode(path.dbufs[l0depth], inode);
	dup_buf_unlock(path.dbufs[l0depth]);

	mark_inode_dirty(path.inode);

	map->irec = dbirec;
	map->flags = EXT2_BMPT_MAP_NEW;

get_mapping:
	map->len = numblks;
	map->flags |= EXT2_BMPT_MAP_MAPPED;
	ret = 0;
	pr_debug("[%s] iblk %lu -> phys %u:%ld", __func__, map->iblk,
		 map->irec.b_blocks[0], map->len);

cleanup:
	ext2_bmpt_free_all_path(&path);
	for (i = 0; i < nblks_allocate; i++) {
		if (ret) {
			dup_buf_forget(dbufs[i]);
			if (!ext2_bmpt_irec_is_null(&irecs[i]))
				ext2_bmpt_free_blocks(inode, &irecs[i]);
		} else
			dup_buf_release(dbufs[i]);
	}

	if (!ext2_bmpt_irec_is_null(&dbirec) && ret) {
		for (i = 0; i < sbi->s_dupinode_dup_cnt; i++) {
			if (dbirec.b_blocks[i])
				ext2_free_blocks(inode, dbirec.b_blocks[i],
						 numblks);
		}
	}
	return ret;
}

int ext2_bmpt_map_blocks(struct inode *inode, int run,
			 struct ext2_bmpt_map_args *map, unsigned int flags)
{
	struct ext2_inode_info *ei = EXT2_I(inode);
	int ret;

	mutex_lock(&ei->truncate_mutex);
	ret = ext2_bmpt_map_blocks_locked(inode, run, map, flags);
	mutex_unlock(&ei->truncate_mutex);
	return ret;
}

void ext2_bmpt_truncate_blocks(struct inode *inode, loff_t offset)
{
	struct ext2_inode_info *ei = EXT2_I(inode);
	int blocksize = inode->i_sb->s_blocksize;
	unsigned long iblock =
		(offset + blocksize - 1) >> EXT2_BLOCK_SIZE_BITS(inode->i_sb);

	mutex_lock(&ei->truncate_mutex);
	ext2_bmpt_unmap_blocks_locked(inode, 0, iblock, -1ul);
	ext2_discard_reservation(inode);
	mutex_unlock(&ei->truncate_mutex);
}

void ext2_bmpt_init_inode(struct inode *inode, int dup_on)
{
	struct ext2_bmpthdr *hdr = ext2_bmpt_run(inode, 0);
	struct ext2_bmpthdr *dup_hdr = ext2_bmpt_run(inode, 1);

	hdr->h_flags = dup_on ? cpu_to_le32(EXT2_BMPT_HDR_FLAGS_DUP) : 0;
	hdr->h_levels = 0;
	hdr->h_magic = cpu_to_le32(EXT2_BMPT_HDR_MAGIC);
	ext2_bmpt_rec_clear(&hdr->h_root);

	dup_hdr->h_flags = 0;
	dup_hdr->h_levels = 0;
	dup_hdr->h_magic = cpu_to_le32(EXT2_BMPT_HDR_MAGIC);
	ext2_bmpt_rec_clear(&dup_hdr->h_root);

	mark_inode_dirty(inode);
}