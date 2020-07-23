#ifndef BMPT_H_
#define BMPT_H_

#include <linux/types.h>

#define EXT2_BMPT_N_DUPS 3
struct ext2_bmptrec {
	__le32 b_blocks[EXT2_BMPT_N_DUPS];
	__le32 b_flags;
};

struct ext2_bmptirec {
	uint32_t b_blocks[EXT2_BMPT_N_DUPS];
	uint32_t b_flags;
};

#define EXT2_BMPT_HDR_MAGIC 0xf5e5c5d5
#define EXT2_BMPT_MAXLEVELS 7

struct ext2_bmpthdr {
	__le32 h_magic;
	__le32 h_levels;
	__le32 h_flags;
	struct ext2_bmptrec h_root;
};

struct ext2_bmptihdr {
	uint32_t h_magic;
	uint32_t h_levels;
	uint32_t h_flags;
	struct ext2_bmptirec h_root;
};

#define EXT2_BMPT_HDR_FLAGS_DUP 0x00000001

#define EXT2_BMPT_ADDR_PER_BLOCK(s)                                            \
	(EXT2_BLOCK_SIZE(s) / sizeof(struct ext2_bmptrec))
#define EXT2_BMPT_ADDR_PER_BLOCK_BITS(s)                                       \
	(EXT2_SB(s)->s_bmpt_addr_per_block_bits)

static inline int ext2_bmpt_irec_is_null(struct ext2_bmptirec *rec)
{
	return !rec->b_blocks[0];
}

static inline void ext2_bmpt_irec_clear(struct ext2_bmptirec *rec)
{
	int i;
	for (i = 0; i < EXT2_BMPT_N_DUPS; i++)
		rec->b_blocks[i] = 0;
	rec->b_flags = 0;
}

typedef long ext2_lblk_t;

#define EXT2_BMPT_GET_BLOCK_ALLOCATE 0x1

#define EXT2_BMPT_MAP_NEW (1 << BH_New)
#define EXT2_BMPT_MAP_MAPPED (1 << BH_Mapped)
#define EXT2_BMPT_MAP_UNWRITTEN (1 << BH_Unwritten)
#define EXT2_BMPT_MAP_BOUNDARY (1 << BH_Boundary)
#define EXT2_BMPT_MAP_FLAGS                                                    \
	(EXT2_BMPT_MAP_NEW | EXT2_BMPT_MAP_MAPPED | EXT2_BMPT_MAP_UNWRITTEN |  \
	 EXT2_BMPT_MAP_BOUNDARY)

struct ext2_bmpt_map_args {
	ext2_lblk_t iblk;
	unsigned long len;
	unsigned int flags;
	struct ext2_bmptirec irec;
};

extern int ext2_bmpt_map_blocks(struct inode *inode, int run,
			 struct ext2_bmpt_map_args *map, unsigned int flags);
extern void ext2_bmpt_unmap_blocks(struct inode *inode, int run,
			    unsigned long start_iblk, unsigned long end_iblk);
extern void ext2_bmpt_truncate_blocks(struct inode *inode, loff_t offset);
extern void ext2_bmpt_init_inode(struct inode *inode, int dup_on);

#endif /* BMPT_H_ */
