/*-------------------------------------------------------------------------
 *
 * rwset.h
 *
 * src/include/access/rwset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RWSET_H
#define RWSET_H

#include "postgres.h"
#include "lib/ilist.h"
#include "storage/block.h"
#include "storage/itemptr.h"

/*
 * Header of the read/write set
 */
typedef struct RWSetHeader
{
	Oid			dbid;
	TransactionId xid;
} RWSetHeader;

/*
 * A page in the read set
 */
typedef struct RWSetPage
{
	BlockNumber blkno;
	int			csn;

	dlist_node	node;
} RWSetPage;

/*
 * A tuple in the read set
 */
typedef struct RWSetTuple
{
	ItemPointerData tid;

	dlist_node	node;
} RWSetTuple;

/*
 * A relation in the read set
 * If you change this struct, also consider changing RxReorderBufferRelation
 */
typedef struct RWSetRelation
{
	Oid			relid;
	bool		is_index;
	int			csn;
	dlist_head	pages;
	dlist_head	tuples;

	dlist_node	node;
} RWSetRelation;

/*
 * A read/write set
 */
typedef struct RWSet
{
	MemoryContext context;
	RWSetHeader header;

	dlist_head	relations;
} RWSet;

extern RWSet *RWSetAllocate(void);
extern void RWSetFree(RWSet *rwset);
extern void RWSetDecode(RWSet *rwset, StringInfo msg);
extern char *RWSetToString(RWSet *rwset);

#endif							/* RWSET_H */
