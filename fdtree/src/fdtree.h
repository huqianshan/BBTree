/*-------------------------------------------------------------------------
 *
 * fdtree.h
 *	  header file for buffer manager implementation.
 *
 * The license is a free non-exclusive, non-transferable license to reproduce, 
 * use, modify and display the source code version of the Software, with or 
 * without modifications solely for non-commercial research, educational or 
 * evaluation purposes. The license does not entitle Licensee to technical support, 
 * telephone assistance, enhancements or updates to the Software. All rights, title 
 * to and ownership interest in the Software, including all intellectual property rights 
 * therein shall remain in HKUST.
 *
 * IDENTIFICATION
 *	  FDTree: fdtree.h, 2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#ifndef _FDTREE_H_
#define _FDTREE_H_

#include "error.h"
#include "storage.h"
#include "btree.h"

#define MAX_LEVELS 10
#define HEADTREE_PAGES_BOUND 256
#define FDTREE_MAX_BLOCKS 100000000
#define FD_TREE 0
#define RESULT_SET_CAPACITY 10
#define FD_TREE_HEAD_OCCUPANCY (0.7)

#define ENTRY_IS_FENCE(e) (((e)->ptr & 0x80000000) != 0)
#define ENTRY_IS_FILTER(e) (((e)->ptr & 0x40000000) != 0)
#define ENTRY_IS_INTERNAL(e) (((e)->ptr & 0x20000000) != 0)
#define ENTRY_SET_INTERNAL(e) ((e)->ptr |= 0xa0000000)
#define ENTRY_SET_NON_INTERNAL(e) ((e)->ptr &= 0xdfffffff)
#define ENTRY_SET_FENCE(e) ((e)->ptr |= 0x80000000)
#define ENTRY_SET_NON_FENCE(e) ((e)->ptr &= 0x7fffffff)
#define ENTRY_SET_FILTER(e) ((e)->ptr |= 0x40000000)
#define ENTRY_SET_NON_FILTER(e) ((e)->ptr &= 0xbfffffff)
#define ENTRY_GET_PTR(e) ((e)->ptr & 0x1fffffff)
#define ENTRY_EQUAL_PTR(e1, e2)	(ENTRY_GET_PTR(e1) == ENTRY_GET_PTR(e2))


#define FDTREE_SLEEP_INTERVAL (100)

#define RANDOM() ((rand() << 15) + rand())

struct FDTree;

typedef struct FDLevel
{
	int fid;	//file id
	FilePtr fhdl;
	int maxBlock;
	int nEntry;
	int nBlock;
	//statistics
	int nMerge;
	int nAccMerge;
} FDLevel;

typedef struct FDLevelState
{
	int type;
	FDTree * tree;
	int fid;
	FilePtr fhdl;
	
	Page curPage;
	PageHead * curPageHead;
	Entry * curEntries;
	int curOffset;
	int maxOffset;	//only used for read
	int blksz;		//only used for read
	int level;
	int nPage;
	int nEntry;
	int nextFencePtr;
	struct FDLevelState * nextState;	//only used for write
	char buf[MBLKSZ];
} FDLevelState;

typedef struct FDMergeState
{
	FDTree * tree;
	int nMergeLevel;
	int nMergeEntry;
	int nStepEntry;
	FDLevelState * state1;
	FDLevelState * state2;
	FDLevelState * state_out;
	Entry * e1;
	Entry * e2;
	int lid;
	bool finish;
} FDMergeState;

typedef struct FDTreeResultSet
{
	int num;
	Entry results[RESULT_SET_CAPACITY];
	bool valid[RESULT_SET_CAPACITY];
	struct FDTreeResultSet * next;
} FDTreeResultSet;

typedef struct FDTree
{
	int n;
	int k;	//size ratio
	int nLevel;	//the current number of levels
//	int adaptLevel;	//the adaptive number of levels
	FDLevel levels[MAX_LEVELS];
	FDLevel tmplevels[MAX_LEVELS];
	BTree headtree;
	BTree tmptree;	//used when merging
	
	FDLevelState * curState;
	FDMergeState * curMergeState;

	bool deamortizedMode;

	FDTreeResultSet resultSet;

	//for range search
	BlockNum curPID;
	int curOffset;
} FDTree;


FDTree * fdtree_bulkload(unsigned int num, int k, char * fname);
int fdtree_point_search(FDTree * tree, Key key, Entry * e = NULL);
int fdtree_search_next(FDTree * tree, Entry * e);
int fdtree_insert(FDTree * tree, Entry entry);
//int fdtree_insert(FDTree * tree, Entry entry, bool insertToPrimaryTree = true);
int fdtree_delete(FDTree * tree, Entry entry);
void fdtree_try(FDTree * tree, int lid);
int fdtree_range_query(FDTree * tree, Key key1, Key key2);
FDTree * fdtree_bulkload_data(long num, int r, bool deamortize, double * ratio);
int fdtree_set_mbuf(int size);
int fdtree_merge_data(FDTree * tree, int lid, double ratio, int range);

int fdtree_checkfile(char * fname, unsigned int * num);
int fdtree_generatefile(char * fname, unsigned int num);
int fdtree_checkLevel(FDTree * tree, int lid);

//void fdtree_deamortizedInit(FDTree * tree);
int fdtree_deamortized_insert(FDTree * tree, Entry entry);
//int fdtree_deamortizedDelete(FDTree * tree, Entry entry);
int fdtree_deamortized_search(FDTree * tree, Key key);

int fdmodel_estimate(int n, int memory, double p, char * fname);

#endif