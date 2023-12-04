/*-------------------------------------------------------------------------
 *
 * btree.h
 *	  Header file for B-tree implementation
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
 *	  FDTree: btree.h,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#ifndef _BTREE_H_
#define _BTREE_H_

#include "storage.h"

#define B_TREE 1
#define BTREE_INIT_SPACE_RATE 0.7
#define BTREE_MAX_LEVEL 10

struct FDTree;

typedef struct BTree
{
	int fid[BTREE_MAX_LEVEL];	//file id
	HANDLE fhdl[BTREE_MAX_LEVEL];
	int levelBlock[BTREE_MAX_LEVEL];
	int maxBlock;
	int maxEntry;
	int nEntry;
	int nBlock;
	int nLevel;
	int root;
	int nInitEntry;
//	HANDLE hStateMutex;
	char buf[MBLKSZ];
} BTree;

typedef struct BTreeState
{
	int type;
	FDTree * fdtree;
	int fid;
	HANDLE fhdl;
	char buf[BLKSZ];
	char mbuf[MBLKSZ];
	Page curPage;

	PageHead * curPageHead;
	Entry * curEntries;
	int curOffset;
	BlockNum pid;
	int level;						//0:leaf level, n: root
	bool forceWrite;
	bool forceRead;

	struct BTree * tree;
	struct BTreeState * nextState;	//only used for write
} BTreeState;

void btree_initEmpty(BTree * tree);
BTreeState * btree_initEmptyState(BTree * tree);
BTreeState * btree_initRead(BTree * tree, bool forceRead = false);
Entry * btree_readNextEntry(BTreeState * state);
BTreeState * btree_initWrite(BTree * tree, bool forceWrite = false);
int btree_writeNextEntry(BTreeState * state, Entry * entry, double initSpaceRate = BTREE_INIT_SPACE_RATE);
int btree_replaceTree(BTree * oldTree, BTree * newTree, BTreeState * state);
void btree_flush(BTreeState * state);
BTree * btree_bulkload(unsigned int num, double initSpaceRate, char * fname);

BlockNum btree_point_query(BTree * tree, Key key, Entry * entry = NULL);
int btree_range_query(BTree * tree, Key key1, Key key2);
int btree_insert(BTree * tree, Entry entry);
int btree_delete(BTree * tree, Entry entry);
int btree_update(BTree * tree, Key key, int val);
int btree_update_pointer(BTree * tree, Entry * e);
int btree_updateAdditionFence(BTree * tree);
int btree_copyBTree(BTree * tree1, BTree * tree2);

int btree_checkLevel(BTree * tree, int maxBlock);
void btree_try(BTree * tree);
void btree_printPageInfo(BTree * tree);

///////////////////////////////////////////////
/////////	internal routine		///////////
///////////////////////////////////////////////

int btree_inPageSearch(Page page, Key key);
void btree_inPageInsert(Page page, Entry entry);
int btree_inPageDelete(Page page, Entry entry);
void btree_split(BTree * tree, Page page, int level, int * trace);

#endif