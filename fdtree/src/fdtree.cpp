/*-------------------------------------------------------------------------
 *
 * fdtree.cpp
 *	  FD-tree interface routines
 *
 * The license is a free non-exclusive, non-transferable license to reproduce,
 * use, modify and display the source code version of the Software, with or
 * without modifications solely for non-commercial research, educational or
 * evaluation purposes. The license does not entitle Licensee to technical
 *support, telephone assistance, enhancements or updates to the Software. All
 *rights, title to and ownership interest in the Software, including all
 *intellectual property rights therein shall remain in HKUST.
 *
 * IDENTIFICATION
 *	  FDTree: fdtree.cpp,2011/01/02 yinan
 *
 *-------------------------------------------------------------------------
 */

/*
 * Principal entry points:
 *
 * fdtree_bulkload() -- bulkload entres into a fdtree.
 *
 * fdtree_point_search() -- perform a point search on a fdtree.
 *
 * fdtree_range_search() -- perform a range search on a fdtree.
 *
 * fdtree_insert() -- insert a entry into a fdtree.
 *
 * fdtree_delete() -- delete a entry from a fdtree.
 *
 * See also these files:
 *		btree.c -- the head tree implementation
 */

#include "fdtree.h"

#include <assert.h>
#include <math.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <fstream>

#include "buffer.h"
#include "storage.h"

using namespace std;

// #define SIMPLE_DEAMORTIZATION
// #define FDTREE_CHECK_MODE

// the struct for multiple I/Os of range query
int MAX_MBUF_NUM = 64;
char mbuf[128 * BLKSZ];

/*
 * internal routine:
 */
int fdtree_init(FDTree *tree);
FDLevelState *fdtree_initReadLevel(FDTree *tree, int lid);
FDLevelState *fdtree_initWriteLevel(FDTree *tree, int lid);
FDLevelState *fdtree_initWriteUpperLevels(FDTree *tree, int lid);
Entry *fdtree_readNextEntry(FDLevelState *state);
int fdtree_writeNextEntry(FDLevelState *state);
int fdtree_inPageSearch(Page page, Key key);
int fdtree_merge(FDTree *tree, int lid);
BlockNum fdtree_headtree_query(BTree *tree, Key key,
                               FDTreeResultSet *set = NULL);

int fdtree_deamortized_merge_init(FDTree *tree);
int fdtree_deamortized_merge_step(FDTree *tree);
int fdtree_deamortized_merge_finish(FDTree *tree);

int fdtree_set_mbuf(int size) {
  MAX_MBUF_NUM = size;
  return size;
}

void fdtree_print(FDTree *tree) {
  printf("\n############ FD-Index ###############\n");
  printf("Level 0: %d / %d; fid:%p\n", tree->headtree.nBlock,
         tree->headtree.maxBlock, tree->headtree.fid);
  for (int i = 1; i < tree->nLevel; i++) {
    printf("Level %d: %d / %d; fid:%d\n", i, tree->levels[i].nBlock,
           tree->levels[i].maxBlock, (int)tree->levels[i].fid);
  }
  printf("\n");
}

FDLevelState *fdtree_initTmpLevel(FDTree *tree, int lid) {
  if (lid == 0)  // initialize the head tree
    return NULL;

  FDLevelState *state = new FDLevelState;
  state->fid = tree->tmplevels[lid].fid;
  state->fhdl = tree->tmplevels[lid].fhdl;
  //	state->fhdl = file_open(tree->levels[lid].fid, true);

  // read the first block
  file_seek(state->fhdl, 0);
  state->blksz = file_read(state->fhdl, state->buf, MBLKSZ);

  state->nPage = state->blksz / BLKSZ;
  state->type = FD_TREE;
  state->curPage = (Page)state->buf;
  state->curPageHead = PAGE_HEAD(state->curPage);
  state->curEntries = PAGE_DATA(state->curPage, Entry);
  state->maxOffset = PAGE_NUM(state->curPage);
  ;
  state->curOffset = 0;
  state->level = lid;
  state->nEntry = tree->tmplevels[lid].nEntry;
  return state;
}

/*
 *	fdtree_initReadLevel() --
 *	Preparing for scaning a specific level.
 *	Allocate and initialize a FDLevelState structure. The returned
 *	structure is suitable for immediate use by fdtree_readNextEntry().
 */

FDLevelState *fdtree_initReadLevel(FDTree *tree, int lid) {
  FDLevelState *state;

  if (lid == 0)  // initialize the head tree
    return (FDLevelState *)btree_initRead(&(tree->headtree));

  state = new FDLevelState;

  /*
   *	initialize the level state info
   */
  state->type = FD_TREE;
  state->level = lid;
  state->nEntry = tree->levels[lid].nEntry;
  state->fid = tree->levels[lid].fid;
  state->fhdl = tree->levels[lid].fhdl;

  /*
   *	reset the file pinter
   */
  file_seek(state->fhdl, 0);

  /*
   *	initalize the current page info
   */
  state->nPage = 0;
  state->blksz = 0;
  state->curPage = (Page)state->buf;
  state->curOffset = 0;
  state->maxOffset = 0;

  return state;
}

/*
 *	fdtree_initWriteLevel() --
 *	Preparing for scaning a specific level.
 *	Allocate and initialize a FDLevelState structure. The returned
 *	structure is suitable for immediate use by fdtree_readNextEntry().
 */

FDLevelState *fdtree_initWriteLevel(FDTree *tree, int lid) {
  FDLevelState *state;

  if (lid == 0)  // initialize the head tree
  {
    if (tree->deamortizedMode)  // don't need to build a new head tree to save
                                // entries.
      state = (FDLevelState *)btree_initEmptyState(&tree->tmptree);
    else
      state = (FDLevelState *)btree_initWrite(&(tree->tmptree));
    ((BTreeState *)state)->fdtree = tree;
    return state;
  }

  state = new FDLevelState;

  /*
   *	allocate the file id, and create the file
   */
  state->fid = file_get_new_fid();
  state->fhdl = file_open(state->fid, false);
  file_seek(state->fhdl, 0);

  /*
   *	initialize the level state
   */
  state->type = FD_TREE;
  state->nPage = 0;
  state->nEntry = 0;
  state->blksz = 0;
  state->level = lid;
  state->nextState = NULL;
  state->nextFencePtr = 0;

  /*
   *	initialize the current page state
   */
  state->curPage = (Page)state->buf;
  state->curPageHead = PAGE_HEAD(state->curPage);
  state->curEntries = PAGE_DATA(state->curPage, Entry);
  state->maxOffset = 0;
  state->curOffset = 0;

  state->tree = tree;

  return state;
}

/*
 *	fdtree_initWriteUpperLevels() --
 *	prepare for writing one new level and all its upper levels.
 *	All levels are linked by nextState field in FDLevelState structure
 *	from bottom up.
 */

FDLevelState *fdtree_initWriteUpperLevels(FDTree *tree, int lid) {
  int i;
  FDLevelState *curState, *preState;

  preState = NULL;
  for (i = 0; i <= lid; i++) {
    curState = fdtree_initWriteLevel(tree, i);
    curState->nextState = preState;
    preState = curState;
  }
  return curState;
}

/*
 *	fdtree_replaceLevels() --
 *	replace the old levels with newly generated levels, and delete
 *	the old levels.
 */

int fdtree_replaceLevels(FDTree *tree, FDLevelState *state, int lid) {
  int i;
  FDLevelState *tmp;

  assert(state->level == lid);

  /*
   *	iterate for lid times, from bottom up
   */
  for (i = lid; i >= 1; i--) {
    /*
     *	delete the data file of the old level
     */
    if (tree->levels[i].fhdl != INVALID_FILE_PTR)
      file_delete(tree->levels[i].fhdl, tree->levels[i].fid);

    /*
     *	replace the old level with the new level
     */
    tree->levels[i].fhdl = state->fhdl;
    tree->levels[i].fid = state->fid;
    tree->levels[i].maxBlock =
        (int)ceil(HEADTREE_PAGES_BOUND * FD_TREE_HEAD_OCCUPANCY) *
        int(pow(double(tree->k), i));
    tree->levels[i].nBlock = state->nPage;
    tree->levels[i].nEntry = state->nEntry;

    tmp = state->nextState;
    delete state;
    state = tmp;
  }
  /*
   *	The lowest level in a FD-tree has no limitation on the capacity.
   */
  tree->levels[tree->nLevel - 1].maxBlock = FDTREE_MAX_BLOCKS;

  /*
   *	replace the old head tree with the new head tree, and delete
   *	the old one.
   */
  btree_replaceTree(&tree->headtree, &tree->tmptree, (BTreeState *)state);

  return 1;
}

/*
 *	fdtree_readNextEntry() --
 *	reutrn the next entry in a level, or return NULL if we
 *	get the end of the level.
 *	We maintain a buffer of a multi-page block for reading
 *	the entries. The size of the access unit is MBLKSZ here.
 *	We directly read the blocks from the data files, skiping
 *	the buffer manager layout.
 *
 *	caller must call fdtree_initReadLevel() to allocate and
 *	initialize the FDLevelState structure.
 */

Entry *fdtree_readNextEntry(FDLevelState *state) {
  if (state->type == B_TREE) return btree_readNextEntry((BTreeState *)state);

  /*
   *	if all entries in the current page has been read,
   *	move to the next page.
   */
  if (state->curOffset >= state->maxOffset) {
    // move the next page in the multi-page block
    state->curPage += BLKSZ;

    /*
     *	if all pages in the multi-page block is exhausted,
     *	read the next block.
     */
    if (state->curPage - state->buf >= state->blksz) {
      // read the next multi-page block into the buffer
      file_seek(state->fhdl, state->nPage);
      state->blksz = file_read(state->fhdl, state->buf, MBLKSZ);
      assert(state->blksz % BLKSZ == 0);
      state->nPage += state->blksz / BLKSZ;
      state->curPage = (Page)state->buf;

      // return NULL if we get the end of the level
      if (state->blksz == 0) return NULL;
    }

    // initialize the current page info
    state->curPageHead = PAGE_HEAD(state->curPage);
    state->curEntries = PAGE_DATA(state->curPage, Entry);
    state->maxOffset = PAGE_NUM(state->curPage);
    state->curOffset = 0;

    //		assert(state->curEntries[state->curOffset].key >= temp.key);
  }

  return &state->curEntries[state->curOffset++];
}

/*
 *	fdtree_writeNextEntry() --
 *	append an entry to a level.
 *	We maintain a buffer of a multi-page block for writing
 *	the entries. The size of the access unit is MBLKSZ here.
 *	We directly write the blocks to the data files, skiping
 *	the buffer manager layout.
 *	Internal fences are inserted to the last slots of pages,
 *	if the second last slot doesn't contain a (external)
 *	fence.
 *	We recursively call this function to add external fences
 *	into its upper levels.
 *
 *	caller must call fdtree_initWriteLevel() to allocate and
 *	initialize the FDLevelState structure.
 */

int fdtree_writeNextEntry(FDLevelState *state, Entry *entry) {
  if (state->type == B_TREE) {
    if (state->tree->deamortizedMode == true) {
      // deamortized mode
      int ret = 0;
      assert(ENTRY_IS_FENCE(entry));
      ret = btree_insert(((BTreeState *)state)->tree, *entry);
      return ret;
    } else
      return btree_writeNextEntry((BTreeState *)state, entry);
  }

  /*
   *	The last slot in a page is reserved for an internal entry. If the
   *	second last slot contains a normal index entry, we insert an
   *	internal fence into the last slot. Otherwise, the last slot is
   *	remained empty.
   */
  if (state->curOffset >= PAGE_MAX_NUM(Entry) - 1) {
    /*
     *	check whether the second last slot contains a fence.
     *	If so, insert an internal fence.
     */
    if (!ENTRY_IS_FENCE(state->curEntries + state->curOffset - 1)) {
      /*
       *	The key value of the internal fence is set to be the key of
       *	the last normal index entry (in the second last slot).
       *	The pointer filed is set to be same as the next external fence
       *	in the level.
       */
      state->curEntries[state->curOffset].key =
          state->curEntries[state->curOffset - 1].key;
      state->curEntries[state->curOffset].ptr = state->nextFencePtr;
      ENTRY_SET_NON_FILTER(state->curEntries + state->curOffset);
      ENTRY_SET_FENCE(state->curEntries + state->curOffset);
      ENTRY_SET_INTERNAL(state->curEntries + state->curOffset);

      elog(DEBUG3,
           "[DEBUG] add an internal fence at block %d. Fence (%d, %d)\n",
           state->nPage, state->curEntries[state->curOffset].key,
           ENTRY_GET_PTR(state->curEntries + state->curOffset));

      state->curOffset++;
    }

    /*
     *	update the page head info
     */
    state->curPageHead->num = state->curOffset;
    state->curPageHead->level = state->level;
    state->curPageHead->pid = state->nPage;

    /*
     *	Insert a external fence into the immediate upper level. The
     *	key value of the external fence is set to be the key of the
     *	last entry in the page. The pointer is set to be the page id
     *	of the current page.
     */
    if (state->nextState != NULL) {
      Entry fence;
      fence.key = state->curEntries[state->curOffset - 1].key;
      fence.ptr = state->nPage;

      ENTRY_SET_NON_FILTER(&fence);
      ENTRY_SET_FENCE(&fence);
      ENTRY_SET_NON_INTERNAL(&fence);

      fdtree_writeNextEntry(state->nextState, &fence);
    }

    /*
     *	Move to the next page in the multi-page block buffer.
     */
    state->curPage += BLKSZ;
    state->nPage++;
    state->curPageHead = PAGE_HEAD(state->curPage);
    state->curEntries = PAGE_DATA(state->curPage, Entry);
    state->curOffset = 0;

    /*
     *	If the write buffer is full, write the pages in the
     *	buffer into the data file.
     */
    if (state->curPage - state->buf >= MBLKSZ) {
      /*
       *	seek and write multi-page block.
       */
      file_seek(state->fhdl,
                state->nPage - (state->curPage - state->buf) / BLKSZ);
      file_write(state->fhdl, state->buf, MBLKSZ);

      /*
       *	Reset the current page to the first page in the buffer.
       */
      state->curPage = (Page)state->buf;
      state->curPageHead = PAGE_HEAD(state->curPage);
      state->curEntries = PAGE_DATA(state->curPage, Entry);
      state->curOffset = 0;
    }
  }

  /*
   *	The entry is directly written to the current slot of the page
   *	in multi-page buffer.
   */
  state->curEntries[state->curOffset] = *entry;
  if (state->curOffset > 0)
    assert(state->curEntries[state->curOffset].key >=
           state->curEntries[state->curOffset - 1].key);
  assert(!ENTRY_IS_INTERNAL(state->curEntries + state->curOffset));

  if (ENTRY_IS_FENCE(state->curEntries + state->curOffset)) {
    /*
     *	update the pointer of the next fence. Since the pointer values
     *	of all external fences in a level are continuous. The pointer
     *	of the next fence is given by the pointer of the current fence
     *	plus 1.
     */
    state->nextFencePtr =
        ENTRY_GET_PTR(state->curEntries + state->curOffset) + 1;

    elog(DEBUG3,
         "[DEBUG] update addition fence.Fence: (%d, %d). ADDITION: %d\n",
         state->curEntries[state->curOffset].key,
         ENTRY_GET_PTR(state->curEntries + state->curOffset),
         ENTRY_IS_INTERNAL(state->curEntries + state->curOffset));
  }

  state->nEntry++;
  state->curOffset++;

  return 0;
}

/*
 *	fdtree_flush() --
 *	flush all entries in the write buffer in a level.
 *	An internal fence is inserted to the last slot of the current
 *	page, if the second last slot doesn't contain a (external)
 *	fence.
 *	We recursively call this function to flush entries in its
 *	immediately upper level, following the link lists in
 *	FDLevelState structure.
 *
 *	caller must call fdtree_initWriteLevel() to allocate and
 *	initialize the FDLevelState structure, and then call
 *	fdtree_writeNextEntry() to write entries.
 */

void fdtree_flush(FDLevelState *state) {
  if (state->type == B_TREE) {
    if (!state->tree->deamortizedMode) {
      // insert a entry with max key value to guarantee the correctness
      Entry entry;
      entry.key = MAX_KEY_VALUE;
      entry.ptr = INVALID_PAGE;
      ENTRY_SET_INTERNAL(&entry);
      btree_writeNextEntry((BTreeState *)state, &entry);

      btree_flush((BTreeState *)state);
      //		file_flush((state)->tree->fhdl);
    }
    return;
  }

  /*
   *	check whether the last entry is a fence with the max key value.
   *	If not, insert an internal fence.
   */
  if (!(ENTRY_IS_FENCE(state->curEntries + state->curOffset - 1) &&
        state->curEntries[state->curOffset - 1].key == MAX_KEY_VALUE)) {
    /*
     *	The key value of the internal fence is set to be max key
     *	value to guide the entries with large key that may be merged
     *	to this level in the future.
     *	The pointer filed is set to be same as the next external fence
     *	in the level.
     */
    state->curEntries[state->curOffset].key = MAX_KEY_VALUE;
    state->curEntries[state->curOffset].ptr = state->nextFencePtr;
    ENTRY_SET_NON_FILTER(state->curEntries + state->curOffset);
    ENTRY_SET_FENCE(state->curEntries + state->curOffset);
    ENTRY_SET_INTERNAL(state->curEntries + state->curOffset);

    elog(DEBUG3, "[DEBUG] add an internal fence at block %d. Fence (%d, %d)\n",
         state->nPage, state->curEntries[state->curOffset].key,
         ENTRY_GET_PTR(state->curEntries + state->curOffset));

    state->curOffset++;
  }

  /*
   *	Insert a external fence into the immediate upper level. The
   *	key value of the external fence is set to be the key of the
   *	last entry in the page. The pointer is set to be the page id
   *	of the current page.
   */
  if (state->nextState != NULL) {
    Entry fence;
    fence.key = state->curEntries[state->curOffset - 1].key;
    fence.ptr = state->nPage;

    ENTRY_SET_NON_FILTER(&fence);
    ENTRY_SET_FENCE(&fence);
    ENTRY_SET_NON_INTERNAL(&fence);

    fdtree_writeNextEntry(state->nextState, &fence);
  }

  /*
   *	update the page head info
   */
  state->curPageHead->num = state->curOffset;
  state->curPageHead->level = state->level;
  state->curPageHead->pid = state->nPage;
  state->curPageHead->next = INVALID_PAGE;

  /*
   *	Move to the next page in the multi-page block buffer.
   */
  state->curPage += BLKSZ;
  state->nPage++;
  state->curPageHead = PAGE_HEAD(state->curPage);
  state->curEntries = PAGE_DATA(state->curPage, Entry);
  state->curOffset = 0;

  /*
   *	write the last multi-page block into the data file, and
   *	then flush file. The number of valid pages is given by
   *	(state->curPage - state->buf)/BLKSZ
   */
  file_seek(state->fhdl, state->nPage - (state->curPage - state->buf) / BLKSZ);
  file_write(state->fhdl, state->buf, state->curPage - state->buf);
  file_flush(state->fhdl);

  /*
   *	continue to flush its immediately upper level
   */
  if (state->nextState) fdtree_flush(state->nextState);
}

/*
 *	fdtree_merge() --
 *	Merge two levels (Level lid and Level lid-1) into a new level and
 *	generate all upper levels on the top of the newly generated level.
 *	When merging the two levels, we need to:
 *	1) skip all (internal and external) fences in the upper level.
 *	2) skip all internal fences in the lower level.
 *	3) skip the pair of matching filter and phantom entries.
 *	4) add internal fences into the newly generate level
 *
 *	A merge may incur recursive merges if the size of new level exceeds
 *	its capacity. The returned value is the ID of the lowest level that
 *	is recursively merged.
 */

int fdtree_merge(FDTree *tree, int lid) {
  FDLevelState *state1, *state2, *state_out;
  Entry *e1, *e2;                // the current entries of the two levels
  int lowestMergeLid = lid;      // id of the lowest level that is recursively
                                 // merged
  int nFilter = 0, nDelete = 0;  // statistics

  if (lid >= tree->nLevel) {
    elog(WARNING, "unexpected merge operation at level %d.\n", lid);
    return 0;
  }

  elog(DEBUG1, "%d th merge on level %d: current #pages %d, max #pages %d\n",
       tree->levels[lid].nMerge, lid, tree->levels[lid].nBlock,
       tree->levels[lid].maxBlock);

#ifdef FDTREE_CHECK_MODE
  for (int i = 1; i <= lid; i++) fdtree_checkLevel(tree, i);
#endif

  // prepare for reading the two levels
  state1 = fdtree_initReadLevel(tree, lid - 1);
  state2 = fdtree_initReadLevel(tree, lid);
  e1 = fdtree_readNextEntry(state1);
  e2 = fdtree_readNextEntry(state2);

  // prepare for writing the newly generated level and all its upper levels
  state_out = fdtree_initWriteUpperLevels(tree, lid);

  /*
   *	The main loop. Scan the two levels and merge into one sortted run.
   *	We need to deal with filter/phantom entries, and internal/external
   *	fences.
   */
  while (1) {
    /*
     *	If we get the ends of both levels, exit from loop.
     */
    if (e1 == NULL && e2 == NULL) break;
    /*
     *	Skip all internal and external fences in the upper level.
     */
    else if (e1 != NULL && (ENTRY_IS_FENCE(e1) || ENTRY_IS_INTERNAL(e1))) {
      e1 = fdtree_readNextEntry(state1);
      if (e1 != NULL && ENTRY_IS_FILTER(e1)) nFilter++;
    }
    /*
     *	Skip all internal fences in the lower level.
     */
    else if (e2 != NULL && ENTRY_IS_INTERNAL(e2)) {
      e2 = fdtree_readNextEntry(state2);
    }
    /*
     *	If we get to the end of the upper level, write the current
     *	entry of the lower level into the newly generated run.
     */
    else if (e1 == NULL) {
      fdtree_writeNextEntry(state_out, e2);
      e2 = fdtree_readNextEntry(state2);
    }
    /*
     *	If we get to the end of the lower level, write the current
     *	entry of the upper level into the newly generated run.
     */
    else if (e2 == NULL) {
      fdtree_writeNextEntry(state_out, e1);
      e1 = fdtree_readNextEntry(state1);
      if (e1 != NULL && ENTRY_IS_FILTER(e1)) nFilter++;
    }
    /*
     *	If a filter entry in the upper level and a entry (non-fence)
     *	in the lower level are matched, discard both entries to physically
     *	perform a deletion.
     */
    else if (!ENTRY_IS_FENCE(e2) && ENTRY_IS_FILTER(e1) && e1->key == e2->key &&
             ENTRY_EQUAL_PTR(e1, e2)) {
      e1 = fdtree_readNextEntry(state1);
      e2 = fdtree_readNextEntry(state2);
      if (e1 != NULL && ENTRY_IS_FILTER(e1)) nFilter++;
      nDelete++;
    }
    /*
     *	In the following two cases, we compare the key values of the
     *	current entries of both two levels. write the entry with smaller
     *	key into the newly generated run.
     */
    else if (e1->key <= e2->key) {
      fdtree_writeNextEntry(state_out, e1);
      e1 = fdtree_readNextEntry(state1);
      if (e1 != NULL && ENTRY_IS_FILTER(e1)) nFilter++;
    } else if (e1->key > e2->key) {
      fdtree_writeNextEntry(state_out, e2);
      e2 = fdtree_readNextEntry(state2);
    }
  }

  /*
   *	flush out all entries in the write buffers of all levels.
   */
  fdtree_flush(state_out);
  assert(state2->nPage == tree->levels[lid].nBlock);

  /*
   *	relace old levels with new levels, and delete old levels.
   */
  fdtree_replaceLevels(tree, state_out, lid);

  delete state1;
  delete state2;

#ifdef FDTREE_CHECK_MODE
  for (int i = 1; i <= lid; i++) fdtree_checkLevel(tree, i);
#endif

  /*
   *	update the statistics info.
   */
  tree->levels[lid - 1].nMerge = 0;
  tree->levels[lid].nMerge++;
  tree->levels[lid].nAccMerge++;

  elog(DEBUG2, "\t%d filter entries\n", nFilter);
  elog(DEBUG2, "\t%d deletions\n", nDelete);

  /*
   *	If the size of the newly generated level exceeds its capacity,
   *	we continue to merge the newly generated level with its
   *	immediately lower level.
   */
  if (tree->levels[lid].nBlock > tree->levels[lid].maxBlock) {
    lowestMergeLid = fdtree_merge(tree, lid + 1);
  }

  return lowestMergeLid;
}

/*
 *	fdtree_init() --
 *	This function is used to initialize a allocated
 *	FDTree structure.
 */

int fdtree_init(FDTree *tree) {
  int i;

  memset(tree, 0, sizeof(FDTree));

  for (i = 0; i < MAX_LEVELS; i++) tree->levels[i].fhdl = INVALID_FILE_PTR;

  tree->curMergeState = NULL;
  tree->deamortizedMode = false;

  return 0;
}

/*
 *	fdtree_bulkload() --
 *	Generate a fdtree index on given index keys. The index keys are
 *	loaded from the specified data file. The data file should contain
 *	the sortted index entries in binary representation. A index entry
 *	contains two field: a 32-bit unsigned index key and a 30-bit
 *	payload. A sample data file is shown as follows:
 *		Key1, Payload1, Key2, Payload2, Key2, Payload2...
 *
 *	If the data file is not spedified, we generate a fdtree with
 *	sequential keys: 1, 2, 3, 4, 5, 6, ...
 *
 *	After bulkloading, all levels except of the lowest level in the
 *	fdtree contain only external fences.
 */

FDTree *fdtree_bulkload(unsigned int num, int k, char *fname) {
  int i;
  Entry entry;
  FDLevelState *state;
  FDTree *tree = new FDTree;
  FILE *file;

  /*
   *	if specify the load file, we check all entries in
   *	the file are in the sortted order.
   */
  if (fname != NULL) {
    int err;

    elog(INFO, "start to check the data file...\n");
    err = fdtree_checkfile(fname, &num);
    if (err == FDTREE_FAILED) return NULL;

    file = fopen(fname, "r");
    if (file == NULL) return NULL;
  }

  elog(INFO, "start to build the index...\n");

  /*
   * initialize the structure.
   */
  fdtree_init(tree);

  //	int nHeadTreeEntry = (HEADTREE_PAGES_BOUND - 1) * PAGE_MAX_NUM(Entry) *
  // FD_TREE_HEAD_OCCUPANCY;
  int nHeadTreeEntry = (HEADTREE_PAGES_BOUND - 1) * PAGE_MAX_NUM(Entry);
  tree->n = num;
  tree->k = k;
  tree->nLevel =
      (int)ceil(log(double(num) / nHeadTreeEntry) / log(double(k))) + 1;

  if (tree->nLevel < 2) tree->nLevel = 2;
  assert(tree->nLevel < MAX_LEVELS);

  /*
   *	prepare for writing the lowest level and all its
   *	upper levels.
   */
  state = fdtree_initWriteUpperLevels(tree, tree->nLevel - 1);

  /*
   *	The main loop. We write all index entries into the lowest level.
   *	It may incure the recursive writes on the upper levels.
   */
  for (i = 0; i < num; i++) {
    if (fname == NULL) {
      /* generate entry */
      entry.key = i;
      entry.ptr = rand() % 1024 + 1;
    } else {
      /* load entry from the file */
      if (fscanf(file, "%u %u", &(entry.key), &(entry.ptr)) < 2) return NULL;
    }
    ENTRY_SET_NON_FENCE(&entry);
    ENTRY_SET_NON_FILTER(&entry);
    fdtree_writeNextEntry(state, &entry);
  }

  /*
   *	flush out all entries in the write buffers of all levels.
   */
  fdtree_flush(state);

  /*
   *	relace old levels with new levels, and delete old levels.
   */
  fdtree_replaceLevels(tree, state, tree->nLevel - 1);

  if (fname != NULL) {
    fclose(file);
  }
  return tree;
}

/*
 *	fdtree_checkLevel() --
 *	Check whether Level lid is a valid level. Only used for
 *	debugging.
 */

int fdtree_checkLevel(FDTree *tree, int lid) {
  FDLevelState *state;
  Entry *e;
  Entry last;   // the last entry
  Entry fence;  // the last external fence

  /*
   * invoke btree_checkLevel to check the head tree.
   */
  if (lid == 0)
    return btree_checkLevel(&tree->headtree, tree->levels[lid + 1].nBlock);

  state = fdtree_initReadLevel(tree, lid);
  last.key = fence.key = MAX_KEY_VALUE;

  while (1) {
    e = fdtree_readNextEntry(state);
    if (e == NULL) break;
    if (last.key != MAX_KEY_VALUE) assert(e->key >= last.key);

    /*
     *	check the pointted positions of fences.
     */
    if (ENTRY_IS_FENCE(e))
      assert(ENTRY_GET_PTR(e) <= tree->levels[lid + 1].nBlock);

    /*
     *	check whether the pointted position of current fences
     *	are the same as those of the next external fence
     *	(the pointted position of previous external fence
     *	plus 1).
     */
    if (ENTRY_IS_FENCE(e) && !ENTRY_IS_INTERNAL(e)) {
      if (fence.key != MAX_KEY_VALUE)
        assert(ENTRY_GET_PTR(e) == ENTRY_GET_PTR(&fence) + 1);

      // update the last external fence;
      fence = *e;
    } else if (ENTRY_IS_INTERNAL(e)) {
      if (fence.key != MAX_KEY_VALUE)
        assert(ENTRY_GET_PTR(e) == ENTRY_GET_PTR(&fence) + 1);
    }

    /*
     *	update the last entry
     */
    last = *e;
  }

  return 0;
}

/*
 *	fdtree_inPageSearch() --
 *	Perform a binary search within a page to find the greatest
 *	key equal to or less than the key. If multiple entries have
 *	the same key, return the first one.
 */

int fdtree_inPageSearch(Page page, Key key) {
  int high = PAGE_NUM(page) - 1;
  int low = 0, mid;
  Entry *data = PAGE_DATA(page, Entry);

  /*
   *	binary search block
   */
  while (high > low) {
    mid = low + ((high - low) / 2);

    if (key - data[mid].key >= 1)
      low = mid + 1;
    else
      high = mid;
  }

  return low;
}

/*
 *	fdtree_point_search() --
 *	Perform a point search on a search key. The fist matched
 *	entry is copied to the paramter e. For all results with
 *	the same key, please use fdtree_range_search().

 On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */

int fdtree_point_search(FDTree *tree, Key key, Entry *e) {
  int i, j = 0, k;
  int offset;
  BlockNum pid = tree->headtree.root;
  Page page;
  Entry *entries;
  BlockNum next_pid = 0;
  FDTreeResultSet *set = &tree->resultSet;

  /*
   *	reset the result set.
   */
  set->num = 0;

  /*
   *	perform a point searh on the head tree. The matched
   *	results are sotred in the reuslt set. It returns the
   *	pid of the next-to-go page in the first sorted run.
   */
  pid = fdtree_headtree_query(&(tree->headtree), key, set);

  /*
   *	the tree is then traversed top to down, following the
   *	guide of pid in each level.
   */
  for (i = 1; i < tree->nLevel; i++) {
    if (pid == INVALID_PAGE) {
      elog(ERROR, "invalid pointer at level %d when searching key %d.\n", i - 1,
           key);
      return 0;
    }

    page = readPage(tree->levels[i].fhdl, tree->levels[i].fid, pid);
    entries = PAGE_DATA(page, Entry);

    /*
     *	find the greatest key equal to or less than the
     *	search key within the current page.
     */
    offset = fdtree_inPageSearch(page, key);

    /*
     *	scan the sorted run from the current entry until
     *	we find a fence. Since last level does not contain
     *	any fence, we do nothing here for the last level.
     */
    if (i < tree->nLevel - 1) {
      next_pid = INVALID_PAGE;

      /*
       *	There must exist such a fence in the current page,
       *	because the last entry in the current page is a fence.
       */
      for (j = offset; j < PAGE_NUM(page); j++) {
        if (ENTRY_IS_FENCE(entries + j)) {
          /*
           *	The pointer of the fence stores the pid of the
           *	page that we need to go in the next level.
           */
          next_pid = ENTRY_GET_PTR(entries + j);
          break;
        }
      }
      assert(ENTRY_IS_FENCE(entries + j));
    }

    /*
     *	scan the sorted run from the current entry until
     *	we find all matched entries that have the same key
     *	as the search key. The phantom entries are
     */
    j = offset;
    while (entries[j].key == key) {
      /*
       *	skip all fences, neither interval fences nor
       *	external fences.
       */
      if (!ENTRY_IS_FENCE(entries + j)) {
        bool deleted = false;
        // nRes++;
        //*e = entries[j];

        /*
         *	the corresponding filter entry is always placed
         *	in the level above the phantom entry, since the
         *	filter entry is inserted into the index later than
         *	the original phantom entry. As a result, the
         *	corresponding filter entry should have been added
         *	into the result set, if it exists.
         *	Here, we scan all enties in the result set to
         *	check whether the current entry is a normal entry
         *	or a phantom entry.
         */
        for (k = 0; k < set->num; k++) {
          /*
           *	if two entries have the same pointers (and keys),
           *	the current entry is a phantom entry and should not
           *	be added into the result set, and the entry in the
           *	result set is a filter entry and should be removed
           *	from the results set.
           */
          if (ENTRY_GET_PTR(set->results + k) == ENTRY_GET_PTR(entries + j)) {
            set->valid[k] = false;
            deleted = true;
            break;
          }
        }

        /*
         *	if there is no matched filter entry in the result set,
         *	the current entry is insert into the result set.
         */
        if (!deleted) {
          /*
           *	current implementation uses a fix-size array to store
           *	all resutls. If the array is full, we discard additional
           *	resutls.
           */
          if (set->num < RESULT_SET_CAPACITY) {
            set->results[set->num] = entries[j];
            set->valid[set->num] = true;
            set->num++;
          } else
            elog(WARNING,
                 "The result set is full, additional matched entries are "
                 "discarded.\n");
        }
      }

      /*
       *	read the matched entries in the next page of current
       *	sortted run, if necessary.
       */
      if (++j >= PAGE_NUM(page) && ++pid < tree->levels[i].nBlock) {
        page = readPage(tree->levels[i].fhdl, tree->levels[i].fid, pid);
        entries = PAGE_DATA(page, Entry);
        j = 0;
      }
    }

    pid = next_pid;
  }

  // here we only output the first result.
  int nRes = 0;
  for (i = 0; i < set->num; i++) {
    if (set->valid[i] == true) {
      if (e != NULL) *e = set->results[i];
      nRes++;
    }
  }

  // used for range scan
  tree->curPID = pid;
  tree->curOffset = j;

  return nRes;
}

BlockNum fdtree_headtree_query(BTree *tree, Key key, FDTreeResultSet *set) {
  int i;
  int offset;
  BlockNum pid = tree->root;
  BlockNum res;
  Page page;
  Entry *entries;

  for (i = 0; i < tree->nLevel; i++) {
    page = readPage(tree->fhdl[tree->nLevel - i - 1],
                    tree->fid[tree->nLevel - i - 1], pid);
    if (PAGE_LEVEL(page) >= tree->nLevel)
      page = readPage(tree->fhdl[tree->nLevel - i - 1],
                      tree->fid[tree->nLevel - i - 1], pid);

    entries = PAGE_DATA(page, Entry);
    offset = fdtree_inPageSearch(page, key);

    pid = ENTRY_GET_PTR(entries + offset);
  }
  assert(PAGE_LEVEL(page) == 0);

  // scan to find the corresponding fence.
  res = INVALID_PAGE;
  for (i = offset; i < PAGE_NUM(page); i++) {
    if (ENTRY_IS_FENCE(entries + i)) {
      res = ENTRY_GET_PTR(entries + i);
      //			assert(res != INVALID_PAGE);
      break;
    }
  }

  //	assert(res != INVALID_PAGE);
  // scan the levels to find the match entries.
  i = offset;
  while (entries[i].key == key) {
    if (!ENTRY_IS_FENCE(entries + i)) {
      if (set) {
        set->results[set->num] = entries[i];
        set->valid[set->num] = true;
      }
      set->num++;
    }

    if (++i >= PAGE_NUM(page) && PAGE_NEXT(page) != INVALID_PAGE) {
      page = readPage(tree->fhdl[0], tree->fid[0], PAGE_NEXT(page));
      i = 0;
    }
  }
  return res;
}

int fdtree_search_next(FDTree *tree, Entry *e) {
  int pid, j;
  Page page;
  Entry *entries;

  pid = tree->curPID;
  j = tree->curOffset;

  page = readPage(tree->levels[0].fhdl, tree->levels[0].fid, pid);
  entries = PAGE_DATA(page, Entry);
  *e = entries[j];

  if (++j >= PAGE_NUM(page) && ++pid < tree->levels[0].nBlock) {
    j = 0;
  }

  tree->curPID = pid;
  tree->curOffset = j;

  return 1;
}

int fdtree_range_query(FDTree *tree, Key key1, Key key2) {
  int i, j;
  int offset;
  BlockNum pid = tree->headtree.root;
  Page page;
  Entry *entries;
  BlockNum next_pid = 0;
  int nRes = 0;
  int nFence;

  //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  /////////	pid = fdtree_headtree_query(&(tree->headtree), key1, nRes);
  pid = fdtree_headtree_query(&(tree->headtree), key1);

  for (i = 1; i < tree->nLevel - 1; i++) {
    nFence = 0;

    if (pid == INVALID_PAGE) {
      printf("invalid pointer at level %d when searching key %d.\n", i - 1,
             key1);
      break;
    }

    page = readPage(tree->levels[i].fhdl, tree->levels[i].fid, pid);
    entries = PAGE_DATA(page, Entry);
    offset = fdtree_inPageSearch(page, key1);

    // find the corresponding fence
    if (i < tree->nLevel - 1) {
      next_pid = INVALID_PAGE;
      for (j = offset; j < PAGE_NUM(page); j++) {
        if (ENTRY_IS_FENCE(entries + j)) {
          next_pid = ENTRY_GET_PTR(entries + j);
          break;
        }
      }
    }

    // scan the levels to find the match entries.
    j = offset;
    while (entries[j].key <= key2) {
      if (ENTRY_IS_FENCE(entries + j) && !ENTRY_IS_INTERNAL(entries + j))
        nFence++;

      if (!ENTRY_IS_FENCE(entries + j) && !ENTRY_IS_FILTER(entries + j)) {
        nRes++;
        //				printf("find entry(%d, %d)\n",
        // entries[j].key, entries[j].ptr);
      }

      if (++j >= PAGE_NUM(page)) {
        if (++pid < tree->levels[i].nBlock) {
          page = readPage(tree->levels[i].fhdl, tree->levels[i].fid, pid);
          entries = PAGE_DATA(page, Entry);
          j = 0;
        } else
          break;
      }
    }
    pid = next_pid;
  }

  // the range scan on leaf level
  if (pid == INVALID_PAGE) {
    printf("invalid pointer at level %d when searching key %d.\n", i - 1, key1);
    return nRes;
  }

  nFence++;
  file_seek(tree->levels[tree->nLevel - 1].fhdl, pid);
  int nbuf = file_read(tree->levels[tree->nLevel - 1].fhdl, mbuf,
                       min(MAX_MBUF_NUM, nFence) * BLKSZ);
  nbuf /= BLKSZ;
  nFence -= nbuf;
  page = mbuf;
  entries = PAGE_DATA(page, Entry);
  offset = fdtree_inPageSearch(page, key1);

  // scan the levels to find the match entries.
  j = offset;
  while (entries[j].key <= key2) {
    if (!ENTRY_IS_FENCE(entries + j) && !ENTRY_IS_FILTER(entries + j)) {
      nRes++;
      //				printf("find entry(%d, %d)\n",
      // entries[j].key, entries[j].ptr);
    }

    if (++j >= PAGE_NUM(page) && ++pid < tree->levels[i].nBlock) {
      page += BLKSZ;
      entries = PAGE_DATA(page, Entry);
      j = 0;
      if (page - mbuf >= nbuf * BLKSZ) {
        if (nFence < 0)
          break;
        else {
          nbuf = file_read(tree->levels[tree->nLevel - 1].fhdl, mbuf,
                           min(MAX_MBUF_NUM, nFence) * BLKSZ);
          nbuf /= BLKSZ;
          nFence -= nbuf;
          page = mbuf;
          entries = PAGE_DATA(page, Entry);
        }
      }
    }
  }

  return nRes;
}

/*
 *	fdtree_insert() --
 *	Insert a entry into the index. The entry is firstly insertted into
 *	the head tree, and is then migrated into lower levels if necessary.
 *
 *	The return value is the level id of the lowest merged level caused
 *	by this insertion.If no merge operation occurs, return 0 (level id
 *	of the head tree).
 */

int fdtree_insert(FDTree *tree, Entry entry) {
  int nLevelMerge = 0;
  BTree *headtree = &(tree->headtree);

  /*
   * ensure the entry is NOT a fence
   */
  ENTRY_SET_NON_FENCE(&entry);

  /*
   * fristly insert the entry into the head tree.
   */
  btree_insert(headtree, entry);

  /*
   * If the head tree exceeds its capacity, we merge it into the
   * next level, and migrate all index entries into the lower levels.
   * We use #entry to check whether L0 is full, instead of #pages.
   * (If using #pages, the external fences from L1 may directly
   * cause L0 full, given a larger k value).
   */
  //	if (tree->headtree.nEntry > tree->headtree.maxEntry *
  // FD_TREE_HEAD_OCCUPANCY)
  if (tree->headtree.nEntry > tree->headtree.maxEntry) {
    /*
     * at this point, all entries at the head tree will be merged
     * into lower levels.We can reset the dirty bits of all head
     * tree pages in the buffer pool to zero to prevent the pages
     * from being written by buffer manager later.
     */
    resetAllDirtyBit();

    /*
     * merge the 1st and 2nd levels. This may cause recursive merge
     * in lower levels.
     */
    nLevelMerge = fdtree_merge(tree, 1);
  }

  return nLevelMerge;
}

/*
 *	fdtree_delete() --
 *	Delete a entry from the index. The entry is firstly insertted into
 *	the head tree as a filter entry.
 *
 *	The return value is the level id of the lowest merged level caused
 *	by this insertion.If no merge operation occurs, return 0 (level id
 *	of the head tree).
 */
int fdtree_delete(FDTree *tree, Entry entry) {
  int del = 0;
  int ret = 0;

  /*
   * try to delete the entry from the head tree. If success, the return
   * value is greater than 0. Otherwise (the entry is not in the head
   * tree), the return value equals to 0.
   */
  del = btree_delete(&tree->headtree, entry);
  if (del == 0) {
    /*
     * ensure the entry is is a filter, and is NOT a fence.
     */
    ENTRY_SET_NON_FENCE(&entry);
    ENTRY_SET_FILTER(&entry);
    assert(ENTRY_IS_FILTER(&entry));

    /*
     * insert the filter entry into the index.
     */
    ret = fdtree_insert(tree, entry);
  }

  return ret;
}

/*
 * fdtree_checkfile() --
 * check the data file, return the number of entries in the file.
 * The keys of all entreis are in increasing order.
 */
int fdtree_checkfile(char *fname, unsigned int *num) {
  unsigned long long n = 0;
  Entry entry, last;
  FILE *file;

  file = fopen(fname, "r");
  if (file == NULL) return FDTREE_FAILED;

  last.key = last.ptr = 0;
  while (1) {
    if (fscanf(file, "%u %u", &(entry.key), &(entry.ptr)) < 2) {
      if (feof(file)) break;

      elog(ERROR, "ERROR: wrong format in data file.\n");
      return FDTREE_FAILED;
    }
    if (entry.key < last.key) {
      elog(ERROR, "ERROR: all entres in the data file should be sortted.\n");
      elog(ERROR, "Wrong key at %d-th entry.\n", n);
      return FDTREE_FAILED;
    }
    n++;
    last = entry;
  }

  fclose(file);

  *num = (unsigned int)n;

  return FDTREE_SUCCESS;
}

void fdtree_try(FDTree *tree, int lid) {
  Page page;
  for (int i = 0; i < tree->levels[lid].nBlock; i++) {
    page = readPage(tree->levels[lid].fhdl, tree->levels[lid].fid, i);
    printf("%d\n", PAGE_NUM(page));
  }
}
