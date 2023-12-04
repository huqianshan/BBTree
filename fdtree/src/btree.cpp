/*-------------------------------------------------------------------------
 *
 * btree.cpp
 *	  B-tree interface routines
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
 *	  FDTree: btree.cpp,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#include "btree.h"

#include <assert.h>
#include <string.h>
using namespace std;

#include <fstream>

#include "buffer.h"
#include "fdtree.h"
#include "storage.h"

#define BTREE_OPT_SUCCESS 1
#define BTREE_OPT_FAILED 0

// #define BTREE_READNEXT_COPY_TO_BUF
// #define BTREE_CHECK_MODE

int btree_checkfile(char *fname, unsigned int *num);

// return the first entry whose key is equal or bigger than search key
int btree_inPageSearch(Page page, Key key) {
  int high = PAGE_NUM(page) - 1;
  int low = 0, mid;
  Entry *data = PAGE_DATA(page, Entry);

  while (high > low) {
    mid = low + ((high - low) / 2);

    if (key - data[mid].key >= 1)
      low = mid + 1;
    else
      high = mid;
  }

  return low;
}

void btree_inPageInsert(Page page, Entry entry) {
  int i, offset;
  Entry *data;
  PageHead *head;

  offset = btree_inPageSearch(page, entry.key);
  data = PAGE_DATA(page, Entry);
  head = PAGE_HEAD(page);

  // multi tuples with the same key
  while (offset < head->num && entry.key > data[offset].key) offset++;

  //	assert(entry.key <= data[offset].key);
  //	if insert the last key
  //	if (key > data[offset].key)
  //		offset ++;

  for (i = PAGE_NUM(page); i > offset; i--) {
    data[i] = data[i - 1];
    if (data[i].key == entry.key && ENTRY_IS_FENCE(&entry) &&
        !ENTRY_IS_INTERNAL(data + i)) {
      offset = i;
      break;
    }
  }

  data[offset] = entry;
  head->num++;

#ifdef BTREE_CHECK_MODE
  for (int i = 1; i < PAGE_NUM(page); i++)
    assert(data[i - 1].key <= data[i].key);
  assert(head->level > 0 || ENTRY_IS_FENCE(data + PAGE_NUM(page) - 1));
#endif

  //	file_seek(tree->fhdl, pid);
  //	file_write(tree->fhdl, page, BLKSZ);
}

int btree_inPageDelete(Page page, Entry entry) {
  int offset, i;
  Entry *data;
  PageHead *head;

  offset = btree_inPageSearch(page, entry.key);
  data = PAGE_DATA(page, Entry);
  head = PAGE_HEAD(page);

  while (data[offset].key == entry.key) {
    if (ENTRY_EQUAL_PTR(data + offset, &entry) &&
        !ENTRY_IS_FENCE(data + offset)) {
      if (offset == PAGE_NUM(page) - 1)
        printf("Remove the last entry in node\n");

      for (i = offset; i < PAGE_NUM(page) - 1; i++) data[i] = data[i + 1];
      head->num--;
      if (head->num == 0) printf("Empty node\n");
      return BTREE_OPT_SUCCESS;
    }
    offset++;
  }
  return BTREE_OPT_FAILED;
}

BTreeState *btree_initWrite(BTree *tree, bool forceWrite) {
  BTreeState *state = new BTreeState;

  tree->nBlock = 0;
  tree->nEntry = 0;
  tree->levelBlock[0] = 0;
  tree->maxBlock = HEADTREE_PAGES_BOUND;
  tree->maxEntry = tree->maxBlock * PAGE_MAX_NUM(Entry);
  tree->root = 0;
  tree->nLevel = 1;
  tree->nInitEntry = -1;
  tree->fid[0] = file_get_new_fid();
  tree->fhdl[0] = file_open(tree->fid[0], false);
  file_seek(tree->fhdl[0], 0);

  state->type = B_TREE;
  state->fid = tree->fid[0];
  state->fhdl = tree->fhdl[0];
  state->curOffset = 0;
  state->curPageHead = PAGE_HEAD(state->buf);
  state->curEntries = PAGE_DATA(state->buf, Entry);
  state->pid = 0;
  state->level = 0;
  state->nextState = NULL;
  state->tree = tree;
  state->forceWrite = forceWrite;

  if (forceWrite) {
    state->curPage = state->mbuf;
    state->curPageHead = PAGE_HEAD(state->curPage);
    state->curEntries = PAGE_DATA(state->curPage, Entry);
  }

  state->curPageHead->num = 0;
  state->curPageHead->next = 0;

  return state;
}

BTreeState *btree_initRead(BTree *tree, bool forceRead) {
  Page page;
  BTreeState *state = new BTreeState;

  state->type = B_TREE;
  state->curOffset = 0;
  state->level = 0;
  state->nextState = NULL;
  state->tree = tree;
  state->fid = tree->fid[0];
  state->fhdl = tree->fhdl[0];
  file_seek(state->fhdl, 0);

  state->forceRead = forceRead;
  if (forceRead) {
    file_read(state->fhdl, state->mbuf, MBLKSZ);
    state->curPage = state->mbuf;
    state->curPageHead = PAGE_HEAD(state->curPage);
    state->curEntries = PAGE_DATA(state->curPage, Entry);
  } else {
#ifdef BTREE_READNEXT_COPY_TO_BUF
    state->curPageHead = PAGE_HEAD(state->buf);
    state->curEntries = PAGE_DATA(state->buf, Entry);

    state->curPageHead->num = 0;
    state->curPageHead->next = 0;
#else
    page = readPage(state->fhdl, state->fid, 0);
    state->curPageHead = PAGE_HEAD(page);
    state->curEntries = PAGE_DATA(page, Entry);
#endif
  }

  // the pages in the previous head tree are no longer used.
  // mark them as clear page to avoid unneccessary writes.
  // DEBUG: this is incorrect!!
  //	for (int i = 0; i < tree->nBlock; i++)
  //		clearDirty(tree->fid, i);

  return state;
}

BTreeState *btree_initEmptyState(BTree *tree) {
  BTreeState *state = new BTreeState;

  state->type = B_TREE;
  state->curOffset = 0;
  state->curPageHead = PAGE_HEAD(state->buf);
  state->curEntries = PAGE_DATA(state->buf, Entry);
  state->pid = tree->nBlock;
  state->level = 0;
  state->nextState = NULL;
  state->tree = tree;
  state->fid = tree->fid[0];
  state->fhdl = tree->fhdl[0];
  state->forceWrite = false;

  return state;
}

void btree_initEmpty(BTree *tree) {
  Page page;
  PageHead *head;
  Entry *data;
  //	BTreeState * state = new BTreeState;

  //	assert(deamortizedMode == true);

  tree->nBlock = 0;
  tree->nEntry = 0;
  tree->maxBlock = HEADTREE_PAGES_BOUND;
  tree->maxEntry = tree->maxBlock * PAGE_MAX_NUM(Entry);
  tree->root = 0;
  tree->nLevel = 1;
  tree->levelBlock[0] = 0;

  tree->fid[0] = file_get_new_fid();
  tree->fhdl[0] = file_open(tree->fid[0], false);

  page = newPage(tree->fhdl[0], tree->fid[0], tree->levelBlock[0]++);

  head = PAGE_HEAD(page);
  data = PAGE_DATA(page, Entry);
  head->num++;
  head->level = 0;
  head->pid = 0;
  head->parent = INVALID_PAGE;
  head->pre = INVALID_PAGE;
  head->next = INVALID_PAGE;

  data[0].key = MAX_KEY_VALUE;
  data[0].ptr = INVALID_PAGE;
  ENTRY_SET_INTERNAL(data + 0);
  ENTRY_SET_FENCE(data + 0);

  tree->nBlock++;
  tree->nEntry++;
  tree->nInitEntry = tree->nEntry;

  markDirty(tree->fid[0], 0);

  /*
          state->type = B_TREE;
          state->curOffset = 0;
          state->curPageHead = PAGE_HEAD(state->buf);
          state->curEntries = PAGE_DATA(state->buf, Entry);
          state->pid = tree->nBlock;
          state->level = 0;
          state->nextState = NULL;
          state->tree = tree;

          return state;
  */
}

Entry *btree_readNextEntry(BTreeState *state) {
  Page page;
  BTree *tree = state->tree;

  // move to the next page
  if (state->curOffset >= state->curPageHead->num) {
    // after reading, the pages in the previous head tree are no longer used.
    // mark them as clear page to avoid unneccessary writes.
    //	clearDirty(state->fid, state->curPageHead->pid);

    if (state->forceRead) {
      if (PAGE_NEXT(state->curPage) != INVALID_PAGE) {
        state->curPage += BLKSZ;
        if (state->curPage - state->mbuf >= MBLKSZ) {
          file_read(state->fhdl, state->mbuf, MBLKSZ);
          state->curPage = state->mbuf;
        }
        state->curPageHead = PAGE_HEAD(state->curPage);
        state->curEntries = PAGE_DATA(state->curPage, Entry);
        state->curOffset = 0;
      } else
        return NULL;
    } else {
      if (state->curPageHead->next != INVALID_PAGE) {
        page = readPage(state->fhdl, state->fid, state->curPageHead->next);

#ifdef BTREE_READNEXT_COPY_TO_BUF
        memcpy(state->buf, page, BLKSZ);
#else
        state->curPageHead = PAGE_HEAD(page);
        state->curEntries = PAGE_DATA(page, Entry);
#endif
        state->curOffset = 0;
      } else
        return NULL;
    }
  }

  return state->curEntries + state->curOffset++;
}

int btree_writeNextEntry(BTreeState *state, Entry *entry,
                         double initSpaceRate) {
  BTree *tree = state->tree;
  Page page;

  if (state->curOffset >= PAGE_MAX_NUM(Entry) * initSpaceRate) {
    if (state->nextState == NULL) {
      // build the upper level if it doesn't exist
      state->nextState = new BTreeState;
      state->nextState->type = B_TREE;
      state->nextState->curOffset = 0;
      state->nextState->curPageHead = PAGE_HEAD(state->nextState->buf);
      state->nextState->curEntries = PAGE_DATA(state->nextState->buf, Entry);
      state->nextState->pid = 0;
      state->nextState->level = state->level + 1;
      state->nextState->nextState = NULL;
      state->nextState->tree = state->tree;

      state->nextState->forceWrite = state->forceWrite;
      if (state->forceWrite) {
        state->nextState->curPage = state->nextState->mbuf;
        state->nextState->curPageHead = PAGE_HEAD(state->nextState->curPage);
        state->nextState->curEntries =
            PAGE_DATA(state->nextState->curPage, Entry);
      }
      state->nextState->fid = file_get_new_fid();
      state->nextState->fhdl = file_open(state->nextState->fid, false);
      file_seek(state->nextState->fhdl, 0);

      // update the root of tree
      tree->root = state->nextState->pid;
      tree->fhdl[tree->nLevel] = state->nextState->fhdl;
      tree->fid[tree->nLevel] = state->nextState->fid;
      tree->nLevel++;
    }

    // insert to its upper level
    Entry fence;
    fence = state->curEntries[state->curOffset - 1];
    fence.ptr = state->pid;
    btree_writeNextEntry(state->nextState, &fence);

    // write the current page
    state->curPageHead->next =
        state->pid + 1;  // bug:state->curPageHead->next = tree->nBlock++;
    state->curPageHead->num = state->curOffset;
    state->curPageHead->level = state->level;
    state->curPageHead->parent = state->nextState->pid;
    state->curPageHead->pid = state->pid;

    // write to disk? or just allocate in buffer pool
    //		file_seek(tree->fhdl, state->pid);
    //		file_write(tree->fhdl, state->buf, BLKSZ);

    if (state->forceWrite) {
      state->curPage += BLKSZ;
      if (state->curPage - state->mbuf >= MBLKSZ) {
        // don't need to seek here
        //		file_seek(state->fhdl, state->pid + 1 - MBLKSZ/BLKSZ);
        file_write(state->fhdl, state->mbuf, MBLKSZ);
        state->curPage = state->mbuf;
      }
      state->curPageHead = PAGE_HEAD(state->curPage);
      state->curEntries = PAGE_DATA(state->curPage, Entry);
    } else {
      // copy the page to buffer
      page = newPage(state->fhdl, state->fid, state->pid);
      memcpy(page, state->buf, BLKSZ);
      markDirty(state->fid, state->pid);
    }

    // update the current page
    state->curOffset = 0;
    state->pid++;
  }

  // write to buffer directly
  state->curEntries[state->curOffset] = *entry;
  state->curOffset++;
  if (state->level == 0) tree->nEntry++;  // we only conter in the leaf level

  return 0;
}

void btree_flush(BTreeState *state) {
  BTree *tree = state->tree;

  state->curPageHead->next = INVALID_PAGE;
  state->curPageHead->num = state->curOffset;
  state->curPageHead->level = state->level;
  state->curPageHead->pid = state->pid;
  state->curPageHead->parent =
      (state->nextState == NULL ? INVALID_PAGE : state->nextState->pid);

  tree->fid[state->level] = state->fid;
  tree->fhdl[state->level] = state->fhdl;
  if (state->forceWrite == true)
    file_write(state->fhdl, state->mbuf, state->curPage - state->mbuf + BLKSZ);
  else {
    // copy the page to buffer
    Page page = newPage(state->fhdl, state->fid, state->pid);
    memcpy(page, state->buf, BLKSZ);
    markDirty(state->fid, state->pid);
    //		file_seek(state->fhdl, state->pid);
    //		file_write(state->fhdl, state->buf, BLKSZ);
  }

  file_flush(state->fhdl);

  tree->levelBlock[state->level] = state->pid + 1;
  if (state->pid >= tree->nBlock)   // change to >=
    tree->nBlock = state->pid + 1;  // change to +1

  if (state->nextState) {
    // insert to its upper level
    Entry fence;
    fence = state->curEntries[state->curOffset - 1];
    fence.ptr = state->pid;
    btree_writeNextEntry(state->nextState, &fence);

    btree_flush(state->nextState);
  } else
    tree->nInitEntry = tree->nEntry;
}

int btree_replaceTree(BTree *oldTree, BTree *newTree, BTreeState *state) {
  int i;
  BTreeState *state_tmp;

  for (i = 0; i < oldTree->nLevel; i++)
    file_delete(oldTree->fhdl[i], oldTree->fid[i]);

  *oldTree = *newTree;

  // delete B-tree states
  assert(state->type == B_TREE);
  while (state != NULL) {
    state_tmp = state->nextState;
    delete state;
    state = state_tmp;
  }

  return 1;
}

BTree *btree_bulkload(unsigned int num, double initSpaceRate, char *fname) {
  Entry entry;
  BTreeState *state;
  BTree *tree = new BTree;
  ifstream file;

  /*
   *	if specify the load file, we check all entries in
   *	the file are in the sortted order.
   */
  if (fname != NULL) {
    int err;

    elog(INFO, "start to check the data file...\n");
    err = btree_checkfile(fname, &num);
    if (err == FDTREE_FAILED) return NULL;

    file.open(fname, ios::in);
    if (!file.is_open()) return NULL;
  }

  state = btree_initWrite(tree, true);

  for (int i = 0; i < num; i++) {
    if (fname == NULL) {
      /* generate entry */
      entry.key = i;
      entry.ptr = rand() % 100;
    } else {
      /* load entry from the file */
      file >> entry.key;
      file >> entry.ptr;
    }
    btree_writeNextEntry(state, &entry, initSpaceRate);
  }

  // insert a entry with max key value to guarantee the correctness
  entry.key = MAX_KEY_VALUE;
  entry.ptr = INVALID_PAGE;
  ENTRY_SET_FENCE(&entry);
  btree_writeNextEntry((BTreeState *)state, &entry);

  btree_flush(state);

  tree->nEntry = num;

  if (fname != NULL) file.close();

  delete state;
  return tree;
}

void btree_try(BTree *tree) {
  // Page page;
  // for (int i = 0; i < tree->nBlock; i++)
  //{
  //	page = readPage(tree->fhdl, tree->fid, i);
  //	printf("%d\n", PAGE_NUM(page));
  // }
}

BlockNum btree_point_query(BTree *tree, Key key, Entry *entry) {
  int i;
  int offset;
  BlockNum pid = tree->root;
  Page page;
  Entry *entries;

  for (i = 0; i < tree->nLevel; i++) {
    page = readPage(tree->fhdl[tree->nLevel - i - 1],
                    tree->fid[tree->nLevel - i - 1], pid);
    entries = PAGE_DATA(page, Entry);
    offset = btree_inPageSearch(page, key);

    pid = ENTRY_GET_PTR(entries + offset);
  }
  assert(PAGE_LEVEL(page) == 0);
  if (entries[offset].key == key) {
    if (entry != NULL) *entry = entries[offset];
    return pid;
  } else
    return INVALID_PAGE;
}

int btree_range_query(BTree *tree, Key key1, Key key2) {
  int i;
  int offset;
  BlockNum pid = tree->root;
  Page page;
  Entry *entries;

  for (i = 0; i < tree->nLevel - 1; i++) {
    page = readPage(tree->fhdl[tree->nLevel - i - 1],
                    tree->fid[tree->nLevel - i - 1], pid);
    entries = PAGE_DATA(page, Entry);
    offset = btree_inPageSearch(page, key1);

    pid = ENTRY_GET_PTR(entries + offset);
  }

  page = readPage(tree->fhdl[0], tree->fid[0], pid);
  entries = PAGE_DATA(page, Entry);
  offset = btree_inPageSearch(page, key1);

  assert(PAGE_LEVEL(page) == 0);
  int num = 0;
  while (entries[offset].key <= key2) {
    num++;
    if (++offset >= PAGE_NUM(page)) {
      if (++pid < tree->levelBlock[0]) {
        page = readPage(tree->fhdl[0], tree->fid[0], pid);
        entries = PAGE_DATA(page, Entry);
        offset = 0;
      } else
        break;
    }
  }
  return num;
}

int btree_update(BTree *tree, Key key, int val) {
  int i;
  int offset;
  BlockNum pid = tree->root;
  Page page;
  Entry *entries;

  for (i = 0; i < tree->nLevel - 1; i++) {
    page = readPage(tree->fhdl[tree->nLevel - i - 1],
                    tree->fid[tree->nLevel - i - 1], pid);
    entries = PAGE_DATA(page, Entry);
    offset = btree_inPageSearch(page, key);

    pid = ENTRY_GET_PTR(entries + offset);
  }

  page = readPage(tree->fhdl[0], tree->fid[0], pid);
  entries = PAGE_DATA(page, Entry);
  offset = btree_inPageSearch(page, key);

  assert(PAGE_LEVEL(page) == 0);
  if (entries[offset].key == key) {
    entries[offset].ptr = val;
    markDirty(tree->fid[0], pid);
    return 1;
  } else
    return 0;
}

int btree_update_pointer(BTree *tree, Entry *e) {
  int i, suc;
  int offset;
  BlockNum pid = tree->root;
  Page page;
  Entry *entries;
  Key key = e->key;

  for (i = 0; i < tree->nLevel; i++) {
    page = readPage(tree->fhdl[tree->nLevel - i - 1],
                    tree->fid[tree->nLevel - i - 1], pid);
    entries = PAGE_DATA(page, Entry);
    offset = btree_inPageSearch(page, key);

    pid = ENTRY_GET_PTR(entries + offset);
  }
  assert(PAGE_LEVEL(page) == 0);
  suc = 0;
  while (offset < PAGE_NUM(page) && entries[offset].key == key) {
    entries[offset].ptr = e->ptr;
    offset++;
    suc = 1;
  }
  return suc;
}

void btree_split(BTree *tree, Page page, int level, int *trace) {
  int i, num, nextfence;
  int offset;
  Page newpage, parent;
  PageHead *head, *newhead, *parenthead;
  Entry *data, *newdata, *parentdata;
  Entry entry;

  tree->nBlock++;
  newpage =
      newPage(tree->fhdl[level], tree->fid[level], tree->levelBlock[level]++);

  num = PAGE_NUM(page);
  head = PAGE_HEAD(page);
  newhead = PAGE_HEAD(newpage);
  data = PAGE_DATA(page, Entry);
  newdata = PAGE_DATA(newpage, Entry);
  memcpy(newdata, data + num / 2, sizeof(Entry) * (num - num / 2));
  newhead->num = num - num / 2;
  newhead->next = PAGE_NEXT(page);
  newhead->level = PAGE_LEVEL(page);
  newhead->parent = PAGE_PARENT(page);
  head->num = num / 2;
  head->next = PAGE_PID(newpage);

  // find the next fence of the middle entry
  for (nextfence = num / 2 - 1; nextfence < num; nextfence++) {
    if (ENTRY_IS_FENCE(data + nextfence)) break;
  }

  assert(ENTRY_IS_FENCE(data + nextfence));

  // if the last entry is not fence, insert a new additional fence
  if (nextfence != num / 2 - 1) {
    entry.key = data[num / 2 - 1].key;
    entry.ptr = data[nextfence].ptr;
    ENTRY_SET_INTERNAL(&entry);
    btree_inPageInsert(page, entry);
  }

  if (level == tree->nLevel - 1) {
    tree->levelBlock[level + 1] = 0;
    tree->fid[tree->nLevel] = file_get_new_fid();
    tree->fhdl[tree->nLevel] = file_open(tree->fid[tree->nLevel], false);
    tree->nLevel++;
    tree->root = tree->levelBlock[level + 1];
    // tree->root = PAGE_PID(parent);

    // build new root node
    tree->nBlock++;
    parent = newPage(tree->fhdl[level + 1], tree->fid[level + 1],
                     tree->levelBlock[level + 1]++);
    parenthead = PAGE_HEAD(parent);
    parentdata = PAGE_DATA(parent, Entry);
    parenthead->pre = INVALID_PAGE;
    parenthead->next = INVALID_PAGE;
    parenthead->parent = INVALID_PAGE;
    parenthead->level = tree->nLevel - 1;
    parenthead->num = 0;

    head->parent = PAGE_PID(parent);
    newhead->parent = PAGE_PID(parent);

    entry.key = newdata[PAGE_NUM(newpage) - 1].key;
    entry.ptr = PAGE_PID(newpage);
    ENTRY_SET_FENCE(&entry);  ///
    btree_inPageInsert(parent, entry);

    entry.key = data[PAGE_NUM(page) - 1].key;
    entry.ptr = PAGE_PID(page);
    ENTRY_SET_FENCE(&entry);  ///
    btree_inPageInsert(parent, entry);
  } else {
    // update the two fences in the upper level
    parent =
        readPage(tree->fhdl[level + 1], tree->fid[level + 1], trace[level + 1]);

    parenthead = PAGE_HEAD(parent);
    parentdata = PAGE_DATA(parent, Entry);
    offset = btree_inPageSearch(parent, newdata[PAGE_NUM(newpage) - 1].key);
    // find the corresponding fence of the original page
    for (i = offset; i < PAGE_NUM(parent); i++)
      if (ENTRY_GET_PTR(parentdata + i) == PAGE_PID(page)) break;
    if (ENTRY_GET_PTR(parentdata + i) != PAGE_PID(page)) i = i;
    parentdata[i].ptr = PAGE_PID(newpage);

    offset = i;  ///
    for (i = PAGE_NUM(parent); i > offset; i--)
      parentdata[i] = parentdata[i - 1];
    entry.key = data[PAGE_NUM(page) - 1].key;
    entry.ptr = PAGE_PID(page);
    ENTRY_SET_FENCE(&entry);  ///
    parentdata[offset] = entry;
    parenthead->num++;
  }

  markDirty(tree->fid[level], PAGE_PID(page));
  markDirty(tree->fid[level], PAGE_PID(newpage));
  markDirty(tree->fid[level + 1], PAGE_PID(parent));

#ifdef BTREE_CHECK_MODE
  parent =
      readPage(tree->fhdl[level + 1], tree->fid[level + 1], PAGE_PARENT(page));
  parenthead = PAGE_HEAD(parent);
  parentdata = PAGE_DATA(parent, Entry);

  for (i = 1; i < PAGE_NUM(parent); i++) {
    assert(parentdata[i].key >= parentdata[i - 1].key);

    page = readPage(tree->fhdl[0], tree->fid[0], ENTRY_GET_PTR(parentdata + i));
    data = PAGE_DATA(page, Entry);
    newdata = PAGE_MAX_KEY(page, Entry);
    if (parentdata[i].key != newdata->key) data = data;
  }
#endif

  if (PAGE_NUM(parent) >= PAGE_MAX_NUM(Entry))
    btree_split(tree, parent, level + 1, trace);
}

int btree_insert(BTree *tree, Entry entry) {
  int offset1;
  Entry *data1;
  Key key = entry.key;
  Page page, page1;
  BlockNum pid = tree->root;
  int trace[5];

  //	assert(tree->fhdl != INVALID_HANDLE_VALUE);

  for (int i = 0; i < tree->nLevel - 1; i++) {
    trace[tree->nLevel - i - 1] = pid;
    page1 = readPage(tree->fhdl[tree->nLevel - i - 1],
                     tree->fid[tree->nLevel - i - 1], pid);
    offset1 = btree_inPageSearch(page1, key);
    data1 = PAGE_DATA(page1, Entry);
    pid = ENTRY_GET_PTR(data1 + offset1);

    assert(pid < tree->nBlock);
  }

  page = readPage(tree->fhdl[0], tree->fid[0], pid);
  btree_inPageInsert(page, entry);
  markDirty(tree->fid[0], pid);
  if (PAGE_NUM(page) >= PAGE_MAX_NUM(Entry)) {
    //		printf("BTREE SPLIT OCUR\n");
    btree_split(tree, page, 0, trace);
  }

  tree->nEntry++;

  return 1;
}

int btree_delete(BTree *tree, Entry entry) {
  int offset, del;
  Entry *data;
  Key key = entry.key;
  Page page;
  BlockNum pid = tree->root;

  assert(tree->fhdl != (int *)INVALID_HANDLE_VALUE);

  for (int i = 0; i < tree->nLevel - 1; i++) {
    page = readPage(tree->fhdl[tree->nLevel - i - 1],
                    tree->fid[tree->nLevel - i - 1], pid);
    offset = btree_inPageSearch(page, key);
    data = PAGE_DATA(page, Entry);
    pid = ENTRY_GET_PTR(data + offset);
  }

  page = readPage(tree->fhdl[0], tree->fid[0], pid);
  del = btree_inPageDelete(page, entry);
  if (del == BTREE_OPT_SUCCESS) {
    tree->nEntry--;
    markDirty(tree->fid[0], pid);
    return 1;
  }
  return 0;
}

void btree_printPageInfo(BTree *tree) {
  /*	Page page;
          PageHead * head;
          int npage = tree->nBlock;

          printf("#### PAGE INFOS ####\n");
          for (int i = 0; i < npage; i++)
          {
                  page = readPage(tree->fhdl, tree->fid, i);
                  head = PAGE_HEAD(page);
                  printf("pid %d: pre %d, next %d, parent %d\n",head->pid,
     head->pre, head->next, head->parent);
          }
  */
}

int btree_copyBTree(BTree *tree1, BTree *tree2) {
  BTreeState *state;
  Entry *e;
  int num;

  state = btree_initRead(tree2);
  num = 0;
  while (1) {
    e = btree_readNextEntry(state);
    if (e == NULL) break;
    num++;
    if (!ENTRY_IS_INTERNAL(e)) {
      assert(ENTRY_IS_FENCE(e));
      btree_insert(tree1, *e);
    }
  }

  return 0;
}

int btree_updateAdditionFence(BTree *tree) {
  BTreeState *state;
  Entry *e;
  int fence, num;

  state = btree_initRead(tree);
  fence = 0;
  num = 0;
  while (1) {
    e = btree_readNextEntry(state);
    if (e == NULL) break;
    num++;
    if (ENTRY_IS_FENCE(e) && !ENTRY_IS_INTERNAL(e)) {
      fence = ENTRY_GET_PTR(e) + 1;
    } else if (ENTRY_IS_INTERNAL(e)) {
      e->ptr = fence;
      ENTRY_SET_INTERNAL(e);
      assert(ENTRY_GET_PTR(e) != INVALID_PAGE);
    }
  }

  return 0;
}

int btree_checkLevel(BTree *tree, int maxBlock) {
  BTreeState *state;
  Entry *e, last, fence;

  state = btree_initRead(tree);
  last.key = fence.key = -1;
  while (1) {
    e = btree_readNextEntry(state);
    if (e == NULL) break;
    if (last.key >= 0) assert(e->key >= last.key);
    if (ENTRY_IS_FENCE(e))
      assert(ENTRY_GET_PTR(e) <= maxBlock || ENTRY_GET_PTR(e) == INVALID_PAGE);
    if (ENTRY_IS_FENCE(e) && !ENTRY_IS_INTERNAL(e)) {
      if (fence.key >= 0) assert(ENTRY_GET_PTR(e) == ENTRY_GET_PTR(&fence) + 1);
      fence = *e;
    } else if (ENTRY_IS_INTERNAL(e)) {
      if (fence.key >= 0) assert(ENTRY_GET_PTR(e) == ENTRY_GET_PTR(&fence) + 1);
    }
    last = *e;
  }

  return 0;
}

/*
 * btree_checkfile() --
 * check the data file, return the number of entries in the file.
 * The keys of all entreis are in increasing order.
 */
int btree_checkfile(char *fname, unsigned int *num) {
  unsigned long long n = 0;
  Entry entry, last;
  ifstream file;

  file.open(fname, ios::in);

  if (!file.is_open()) return FDTREE_FAILED;

  last.key = last.ptr = 0;
  while (file.good()) {
    file >> entry.key >> entry.ptr;
    if (entry.key < last.key) {
      elog(ERROR, "ERROR: all entres in the data file should be sortted.\n");
      elog(ERROR, "Wrong key at %d-th entry.\n", n);
      return FDTREE_FAILED;
    }
    n++;
    last = entry;
  }

  file.close();

  *num = n;

  return FDTREE_SUCCESS;
}
