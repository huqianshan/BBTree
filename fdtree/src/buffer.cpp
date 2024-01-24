/*-------------------------------------------------------------------------
 *
 * buffer.cpp
 *	  buffer manager interface routines
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
 *	  FDTree: buffer.cpp,2010/02/24 yinan
 *
 *-------------------------------------------------------------------------
 */

/*
 * Principal entry points:
 *
 * ReadPage() -- find or create a buffer holding the requested page.
 *
 * NewPage() -- find or create a buffer holding the new page.
 *
 * markDirty() -- mark a buffer's contents as "dirty".
 *		The disk write is delayed until buffer replacement.
 *
 * See also these files:
 *		lruque.c -- chooses victim for buffer replacement
 *		pageTable.c -- manages the buffer lookup table
 */

#include "buffer.h"

#include <assert.h>

#include <cstring>

#include "lruque.h"
#include "pageTable.h"
#include "storage.h"

/*
 * Data structure used for buffer management
 */

LRUNode* pnodes = NULL;   // LRU queue nodes
LRUQue* pque = NULL;      // LRU queue head
PageTable* table = NULL;  // Hash table of the pages in buffer pool
Page* pages = NULL;       // the pointers of the pages in buffer pool
int maxpg = 0;
bool writeForce = false;  // write back or write though?

/*
 * Statistics variables used for buffer management
 */

int stat_read;
int stat_hit;

/*
 * deleteBufPool() --
 *		free the space used by buffer management.
 */

void deleteBufPool() {
  delete[] pages[0];
  delete[] pages;
  delete[] pnodes;
}

/*
 * initBufPool() --
 *		allocate the memory for the buffer pool.
 *		it support both write back and write though policies.
 */

int initBufPool(int size, bool force) {
  int num = size / BLKSZ;
  maxpg = num;
  writeForce = force;

  if (pages != NULL) delete[] pages;
  if (pnodes != NULL) delete[] pnodes;

  pages = new Page[num];
  pages[0] = new char[num * BLKSZ];
  for (int i = 1; i < num; i++) pages[i] = pages[i - 1] + BLKSZ;

  pnodes = new LRUNode[num];
  memset(pnodes, 0, sizeof(LRUNode) * num);
  pque = LRUque_init(pnodes, num);
  table = pgtbl_init();

  stat_read = 0;
  stat_hit = 0;
  return 0;
}

/*
 * initBufStat() --
 *		initial the statitics info.
 */

void initBufStat() {
  stat_read = 0;
  stat_hit = 0;
}

/*
 * printBufState() --
 *		print out the statitics info.
 */

void printBufStat() {
  printf("#### buffer statistics info ####\n");
  printf("  reads: %d\n", stat_read);
  printf("  misses: %d\n", stat_read - stat_hit);
  printf("  hits: %d\n", stat_hit);
  printf("  hit rate: %f\n", (float)stat_hit / stat_read);
  printf("################################\n");
}

/*
 * ClearDirty() --
 *		Reset the dirty bit of a page.
 */

void clearDirty(int fid, BlockNum offset) {
  FilePage fp;
  fp.fileid = fid;
  fp.num = offset;

  int id;
  id = pgtbl_find(table, &fp);
  if (id != NO_PAGE) pnodes[id].dirty = 0;
}

/*
 * MarkDirty() --
 *		Marks buffer contents as dirty (actual write happens later).
 */

void markDirty(int fid, BlockNum offset) {
  FilePage fp;
  fp.fileid = fid;
  fp.num = offset;

  int id;
  id = pgtbl_find(table, &fp);
  if (id != NO_PAGE) {
    // mark the dirty bit
    if (writeForce)
      writePage(pnodes[id].fhdl, pnodes[id].fileid, pnodes[id].num, pages[id]);
    else {
      //			if (pnodes[id].dirty == true)
      //				crewrite++;
      pnodes[id].dirty = true;
    }
  }
}

/*
 *	flushAllDirtyPage() --
 *		Write all dirty pages to disk.
 */

void flushAllDirtyPage() {
  int i;
  for (i = 0; i < maxpg; i++) {
    if (pnodes[i].dirty == true) {
      pnodes[i].dirty = false;
      writePage(pnodes[i].fhdl, pnodes[i].fileid, pnodes[i].num, pages[i]);
    }
  }
}

/*
 *	resetAllDirtyBit() --
 *	Reset the dirty bits of all pages to zeros.
 */

void resetAllDirtyBit() {
  int i;
  for (i = 0; i < maxpg; i++) {
    if (pnodes[i].dirty == true) pnodes[i].dirty = false;
  }
}

/*
 *	newPage() --
 *	Allocate a new page in the buffer pool for a disk page.
 *	If there is no empty slot, evict a page and write the page
 *	to disk if neccessary.
 */

Page newPage(HANDLE fhdl, int fid, BlockNum offset) {
  int id;
  FilePage fp;
  fp.fileid = fid;
  fp.num = offset;

  // don't resident in buffers
  FilePage ofp;
  id = LRUque_pushBack(pque, pque->front);
  if (pnodes[id].dirty == true) {
    // write the dirty page
    printf("flush dirty page [%d@file %d]\n", pnodes[id].num,
           pnodes[id].fileid);
    writePage(pnodes[id].fhdl, pnodes[id].fileid, pnodes[id].num, pages[id]);

    pnodes[id].dirty = false;
  }

  ofp.fileid = pnodes[id].fileid;
  ofp.num = pnodes[id].num;
  pgtbl_reinsert(table, &ofp, &fp, id);
  pnodes[id].fileid = fp.fileid;
  pnodes[id].num = fp.num;
  pnodes[id].dirty = true;  // new allocated pages are always dirty
  pnodes[id].fhdl = fhdl;

  // initialize the page head
  Page page = pages[id];
  std::memset(page, 0, BLKSZ);
  PageHead* head = PAGE_HEAD(page);
  head->level = 0;
  head->num = 0;
  head->next = head->pre = head->parent = INVALID_PAGE;
  head->pid = offset;

  return page;
}

/*
 *	readPage() --
 *	read a disk page and store it in a page in the buffer pool.
 *	If there is no empty slot, evict a page and write the page
 *	to disk if neccessary.
 */

Page readPage(HANDLE fhdl, int fid, BlockNum offset) {
  FilePage fp;
  fp.fileid = fid;
  fp.num = offset;

  int id;
  stat_read++;
  id = pgtbl_find(table, &fp);
  if (id != NO_PAGE) {
    assert(PAGE_PID(pages[id]) == offset);
    // reinsert the accessed page to the tail of list
    LRUque_pushBack(pque, pnodes + id);
    stat_hit++;
    return pages[id];
  }

  // don't resident in buffers
  FilePage ofp;

  id = LRUque_pushBack(pque, pque->front);
  if (pnodes[id].dirty == true) {
    // write the dirty page
    printf("flush dirty page [%d@file %d]\n", pnodes[id].num,
           pnodes[id].fileid);
    writePage(pnodes[id].fhdl, pnodes[id].fileid, pnodes[id].num, pages[id]);

    pnodes[id].dirty = false;
  }

  ofp.fileid = pnodes[id].fileid;
  ofp.num = pnodes[id].num;
  pgtbl_reinsert(table, &ofp, &fp, id);
  pnodes[id].fileid = fp.fileid;
  pnodes[id].num = fp.num;
  pnodes[id].fhdl = fhdl;

  Page page = pages[id];
  int nRead;

  file_seek(fhdl, offset);
  nRead = file_read(fhdl, page, BLKSZ);

  assert(nRead == BLKSZ);
  assert(PAGE_PID(pages[id]) == offset);

  return page;
}

/*
 *	writePage() --
 *	write a page in the buffer pool to disk.
 *	invoked by flushAllDirtyPage().
 */

int writePage(HANDLE fhdl, int fid, BlockNum offset, Page page) {
  file_seek(fhdl, offset);
  file_write(fhdl, page, BLKSZ);

  return 0;
}