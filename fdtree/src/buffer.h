/*-------------------------------------------------------------------------
 *
 * buffer.h
 *	  header file for buffer manager implementation.
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
 *	  FDTree: buffer.h, 2010/02/24 yinan
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

#ifndef _BUFFER_H
#define _BUFFER_H

#include <stdio.h>

#include <queue>

#include "storage.h"

using namespace std;

typedef union BlockPoint {
  BlockNum num;
  Page point;
} BlockPoint;

typedef struct FilePage {
  int fileid;
  BlockNum num;
} FilePage;

int initBufPool(int size, bool force = false);
void deletePagePool();

Page newPage(HANDLE fhdl, int fid, BlockNum offset);
Page readPage(HANDLE fhdl, int fid, BlockNum offset);
int writePage(HANDLE fhdl, int fid, BlockNum offset, Page page);
void clearDirty(int fid, BlockNum offset);
void markDirty(int fid, BlockNum offset);
void flushAllDirtyPage();
void resetAllDirtyBit();

void printBufStat();
void initBufStat();

#endif