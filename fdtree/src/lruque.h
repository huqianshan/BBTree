/*-------------------------------------------------------------------------
 *
 * lruque.h
 *	  Header file for LRU queue implementation of buffer manager
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
 *	  FDTree: lruque.h,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#ifndef _LRUQUE_H_
#define _LRUQUE_H_

#include "buffer.h"

typedef struct LRUNode
{
	int id;
	int fileid;
	BlockNum num;
	bool dirty;
	HANDLE fhdl;
	struct LRUNode * next;
	struct LRUNode * pre;
} LRUNode;

typedef struct LRUQue
{
	int num;
	LRUNode * front;
	LRUNode * back;
} LRUQue;

LRUQue * LRUque_init(LRUNode * array, int n);
int LRUque_pushBack(LRUQue * que, LRUNode * item);

#endif