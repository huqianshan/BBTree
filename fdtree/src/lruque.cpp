/*-------------------------------------------------------------------------
 *
 * lruque.cpp
 *	  LRU queue interface routines for buffer manager
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
 *	  FDTree: lruque.cpp,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#include <stdio.h>
#include "lruque.h"

LRUQue * LRUque_init(LRUNode * pool, int n)
{
	LRUQue * que = new LRUQue;
	que->front = &pool[0];
	que->back = &pool[n - 1];
	que->num = n;

	int i = 0;
	for (i = 0; i < n; i++)
	{
		pool[i].id = i;
		pool[i].fileid = 0;
		pool[i].num = INVALID_PAGE;
		pool[i].dirty = false;
		if (i < n - 1)
			pool[i].next = &pool[i+1];
		else
			pool[i].next = NULL;
		if (i > 0)
			pool[i].pre = &pool[i - 1];
		else
			pool[i].pre = NULL;
	}
	return que;
}

int LRUque_pushBack(LRUQue * que, LRUNode * item)
{
	if (item->pre != NULL)
		item->pre->next = item->next;
	if (item->next != NULL)
		item->next->pre = item->pre;
	if (que->back == item)
		que->back = item->pre;
	if (que->front == item)
		que->front = item->next;

	que->back->next = item;
	item->pre = que->back;
	item->next = NULL;
	que->back = item;

//	printf("block: %d\n", item->id);
	return item->id;
}