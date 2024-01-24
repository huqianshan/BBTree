/*-------------------------------------------------------------------------
 *
 * pageTable.h
 *	  Header file for page table implementation of buffer manager
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
 *	  FDTree: pageTable.h,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#ifndef _PAGE_TABLE_
#define _PAGE_TABLE_

#include "buffer.h"

//typedef long int BlockNum;

#define BUCKET_SIZE 7999
#define NO_PAGE -1

typedef struct PageTableNode
{
	FilePage page;
	int bid;
	PageTableNode * next;
} PageTableNode;

typedef struct PageTable
{
	PageTableNode * bucket[BUCKET_SIZE];
	int BuckeNum;
} PageTable;

PageTable * pgtbl_init();
int pgtbl_find( PageTable * table, FilePage * page);
void pgtbl_reinsert( PageTable * table, FilePage * opage, FilePage * npage, int bid);

#endif
