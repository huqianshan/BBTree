/*-------------------------------------------------------------------------
 *
 * pageTable.cpp
 *	  Page table interface routines for buffer manager
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
 *	  FDTree: pageTable.cpp,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#include <stdio.h>
#include <assert.h>
#include "pageTable.h"

int pgtbl_hash( FilePage * page)
{
	unsigned long t = (unsigned long)(page->fileid);
	t = t << 22;
	t += page->num;
	return t % BUCKET_SIZE;
}

PageTable * pgtbl_init()
{
	PageTable * table = new PageTable();
	for (int i = 0; i < BUCKET_SIZE; i++)
		table->bucket[i] = NULL;
	return table;
}

int pgtbl_find( PageTable * table, FilePage * page)
{
	PageTableNode * p;
	
	int index = pgtbl_hash( page);	
	for (p = table->bucket[index]; p != NULL; p = p->next)
	{	
		if (p->page.fileid == page->fileid && p->page.num == page->num)
			return p->bid;
	}
	return NO_PAGE;
}

void pgtbl_reinsert( PageTable * table, FilePage * opage, FilePage * npage, int bid)
{
	PageTableNode * p, * pre, *b;
	int index;

	if (opage->fileid == 0 && opage->num == INVALID_PAGE)
		opage = NULL;

	//delete
	if (opage != NULL)
	{
		index = pgtbl_hash( opage);

		pre = NULL;
		for (p = table->bucket[index]; p != NULL; p = p->next)
		{	
			if (p->page.fileid == opage->fileid && p->page.num == opage->num)
				break;
			pre = p;
		}
		if (p != NULL)
		{
			assert(p->bid == bid);
			if (pre != NULL)
				pre->next = p->next;
			else
				table->bucket[index] = p->next;
		}
		else
			p = new PageTableNode;
	}
	else
		p = new PageTableNode;

	p->page.fileid = npage->fileid;
	p->page.num = npage->num;
	p->bid = bid;

	//insert
	index = pgtbl_hash( npage);
	b = table->bucket[index];
	p->next = b;
	table->bucket[index] = p;
}