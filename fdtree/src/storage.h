/*-------------------------------------------------------------------------
 *
 * storage.h
 *	  Header file for storage manager
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
 *	  FDTree: storage.h,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#ifndef _STORAGE_H_
#define _STORAGE_H_

#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

////////////////////////////////////////////////////
//////		Database Record			////////////////
////////////////////////////////////////////////////

typedef long BlockNum;
typedef int Key;

#define MAX_KEY_VALUE 2147483647

typedef struct Entry {
  Key key;
  BlockNum ptr;
} Entry;

////////////////////////////////////////////////////
//////		Page Control			////////////////
////////////////////////////////////////////////////

#define BLKSZ 2048
#define MBLKSZ 524288
// #define MBLKSZ 131072

#define INVALID_PAGE 100000000

typedef char *Page;

typedef struct PageHead {
  BlockNum next;
  BlockNum pre;
  int level;
  int flag;
  int num;
  int parent;
  int pid;
} PageHead;

#define PAGE_HEAD(page) ((PageHead *)page)
#define PAGE_HEAD_SIZE sizeof(PageHead)
#define PAGE_MAX_NUM(item) ((BLKSZ - PAGE_HEAD_SIZE) / sizeof(item))
#define PAGE_LEVEL(page) (((PageHead *)(page))->level)
#define PAGE_NUM(page) (((PageHead *)(page))->num)
#define PAGE_DATA(page, item) ((item *)((page) + PAGE_HEAD_SIZE))
#define PAGE_NEXT(page) (((PageHead *)(page))->next)
#define PAGE_PARENT(page) (((PageHead *)(page))->parent)
#define PAGE_PID(page) (((PageHead *)(page))->pid)
#define PAGE_MAX_KEY(page, item) \
  ((item *)((page) + PAGE_HEAD_SIZE + sizeof(item) * (PAGE_NUM(page) - 1)))

void print_page(Page page);

////////////////////////////////////////////////////
//////		File Operations			////////////////
////////////////////////////////////////////////////

#define INVALID_FILE_PTR (-1)
#define INVALID_HANDLE_VALUE (NULL)
typedef int FilePtr;
typedef int HANDLE;
typedef ssize_t DWORD;

int file_get_new_fid();
FilePtr file_open(int fid, bool isExist);
FilePtr file_reopen(int fid);
int file_close(FilePtr fhdl);
void file_seek(FilePtr fhdl, long long offset);
bool file_trySeek(FilePtr fhdl, long long offset);
DWORD file_read(FilePtr fhdl, Page buffer, long num);
DWORD file_write(FilePtr fhdl, Page buffer, long num);
void file_flush(FilePtr fhdl);
bool file_clearDataDir();
void file_delete(FilePtr fhdl, int fid);
void file_delete(FilePtr fhdlr, FilePtr fhdls, int fid);

#endif