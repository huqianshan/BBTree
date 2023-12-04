/*-------------------------------------------------------------------------
 *
 * storage.cpp
 *	  Storage manager interface routines
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
 *	  FDTree: storage.cpp,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#include "storage.h"

#include "error.h"

// data path for storing data
char DATAPATH[256] = "data";

int file_get_new_fid() {
  static int curFileId = 100;
  return curFileId++;
}

void print_page(Page page) {
  printf("page: \n");
  Entry* data = PAGE_DATA(page, Entry);
  for (int i = 0; i < PAGE_NUM(page); i++) printf("%d, ", data[i].key);
  printf("\n");
}

int file_open(int fid, bool isExist) {
  char path[256];
  sprintf(path, "%s/%d.dat", DATAPATH, fid);

  int openflag = isExist ? O_RDWR : O_RDWR | O_CREAT;

  int fhdl = open(path, openflag, S_IRUSR | S_IWUSR);
  if (fhdl == -1) {
    elog(ERROR, "ERROR: FileOpen failed (error=%d)\n", errno);
    exit(1);
  }
  return fhdl;
}

int file_close(int fhdl) {
  if (close(fhdl) == -1) {
    elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n", errno);
    exit(1);
  }
  return 1;
}

int file_reopen(int fid) {
  char path[256];
  sprintf(path, "%s/%d.dat", DATAPATH, fid);

  int openflag = O_RDONLY;

  int hdl = open(path, openflag);
  if (hdl == -1) {
    elog(ERROR, "ERROR: FileOpen failed (error=%d)\n", errno);
    exit(1);
  }
  return hdl;
}

void file_seek(int fhdl, long long offset) {
  if (lseek(fhdl, offset * BLKSZ, SEEK_SET) == -1) {
    elog(ERROR, "ERROR: lseek failed (error=%d)\n", errno);
    exit(1);
  }
}

bool file_trySeek(int fhdl, long long offset) {
  if (lseek(fhdl, offset * BLKSZ, SEEK_SET) == -1)
    return false;
  else
    return true;
}
ssize_t file_read(int fhdl, Page buffer, long num) {
  ssize_t nread;
  nread = read(fhdl, buffer, num);
  if (nread == -1) {
    elog(ERROR, "ERROR: FileRead I/O failed (error=%d)\n", errno);
    exit(1);
  }
  return nread;
}

ssize_t file_write(int fhdl, Page buffer, long num) {
  ssize_t nwritten;
  nwritten = write(fhdl, buffer, num);
  if (nwritten == -1 || nwritten != num) {
    elog(ERROR, "ERROR: FileWrite I/O failed (error=%d)\n", errno);
    exit(1);
  }
  return nwritten;
}

ssize_t file_tryWrite(int fhdl, Page buffer, long num) {
  ssize_t nwritten;
  nwritten = write(fhdl, buffer, num);
  if (nwritten == -1 || nwritten != num) {
    return 0;
  }
  return nwritten;
}

void file_flush(int fhdl) {
  if (fsync(fhdl) == -1) {
    elog(ERROR, "ERROR: FileFlush I/O failed (error=%d)\n", errno);
    exit(1);
  }
}

void file_delete(int fhdlr, int fhdls, int fid) {
  char path[256];
  sprintf(path, "%s/%d.dat", DATAPATH, fid);

  if (close(fhdlr) == -1) {
    elog(ERROR, "ERROR: FileClose I/O failed (error=%d)\n", errno);
    exit(1);
  }

  if (close(fhdls) == -1) {
    elog(ERROR, "ERROR: FileClose I/O failed (error=%d)\n", errno);
    exit(1);
  }

  if (unlink(path) == -1) {
    elog(ERROR, "ERROR: FileDelete I/O failed (error=%d)\n", errno);
    exit(1);
  }
}

void file_delete(int fhdl, int fid) {
  char path[256];
  sprintf(path, "%s/%d.dat", DATAPATH, fid);

  if (close(fhdl) == -1) {
    elog(ERROR, "ERROR: FileClose I/O failed (error=%d)\n", errno);
    exit(1);
  }

  if (unlink(path) == -1) {
    elog(ERROR, "ERROR: FileDelete I/O failed (error=%d)\n", errno);
    exit(1);
  }
}

bool file_clearDataDir() {
  DIR* dir;
  struct dirent* entry;
  int fid;
  const char* path = DATAPATH;
  char pdir[256];

  if ((dir = opendir(path)) == NULL) {
    elog(ERROR, "ERROR: opendir failed (error=%d)\n", errno);
    return false;
  }

  while ((entry = readdir(dir)) != NULL) {
    if (sscanf(entry->d_name, "%d.dat", &fid) < 1) continue;

    sprintf(pdir, "%s/%s", path, entry->d_name);

    if (entry->d_type != DT_DIR) {
      if (unlink(pdir) == -1) {
        elog(ERROR, "ERROR: FileDelete I/O failed (error=%d)\n", errno);
        closedir(dir);
        return false;
      }
    }
  }

  closedir(dir);
  return true;
}