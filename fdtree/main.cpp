/*-------------------------------------------------------------------------
 *
 * main.cpp
 *	  main file
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
 *	  FDTree: main.cpp,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#include <assert.h>
#include <math.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <fstream>

#include "Timer.h"
#include "btree.h"
#include "buffer.h"
#include "error.h"
#include "fdtree.h"
#include "storage.h"

using namespace std;

#define FDTREE_MERGE_LAST_LEVEL (2147483647)

int ELOG_LEVEL = INFO;

extern char DATAPATH[256];
char DEVICEPATH[256] = "./config/drive.dat";
char* loadfile = NULL;
char* queryfile = NULL;

int checkquery(char* fname, unsigned int* num, double* psearch, double* pinsert,
               double* pdelete, double* pupdate);

/*
 *	fdtree_test() --
 *	mix workload test on fdtree.
 */
void fdtree_test(int n, int buffer_size, int l, int k, int num_query,
                 int p_search, int p_insert, int p_delete, int p_update) {
  int opt, nLevelMerge = 0, ret, i;
  int nInsert = 0, nSearch = 0, nDelete = 0, nUpdate = 0;
  int key;
  Entry entry;
  int mark = 0;

  if (k == 0) {
    if (l == 0)
      k = fdmodel_estimate(n, buffer_size, p_insert / 100.0, DEVICEPATH);
    else
      k = (int)(ceil(
          pow(((n + 100000) / PAGE_MAX_NUM(Entry)) / HEADTREE_PAGES_BOUND,
              1.0 / (l - 1))));
  }

  if (k >= PAGE_MAX_NUM(Entry)) {
    elog(ERROR, "bad parameter l: please increase the value of l.\n");
    return;
  }

  elog(DEBUG1, "initialize buffer pool...\n");
  initBufPool(buffer_size);

  Setup(2);
  Start(2);
  FDTree* tree = fdtree_bulkload(n, k, loadfile);
  Stop(2);
  elog(INFO, "building time: %fs\n", GetElapsedTime(2));
  elog(DEBUG1, "l: %d, k/f: %d/%d\n", tree->nLevel, tree->k,
       PAGE_MAX_NUM(Entry));

  l = tree->nLevel;

  elog(INFO, "start to warm up...\n");
  for (i = 0; i < 100; i++) {
    key = RANDOM() % n;
    ret = fdtree_point_search(tree, key);
    if (ret == 0) elog(DEBUG2, "key %u not found.\n", key);
  }

  elog(INFO, "start to test...\n");

  Setup(0);  // accumulated overall time
  Reset(0);
  Setup(1);  // accumulated search time
  Reset(1);
  Setup(2);  // accumulated insert time
  Reset(2);
  Setup(3);  // accumulated delete time
  Reset(3);
  Setup(4);  // accumulated update time
  Reset(4);

  if (num_query == 0) num_query = FDTREE_MERGE_LAST_LEVEL;

  Start(0);
  // loop until the last level is updated
  for (i = 0; i < num_query; i++) {
    opt = rand() % 100;
    if (opt < p_search) {
      // search
      nSearch++;
      key = RANDOM() % n;
      Start(1);
      ret = fdtree_point_search(tree, key);
      Stop(1);
      if (ret == 0) elog(DEBUG2, "key %u not found.\n", key);
    } else if (opt < p_search + p_insert) {
      // insertion
      nInsert++;
      entry.key = (RANDOM() % n);
      entry.ptr = RANDOM() % 1024 + 1;
      Start(2);
      nLevelMerge = fdtree_insert(tree, entry);
      Stop(2);

      if (nLevelMerge >= l - 1 && num_query == FDTREE_MERGE_LAST_LEVEL) break;

    } else if (opt < p_search + p_insert + p_delete) {
      nDelete++;
      key = RANDOM() % n;
      Start(3);
      ret = fdtree_point_search(tree, key, &entry);
      if (ret != 0) nLevelMerge = fdtree_delete(tree, entry);
      Stop(3);

      if (nLevelMerge >= l - 1 && num_query == FDTREE_MERGE_LAST_LEVEL) break;
    } else {
      nUpdate++;
      key = RANDOM() % n;
      Start(4);
      ret = fdtree_point_search(tree, key, &entry);
      if (ret != 0) nLevelMerge = fdtree_delete(tree, entry);
      entry.ptr = rand();
      nLevelMerge += fdtree_insert(tree, entry);
      Stop(4);

      if (nLevelMerge >= l - 1 && num_query == FDTREE_MERGE_LAST_LEVEL) break;
    }
  }

  Stop(0);

  elog(INFO, "tree size: %d, k: %d, level: %d\n", n, tree->k, tree->nLevel);
  elog(INFO, "number of insertion: %d\n", nInsert);
  elog(INFO, "number of search: %d\n", nSearch);
  elog(INFO, "number of delete: %d\n", nDelete);
  elog(INFO, "number of update: %d\n", nUpdate);
  for (i = 1; i < tree->nLevel; i++)
    elog(INFO, "number of merges at level %d: %d\n", i,
         tree->levels[i].nAccMerge);

  elog(INFO, "total time: %fs\n", GetElapsedTime(0));
  elog(INFO, "search time: %fs\n", GetElapsedTime(1));
  elog(INFO, "insert time: %fs\n", GetElapsedTime(2));
  elog(INFO, "delete time: %fs\n", GetElapsedTime(3));
  elog(INFO, "update time: %fs\n", GetElapsedTime(4));
}

/*
 *	fdtree_test_load() --
 *	mix workload test on fdtree. The queries are loaded from
 *	the specified file.
 */
void fdtree_test_load(int n, int buffer_size, int l, int k, char* fname) {
  int ret, i;
  int nInsert = 0, nSearch = 0, nDelete = 0, nUpdate = 0;
  double psearch, pinsert, pdelete, pupdate, p;
  int key;
  unsigned int num_query;
  Entry entry;
  char c;
  int err;
  FILE* file;

  err = checkquery(fname, &num_query, &psearch, &pinsert, &pdelete, &pupdate);
  if (err == FDTREE_FAILED) return;

  p = (pinsert + pdelete + 2 * pupdate) /
      (psearch + pinsert + 2 * pdelete + 3 * pupdate);

  if (k == 0) {
    if (l == 0)
      k = fdmodel_estimate(n, buffer_size, p, DEVICEPATH);
    else
      k = (int)(ceil(
          pow(((n + 100000) / PAGE_MAX_NUM(Entry)) / HEADTREE_PAGES_BOUND,
              1.0 / (l - 1))));
  }

  if (k >= PAGE_MAX_NUM(Entry)) {
    elog(ERROR, "bad parameter l: please increase the value of l.\n");
    return;
  }

  elog(DEBUG1, "initialize buffer pool...\n");
  initBufPool(buffer_size);

  Setup(2);
  Start(2);
  FDTree* tree = fdtree_bulkload(n, k, loadfile);
  Stop(2);
  elog(INFO, "building time: %fs\n", GetElapsedTime(2));
  elog(DEBUG1, "l: %d, k/f: %d/%d\n", tree->nLevel, tree->k,
       PAGE_MAX_NUM(Entry));

  n = tree->n;
  l = tree->nLevel;

  elog(INFO, "start to warm up...\n");
  for (i = 0; i < 100; i++) {
    key = RANDOM() % n;
    ret = fdtree_point_search(tree, key);
    if (ret == 0) elog(DEBUG2, "key %u not found.\n", key);
  }

  elog(INFO, "start to test...\n");

  Setup(0);  // accumulated overall time
  Reset(0);
  Setup(1);  // accumulated search time
  Reset(1);
  Setup(2);  // accumulated insert time
  Reset(2);
  Setup(3);  // accumulated delete time
  Reset(3);
  Setup(4);  // accumulated update time
  Reset(4);

  file = fopen(fname, "r");
  if (file == NULL) return;

  Start(0);
  // loop until the last level is updated
  for (i = 0; i < num_query; i++) {
    if (fscanf(file, "%c", &c) < 1) return;

    if (c == 's') {
      // search
      nSearch++;
      if (fscanf(file, "%u", &key) < 1) return;
      Start(1);
      ret = fdtree_point_search(tree, key);
      Stop(1);
      if (ret == 0) elog(DEBUG2, "key %u not found.\n", key);

      fscanf(file, "%c", &c);
    } else if (c == 'i') {
      // insertion
      nInsert++;
      if (fscanf(file, "%u", &(entry.key)) < 1) return;
      if (fscanf(file, "%u", &(entry.ptr)) < 1) return;
      Start(2);
      fdtree_insert(tree, entry);
      Stop(2);

      fscanf(file, "%c", &c);
    } else if (c == 'd') {
      nDelete++;
      if (fscanf(file, "%u", &(entry.key)) < 1) return;
      Start(3);
      ret = fdtree_point_search(tree, key, &entry);
      if (ret != 0) fdtree_delete(tree, entry);
      Stop(3);

      fscanf(file, "%c", &c);
    } else {
      nUpdate++;
      if (fscanf(file, "%u", &key) < 1) return;

      Start(4);
      ret = fdtree_point_search(tree, key, &entry);
      if (ret != 0) fdtree_delete(tree, entry);

      if (fscanf(file, "%u", &(entry.ptr)) < 1) return;
      fdtree_insert(tree, entry);
      Stop(4);

      fscanf(file, "%c", &c);
    }
  }

  Stop(0);

  fclose(file);

  elog(INFO, "tree size: %d, k: %d, level: %d\n", n, tree->k, tree->nLevel);
  elog(INFO, "number of insertion: %d\n", nInsert);
  elog(INFO, "number of search: %d\n", nSearch);
  elog(INFO, "number of delete: %d\n", nDelete);
  elog(INFO, "number of update: %d\n", nUpdate);
  for (i = 1; i < tree->nLevel; i++)
    elog(INFO, "number of merges at level %d: %d\n", i,
         tree->levels[i].nAccMerge);

  elog(INFO, "total time: %fs\n", GetElapsedTime(0));
  elog(INFO, "search time: %fs\n", GetElapsedTime(1));
  elog(INFO, "insert time: %fs\n", GetElapsedTime(2));
  elog(INFO, "delete time: %fs\n", GetElapsedTime(3));
  elog(INFO, "update time: %fs\n", GetElapsedTime(4));
}

/*
 *	btree_test() --
 *	mix workload test on btree.
 */
void btree_test(int n, int buffer_size, int num_query, int p_search,
                int p_insert, int p_delete, int p_update) {
  int opt, ret, i;
  int nInsert = 0, nSearch = 0, nDelete = 0, nUpdate = 0;
  int key;
  int nLevelMerge = 0;
  Entry entry;
  int mark = 0;

  initBufPool(buffer_size);
  elog(DEBUG1, "initialize buffer pool...\n");

  Setup(2);
  Start(2);
  BTree* tree = btree_bulkload(n, 0.7, loadfile);
  Stop(2);
  elog(INFO, "building time: %fs\n", GetElapsedTime(2));

  for (i = 0; i < buffer_size / BLKSZ; i++) {
    key = RANDOM() % n;
    ret = btree_point_query(tree, key);
    if (ret == 0) elog(DEBUG2, "key %u not found.\n", key);
  }
  elog(DEBUG1, "start to warm up...\n");
  elog(INFO, "start to test...\n");

  Setup(0);  // accumulated overall time
  Reset(0);
  Setup(1);  // accumulated search time
  Reset(1);
  Setup(2);  // accumulated insert time
  Reset(2);
  Setup(3);  // accumulated delete time
  Reset(3);
  Setup(4);  // accumulated update time
  Setup(4);

  Start(0);
  // loop until the last level is updated
  for (i = 0; i < num_query; i++) {
    opt = rand() % 100;
    if (opt < p_search) {
      // search
      nSearch++;
      key = RANDOM() % n;
      Start(1);
      ret = btree_point_query(tree, key);
      Stop(1);
      if (ret == 0) elog(ERROR, "error at key %d!\n", key);
    } else if (opt < p_search + p_insert) {
      // insertion
      nInsert++;
      entry.key = (RANDOM() % n);
      entry.ptr = RANDOM() % 1024 + 1;
      Start(2);
      nLevelMerge = btree_insert(tree, entry);
      Stop(2);
    } else if (opt < p_search + p_insert + p_delete) {
      nDelete++;
      key = RANDOM() % n;
      Start(3);
      ret = btree_point_query(tree, key, &entry);
      if (ret != INVALID_PAGE) nLevelMerge = btree_delete(tree, entry);
      Stop(3);
    } else {
      nUpdate++;
      key = RANDOM() % n;
      Start(4);
      btree_update(tree, key, rand());
      Stop(4);
    }
  }

  Stop(0);

  elog(INFO, "n: %ds\n", n);

  elog(INFO, "number of search: %d\n", nSearch);
  elog(INFO, "number of insertion: %d\n", nInsert);
  elog(INFO, "number of delete: %d\n", nDelete);
  elog(INFO, "number of update: %d\n", nUpdate);
  elog(INFO, "####################################\n");

  elog(INFO, "overall time: %fs\n", GetElapsedTime(0));
  elog(INFO, "search time: %fs\n", GetElapsedTime(1));
  elog(INFO, "insert time: %fs\n", GetElapsedTime(2));
  elog(INFO, "delete time: %fs\n", GetElapsedTime(3));
  elog(INFO, "update time: %fs\n", GetElapsedTime(4));
}

/*
 *	generateFile() --
 *	a sample program to generate data file.
 */
int generateFile(char* fname, unsigned int num) {
  unsigned long long i = 0;
  Entry entry;
  ofstream file;

  file.open(fname, ios::out);

  if (!file.is_open()) return FDTREE_FAILED;

  for (i = 0; i < num; i++) {
    entry.key = i * 2 + 1;
    entry.ptr = rand() % 100;
    file << entry.key << " ";
    file << entry.ptr << " ";
  }

  file.close();

  return FDTREE_SUCCESS;
}

int generateQuery(char* fname, int n) {
  int i, opt;
  unsigned int key, ptr;
  FILE* file;
  int p_search = 80;
  int p_insert = 20;
  int p_delete = 0;
  int p_update = 0;
  int nkey = 16 * 1024 * 1024;

  file = fopen(fname, "w");
  if (file == NULL) return FDTREE_FAILED;

  // loop until the last level is updated
  for (i = 0; i < n; i++) {
    opt = rand() % 100;
    key = (RANDOM() % nkey) * 2 + 1;
    ptr = RANDOM() % 100;
    if (opt < p_search) {
      // search
      fprintf(file, "s %u\n", key);
    } else if (opt < p_search + p_insert) {
      // insertion
      fprintf(file, "i %u %u\n", key, ptr);
    } else if (opt < p_search + p_insert + p_delete) {
      fprintf(file, "d %u\n", key);
    } else {
      fprintf(file, "u %u %u\n", key, ptr);
    }
  }

  fclose(file);

  return FDTREE_SUCCESS;
}

int checkquery(char* fname, unsigned int* num, double* psearch, double* pinsert,
               double* pdelete, double* pupdate) {
  FILE* file;
  char c;
  unsigned int key, ptr;
  unsigned int nsearch, ninsert, ndelete, nupdate, nopt;

  file = fopen(fname, "r");
  if (file == 0) return FDTREE_FAILED;

  *num = 0;
  nsearch = ninsert = ndelete = nupdate = 0;
  while (1) {
    if (fscanf(file, "%c", &c) < 1) {
      if (feof(file)) break;

      elog(ERROR, "wrong query file format\n");
      return FDTREE_FAILED;
    }

    if (fscanf(file, "%u", &key) < 1) {
      elog(ERROR, "wrong query file format\n");
      return FDTREE_FAILED;
    }

    if (c == 's') {
      nsearch++;
    } else if (c == 'i') {
      ninsert++;
      fscanf(file, "%u", &ptr);
    } else if (c == 'd') {
      ndelete++;
    } else if (c == 'u') {
      nupdate++;
      fscanf(file, "%u", &ptr);
    } else {
      elog(ERROR, "wrong query file format\n");
      return FDTREE_FAILED;
    }

    fscanf(file, "%c", &c);
    *num = *num + 1;
  }

  nopt = nsearch + ninsert + ndelete + nupdate;
  *psearch = ((double)(nsearch)) / nopt;
  *pinsert = ((double)(ninsert)) / nopt;
  *pdelete = ((double)(ndelete)) / nopt;
  *pupdate = ((double)(nupdate)) / nopt;

  return FDTREE_SUCCESS;
}

/*
 *	getArguVal() --
 *	parse the integer.
 */
int getArguVal(char* str) {
  char* c;
  int base;

  c = strchr(str, 'k');
  if (c == NULL) c = strchr(str, 'K');
  if (c == NULL) c = strchr(str, 'm');
  if (c == NULL) c = strchr(str, 'M');
  if (c == NULL) c = strchr(str, 'g');
  if (c == NULL) c = strchr(str, 'G');

  if (c == NULL) return atoi(str);

  if (*c == 'k' || *c == 'K')
    base = 1024;
  else if (*c == 'm' || *c == 'M')
    base = 1024 * 1024;
  else if (*c = 'g' || *c == 'G')
    base = 1024 * 1024 * 1024;
  else
    base = 1;

  *c = '\0';
  return base * atoi(str);
}

void version() {
  char str[256];
  ifstream file;

  file.open("VERSION.txt", ios::in);
  if (!file.is_open()) return;

  file.getline(str, 256);
  printf("%s\n", str);
}

/*
 *	help() --
 *	print the help message
 */
void help() {
  printf("Usage:\n");
  printf("  fdtree.exe [options]* -c <int> -n <int>\n");
  printf("  -c/--cardinality <int> specify the index cardinality\n");
  printf("  -n/--nquery <int>      specify the number of queries\n");
  printf(
      "                         If -q 0, perform sufficient queries (and "
      "insertions)");
  printf("                         to make the lowest two levels be merged\n");
  printf("Options:\n");
  printf(
      "  -k <int>               specify the size ratio between adjacent "
      "levels.\n");
  printf(
      "                         (default: using cost model to automatically "
      "choose k)\n");
  printf("  -l <int>               specify the number of levels\n");
  printf(
      "                         (default: using cost model to automatically "
      "choose l)\n");
  printf("  -b/--buffer <int>      specify the size of buffer pool\n");
  printf("                         (default: 16M)\n");
  printf("  -p/--path <str>        specify the data directory\n");
  printf("                         (default: ./data/)\n");
  printf("  -d/--drive <str>       specify the config file for the drive\n");
  printf("                         (default: ./config/drive.dat)\n");
  printf(
      "  -f/--loadfile <str>    specify the data file for building the "
      "index\n");
  printf("                         if -f is specified, ignore -c\n");
  printf("  -q/--queryfile <str>   specify the query file\n");
  printf("                         if -q is specified, ignore -n\n");
  printf("  -r/--ratio <int> <int> <int> <int>\n");
  printf(
      "                         specify the percentage of search, insertion, "
      "deletion\n");
  printf("                         and update, respectively (0-100)\n");
  printf("                         (default: 50%% search + 50%% insertion)\n");
  printf("  -v/--version           print version\n");
  printf("  -h/--help              print this usage message\n");
}

int main(int argc, char* argv[]) {
  int i;
  int n = 0;
  int b = 1024 * 1024 * 16;
  int index = 0;
  int p = 50;
  int l = 0;
  int k = 0;
  int num_query = 0, p_search = 50, p_insert = 50, p_delete = 0, p_update = 0;

  srand(time(NULL));

  for (i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0 || strcmp(argv[i], "--cardinality") == 0)
      n = getArguVal(argv[++i]);
    else if (strcmp(argv[i], "-b") == 0 || strcmp(argv[i], "--buffer") == 0)
      b = getArguVal(argv[++i]);
    else if (strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--path") == 0)
      strcpy(DATAPATH, argv[++i]);
    else if (strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "--drive") == 0)
      strcpy(DEVICEPATH, argv[++i]);
    else if (strcmp(argv[i], "-f") == 0 || strcmp(argv[i], "--loadfile") == 0) {
      loadfile = new char[256];
      strcpy(loadfile, argv[++i]);
    } else if (strcmp(argv[i], "-q") == 0 ||
               strcmp(argv[i], "--queryfile") == 0) {
      queryfile = new char[256];
      strcpy(queryfile, argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0 || strcmp(argv[i], "--nquery") == 0)
      num_query = getArguVal(argv[++i]);
    else if (strcmp(argv[i], "-r") == 0 || strcmp(argv[i], "--ratio") == 0) {
      p_search = atoi(argv[++i]);
      p_insert = atoi(argv[++i]);
      p_delete = atoi(argv[++i]);
      p_update = atoi(argv[++i]);
    } else if (strcasecmp(argv[i], "-e") == 0) {
    } else if (strcmp(argv[i], "-e") == 0) {
      i++;
      if (strcmp(argv[i], "ERROR") == 0)
        ELOG_LEVEL = ERROR;
      else if (strcmp(argv[i], "WARNING") == 0)
        ELOG_LEVEL = WARNING;
      else if (strcmp(argv[i], "INFO") == 0)
        ELOG_LEVEL = INFO;
      else if (strcmp(argv[i], "DEBUG1") == 0)
        ELOG_LEVEL = DEBUG1;
      else if (strcmp(argv[i], "DEBUG2") == 0)
        ELOG_LEVEL = DEBUG2;
      else if (strcmp(argv[i], "DEBUG3") == 0)
        ELOG_LEVEL = DEBUG3;
      else {
        elog(ERROR, "invalid elog mode.\n");
        goto reinput;
      }
    } else if (strcmp(argv[i], "-l") == 0 || strcmp(argv[i], "--level") == 0)
      l = atoi(argv[++i]);
    else if (strcmp(argv[i], "-k") == 0)
      k = atoi(argv[++i]);
    else if (strcmp(argv[i], "-i") == 0 || strcmp(argv[i], "--index") == 0) {
      i++;
      if (strcmp(argv[i], "fdtree") == 0)
        index = 0;
      else if (strcmp(argv[i], "btree") == 0)
        index = 1;
      else
        goto reinput;
    } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      help();
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--version") == 0) {
      version();
      return 0;
    } else
      goto reinput;
  }

  if (loadfile == NULL && n == 0) {
    elog(ERROR, "ERROR: please specify -c\n");
    goto reinput;
  }

  if (queryfile == NULL && num_query == 0) {
    elog(ERROR, "ERROR: please specify -n\n");
    goto reinput;
  }

  if (p_search < 0 || p_search > 100 || p_insert < 0 || p_insert > 100 ||
      p_delete < 0 || p_delete > 100 || p_update < 0 || p_update > 100) {
    elog(ERROR,
         "ERROR: wrong format of -r. All percentages should be in the range "
         "[0, 100].\n");
    goto reinput;
  }

  if (p_search + p_insert + p_delete + p_update != 100) {
    elog(ERROR,
         "ERROR: wrong fromat of -r. The sum of all percentages should equal "
         "to 100.\n");
    goto reinput;
  }

  file_clearDataDir();

  switch (index) {
    case 0:
      // FD-Tree Test
      elog(DEBUG1, "test on FD-tree\n");
      elog(DEBUG1, "buffer size: %d\n", b);
      elog(DEBUG1, "Index cardinality: %d\n", n);

      if (queryfile == NULL)
        fdtree_test(n, b, l, k, num_query, p_search, p_insert, p_delete,
                    p_update);
      else
        fdtree_test_load(n, b, l, k, queryfile);

      break;
    case 1:
      // B+-Tree Test
      elog(DEBUG1, "test on B+-tree\n");
      elog(DEBUG1, "buffer size: %d\n", b);
      elog(DEBUG1, "Index cardinality: %d\n", n);

      btree_test(n, b, num_query, p_search, p_insert, p_delete, p_update);

      break;
    default:
      goto reinput;
  }

  elog(DEBUG1, "finished.\n");
  return 0;

reinput:
  help();
  elog(ERROR, "Invalid Parameters.\n");
  return 0;
}