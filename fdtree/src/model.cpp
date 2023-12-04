/*-------------------------------------------------------------------------
 *
 * model.cpp
 *	  FD-tree cost model routines
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
 *	  FDTree: model.cpp,2011/01/01 yinan
 *
 *-------------------------------------------------------------------------
 */

/*
 * Principal entry points:
 *
 * fdmodel_estimate() -- estimate the optimal k value.
 *
 * See also these files:
 *		fdtree.c -- the fd-tree implementation
 */

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include <fstream>

#include "fdtree.h"
#include "storage.h"

using namespace std;

#define FDMODEL_MAX_HEIGHT 10

/* Device infomation structure */
typedef struct ModelDevice {
  double bandwidth_rndr;
  double bandwidth_rndw;
  double bandwidth_seqr;
  double bandwidth_seqw;
} ModelDevice;

/* FD-tree level state structure */
typedef struct ModelLevelState {
  unsigned long long maxsize;
  unsigned long long nsize;
  unsigned long long nfence;
} ModelLevelState;

/* FD-tree state structure */
typedef struct ModelIndexState {
  int n;
  int l;
  int f;
  int k;
  unsigned long long mem;
  double nRead;
  double nWrite;
  ModelLevelState states[FDMODEL_MAX_HEIGHT];
} ModelIndexState;

/*
 *	fdmodel_init() --
 *	Initialize the index state structure, including the height
 *	of the index, the statistics infos for each level.
 */
void fdmodel_init(ModelIndexState* tree, int n, int k, int mem) {
  int i;
  ModelLevelState* state = tree->states;

  tree->nRead = tree->nWrite = 0;
  tree->f = PAGE_MAX_NUM(Entry);
  tree->k = k;
  tree->n = n;
  tree->mem = mem;

  /* set the maxmium size of all levels. */
  state[0].maxsize = HEADTREE_PAGES_BOUND * BLKSZ / sizeof(Entry);

  tree->l = 0;
  while (state[tree->l++].maxsize < n) {
    assert(tree->l < FDMODEL_MAX_HEIGHT);
    state[tree->l].maxsize = state[tree->l - 1].maxsize * k;
  }

  /* set the number of entries and fences of all levels. */
  state[tree->l - 1].nsize = n;
  for (i = tree->l - 2; i >= 0; i--) {
    state[i].nfence = state[i + 1].nsize / tree->f;
    state[i].nsize = (state[i].maxsize - state[i].nfence) / 2 + state[i].nfence;
    //		state[i].nsize = state[i].nfence;
  }
}

/*
 *	fdmodel_merge() --
 *	Estimate the time of each merge operation by simulating the
 *	merge process. This may invokes recursive merge operation.
 */
int fdmodel_merge(ModelIndexState* tree, int lid) {
  int mergelevel = lid;
  ModelLevelState* state = tree->states;

  assert(state[lid - 1].maxsize <= state[lid - 1].nsize);

  if (lid == 1)
    tree->nRead += state[lid].nsize;
  else
    tree->nRead += state[lid].nsize + state[lid - 1].nsize;
  tree->nWrite +=
      state[lid].nsize + state[lid - 1].nsize - state[lid - 1].nfence;
  state[lid].nsize += state[lid - 1].maxsize - state[lid - 1].nfence;

  //	if (lid > 1)
  //		printf("merge level %d, %f, %f\n", lid, state[lid].nsize,
  //state[lid].maxsize);

  if (state[lid].nsize >= state[lid].maxsize && lid < tree->l - 1) {
    mergelevel = fdmodel_merge(tree, lid + 1);
  }
  state[lid - 1].nfence = state[lid].nsize / tree->f;
  state[lid - 1].nsize =
      state[lid - 1].nsize - state[lid - 1].maxsize + state[lid - 1].nfence;

  return mergelevel;
}

/*
 *	fdmodel_insert() --
 *	Estimate the time of each insertion by simulating the
 *	insertion process.
 */
double fdmodel_insert(ModelIndexState* tree, ModelDevice* device) {
  int mergelevel;
  unsigned long long ninsert;
  double cost;

  /* initialize the accumulated cost of read/write */
  tree->nRead = 0;
  tree->nWrite = 0;
  ninsert = 0;

  do {
    /* insert as much entries as possible into the head tree. */
    tree->states[0].nsize = tree->states[0].maxsize;

    /* accumulate the number of insertted entries. */
    ninsert += tree->states[0].nsize - tree->states[0].nfence;

    /* simulate the merge process */
    mergelevel = fdmodel_merge(tree, 1);
  } while (mergelevel < tree->l - 1);

  cost = tree->nRead * sizeof(Entry) / device->bandwidth_seqr +
         tree->nWrite * sizeof(Entry) / device->bandwidth_seqw;

  return cost / ninsert;
}

/*
 *	fdmodel_search() --
 *	Estimate the time of each search based on a cost model.
 */
double fdmodel_search(ModelIndexState* tree, ModelDevice* device) {
  int memLevel;
  unsigned long long memEntry;
  double cost;

  memLevel = 1 + (int)floor(log((double)tree->mem /
                                (tree->states[0].maxsize * sizeof(Entry))) /
                            log((double)tree->k));
  memEntry = (unsigned long long)(tree->states[0].maxsize *
                                  pow((double)tree->k, (double)memLevel - 1));
  cost = (tree->l - memLevel -
          ((double)tree->mem - memEntry * sizeof(Entry)) /
              (double)tree->states[memLevel].maxsize / sizeof(Entry)) *
         BLKSZ / device->bandwidth_rndr;

  return cost;
}

/*
 *	fdmodel_readline() --
 *	read a line from the specified file.
 */
int fdmodel_readline(FILE* file, char* str, int maxLength) {
  char c;
  int num = 0;

  if (file == NULL) return 0;

  while (1) {
    c = fgetc(file);
    if (c == EOF || c == '\n' || num == maxLength - 1) break;

    str[num++] = c;
  }
  str[num] = '\0';

  return num;
}

/*
 *	fdmodel_loadDeviceInfo() --
 *	Load the device information from the configuration file,
 *	including the bandwidth of random read/write, sequential
 *	read/write.
 */
int fdmodel_loadDeviceInfo(char* fname, ModelDevice* device) {
  int value;
  char str[256];
  ifstream file;

  file.open(fname, ios::in);

  if (!file.is_open()) return 1;

  while (file.good()) {
    file >> str;

    /* skip comment lines */
    if (str[0] == '#' || str[0] == '\0') {
      file.getline(str, 256);
      continue;
    }

    if (strcmp(str, "rndread:") == 0) {
      file >> value;
      device->bandwidth_rndr = (double)value;
    } else if (strcmp(str, "rndwrite:") == 0) {
      file >> value;
      device->bandwidth_rndw = (double)value;
    } else if (strcmp(str, "seqread:") == 0) {
      file >> value;
      device->bandwidth_seqr = (double)value;
    } else if (strcmp(str, "seqwrite:") == 0) {
      file >> value;
      device->bandwidth_seqw = (double)value;
    } else
      return 1;
  }

  file.close();
  return 0;
}

/*
 *	fdmodel_estimate() --
 *	Enumerate all possible values of k, and return the optimal
 *	one based on a cost model.
 *	Input:
 *	n - the number of entries in the index.
 *	memory - the size of memory allocated to the index.
 *	p - insertion rate
 */
int fdmodel_estimate(int n, int memory, double p, char* fname) {
  int min_k, k;
  double t_search, t_insert, t_overall, min_cost;
  ModelDevice device;
  ModelIndexState tree;

  fdmodel_loadDeviceInfo(fname, &device);

  n = (int)(n * 1.05);
  min_cost = 100000000;
  k = 130;
  for (k = 4; k < PAGE_MAX_NUM(Entry) - 1; k++) {
    fdmodel_init(&tree, n, k, memory);

    t_insert = fdmodel_insert(&tree, &device);
    t_search = fdmodel_search(&tree, &device);

    t_overall = t_search * (1 - p) + t_insert * p;

    if (t_overall < min_cost) {
      min_cost = t_overall;
      min_k = k;
    }
  }

  return min_k;
}