/*-------------------------------------------------------------------------
 *
 * error.h
 *	  Header file for error handling
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
 *	  FDTree: error.h,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#ifndef _ERROR_H_
#define _ERROR_H_
#include <errno.h>

#include <string>
#define ERROR 0
#define WARNING 1
#define INFO 2
#define DEBUG1 3
#define DEBUG2 4
#define DEBUG3 5

void elog(int level, const std::string format, ...);

#define FDTREE_SUCCESS 0
#define FDTREE_FAILED 1

#endif