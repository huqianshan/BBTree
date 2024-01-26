/*-------------------------------------------------------------------------
 *
 * error.cpp
 *	  Error handling interface routines
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
 *	  FDTree: error.cpp,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#include "error.h"

#include <stdarg.h>
#include <stdio.h>

extern int ELOG_LEVEL;

void elog(int level, const std::string format, ...) {
  va_list args;
  char *filename = __FILE__;
  int lineno = __LINE__;

  if (level <= ELOG_LEVEL) {
    va_start(args, format.c_str());
    vprintf(format.c_str(), args);
    va_end(args);
  }
}
