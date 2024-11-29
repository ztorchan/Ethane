#ifndef ETHANE_POSIXWRAPPER_INCLUDE_
#define ETHANE_POSIXWRAPPER_INCLUDE_

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

void InitWrapper();

void DestroyWrapper();

int ethane_stat(const char* path, struct stat* st);

int ethane_mkdir(const char *path, mode_t mode);

int ethane_rmdir(const char *path);

int ethane_creat(const char *path, mode_t mode);

int ethane_unlink(const char *path);

// int ethane_rename();

#ifdef __cplusplus
}
#endif

#endif // ETHANE_POSIXWRAPPER_INCLUDE_