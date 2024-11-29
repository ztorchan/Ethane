#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/stat.h>

#include "type.h"
#include "hook/ethane_wrapper.h"

#define ETHANE_MOUNT_POINT "/ethane"
#define ETHANE_FD_BASE (65536)

__attribute__((constructor)) static void setup(void) { InitWrapper(); }

__attribute__((destructor)) static void teardown(void) { DestroyWrapper(); }


static inline void assign_fnptr(void **fnptr, void *fn) {
  *fnptr = fn;
}

#define ASSIGN_FN(fn)                                                          \
  do {                                                                         \
    if (libc_##fn == NULL) {                                                   \
      assign_fnptr((void **)&libc_##fn, dlsym(RTLD_NEXT, #fn));                \
    }                                                                          \
  } while (0)

static inline int is_ethane_path(const char* path) {
  return strncmp(path, ETHANE_MOUNT_POINT, strlen(ETHANE_MOUNT_POINT)) == 0;
}

static inline int is_ethane_fd(int fd) {
  return fd >= ETHANE_FD_BASE;
}

static inline const char* get_ethane_path(const char* path) {
  return path + strlen(ETHANE_MOUNT_POINT);
}

typedef int (*mkdir_t)(const char *path, mode_t mode);
static mkdir_t libc_mkdir = NULL;

int mkdir(const char *path, mode_t mode) {
  ASSIGN_FN(mkdir);
  if (is_ethane_path(path)) {
    return ethane_mkdir(get_ethane_path(path), mode);
  }
  return libc_mkdir(path, mode);
}

typedef int (*rmdir_t)(const char *path);
static rmdir_t libc_rmdir = NULL;

int rmdir(const char *path) {
  ASSIGN_FN(rmdir);
  if (is_ethane_path(path)) {
    return ethane_rmdir(get_ethane_path(path));
  }
  return libc_rmdir(path);
}

typedef int (*creat_t)(const char *path, mode_t mode);
static creat_t libc_creat = NULL;

int creat(const char *path, mode_t mode) {
  ASSIGN_FN(creat);
  if (is_ethane_path(path)) {
    return ethane_creat(get_ethane_path(path), mode);
  }
  return libc_creat(path, mode);
}

typedef int (*unlink_t)(const char *path);
static unlink_t libc_unlink = NULL;

int unlink(const char *path) {
  ASSIGN_FN(unlink);
  if (is_ethane_path(path)) {
    return ethane_unlink(get_ethane_path(path));
  }
  return libc_unlink(path);
}

typedef int (*stat_t)(const char* path, struct stat* st);
static stat_t libc_stat = NULL;

int stat(const char* path, struct stat* st) {
  ASSIGN_FN(stat);
  if (is_ethane_path(path)) {
    return ethane_stat(get_ethane_path(path), st);
  }
  return libc_stat(path, st);
}

typedef int (*mknod_t)(const char *path, mode_t mode, dev_t dev);
static mknod_t libc_mknod = NULL;

int mknod(const char *path, mode_t mode, dev_t dev) {
  ASSIGN_FN(mknod);
  if (is_ethane_path(path)) {
    return ethane_creat(get_ethane_path(path), mode);
  } 
  return libc_mknod(path, mode, dev);
}
