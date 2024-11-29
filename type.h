#ifndef TYPE_H
#define TYPE_H

#include <sys/stat.h>
#include <stdint.h>

enum EthaneOpType {
  Mkdir,
  Rmdir,
  Creat,
  Unlink,
  Stat,
};

struct MDRequest {
  enum EthaneOpType op;
  char path[511];
  uint32_t mode;
  uint64_t client_id;
};

struct MDResponse {
  int ret;
  struct stat st;
};

#endif