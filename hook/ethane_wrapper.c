#include <stdint.h>
#include <mqueue.h>
#include <stddef.h>
#include <stdlib.h>


#include "debug.h"
#include "type.h"
#include "hook/ethane_wrapper.h"

mqd_t req_mq;
mqd_t resp_mq;
uint64_t client_id;

void InitWrapper() {
  req_mq = mq_open("test_ethane_req_mq", O_RDWR);
  if (req_mq == -1) {
    pr_err("failed to open request mq");
    exit(-1);
  }

  const char *env_rank = getenv("OMPI_COMM_WORLD_NODE_RANK");
  client_id = (env_rank != NULL) ? atoi(env_rank) : 0;
  char resp_mq_name[64];
  sprintf(resp_mq_name, "%s%ld", "test_ethane_resp_mq_", client_id);
  req_mq = mq_open(resp_mq_name, O_RDWR);
  if (req_mq == -1) {
    pr_err("failed to open request mq");
    exit(-1);
  }
}

void DestroyWrapper() {
  mq_close(req_mq);
  mq_close(resp_mq);
}

int zenithfs_stat(const char* path, struct stat* st) {
    struct MDRequest req = {
      .op = Stat,
      .client_id = client_id,
    };
    strcpy(req.path, path);
    struct MDResponse resp;
    mq_send(req_mq, (char *)&req, sizeof(struct MDRequest), 0);
    mq_receive(resp_mq, (char *)&resp, sizeof(struct MDResponse), NULL);
    *st = resp.st;
    return resp.ret;
}

int zenithfs_mkdir(const char *path, mode_t mode) {
  struct MDRequest req = {
    .op = Mkdir,
    .client_id = client_id,
    .mode = mode,
  };
  strcpy(req.path, path);
  struct MDResponse resp;
  mq_send(req_mq, (char *)&req, sizeof(struct MDRequest), 0);
  mq_receive(resp_mq, (char *)&resp, sizeof(struct MDResponse), NULL);
  return resp.ret;
}

int zenithfs_rmdir(const char *path) {
  struct MDRequest req = {
    .op = Rmdir,
    .client_id = client_id,
  };
  strcpy(req.path, path);
  struct MDResponse resp;
  mq_send(req_mq, (char *)&req, sizeof(struct MDRequest), 0);
  mq_receive(resp_mq, (char *)&resp, sizeof(struct MDResponse), NULL);
  return resp.ret;
}

int zenithfs_creat(const char *path, mode_t mode) {
  struct MDRequest req = {
    .op = Creat,
    .client_id = client_id,
    .mode = mode,
  };
  strcpy(req.path, path);
  struct MDResponse resp;
  mq_send(req_mq, (char *)&req, sizeof(struct MDRequest), 0);
  mq_receive(resp_mq, (char *)&resp, sizeof(struct MDResponse), NULL);
  return resp.ret;
}

int zenithfs_unlink(const char *path) {
  struct MDRequest req = {
    .op = Unlink,
    .client_id = client_id,
  };
  strcpy(req.path, path);
  struct MDResponse resp;
  mq_send(req_mq, (char *)&req, sizeof(struct MDRequest), 0);
  mq_receive(resp_mq, (char *)&resp, sizeof(struct MDResponse), NULL);
  return resp.ret;
}
