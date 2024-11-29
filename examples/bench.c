/*
 * Copyright 2023 Regents of Nanjing University of Aeronautics and Astronautics and 
 * Hohai University, Miao Cai <miaocai@nuaa.edu.cn> and Junru Shen <jrshen@hhu.edu.cn>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free 
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 */

#include <coro.h>
#include <dmpool.h>
#include <syscall.h>
#include <unistd.h>
#include <stdatomic.h>

#include "ethanefs.h"
#include "bench.h"

#include <debug.h>

#include "examples/random.h"
#include "rand.h"
#include "ethane.h"

#define SHOW_THROUGHPUT_INTERVAL         1000

#define DELAY_US   0

#define K 224

#define SET_WORKER_FN(fn)      void worker_fn(ethanefs_cli_t *) __attribute__((alias(#fn)))

// =========== =========

struct ThreadLocalStatistic {
  uint64_t thread_id;
  atomic_uint_fast64_t total_cnt;

  atomic_uint_fast64_t mkdir_cnt;
  atomic_uint_fast64_t rmdir_cnt;
  atomic_uint_fast64_t creat_cnt;
  atomic_uint_fast64_t unlink_cnt;
  atomic_uint_fast64_t stat_cnt;

  atomic_uint_fast64_t mkdir_time;
  atomic_uint_fast64_t rmdir_time;
  atomic_uint_fast64_t creat_time;
  atomic_uint_fast64_t unlink_time;
  atomic_uint_fast64_t stat_time;
};


int test_mkdir(ethanefs_cli_t *cli, const char* path, struct ThreadLocalStatistic* statistic) {
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  bench_timer_start(&timer);
  int ret = ethanefs_mkdir(cli, path, 0777);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&statistic->mkdir_time, elapsed_ns);
  atomic_fetch_add(&statistic->mkdir_cnt, 1);
  if (ret != 0) {
    // pr_err("mkdir %s failed: %s", path, strerror(-ret));
  }
  if (atomic_fetch_add(&statistic->total_cnt, 1) % 10000 == 0) {
    pr_info("thread %ld:\nmkdir == latency: %ld ns, throughput: %f per sec\nrmdir == latency: %ld ns, throughput: %f per sec\ncreat == latency: %ld ns, throughput: %f per sec\nunlink == latency: %ld ns, throughput: %f per sec\nstat == latency: %ld ns, throughput: %f per sec\n",
            statistic->thread_id,
            statistic->mkdir_time / statistic->mkdir_cnt, statistic->mkdir_cnt / (statistic->mkdir_time / 1000000000.0),
            statistic->rmdir_time / statistic->rmdir_cnt, statistic->rmdir_cnt / (statistic->rmdir_time / 1000000000.0),
            statistic->creat_time / statistic->creat_cnt, statistic->creat_cnt / (statistic->creat_time / 1000000000.0),
            statistic->unlink_time / statistic->unlink_cnt, statistic->unlink_cnt / (statistic->unlink_time / 1000000000.0),
            statistic->stat_time / statistic->stat_cnt, statistic->stat_cnt / (statistic->stat_time / 1000000000.0));
  }
  return ret;
}

static void test_mkdir_recur(ethanefs_cli_t *cli, const char *path, bool verbose, bool force, struct ThreadLocalStatistic* statistic) {
    char buf[1024];
    char *p, *q;
    int ret;

    strcpy(buf, path);
    p = buf;
    while ((q = strchr(p + 1, '/')) != NULL) {
        *q = '\0';
        ret = test_mkdir(cli, buf, statistic);
        if (ret && ret != -17 && force) {
            pr_err("mkdir %s failed: %s", buf, strerror(-ret));
            exit(1);
        }
        if (verbose) {
            pr_info("mkdir %s done: %s", buf, strerror(-ret));
        }
        *q = '/';
        p = q;
    }
}

int test_rmdir(ethanefs_cli_t *cli, const char* path, struct ThreadLocalStatistic* statistic) {
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  bench_timer_start(&timer);
  int ret = ethanefs_rmdir(cli, path);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&statistic->rmdir_time, elapsed_ns);
  atomic_fetch_add(&statistic->rmdir_cnt, 1);
  if (ret != 0) {
    // pr_err("rmdir %s failed: %s", path, strerror(-ret));
  }
  if ((atomic_fetch_add(&statistic->total_cnt, 1) % 10000) == 0) {
    // pr_info("%ld", statistic->mkdir_cnt);
    // pr_info("%ld", statistic->mkdir_time);
    // pr_info("%ld", statistic->rmdir_cnt);
    // pr_info("%ld", statistic->rmdir_time);
    // pr_info("%ld", statistic->creat_cnt);
    // pr_info("%ld", statistic->creat_time);
    // pr_info("%ld", statistic->unlink_cnt);
    // pr_info("%ld", statistic->unlink_time);
    // pr_info("%ld", statistic->stat_cnt);
    // pr_info("%ld", statistic->stat_time);
    pr_info("thread %ld:\nmkdir == latency: %ld ns, throughput: %f per sec\nrmdir == latency: %ld ns, throughput: %f per sec\ncreat == latency: %ld ns, throughput: %f per sec\nunlink == latency: %ld ns, throughput: %f per sec\nstat == latency: %ld ns, throughput: %f per sec\n",
            statistic->thread_id,
            statistic->mkdir_time / statistic->mkdir_cnt, statistic->mkdir_cnt / (statistic->mkdir_time / 1000000000.0),
            statistic->rmdir_time / statistic->rmdir_cnt, statistic->rmdir_cnt / (statistic->rmdir_time / 1000000000.0),
            statistic->creat_time / statistic->creat_cnt, statistic->creat_cnt / (statistic->creat_time / 1000000000.0),
            statistic->unlink_time / statistic->unlink_cnt, statistic->unlink_cnt / (statistic->unlink_time / 1000000000.0),
            statistic->stat_time / statistic->stat_cnt, statistic->stat_cnt / (statistic->stat_time / 1000000000.0));
  }
  return ret;
}

int test_creat(ethanefs_cli_t *cli, const char* path, struct ThreadLocalStatistic* statistic) {
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  bench_timer_start(&timer);
  ethanefs_open_file_t *fh = ethanefs_create(cli, path, 0777);
  if (!fh) {
    // pr_err("mkdir %s failed", path);
  }
  ethanefs_close(cli, fh);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&statistic->creat_time, elapsed_ns);
  atomic_fetch_add(&statistic->creat_cnt, 1);
  if (atomic_fetch_add(&statistic->total_cnt, 1) % 10000 == 0) {
    pr_info("thread %ld:\nmkdir == latency: %ld ns, throughput: %f per sec\nrmdir == latency: %ld ns, throughput: %f per sec\ncreat == latency: %ld ns, throughput: %f per sec\nunlink == latency: %ld ns, throughput: %f per sec\nstat == latency: %ld ns, throughput: %f per sec\n",
            statistic->thread_id,
            statistic->mkdir_time / statistic->mkdir_cnt, statistic->mkdir_cnt / (statistic->mkdir_time / 1000000000.0),
            statistic->rmdir_time / statistic->rmdir_cnt, statistic->rmdir_cnt / (statistic->rmdir_time / 1000000000.0),
            statistic->creat_time / statistic->creat_cnt, statistic->creat_cnt / (statistic->creat_time / 1000000000.0),
            statistic->unlink_time / statistic->unlink_cnt, statistic->unlink_cnt / (statistic->unlink_time / 1000000000.0),
            statistic->stat_time / statistic->stat_cnt, statistic->stat_cnt / (statistic->stat_time / 1000000000.0));
  }
  return 0;
}

int test_unlink(ethanefs_cli_t *cli, const char* path, struct ThreadLocalStatistic* statistic) {
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  bench_timer_start(&timer);
  int ret = ethanefs_unlink(cli, path);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&statistic->unlink_time, elapsed_ns);
  atomic_fetch_add(&statistic->unlink_cnt, 1);
  if (ret != 0) {
    // pr_err("unlink %s failed: %s", path, strerror(-ret));
  }
  if (atomic_fetch_add(&statistic->total_cnt, 1) % 10000 == 0) {
    pr_info("thread %ld:\nmkdir == latency: %ld ns, throughput: %f per sec\nrmdir == latency: %ld ns, throughput: %f per sec\ncreat == latency: %ld ns, throughput: %f per sec\nunlink == latency: %ld ns, throughput: %f per sec\nstat == latency: %ld ns, throughput: %f per sec\n",
            statistic->thread_id,
            statistic->mkdir_time / statistic->mkdir_cnt, statistic->mkdir_cnt / (statistic->mkdir_time / 1000000000.0),
            statistic->rmdir_time / statistic->rmdir_cnt, statistic->rmdir_cnt / (statistic->rmdir_time / 1000000000.0),
            statistic->creat_time / statistic->creat_cnt, statistic->creat_cnt / (statistic->creat_time / 1000000000.0),
            statistic->unlink_time / statistic->unlink_cnt, statistic->unlink_cnt / (statistic->unlink_time / 1000000000.0),
            statistic->stat_time / statistic->stat_cnt, statistic->stat_cnt / (statistic->stat_time / 1000000000.0));
  }
  return ret;
}

int test_stat(ethanefs_cli_t *cli, const char* path, struct ThreadLocalStatistic* statistic) {
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  struct stat st;
  bench_timer_start(&timer);
  int ret = ethanefs_getattr(cli, path, &st);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&statistic->stat_time, elapsed_ns);
  atomic_fetch_add(&statistic->stat_cnt, 1);
  if (ret != 0) {
    // pr_err("stat %s failed: %s", path, strerror(-ret));
  }
  if (atomic_fetch_add(&statistic->total_cnt, 1) % 10000 == 0) {
    pr_info("thread %ld:\nmkdir == latency: %ld ns, throughput: %f per sec\nrmdir == latency: %ld ns, throughput: %f per sec\ncreat == latency: %ld ns, throughput: %f per sec\nunlink == latency: %ld ns, throughput: %f per sec\nstat == latency: %ld ns, throughput: %f per sec\n",
            statistic->thread_id,
            statistic->mkdir_time / statistic->mkdir_cnt, statistic->mkdir_cnt / (statistic->mkdir_time / 1000000000.0),
            statistic->rmdir_time / statistic->rmdir_cnt, statistic->rmdir_cnt / (statistic->rmdir_time / 1000000000.0),
            statistic->creat_time / statistic->creat_cnt, statistic->creat_cnt / (statistic->creat_time / 1000000000.0),
            statistic->unlink_time / statistic->unlink_cnt, statistic->unlink_cnt / (statistic->unlink_time / 1000000000.0),
            statistic->stat_time / statistic->stat_cnt, statistic->stat_cnt / (statistic->stat_time / 1000000000.0));
  }
  return ret;
}


static void bench_test(ethanefs_cli_t *cli) {
  static atomic_uint_fast64_t thread_id = 0;
  struct ThreadLocalStatistic statistic;
  statistic.thread_id = atomic_fetch_add(&thread_id, 1);
  statistic.total_cnt = 0;
  statistic.mkdir_cnt = 1;
  statistic.rmdir_cnt = 1;
  statistic.creat_cnt = 1;
  statistic.unlink_cnt = 1;
  statistic.stat_cnt = 1;
  statistic.mkdir_time = 1;
  statistic.rmdir_time = 1;
  statistic.creat_time = 1;
  statistic.unlink_time = 1;
  statistic.stat_time = 1;

  int ret = 0;
  struct stat buf;
  const int depth = 4;
  const int total_meta = 251000;
  const int total_file = total_meta / depth;
  const int stat_count = 100000000;

  char basic_path[64];
  sprintf(basic_path, "/uniuqe_dir.%ld/", thread_id);

  char path[1024];
  char dir_name[64];
  char file_name[64];
  int i;
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file%d", i);
    sprintf(dir_name, "dir%d", i);
    for (int d = 0; d < depth - 2; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    test_mkdir_recur(cli, path, true, true, &statistic);
    strcat(path, file_name);
    test_creat(cli, path, &statistic);
  }

  int err = 0;
  for (i = 0; i < stat_count; i++) {
    int id = i % total_file;
    strcpy(path, basic_path);
    sprintf(file_name, "file%d", i);
    sprintf(dir_name, "dir%d", i);
    for (int d = 0; d < depth - 2; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    test_stat(cli, path, &statistic);
  }
  
}


// =========== =========

static void mkdir_recur(ethanefs_cli_t *cli, const char *path, bool verbose, bool force) {
    char buf[1024];
    char *p, *q;
    int ret;

    strcpy(buf, path);
    p = buf;
    while ((q = strchr(p + 1, '/')) != NULL) {
        *q = '\0';
        ret = ethanefs_mkdir(cli, buf, 0777);
        if (ret && force) {
            pr_err("mkdir %s failed: %s", buf, strerror(-ret));
            exit(1);
        }
        if (verbose) {
            pr_info("mkdir %s done: %s", buf, strerror(-ret));
        }
        *q = '/';
        p = q;
    }
}

static void inject_throttling_delay(long delay_us) {
    struct bench_timer timer;
    if (delay_us == 0) {
        return;
    }
    bench_timer_start(&timer);
    while (bench_timer_end(&timer) < delay_us * 1000) {
        coro_yield();
    }
}

// static void bench_mds(ethanefs_cli_t *cli) {
//   mqd_t req_mq;
//   struct mq_attr req_mq_attr = {
//     .mq_flags = 0,
//     .mq_maxmsg = 128,
//     .mq_msgsize = sizeof(struct MDRequest),
//     .mq_curmsgs = 0
//   };
//   mq_unlink("/test_ethane_req_mq");
//   req_mq = mq_open("/test_ethane_req_mq", O_CREAT | O_RDWR, 0666, &req_mq_attr);
//   if (req_mq == -1) {
//     pr_err("failed to open request mq: %s", strerror(errno));
//     exit(-1);
//   }

//   mqd_t resp_mqs[64];
//   struct mq_attr resp_mq_attr = {
//     .mq_flags = 0,
//     .mq_maxmsg = 8,
//     .mq_msgsize = sizeof(struct MDResponse),
//     .mq_curmsgs = 0
//   };
//   for (int i = 0; i < 64; i++) {
//     char resp_mq_name[64];
//     sprintf(resp_mq_name, "%s%d", "/test_ethane_resp_mq_", i);
//     mq_unlink(resp_mq_name);
//     resp_mqs[i] = mq_open(resp_mq_name, O_CREAT | O_RDWR, 0666, &resp_mq_attr);
//     if (resp_mqs[i] == -1) {
//       pr_err("failed to open response mq: %s", strerror(errno));
//       exit(-1);
//     }
//   }

//   int ret = 0;
//   while (true) {
//     struct MDRequest req;
//     struct MDResponse resp;

//     mq_receive(req_mq, &req, sizeof(struct MDRequest), NULL);
//     switch (req.op) {
//       case Mkdir:
//         ret = ethanefs_mkdir(cli, req.path, req.mode);
//         break;
//       case Rmdir:
//         ret = ethanefs_rmdir(cli, req.path);
//         break;
//       case Creat:
//         ethanefs_open_file_t *fh = ethanefs_create(cli, req.path, req.mode);
//         if (!fh) {
//           ret = -1;
//         }
//         ethanefs_close(cli, fh);
//         break;
//       case Unlink:
//         ret = ethanefs_unlink(cli, req.path);
//         break;
//       case Stat:
//         ret = ethanefs_getattr(cli, req.path, &resp.st);
//         break;
//       default:
//         break;
//     }
//     resp.ret = ret;
//     mq_send(resp_mqs[req.client_id], &resp, sizeof(struct MDResponse), 0);
//   }
// }

static void bench_private(ethanefs_cli_t *cli) {
    struct bench_timer timer;
    long elapsed_ns = 0;
    unsigned int seed;
    char path[256];
    int i, ret, id;

    seed = get_rand_seed();

    id = ethanefs_get_cli_id(cli);

    sprintf(path, "/ethane-%d", id);
    ret = ethanefs_mkdir(cli, path, 0777);
    if (ret) {
        printf("%d: create failed: %d\n", id, ret);
        exit(1);
    }

    bench_timer_start(&timer);

    for (i = 0; i < 160000; i++) {
        inject_throttling_delay(DELAY_US);

        sprintf(path, "/ethane-%d/dir-%d", id, i);
        ret = ethanefs_mkdir(cli, path, 0666);
        if (ret) {
            printf("%d: create failed: %d\n", id, ret);
            exit(1);
        }

        if ((i + 1) % SHOW_THROUGHPUT_INTERVAL == 0) {
            elapsed_ns += bench_timer_end(&timer);
            printf("%d: %lu op/s (%d)\n", id, (i + 1) * 1000000000L / elapsed_ns, i + 1);
            bench_timer_start(&timer);
        }
    }

    printf("%d: done\n", id);
}

static void bench_path_lookup(ethanefs_cli_t *cli) {
    struct bench_timer timer;
    long elapsed_ns = 0;
    unsigned int seed;
    char path[1024];
    char dir_name[64];
    char file_name[64];
    int i, ret, id;
    struct stat buf;

    const int depth = 8;
    const int total_meta = 252000;
    const int total_file = total_meta / depth;
    const int stat_count = 10000000;

    seed = get_rand_seed();

    id = ethanefs_get_cli_id(cli);

    // mkdir and creat
    for (i = 0; i < total_file; i++) {
        inject_throttling_delay(DELAY_US);

        strcpy(path, "/");
        sprintf(file_name, "file%d", i);
        sprintf(dir_name, "dir%d", i);
        for (int d = 0; d < depth - 1; d++) {
          strcat(path, dir_name);
          strcat(path, "/");
        }
        mkdir_recur(cli, path, true, true);
        strcat(path, file_name);
        ethanefs_open_file_t *fh = ethanefs_create(cli, path, 0777);
        if (!fh) {
            printf("%d: create failed: %d\n", id, ret);
            exit(1);
        }
        ethanefs_close(cli, fh);
    }

    
    // stat
    int err = 0;
    for (i = 0; i < stat_count; i++) {
        inject_throttling_delay(DELAY_US);

        int id = i % total_file;
        strcpy(path, "/");
        sprintf(file_name, "file%d", id);
        sprintf(dir_name, "dir%d", id);
        for (int d = 0; d < depth - 1; d++) {
          strcat(path, dir_name);
          strcat(path, "/");
        }
        strcat(path, file_name);

        bench_timer_start(&timer);
        ret = ethanefs_getattr(cli, path, &buf);
        elapsed_ns += bench_timer_end(&timer);
        if (ret) {
            pr_err("%d: stat failed: %d (%s)", ethanefs_get_cli_id(cli), ret, path);
            err++;
        }
        
        if ((i + 1) % SHOW_THROUGHPUT_INTERVAL == 0) {
            pr_info("step: %d", i);
            pr_info("throught: %lu op/s (%d) ", (i + 1) * 1000000000L / elapsed_ns, i + 1);
            pr_info("latency: %lu ns (%d) ", elapsed_ns / (i + 1), i + 1);
        }
    }

    printf("%d: done\n", id);
}

static void bench_path_walk(ethanefs_cli_t *cli) {
    const char *target = "/linux/tools/testing/selftests/rcutorture/formal/srcu-cbmc/empty_includes/uapi/linux";
    if (!strcmp(ethanefs_get_hostname(), "node140")) {
        mkdir_recur(cli, target, true,true);
        ethanefs_dump_cli(cli);
        sleep(10);
        ethanefs_dump_remote(cli);
    }
    ethanefs_force_checkpoint(cli);
}

static void bench_skewed_path_walk(ethanefs_cli_t *cli) {
    struct bench_timer timer;
    long id, elapsed_ns;
    struct stat buf;
    char path[256];
    int i, ret;
    int err = 0;

    init_seed();

    init_zipf_generator(0, 10000);

    elapsed_ns = 0;

    bench_timer_start(&timer);

    for (i = 0; ; i++) {
        //id = zipf_next() % 200000;
        id = uniform_next() % 200000;

        sprintf(path, "/a/f%06ld/a1/a2/a3/a4/a5/a6/a7/a8", id);
        ret = ethanefs_getattr(cli, path, &buf);
        if (ret) {
            pr_err("%d: stat failed: %d (%s)", ethanefs_get_cli_id(cli), ret, path);
            err++;
        }

        if ((i + 1) % SHOW_THROUGHPUT_INTERVAL == 0) {
            elapsed_ns += bench_timer_end(&timer);
            pr_info("%lu op/s (%d) err=%d", (i + 1) * 1000000000L / elapsed_ns, i + 1, err);
            bench_timer_start(&timer);
        }
    }
}

static void bench_io_write(ethanefs_cli_t *cli) {
    const int nr_ios = 2560;

    ethanefs_open_file_t *fh;
    struct bench_timer timer;
    long elapsed_ns;
    char path[256];
    void *buf;
    long ret;
    int i;

    elapsed_ns = 0;

    bench_timer_start(&timer);

    buf = malloc(IO_SIZE);
    strcpy(buf, "teststring");

    sprintf(path, "/cli-%d", ethanefs_get_cli_id(cli));
    fh = ethanefs_create(cli, path, 0777);
    ethane_assert(!IS_ERR(fh));
    ret = ethanefs_truncate(cli, fh, IO_SIZE);
    ethane_assert(!ret);

    pr_info("bench_io: use IO size: %lu, file: %s, nr_ios: %d", IO_SIZE, path, nr_ios);

    for (i = 0; i < nr_ios; i++) {
        ret = ethanefs_write(cli, fh, buf, IO_SIZE, 0);
        ethane_assert(ret == IO_SIZE);

        if ((i + 1) % SHOW_THROUGHPUT_INTERVAL == 0) {
            elapsed_ns += bench_timer_end(&timer);
            pr_info("%lu IOPS (%d)", (i + 1) * 1000000000L / elapsed_ns, i + 1);
            bench_timer_start(&timer);
        }
    }
}

static void bench_io_read(ethanefs_cli_t *cli) {
    const int nr_ios = 1048576;

    ethanefs_open_file_t *fh;
    struct bench_timer timer;
    long elapsed_ns;
    char path[256];
    void *buf;
    long ret;
    int i;

    elapsed_ns = 0;

    bench_timer_start(&timer);

    buf = malloc(IO_SIZE * nr_ios);
    strcpy(buf, "teststring");

    sprintf(path, "/a");
    fh = ethanefs_open(cli, path);
    ethane_assert(!IS_ERR(fh));

    pr_info("bench_io: use IO size: %lu, file: %s, nr_ios: %d", IO_SIZE, path, nr_ios);

    for (i = 0; i < nr_ios; i++) {
        ret = ethanefs_read(cli, fh, buf, IO_SIZE, i * IO_SIZE);
        if (!ret) {
            pr_err("ret err: %s", strerror(-ret));
        }

        if ((i + 1) % SHOW_THROUGHPUT_INTERVAL == 0) {
            elapsed_ns += bench_timer_end(&timer);
            pr_info("%lu IOPS (%d)", (i + 1) * 1000000000L / elapsed_ns, i + 1);
            bench_timer_start(&timer);
        }
    }
}

static void bench_path_walk_lat(ethanefs_cli_t *cli) {
    //const char *target = "/linux/tools/testing/selftests/rcutorture/formal/srcu-cbmc/empty_includes/uapi/linux";
    const char *target = "/linux/tools";
    struct bench_timer timer;
    long elapsed_ns;
    struct stat buf;
    int cnt = 0;
    bench_timer_start(&timer);
    mkdir_recur(cli, target, true,false);
    while (true) {
        cnt++;
        ethanefs_test_remote_path_walk(cli, target);
        elapsed_ns = bench_timer_end(&timer);
        if (elapsed_ns > 3000000000ul) {
            printf("%lu IOPS\n", cnt * 1000000000ul / elapsed_ns);
            bench_timer_start(&timer);
            cnt = 0;
        }
    }
}

SET_WORKER_FN(bench_test);
