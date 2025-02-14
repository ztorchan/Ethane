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
#include <assert.h>
#include <stdlib.h>

#include "cachefs.h"
#include "ethanefs.h"
#include "bench.h"

#include <debug.h>

#include "examples/random.h"
#include "rand.h"
#include "ethane.h"

#define PRINT_INTERVAL                  10000

#define SHOW_THROUGHPUT_INTERVAL         1000

#define DELAY_US   10

#define K 224

#define SET_WORKER_FN(fn)      void worker_fn(ethanefs_cli_t *) __attribute__((alias(#fn)))

#define NODE_ID 0

// =========== =========

struct ThreadLocalStatistic {
  atomic_uint_fast64_t mkdir_cnt;
  atomic_uint_fast64_t rmdir_cnt;
  atomic_uint_fast64_t creat_cnt;
  atomic_uint_fast64_t unlink_cnt;
  atomic_uint_fast64_t stat_cnt;

  atomic_uint_fast64_t mkdir_fail_cnt;
  atomic_uint_fast64_t rmdir_fail_cnt;
  atomic_uint_fast64_t creat_fail_cnt;
  atomic_uint_fast64_t unlink_fail_cnt;
  atomic_uint_fast64_t stat_fail_cnt;

  atomic_uint_fast64_t mkdir_time;
  atomic_uint_fast64_t rmdir_time;
  atomic_uint_fast64_t creat_time;
  atomic_uint_fast64_t unlink_time;
  atomic_uint_fast64_t stat_time;

  struct bench_timer timer;
  void* _ext;
};

struct GlobalStatistic {
  atomic_uint_fast64_t total_cnt;
  atomic_uint_fast64_t thread_num;
  atomic_uint_fast64_t running_thread;
  struct ThreadLocalStatistic thread_statistic[256];
};

struct GlobalStatistic global_statistic = {
  .total_cnt = 0,
  .thread_num = 0,
  .running_thread = 0,
};

uint64_t zipf_size;
double* zipf_probs;

void init_zipf_probs(double alpha, int n) {
  zipf_size = n;
  zipf_probs = (double*)malloc(sizeof(double) * (n + 1));
  double c = 0;
  for (int i = 1; i <= n; i++) {
    c += 1.0 / pow((double) i, alpha);
  }
  c = 1.0 / c;

  double sum_probs = 0;
  zipf_probs[0] = 0;
  for (int i = 1; i <= n; i++) {
    sum_probs += c / pow((double) i, alpha);
    zipf_probs[i] = sum_probs;
  }
}

uint64_t zipf_generate(uint64_t base, uint64_t bias) {
  double z;
  do {
    z = (double)rand() / RAND_MAX;
  } while (z <= 0 || z >= 1);

  uint64_t low = 1, high = zipf_size, mid;
  uint64_t zipf_value = 0;
  do {
    mid = floor((low + high) / 2);
    if (zipf_probs[mid] >= z && zipf_probs[mid - 1] < z) {
      zipf_value = mid;
    } else if (zipf_probs[mid] >= z) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  } while (low <= high);
  assert((zipf_value >= 1) && (zipf_value <= zipf_size));
  return base + zipf_value + bias;
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

void init_statistic(uint64_t thread_id) {
  atomic_init(&global_statistic.thread_statistic[thread_id].mkdir_cnt, 1);
  atomic_init(&global_statistic.thread_statistic[thread_id].rmdir_cnt, 1);
  atomic_init(&global_statistic.thread_statistic[thread_id].creat_cnt, 1);
  atomic_init(&global_statistic.thread_statistic[thread_id].unlink_cnt, 1);
  atomic_init(&global_statistic.thread_statistic[thread_id].stat_cnt, 1);
  atomic_init(&global_statistic.thread_statistic[thread_id].mkdir_fail_cnt, 0);
  atomic_init(&global_statistic.thread_statistic[thread_id].rmdir_fail_cnt, 0);
  atomic_init(&global_statistic.thread_statistic[thread_id].creat_fail_cnt, 0);
  atomic_init(&global_statistic.thread_statistic[thread_id].unlink_fail_cnt, 0);
  atomic_init(&global_statistic.thread_statistic[thread_id].stat_fail_cnt, 0);
  atomic_init(&global_statistic.thread_statistic[thread_id].mkdir_time, 1);
  atomic_init(&global_statistic.thread_statistic[thread_id].rmdir_time, 1);
  atomic_init(&global_statistic.thread_statistic[thread_id].creat_time, 1);
  atomic_init(&global_statistic.thread_statistic[thread_id].unlink_time, 1);
  atomic_init(&global_statistic.thread_statistic[thread_id].stat_time, 1);
}

void print_statistic() {
  pr_info("total cnt: %ld", atomic_load(&global_statistic.total_cnt));

  uint64_t total_mkdir_cnt = 0;
  uint64_t total_mkdir_fail_cnt = 0;
  uint64_t total_mkdir_time = 0;
  uint64_t total_rmdir_cnt = 0;
  uint64_t total_rmdir_fail_cnt = 0;
  uint64_t total_rmdir_time = 0;
  uint64_t total_creat_cnt = 0;
  uint64_t total_creat_fail_cnt = 0;
  uint64_t total_creat_time = 0;
  uint64_t total_unlink_cnt = 0;
  uint64_t total_unlink_fail_cnt = 0;
  uint64_t total_unlink_time = 0;
  uint64_t total_stat_cnt = 0;
  uint64_t total_stat_fail_cnt = 0;
  uint64_t total_stat_time = 0;

  uint64_t total_mkdir_latency = 0;
  double total_mkdir_throughput = 0;
  uint64_t total_rmdir_latency = 0;
  double total_rmdir_throughput = 0;
  uint64_t total_creat_latency = 0;
  double total_creat_throughput = 0;
  uint64_t total_unlink_latency = 0;
  double total_unlink_throughput = 0;
  uint64_t total_stat_latency = 0;
  double total_stat_throughput = 0;

  for (int i = 0; i < global_statistic.thread_num; i++) {
    total_mkdir_cnt += atomic_load(&global_statistic.thread_statistic[i].mkdir_cnt);
    total_mkdir_fail_cnt += atomic_load(&global_statistic.thread_statistic[i].mkdir_fail_cnt);
    total_mkdir_time += atomic_load(&global_statistic.thread_statistic[i].mkdir_time);
    total_rmdir_cnt += atomic_load(&global_statistic.thread_statistic[i].rmdir_cnt);
    total_rmdir_fail_cnt += atomic_load(&global_statistic.thread_statistic[i].rmdir_fail_cnt);
    total_rmdir_time += atomic_load(&global_statistic.thread_statistic[i].rmdir_time);
    total_creat_cnt += atomic_load(&global_statistic.thread_statistic[i].creat_cnt);
    total_creat_fail_cnt += atomic_load(&global_statistic.thread_statistic[i].creat_fail_cnt);
    total_creat_time += atomic_load(&global_statistic.thread_statistic[i].creat_time);
    total_unlink_cnt += atomic_load(&global_statistic.thread_statistic[i].unlink_cnt);
    total_unlink_fail_cnt += atomic_load(&global_statistic.thread_statistic[i].unlink_fail_cnt);
    total_unlink_time += atomic_load(&global_statistic.thread_statistic[i].unlink_time);
    total_stat_cnt += atomic_load(&global_statistic.thread_statistic[i].stat_cnt);
    total_stat_fail_cnt += atomic_load(&global_statistic.thread_statistic[i].stat_fail_cnt);
    total_stat_time += atomic_load(&global_statistic.thread_statistic[i].stat_time);

    total_mkdir_throughput += atomic_load(&global_statistic.thread_statistic[i].mkdir_cnt) / (atomic_load(&global_statistic.thread_statistic[i].mkdir_time) / 1000000000.0);
    total_rmdir_throughput += atomic_load(&global_statistic.thread_statistic[i].rmdir_cnt) / (atomic_load(&global_statistic.thread_statistic[i].rmdir_time) / 1000000000.0);
    total_creat_throughput += atomic_load(&global_statistic.thread_statistic[i].creat_cnt) / (atomic_load(&global_statistic.thread_statistic[i].creat_time) / 1000000000.0);
    total_unlink_throughput += atomic_load(&global_statistic.thread_statistic[i].unlink_cnt) / (atomic_load(&global_statistic.thread_statistic[i].unlink_time) / 1000000000.0);
    total_stat_throughput += atomic_load(&global_statistic.thread_statistic[i].stat_cnt) / (atomic_load(&global_statistic.thread_statistic[i].stat_time) / 1000000000.0);
  }

  total_mkdir_latency = total_mkdir_time / total_mkdir_cnt;
  total_rmdir_latency = total_rmdir_time / total_rmdir_cnt;
  total_creat_latency = total_creat_time / total_creat_cnt;
  total_unlink_latency = total_unlink_time / total_unlink_cnt;
  total_stat_latency = total_stat_time / total_stat_cnt;

  pr_info("total mkdir (%ld/%ld) == latency: %ld ns, throughput: %f per sec", total_mkdir_cnt - total_mkdir_fail_cnt, total_mkdir_cnt, total_mkdir_time / total_mkdir_cnt, total_mkdir_throughput);
  pr_info("total rmdir (%ld/%ld) == latency: %ld ns, throughput: %f per sec", total_rmdir_cnt - total_rmdir_fail_cnt, total_rmdir_cnt, total_rmdir_time / total_rmdir_cnt, total_rmdir_throughput);
  pr_info("total creat (%ld/%ld) == latency: %ld ns, throughput: %f per sec", total_creat_cnt - total_creat_fail_cnt, total_creat_cnt, total_creat_time / total_creat_cnt, total_creat_throughput);
  pr_info("total unlink (%ld/%ld) == latency: %ld ns, throughput: %f per sec", total_unlink_cnt - total_unlink_fail_cnt, total_unlink_cnt, total_unlink_time / total_unlink_cnt, total_unlink_throughput);
  pr_info("total stat (%ld/%ld) == latency: %ld ns, throughput: %f per sec", total_stat_cnt - total_stat_fail_cnt, total_stat_cnt, total_stat_time / total_stat_cnt, total_stat_throughput);
}

int test_mkdir(ethanefs_cli_t *cli, const char* path, uint64_t thread_id) {
  usleep(10);
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  bench_timer_start(&timer);
  int ret = ethanefs_mkdir(cli, path, 0777);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].mkdir_time, elapsed_ns);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].mkdir_cnt, 1);
  if (ret != 0) {
    // pr_err("mkdir %s failed: %s", path, strerror(-ret));
    atomic_fetch_add(&global_statistic.thread_statistic[thread_id].mkdir_fail_cnt, 1);
  }
  if (atomic_fetch_add(&global_statistic.total_cnt, 1) % PRINT_INTERVAL == 0) {
    print_statistic();
  }
  return ret;
}

static void test_mkdir_recur(ethanefs_cli_t *cli, const char *path, bool verbose, bool force, uint64_t thread_id) {
    char buf[1024];
    char *p, *q;
    int ret;

    strcpy(buf, path);
    p = buf;
    while ((q = strchr(p + 1, '/')) != NULL) {
        *q = '\0';
        ret = test_mkdir(cli, buf, thread_id);
        if (ret && ret != -17 && force) {
            pr_err("mkdir %s failed: %s", buf, strerror(-ret));
        }
        if (verbose) {
            pr_info("mkdir %s done: %s", buf, strerror(-ret));
        }
        *q = '/';
        p = q;
    }
}

int test_rmdir(ethanefs_cli_t *cli, const char* path, uint64_t thread_id) {
  usleep(10);
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  bench_timer_start(&timer);
  int ret = ethanefs_rmdir(cli, path);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].rmdir_time, elapsed_ns);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].rmdir_cnt, 1);
  if (ret != 0) {
    // pr_err("rmdir %s failed: %s", path, strerror(-ret));
    atomic_fetch_add(&global_statistic.thread_statistic[thread_id].rmdir_fail_cnt, 1);
  }
  if (atomic_fetch_add(&global_statistic.total_cnt, 1) % PRINT_INTERVAL == 0) {
    print_statistic();
  }
  return ret;
}

int test_creat(ethanefs_cli_t *cli, const char* path, uint64_t thread_id) {
  usleep(10);
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  bench_timer_start(&timer);
  ethanefs_open_file_t *fh = ethanefs_create(cli, path, 0777);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].creat_time, elapsed_ns);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].creat_cnt, 1);
  if (atomic_fetch_add(&global_statistic.total_cnt, 1) % PRINT_INTERVAL == 0) {
    print_statistic();
  }
  if (IS_ERR(fh)) {
    // pr_err("thread %ld creat %s failed: %s", thread_id, path, strerror(-PTR_ERR(fh)));
    atomic_fetch_add(&global_statistic.thread_statistic[thread_id].creat_fail_cnt, 1);
    return PTR_ERR(fh);
  } 
  ethanefs_close(cli, fh);
  return 0;
}

int test_unlink(ethanefs_cli_t *cli, const char* path, uint64_t thread_id) {
  usleep(10);
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  bench_timer_start(&timer);
  int ret = ethanefs_unlink(cli, path);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].unlink_time, elapsed_ns);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].unlink_cnt, 1);
  if (ret != 0) {
    // pr_err("unlink %s failed: %s", path, strerror(-ret));
    atomic_fetch_add(&global_statistic.thread_statistic[thread_id].unlink_fail_cnt, 1);
  }
  if (atomic_fetch_add(&global_statistic.total_cnt, 1) % 10000 == 0) {
    print_statistic();
  }
  return ret;
}

int test_stat(ethanefs_cli_t *cli, const char* path, uint64_t thread_id) {
  // usleep(10);
  struct bench_timer timer;
  uint64_t elapsed_ns = 0;
  struct stat st;
  bench_timer_start(&timer);
  int ret = ethanefs_getattr(cli, path, &st);
  elapsed_ns += bench_timer_end(&timer);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].stat_time, elapsed_ns);
  atomic_fetch_add(&global_statistic.thread_statistic[thread_id].stat_cnt, 1);
  if (ret != 0) {
    // pr_err("stat %s failed: %s", path, strerror(-ret));
    atomic_fetch_add(&global_statistic.thread_statistic[thread_id].stat_fail_cnt, 1);
  }
  if (atomic_fetch_add(&global_statistic.total_cnt, 1) % PRINT_INTERVAL == 0) {
    print_statistic();
  }
  return ret;
}


static void bench_test(ethanefs_cli_t *cli) {
  uint64_t thread_id = atomic_fetch_add(&global_statistic.thread_num, 1);
  init_statistic(thread_id);

  int ret = 0;
  struct stat buf;
  const int depth = 4;
  const int total_meta = 251000;
  const int total_file = total_meta / depth;
  const int stat_count = 100000000;

  char basic_path[64];
  // sprintf(basic_path, "/uniuqe_dir.%ld/", thread_id);
  sprintf(basic_path, "/");

  char path[1024];
  char dir_name[64];
  char file_name[64];
  int i;
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file%ld", thread_id * total_file + i);
    sprintf(dir_name, "dir%ld", thread_id * total_file + i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    test_mkdir_recur(cli, path, false, true, thread_id);
    strcat(path, file_name);
    test_creat(cli, path, thread_id);
  }

  // int err = 0;
  // for (i = 0; i < stat_count; i++) {
  //   int id = i % total_file;
  //   strcpy(path, basic_path);
  //   sprintf(file_name, "file%ld", thread_id * total_file + id);
  //   sprintf(dir_name, "dir%ld", thread_id * total_file + id);
  //   for (int d = 0; d < depth - 1; d++) {
  //     strcat(path, dir_name);
  //     strcat(path, "/");
  //   }
  //   strcat(path, file_name);
  //   test_stat(cli, path, thread_id);
  // }

  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file%ld", thread_id * total_file + i);
    sprintf(dir_name, "dir%ld", thread_id * total_file + i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    strcat(path, file_name);
    test_unlink(cli, path, thread_id);
  }
  
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(dir_name, "dir%ld", thread_id * total_file + i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    // Remove from deepest to shallowest
    for (int d = depth - 1; d > 0; d--) {
      path[strlen(path) - 1] = '\0';
      test_rmdir(cli, path, thread_id);
      path[strlen(path) - strlen(dir_name)] = '\0'; // Trim last directory
    }
  }
}

// =========== motivation ============

static void bench_motivation_remote_time(ethanefs_cli_t *cli) { 
  uint64_t thread_id = atomic_fetch_add(&global_statistic.thread_num, 1);
  atomic_fetch_add(&global_statistic.running_thread, 1);
  init_statistic(thread_id);

  int ret = 0;
  struct stat buf;
  const int depth = 4;
  const int total_meta = 40000;
  const int total_file = total_meta / depth;

  char basic_path[64];
  // sprintf(basic_path, "/uniuqe_dir.%ld/", thread_id);
  sprintf(basic_path, "/");

  char path[1024];
  char dir_name[64];
  char file_name[64];
  int i;
  // for (i = 0; i < total_file; i++) {
  //   strcpy(path, basic_path);
  //   sprintf(file_name, "file%ld", thread_id * total_file + i);
  //   sprintf(dir_name, "dir%ld", thread_id * total_file + i);
  //   for (int d = 0; d < depth - 1; d++) {
  //     strcat(path, dir_name);
  //     strcat(path, "/");
  //   }
  //   test_mkdir_recur(cli, path, false, true, thread_id);
  //   strcat(path, file_name);
  //   test_creat(cli, path, thread_id);
  // }

  // int err = 0;
  // for (i = 0; i < total_file; i++) {
  //   int id = i % total_file;
  //   strcpy(path, basic_path);
  //   sprintf(file_name, "file%ld", thread_id * total_file + id);
  //   sprintf(dir_name, "dir%ld", thread_id * total_file + id);
  //   for (int d = 0; d < depth - 1; d++) {
  //     strcat(path, dir_name);
  //     strcat(path, "/");
  //   }
  //   strcat(path, file_name);
  //   test_stat(cli, path, thread_id);
  // }

  // for (i = 0; i < total_file; i++) {
  //   strcpy(path, basic_path);
  //   sprintf(file_name, "file%ld", thread_id * total_file + i);
  //   sprintf(dir_name, "dir%ld", thread_id * total_file + i);
  //   for (int d = 0; d < depth - 1; d++) {
  //     strcat(path, dir_name);
  //     strcat(path, "/");
  //   }
  //   strcat(path, file_name);
  //   test_unlink(cli, path, thread_id);
  // }
  
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(dir_name, "dir%ld", thread_id * total_file + i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    // Remove from deepest to shallowest
    for (int d = depth - 1; d > 0; d--) {
      path[strlen(path) - 1] = '\0';
      test_rmdir(cli, path, thread_id);
      path[strlen(path) - strlen(dir_name)] = '\0'; // Trim last directory
    }
  }

  print_statistic();
  pr_info("remote access cnt:\n"
          "RDMA READ: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n"
          "RDMA WRITE: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n"
          "RDMA CAS: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n"
          "RDMA FAA: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n",
          dm_access_counter[0][0], dm_access_counter[0][1], dm_access_counter[0][2], dm_access_counter[0][3], dm_access_counter[0][4], dm_access_counter[0][5], dm_access_counter[0][6], dm_access_counter[0][7], dm_access_counter[0][8], dm_access_counter[0][9], dm_access_counter[0][10], dm_access_counter[0][11], dm_access_counter[0][12], dm_access_counter[0][13],
          dm_access_counter[1][0], dm_access_counter[1][1], dm_access_counter[1][2], dm_access_counter[1][3], dm_access_counter[1][4], dm_access_counter[1][5], dm_access_counter[1][6], dm_access_counter[1][7], dm_access_counter[1][8], dm_access_counter[1][9], dm_access_counter[1][10], dm_access_counter[1][11], dm_access_counter[1][12], dm_access_counter[1][13],
          dm_access_counter[2][0], dm_access_counter[2][1], dm_access_counter[2][2], dm_access_counter[2][3], dm_access_counter[2][4], dm_access_counter[2][5], dm_access_counter[2][6], dm_access_counter[2][7], dm_access_counter[2][8], dm_access_counter[2][9], dm_access_counter[2][10], dm_access_counter[2][11], dm_access_counter[2][12], dm_access_counter[2][13],
          dm_access_counter[3][0], dm_access_counter[3][1], dm_access_counter[3][2], dm_access_counter[3][3], dm_access_counter[3][4], dm_access_counter[3][5], dm_access_counter[3][6], dm_access_counter[3][7], dm_access_counter[3][8], dm_access_counter[3][9], dm_access_counter[3][10], dm_access_counter[3][11], dm_access_counter[3][12], dm_access_counter[3][13]);
}

static void bench_motivation_load(ethanefs_cli_t *cli) {
  uint64_t thread_id = atomic_fetch_add(&global_statistic.thread_num, 1);
  atomic_fetch_add(&global_statistic.running_thread, 1);
  init_statistic(thread_id);

  int ret = 0;
  struct stat buf;
  const int depth = 4;
  const int total_meta = 1000000;
  const int total_file = total_meta / depth;

  char basic_path[64];
  // sprintf(basic_path, "/uniuqe_dir.%ld/", thread_id);
  sprintf(basic_path, "/");

  char path[1024];
  char dir_name[64];
  char file_name[64];
  int i;
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file%d", i);
    sprintf(dir_name, "dir%d", i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    test_mkdir_recur(cli, path, false, true, thread_id);
    strcat(path, file_name);
    test_creat(cli, path, thread_id);
  }
  atomic_fetch_sub(&global_statistic.running_thread, 1);
}

int uint64_cmpfunc(const void * a, const void * b) {
  if ((*(const uint64_t*)a) < (*(const uint64_t*)b)) {
    return -1;
  } else if ((*(const uint64_t*)a) > (*(const uint64_t*)b)) {
    return 1;
  }
  return 0;
}

static void bench_motivation_stat(ethanefs_cli_t *cli) {
  uint64_t thread_id = atomic_fetch_add(&global_statistic.thread_num, 1);
  atomic_fetch_add(&global_statistic.running_thread, 1);
  init_statistic(thread_id);

  int ret = 0;
  struct stat buf;
  const int depth = 4;
  const int total_meta = 1000000;
  const int total_file = total_meta / depth;
  const int stat_count = 2000000;

  char basic_path[64];
  // sprintf(basic_path, "/uniuqe_dir.%ld/", thread_id);
  sprintf(basic_path, "/");

  char path[1024];
  char dir_name[64];
  char file_name[64];
  int i;

  global_statistic.thread_statistic[thread_id]._ext = (void*)malloc(sizeof(uint64_t) * stat_count);
  uint64_t* stat_lat = (uint64_t*)global_statistic.thread_statistic[thread_id]._ext;
  for (i = 0; i < stat_count; i++) {
    int id = random() % total_file;
    strcpy(path, basic_path);
    sprintf(file_name, "file%d", id);
    sprintf(dir_name, "dir%d", id);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    strcat(path, file_name);
    // stat
    struct bench_timer timer;
    uint64_t elapsed_ns = 0;
    bench_timer_start(&timer);
    test_stat(cli, path, thread_id);
    stat_lat[i] = bench_timer_end(&timer);
  }

  if (atomic_fetch_sub(&global_statistic.running_thread, 1) == 1) {
    // statistic
    print_statistic();
    // cache hit
    pr_info("cache hit: %ld, total: %ld, hit rate: %f", total_hit_in_cache, total_fetch, (double)total_hit_in_cache / total_fetch);
    // IO time
    pr_info("remote access cnt:\n"
          "RDMA READ: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n"
          "RDMA WRITE: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n"
          "RDMA CAS: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n"
          "RDMA FAA: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n",
          dm_access_counter[0][0], dm_access_counter[0][1], dm_access_counter[0][2], dm_access_counter[0][3], dm_access_counter[0][4], dm_access_counter[0][5], dm_access_counter[0][6], dm_access_counter[0][7], dm_access_counter[0][8], dm_access_counter[0][9], dm_access_counter[0][10], dm_access_counter[0][11], dm_access_counter[0][12], dm_access_counter[0][13],
          dm_access_counter[1][0], dm_access_counter[1][1], dm_access_counter[1][2], dm_access_counter[1][3], dm_access_counter[1][4], dm_access_counter[1][5], dm_access_counter[1][6], dm_access_counter[1][7], dm_access_counter[1][8], dm_access_counter[1][9], dm_access_counter[1][10], dm_access_counter[1][11], dm_access_counter[1][12], dm_access_counter[1][13],
          dm_access_counter[2][0], dm_access_counter[2][1], dm_access_counter[2][2], dm_access_counter[2][3], dm_access_counter[2][4], dm_access_counter[2][5], dm_access_counter[2][6], dm_access_counter[2][7], dm_access_counter[2][8], dm_access_counter[2][9], dm_access_counter[2][10], dm_access_counter[2][11], dm_access_counter[2][12], dm_access_counter[2][13],
          dm_access_counter[3][0], dm_access_counter[3][1], dm_access_counter[3][2], dm_access_counter[3][3], dm_access_counter[3][4], dm_access_counter[3][5], dm_access_counter[3][6], dm_access_counter[3][7], dm_access_counter[3][8], dm_access_counter[3][9], dm_access_counter[3][10], dm_access_counter[3][11], dm_access_counter[3][12], dm_access_counter[3][13]);
    // tail latency
    uint64_t* lats = (uint64_t*)malloc(sizeof(uint64_t) * global_statistic.thread_num * stat_count);
    for (uint64_t i = 0; i < global_statistic.thread_num; i++) {
      memmove(lats + i * stat_count, global_statistic.thread_statistic[i]._ext, sizeof(uint64_t) * stat_count);
      free(global_statistic.thread_statistic[i]._ext);
    }

    qsort(lats, global_statistic.thread_num * stat_count, sizeof(uint64_t), uint64_cmpfunc);
    pr_info("min latency: %ld, P10 latency: %ld, P50 latency: %ld, P99 latency: %ld, P999 latency: %ld, P9999 latency: %ld", lats[0], lats[stat_count * global_statistic.thread_num / 10], lats[stat_count * global_statistic.thread_num / 2], lats[stat_count * global_statistic.thread_num * 99 / 100], lats[stat_count * global_statistic.thread_num * 999 / 1000], lats[stat_count * global_statistic.thread_num * 9999 / 10000]);
    free(lats);
  }
}

// =========== =========

static void bench_evaluation_write_throughput_workload_A(ethanefs_cli_t *cli) {
  uint64_t thread_id = atomic_fetch_add(&global_statistic.thread_num, 1);
  atomic_fetch_add(&global_statistic.running_thread, 1);
  init_statistic(thread_id);

  int ret = 0;
  uint64_t total_file = 250000;

  char basic_path[64];
  char path[1024];
  char file_name[64];

  sprintf(basic_path, "/private-dir.%d.%ld/", NODE_ID, thread_id);
  test_mkdir_recur(cli, basic_path, false, true, thread_id);

  for (uint64_t i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file.%ld", i);
    strcat(path, file_name);
    // stat
    struct bench_timer timer;
    uint64_t elapsed_ns = 0;
    bench_timer_start(&timer);
    test_creat(cli, path, thread_id);
  }

  for (uint64_t i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file.%ld", i);
    strcat(path, file_name);
    // stat
    struct bench_timer timer;
    uint64_t elapsed_ns = 0;
    bench_timer_start(&timer);
    test_unlink(cli, path, thread_id);
  }

  if (atomic_fetch_sub(&global_statistic.running_thread, 1) == 1) {
    // statistic
    print_statistic();
  }
}

static void bench_evaluation_write_throughput_workload_B(ethanefs_cli_t *cli) {
  uint64_t thread_id = atomic_fetch_add(&global_statistic.thread_num, 1);
  atomic_fetch_add(&global_statistic.running_thread, 1);
  init_statistic(thread_id);

  int ret = 0;
  struct stat buf;
  const int depth = 8;
  const int total_meta = 20000;
  const int total_file = total_meta / depth;

  char basic_path[64];
  // sprintf(basic_path, "/uniuqe_dir.%ld/", thread_id);
  sprintf(basic_path, "/");

  char path[1024];
  char dir_name[64];
  char file_name[64];
  int i;
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file%ld", (thread_id + 64 * NODE_ID) * total_file + i);
    sprintf(dir_name, "dir%ld", (thread_id + 64 * NODE_ID) * total_file + i);
    for (int d = 0; d < depth; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    test_mkdir_recur(cli, path, false, true, thread_id);
    // strcat(path, file_name);
    // test_creat(cli, path, thread_id);
  }

  // for (i = 0; i < total_file; i++) {
  //   strcpy(path, basic_path);
  //   sprintf(file_name, "file%ld", (thread_id + 64 * NODE_ID) * total_file + i);
  //   sprintf(dir_name, "dir%ld", (thread_id + 64 * NODE_ID) * total_file + i);
  //   for (int d = 0; d < depth - 1; d++) {
  //     strcat(path, dir_name);
  //     strcat(path, "/");
  //   }
  //   strcat(path, file_name);
  //   test_unlink(cli, path, thread_id);
  // }
  
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(dir_name, "dir%ld", (thread_id + 64 * NODE_ID) * total_file + i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    // Remove from deepest to shallowest
    for (int d = depth; d > 0; d--) {
      path[strlen(path) - 1] = '\0';
      test_rmdir(cli, path, thread_id);
      path[strlen(path) - strlen(dir_name)] = '\0'; // Trim last directory
    }
  }
}

static void bench_evaluation_write_latency(ethanefs_cli_t *cli) {
  uint64_t thread_id = atomic_fetch_add(&global_statistic.thread_num, 1);
  init_statistic(thread_id);

  int ret = 0;
  struct stat buf;
  const int depth = 4;
  const int total_meta = 10000;
  const int total_file = total_meta / depth;

  char basic_path[64];
  // sprintf(basic_path, "/uniuqe_dir.%ld/", thread_id);
  sprintf(basic_path, "/");

  char path[1024];
  char dir_name[64];
  char file_name[64];
  int i;
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file%ld", thread_id * total_file + i);
    sprintf(dir_name, "dir%ld", thread_id * total_file + i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    test_mkdir_recur(cli, path, false, true, thread_id);
    strcat(path, file_name);
    test_creat(cli, path, thread_id);
  }

  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file%ld", thread_id * total_file + i);
    sprintf(dir_name, "dir%ld", thread_id * total_file + i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    strcat(path, file_name);
    test_unlink(cli, path, thread_id);
  }
  
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(dir_name, "dir%ld", thread_id * total_file + i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    // Remove from deepest to shallowest
    for (int d = depth - 1; d > 0; d--) {
      path[strlen(path) - 1] = '\0';
      test_rmdir(cli, path, thread_id);
      path[strlen(path) - strlen(dir_name)] = '\0'; // Trim last directory
    }
  }
  print_statistic();
}

// =========== =========

static void bench_evalution_stat_load(ethanefs_cli_t *cli) {
  uint64_t thread_id = atomic_fetch_add(&global_statistic.thread_num, 1);
  atomic_fetch_add(&global_statistic.running_thread, 1);
  init_statistic(thread_id);

  int ret = 0;
  struct stat buf;
  const int depth = 8;
  const int total_meta = 1000000;
  const int total_file = total_meta / depth;

  char basic_path[64];
  // sprintf(basic_path, "/uniuqe_dir.%ld/", thread_id);
  sprintf(basic_path, "/");

  char path[1024];
  char dir_name[64];
  char file_name[64];
  int i;
  for (i = 0; i < total_file; i++) {
    strcpy(path, basic_path);
    sprintf(file_name, "file%d", i);
    sprintf(dir_name, "dir%d", i);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    test_mkdir_recur(cli, path, false, true, thread_id);
    strcat(path, file_name);
    test_creat(cli, path, thread_id);
  }
  atomic_fetch_sub(&global_statistic.running_thread, 1);
}

static void bench_evalution_stat(ethanefs_cli_t *cli) {
  uint64_t thread_id = atomic_fetch_add(&global_statistic.thread_num, 1);
  atomic_fetch_add(&global_statistic.running_thread, 1);
  init_statistic(thread_id);

  int ret = 0;
  struct stat buf;
  const int depth = 8;
  const int total_meta = 1000000;
  const int total_file = total_meta / depth;
  const uint64_t stat_count = 1000000;
  const uint64_t group_size = 1000;
  const uint64_t total_group = total_file / group_size;
  uint64_t cur_group = 0;

  global_statistic.thread_statistic[thread_id]._ext = (void*)malloc(sizeof(uint64_t) * stat_count);
  uint64_t* stat_lat = (uint64_t*)global_statistic.thread_statistic[thread_id]._ext;

  char basic_path[64];
  // sprintf(basic_path, "/uniuqe_dir.%ld/", thread_id);
  sprintf(basic_path, "/");

  char path[1024];
  char dir_name[64];
  char file_name[64];
  int i;
  uint64_t cur_total_lat = 0;
  for (i = 0; i < stat_count; i++) {
    cur_total_lat = global_statistic.thread_statistic[thread_id].stat_time;
    if (random() % 1000 < 10) {
      cur_group = random() % total_group;
    }
    uint64_t file_id = cur_group * group_size + random() % group_size;
    strcpy(path, basic_path);
    sprintf(file_name, "file%ld", file_id);
    sprintf(dir_name, "dir%ld", file_id);
    for (int d = 0; d < depth - 1; d++) {
      strcat(path, dir_name);
      strcat(path, "/");
    }
    strcat(path, file_name);
    test_stat(cli, path, thread_id);
    stat_lat[i] = global_statistic.thread_statistic[thread_id].stat_time - cur_total_lat;
    cur_total_lat = global_statistic.thread_statistic[thread_id].stat_time;
  }
  if (atomic_fetch_sub(&global_statistic.running_thread, 1) == 1) {
    print_statistic();
    // tail latency
    uint64_t* lats = (uint64_t*)malloc(sizeof(uint64_t) * global_statistic.thread_num * stat_count);
    for (uint64_t i = 0; i < global_statistic.thread_num; i++) {
      memmove(lats + i * stat_count, global_statistic.thread_statistic[i]._ext, sizeof(uint64_t) * stat_count);
      free(global_statistic.thread_statistic[i]._ext);
    }

    qsort(lats, global_statistic.thread_num * stat_count, sizeof(uint64_t), uint64_cmpfunc);
    pr_info("min latency: %ld, P10 latency: %ld, P50 latency: %ld, P99 latency: %ld, P999 latency: %ld, P9999 latency: %ld", lats[0], lats[stat_count * global_statistic.thread_num / 10], lats[stat_count * global_statistic.thread_num / 2], lats[stat_count * global_statistic.thread_num * 99 / 100], lats[stat_count * global_statistic.thread_num * 999 / 1000], lats[stat_count * global_statistic.thread_num * 9999 / 10000]);
    free(lats);
    // hit rate
    pr_info("cache hit: %ld, total: %ld, hit rate: %f", total_hit_in_cache, total_fetch, (double)total_hit_in_cache / total_fetch);
    // remote access
    pr_info("remote access cnt:\n"
          "RDMA READ: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n"
          "RDMA WRITE: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n"
          "RDMA CAS: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n"
          "RDMA FAA: [0, 8]: %ld, (8, 16]: %ld, (16, 32]: %ld, (32, 64]: %ld, (64, 96]: %ld, (96, 128]: %ld, (128, 192]: %ld, (192, 256]: %ld, (256, 384]: %ld, (384, 512]: %ld, (512, 768]: %ld, (768, 1024]: %ld, (1024, 1536]: %ld, (1536, +): %ld\n",
          dm_access_counter[0][0], dm_access_counter[0][1], dm_access_counter[0][2], dm_access_counter[0][3], dm_access_counter[0][4], dm_access_counter[0][5], dm_access_counter[0][6], dm_access_counter[0][7], dm_access_counter[0][8], dm_access_counter[0][9], dm_access_counter[0][10], dm_access_counter[0][11], dm_access_counter[0][12], dm_access_counter[0][13],
          dm_access_counter[1][0], dm_access_counter[1][1], dm_access_counter[1][2], dm_access_counter[1][3], dm_access_counter[1][4], dm_access_counter[1][5], dm_access_counter[1][6], dm_access_counter[1][7], dm_access_counter[1][8], dm_access_counter[1][9], dm_access_counter[1][10], dm_access_counter[1][11], dm_access_counter[1][12], dm_access_counter[1][13],
          dm_access_counter[2][0], dm_access_counter[2][1], dm_access_counter[2][2], dm_access_counter[2][3], dm_access_counter[2][4], dm_access_counter[2][5], dm_access_counter[2][6], dm_access_counter[2][7], dm_access_counter[2][8], dm_access_counter[2][9], dm_access_counter[2][10], dm_access_counter[2][11], dm_access_counter[2][12], dm_access_counter[2][13],
          dm_access_counter[3][0], dm_access_counter[3][1], dm_access_counter[3][2], dm_access_counter[3][3], dm_access_counter[3][4], dm_access_counter[3][5], dm_access_counter[3][6], dm_access_counter[3][7], dm_access_counter[3][8], dm_access_counter[3][9], dm_access_counter[3][10], dm_access_counter[3][11], dm_access_counter[3][12], dm_access_counter[3][13]);
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

    const int depth = 1;
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

// SET_WORKER_FN(bench_motivation_load);
SET_WORKER_FN(bench_evalution_stat);