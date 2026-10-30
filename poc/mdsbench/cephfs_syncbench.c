// Ganesha-style stable-write load on libcephfs: each worker thread owns one
// file and loops ceph_ll_write() + ceph_ll_fsync(fh, syncdataonly), the call
// pair FSAL_CEPH issues for a FILE_SYNC WRITE.  Threads are spread across
// M independent mounts (one Client, one client_lock each).
//
// --async QD instead keeps QD non-blocking writes with the fsync flag in
// flight per thread (ceph_ll_nonblocking_readv_writev), as FSAL_CEPH does
// with async I/O enabled. --fscrypt runs inside an encrypted directory.
//
// Control: prints READY once files exist; SIGUSR1 starts the measurement
// window, SIGUSR2 ends it, SIGTERM stops and prints one JSON result line.
// If the mutexprof shim is preloaded, the window also brackets its counters.
#define _GNU_SOURCE
#include <cephfs/libcephfs.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <linux/fscrypt.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static struct {
  const char *conf, *id, *dir, *prof_out;
  int mounts, threads, iosize, sdo, nosync, append, client_oc, pin_ranks, qd, fscrypt;
  long file_mb;
} o = {
  .conf = "ceph.conf", .id = "admin", .dir = "/bench/run",
  .mounts = 1, .threads = 1, .iosize = 4096, .client_oc = -1, .file_mb = 64,
};

static const char *g_sets[16];
static int g_nsets;

static atomic_int g_record, g_stop;
static pthread_barrier_t g_ready;

struct worker;

struct slot {
  struct ceph_ll_io_info io;
  struct iovec iov;
  struct worker *w;
  uint64_t t0, lat;
  int busy, done;
};

struct worker {
  int idx;
  struct ceph_mount_info *cm;
  uint32_t *lat_us, *fsync_us;
  size_t n, cap;
  int err;
  pthread_t tid;
  pthread_mutex_t mu;
  pthread_cond_t cv;
  struct slot *slots;
};

static inline uint64_t now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

static void record(struct worker *w, uint64_t lat, uint64_t fs)
{
  if (w->n == w->cap) {
    w->cap = w->cap ? w->cap * 2 : 65536;
    w->lat_us = realloc(w->lat_us, w->cap * sizeof(uint32_t));
    w->fsync_us = realloc(w->fsync_us, w->cap * sizeof(uint32_t));
  }
  w->lat_us[w->n] = lat / 1000;
  w->fsync_us[w->n] = fs / 1000;
  w->n++;
}

// Runs on the client's finisher thread with client_lock held: only signal.
static void io_done(struct ceph_ll_io_info *io)
{
  struct slot *sl = io->priv;
  struct worker *w = sl->w;
  pthread_mutex_lock(&w->mu);
  sl->lat = now_ns() - sl->t0;
  sl->done = 1;
  pthread_cond_signal(&w->cv);
  pthread_mutex_unlock(&w->mu);
}

static uint64_t next_off(uint64_t *seed, uint64_t *off, uint64_t nblocks)
{
  if (o.append) {
    uint64_t r = *off;
    *off += o.iosize;
    return r;
  }
  *seed ^= *seed << 13; *seed ^= *seed >> 7; *seed ^= *seed << 17;
  return (*seed % nblocks) * o.iosize;
}

static void async_loop(struct worker *w, struct Fh *fh, char *buf, uint64_t nblocks)
{
  uint64_t seed = 0x9E3779B97F4A7C15ull * (w->idx + 1), off = 0;
  w->slots = calloc(o.qd, sizeof(*w->slots));
  pthread_mutex_lock(&w->mu);
  for (;;) {
    int stopping = atomic_load_explicit(&g_stop, memory_order_relaxed) || w->err;
    int busy = 0;
    for (int i = 0; i < o.qd; i++) {
      struct slot *sl = &w->slots[i];
      if (sl->busy && sl->done) {
        sl->busy = sl->done = 0;
        if (sl->io.result != o.iosize) {
          fprintf(stderr, "t%d: async write: %ld\n", w->idx, (long)sl->io.result);
          w->err = 1;
        } else if (atomic_load_explicit(&g_record, memory_order_relaxed)) {
          record(w, sl->lat, 0);
        }
      }
      if (!sl->busy && !stopping) {
        memset(&sl->io, 0, sizeof(sl->io));
        sl->w = w;
        sl->iov.iov_base = buf;
        sl->iov.iov_len = o.iosize;
        sl->io.callback = io_done;
        sl->io.priv = sl;
        sl->io.fh = fh;
        sl->io.iov = &sl->iov;
        sl->io.iovcnt = 1;
        sl->io.off = next_off(&seed, &off, nblocks);
        sl->io.write = true;
        sl->io.fsync = !o.nosync;
        sl->io.syncdataonly = o.sdo;
        sl->busy = 1;
        sl->t0 = now_ns();
        pthread_mutex_unlock(&w->mu);
        int64_t r = ceph_ll_nonblocking_readv_writev(w->cm, &sl->io);
        pthread_mutex_lock(&w->mu);
        if (r < 0) {
          fprintf(stderr, "t%d: async submit: %ld\n", w->idx, (long)r);
          sl->busy = 0;
          w->err = 1;
        }
      }
      busy += sl->busy;
    }
    if (stopping && !busy)
      break;
    int ready = 0;
    for (int i = 0; i < o.qd; i++)
      ready |= w->slots[i].busy && w->slots[i].done;
    if (!ready) {
      struct timespec ts;
      clock_gettime(CLOCK_REALTIME, &ts);
      ts.tv_nsec += 100000000;
      if (ts.tv_nsec >= 1000000000) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000;
      }
      pthread_cond_timedwait(&w->cv, &w->mu, &ts);
    }
  }
  pthread_mutex_unlock(&w->mu);
  free(w->slots);
}

static void *worker_main(void *arg)
{
  struct worker *w = arg;
  struct ceph_mount_info *cm = w->cm;
  UserPerm *perms = ceph_mount_perms(cm);
  struct ceph_statx stx;
  struct Inode *dir = NULL, *in = NULL;
  struct Fh *fh = NULL;
  char path[512], name[64];
  uint64_t fsize = (uint64_t)o.file_mb << 20;
  size_t chunk = 4 << 20;
  char *buf = NULL;
  int r;

  snprintf(path, sizeof(path), "%s/t%d", o.dir, w->idx);
  snprintf(name, sizeof(name), "f%d", w->idx);
  r = ceph_ll_walk(cm, path, &dir, &stx, CEPH_STATX_INO, 0, perms);
  if (r < 0) {
    fprintf(stderr, "t%d: walk %s: %s\n", w->idx, path, strerror(-r));
    goto fail;
  }
  r = ceph_ll_create(cm, dir, name, 0644, O_RDWR | O_CREAT | O_TRUNC, &in, &fh,
                     &stx, CEPH_STATX_INO, 0, perms);
  if (r < 0) {
    fprintf(stderr, "t%d: create: %s\n", w->idx, strerror(-r));
    goto fail;
  }
  if (posix_memalign((void **)&buf, 4096, chunk > (size_t)o.iosize ? chunk : o.iosize))
    goto fail;
  memset(buf, 'a' + w->idx % 26, chunk);
  if (!o.append) {
    for (uint64_t off = 0; off < fsize; off += chunk) {
      r = ceph_ll_write(cm, fh, off, chunk, buf);
      if (r != (int)chunk) {
        fprintf(stderr, "t%d: prealloc write: %d\n", w->idx, r);
        goto fail;
      }
    }
    ceph_ll_fsync(cm, fh, 0);
  }
  pthread_barrier_wait(&g_ready);

  uint64_t seed = 0x9E3779B97F4A7C15ull * (w->idx + 1), off = 0;
  uint64_t nblocks = fsize / o.iosize;
  if (o.qd > 0)
    async_loop(w, fh, buf, nblocks);
  while (o.qd == 0 && !atomic_load_explicit(&g_stop, memory_order_relaxed)) {
    if (!o.append) {
      seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
      off = (seed % nblocks) * o.iosize;
    }
    uint64_t t0 = now_ns();
    r = ceph_ll_write(cm, fh, off, o.iosize, buf);
    if (r != o.iosize) {
      fprintf(stderr, "t%d: write: %d\n", w->idx, r);
      w->err = 1;
      break;
    }
    uint64_t t1 = now_ns();
    if (!o.nosync && (r = ceph_ll_fsync(cm, fh, o.sdo)) < 0) {
      fprintf(stderr, "t%d: fsync: %d\n", w->idx, r);
      w->err = 1;
      break;
    }
    uint64_t t2 = now_ns();
    if (o.append)
      off += o.iosize;
    if (atomic_load_explicit(&g_record, memory_order_relaxed))
      record(w, t2 - t0, t2 - t1);
  }
  ceph_ll_close(cm, fh);
  ceph_ll_put(cm, in);
  ceph_ll_put(cm, dir);
  free(buf);
  return NULL;
fail:
  w->err = 1;
  pthread_barrier_wait(&g_ready);
  return NULL;
}

static int cmp_u32(const void *a, const void *b)
{
  uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
  return x < y ? -1 : x > y;
}

static void pcts(const char *label, uint32_t *v, size_t n)
{
  qsort(v, n, sizeof(*v), cmp_u32);
  double sum = 0;
  for (size_t i = 0; i < n; i++)
    sum += v[i];
#define P(q) (n ? v[(size_t)((q) * (n - 1))] : 0)
  printf("\"%s_us\":{\"mean\":%.1f,\"p50\":%u,\"p90\":%u,\"p99\":%u,\"p999\":%u,\"max\":%u}",
         label, n ? sum / n : 0.0, P(0.5), P(0.9), P(0.99), P(0.999), n ? v[n - 1] : 0);
#undef P
}

static struct ceph_mount_info *make_mount(int k)
{
  struct ceph_mount_info *cm;
  char val[64];
  if (ceph_create(&cm, o.id) < 0 || ceph_conf_read_file(cm, o.conf) < 0) {
    fprintf(stderr, "mount %d: create/conf failed\n", k);
    exit(1);
  }
  const char *quiet[] = {"debug_client", "debug_ms", "debug_objecter",
                         "debug_objectcacher", "debug_monc", "debug_auth", NULL};
  for (int i = 0; quiet[i]; i++)
    ceph_conf_set(cm, quiet[i], "0/0");
  ceph_conf_set(cm, "admin_socket", "");
  snprintf(val, sizeof(val), "/tmp/syncbench.%d.%d.log", getpid(), k);
  ceph_conf_set(cm, "log_file", val);
  if (o.client_oc >= 0)
    ceph_conf_set(cm, "client_oc", o.client_oc ? "true" : "false");
  for (int i = 0; i < g_nsets; i++) {
    char key[128];
    const char *eq = strchr(g_sets[i], '=');
    snprintf(key, sizeof(key), "%.*s", (int)(eq - g_sets[i]), g_sets[i]);
    if (ceph_conf_set(cm, key, eq + 1) < 0) {
      fprintf(stderr, "conf set %s failed\n", g_sets[i]);
      exit(1);
    }
  }
  int r = ceph_mount(cm, "/");
  if (r < 0) {
    fprintf(stderr, "mount %d: %s\n", k, strerror(-r));
    exit(1);
  }
  return cm;
}

int main(int argc, char **argv)
{
  static const struct option lo[] = {
    {"conf", 1, 0, 'c'}, {"id", 1, 0, 'i'}, {"dir", 1, 0, 'D'},
    {"mounts", 1, 0, 'm'}, {"threads", 1, 0, 't'}, {"iosize", 1, 0, 's'},
    {"syncdataonly", 1, 0, 'S'}, {"nosync", 0, 0, 'N'}, {"append", 0, 0, 'A'},
    {"client-oc", 1, 0, 'O'}, {"file-mb", 1, 0, 'F'}, {"pin-ranks", 1, 0, 'P'},
    {"prof-out", 1, 0, 'R'}, {"async", 1, 0, 'a'}, {"fscrypt", 0, 0, 'E'}, {"set", 1, 0, 'X'},
    {0, 0, 0, 0}};
  int c;
  while ((c = getopt_long(argc, argv, "", lo, NULL)) != -1) {
    switch (c) {
    case 'c': o.conf = optarg; break;
    case 'i': o.id = optarg; break;
    case 'D': o.dir = optarg; break;
    case 'm': o.mounts = atoi(optarg); break;
    case 't': o.threads = atoi(optarg); break;
    case 's': o.iosize = atoi(optarg); break;
    case 'S': o.sdo = atoi(optarg); break;
    case 'N': o.nosync = 1; break;
    case 'A': o.append = 1; break;
    case 'O': o.client_oc = atoi(optarg); break;
    case 'F': o.file_mb = atol(optarg); break;
    case 'P': o.pin_ranks = atoi(optarg); break;
    case 'R': o.prof_out = optarg; break;
    case 'a': o.qd = atoi(optarg); break;
    case 'E': o.fscrypt = 1; break;
    case 'X':
      if (!strchr(optarg, '=') || g_nsets == 16)
        return 2;
      g_sets[g_nsets++] = optarg;
      break;
    default: return 2;
    }
  }
  if (o.mounts > o.threads)
    o.mounts = o.threads;
  // The shim is already mapped; keep it out of anything we might spawn.
  unsetenv("LD_PRELOAD");

  sigset_t ss;
  sigemptyset(&ss);
  sigaddset(&ss, SIGUSR1);
  sigaddset(&ss, SIGUSR2);
  sigaddset(&ss, SIGTERM);
  sigaddset(&ss, SIGINT);
  pthread_sigmask(SIG_BLOCK, &ss, NULL);

  struct ceph_mount_info **cms = calloc(o.mounts, sizeof(*cms));
  for (int k = 0; k < o.mounts; k++)
    cms[k] = make_mount(k);

  // Directory per thread; optionally pin them round-robin across MDS ranks.
  ceph_mkdirs(cms[0], o.dir, 0755);
  if (o.fscrypt) {
    char key[32], keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
    for (size_t i = 0; i < sizeof(key); i++)
      key[i] = (char)(i * 37 + 11);
    for (int k = 0; k < o.mounts; k++) {
      int r = ceph_add_fscrypt_key(cms[k], key, sizeof(key), keyid, 0);
      if (r < 0) {
        fprintf(stderr, "add_fscrypt_key: %s\n", strerror(-r));
        return 1;
      }
    }
    struct fscrypt_policy_v2 policy = {
      .version = FSCRYPT_POLICY_V2,
      .contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS,
      .filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS,
      .flags = FSCRYPT_POLICY_FLAGS_PAD_32,
    };
    memcpy(policy.master_key_identifier, keyid, FSCRYPT_KEY_IDENTIFIER_SIZE);
    int fd = ceph_open(cms[0], o.dir, O_DIRECTORY, 0);
    int r = fd < 0 ? fd : ceph_set_fscrypt_policy_v2(cms[0], fd, &policy);
    if (r < 0) {
      fprintf(stderr, "set_fscrypt_policy_v2 %s: %s\n", o.dir, strerror(-r));
      return 1;
    }
    ceph_close(cms[0], fd);
  }
  for (int i = 0; i < o.threads; i++) {
    char p[512], v[16];
    snprintf(p, sizeof(p), "%s/t%d", o.dir, i);
    int r = ceph_mkdir(cms[0], p, 0755);
    if (r < 0 && r != -EEXIST) {
      fprintf(stderr, "mkdir %s: %s\n", p, strerror(-r));
      return 1;
    }
    if (o.pin_ranks > 0) {
      int n = snprintf(v, sizeof(v), "%d", i % o.pin_ranks);
      ceph_setxattr(cms[0], p, "ceph.dir.pin", v, n, 0);
    }
  }

  pthread_barrier_init(&g_ready, NULL, o.threads + 1);
  struct worker *ws = calloc(o.threads, sizeof(*ws));
  for (int i = 0; i < o.threads; i++) {
    ws[i].idx = i;
    ws[i].cm = cms[i % o.mounts];
    pthread_mutex_init(&ws[i].mu, NULL);
    pthread_cond_init(&ws[i].cv, NULL);
    pthread_create(&ws[i].tid, NULL, worker_main, &ws[i]);
  }
  pthread_barrier_wait(&g_ready);
  printf("READY\n");
  fflush(stdout);

  void (*prof_reset)(void) = dlsym(RTLD_DEFAULT, "mutexprof_reset");
  void (*prof_dump)(const char *) = dlsym(RTLD_DEFAULT, "mutexprof_dump");
  uint64_t t_start = 0, t_end = 0;
  for (int sig; !atomic_load(&g_stop);) {
    if (sigwait(&ss, &sig))
      continue;
    if (sig == SIGUSR1 && !t_start) {
      if (prof_reset)
        prof_reset();
      t_start = now_ns();
      atomic_store(&g_record, 1);
    } else if (sig == SIGUSR2 && t_start && !t_end) {
      atomic_store(&g_record, 0);
      t_end = now_ns();
      if (prof_dump && o.prof_out)
        prof_dump(o.prof_out);
    } else if (sig == SIGTERM || sig == SIGINT) {
      atomic_store(&g_stop, 1);
    }
  }
  if (t_start && !t_end) {
    atomic_store(&g_record, 0);
    t_end = now_ns();
  }
  int errs = 0;
  size_t total = 0;
  for (int i = 0; i < o.threads; i++) {
    pthread_join(ws[i].tid, NULL);
    errs += ws[i].err;
    total += ws[i].n;
  }
  uint32_t *lat = malloc((total + 1) * sizeof(uint32_t));
  uint32_t *fs = malloc((total + 1) * sizeof(uint32_t));
  size_t at = 0;
  for (int i = 0; i < o.threads; i++) {
    memcpy(lat + at, ws[i].lat_us, ws[i].n * sizeof(uint32_t));
    memcpy(fs + at, ws[i].fsync_us, ws[i].n * sizeof(uint32_t));
    at += ws[i].n;
  }
  double secs = (t_end - t_start) / 1e9;
  printf("{\"threads\":%d,\"mounts\":%d,\"iosize\":%d,\"syncdataonly\":%d,"
         "\"nosync\":%d,\"append\":%d,\"client_oc\":%d,\"pin_ranks\":%d,"
         "\"async_qd\":%d,\"fscrypt\":%d,"
         "\"errors\":%d,\"ops\":%zu,\"secs\":%.3f,\"ops_per_sec\":%.1f,",
         o.threads, o.mounts, o.iosize, o.sdo, o.nosync, o.append, o.client_oc,
         o.pin_ranks, o.qd, o.fscrypt, errs, total, secs, secs > 0 ? total / secs : 0.0);
  pcts("op", lat, total);
  printf(",");
  pcts("fsync", fs, total);
  printf("}\n");
  fflush(stdout);
  for (int k = 0; k < o.mounts; k++) {
    ceph_unmount(cms[k]);
    ceph_release(cms[k]);
  }
  return errs ? 1 : 0;
}
