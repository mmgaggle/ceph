// LD_PRELOAD pthread mutex profiler: per-mutex hold time, contended wait
// time and acquisition counts, attributed to the first call site seen.
// Accounting is off until mutexprof_reset() is called; mutexprof_dump()
// stops it and writes a report.  Condition waits end and restart a hold.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <execinfo.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TABLE_BITS 16
#define TABLE_SIZE (1u << TABLE_BITS)

struct ent {
  _Atomic uintptr_t key;
  _Atomic uint64_t acq, cont, wait_ns, hold_ns, t_acq;
  _Atomic uintptr_t site_first, site_cont;
};

static struct ent table[TABLE_SIZE];

// Sampled per-call-site attribution: 1 in SAMPLE_EVERY acquisitions records a
// short backtrace and the hold that follows is charged to that stack.
#define SAMPLE_EVERY 16
#define BT_DEPTH 7
#define SITE_BITS 12
#define SITE_SIZE (1u << SITE_BITS)
struct site {
  _Atomic uint64_t hash;
  uintptr_t mutex;
  void *bt[BT_DEPTH];
  int depth;
  _Atomic uint64_t n, hold_ns, wait_ns;
};
static struct site sites[SITE_SIZE];
struct held {
  void *m;
  uint64_t t;
  struct site *s;
};
static __thread unsigned tl_count;
static __thread struct held tl_held[8];
static __thread int tl_nheld;
static __thread int tl_in_bt;
static atomic_int enabled;
static uint64_t t_enable;

static int (*real_lock)(pthread_mutex_t *);
static int (*real_trylock)(pthread_mutex_t *);
static int (*real_unlock)(pthread_mutex_t *);
static int (*real_cond_wait)(pthread_cond_t *, pthread_mutex_t *);
static int (*real_cond_timedwait)(pthread_cond_t *, pthread_mutex_t *,
                                  const struct timespec *);
static int (*real_cond_clockwait)(pthread_cond_t *, pthread_mutex_t *,
                                  clockid_t, const struct timespec *);

static void init(void)
{
  real_lock = dlsym(RTLD_NEXT, "pthread_mutex_lock");
  real_trylock = dlsym(RTLD_NEXT, "pthread_mutex_trylock");
  real_unlock = dlsym(RTLD_NEXT, "pthread_mutex_unlock");
  real_cond_wait = dlvsym(RTLD_NEXT, "pthread_cond_wait", "GLIBC_2.3.2");
  real_cond_timedwait = dlvsym(RTLD_NEXT, "pthread_cond_timedwait", "GLIBC_2.3.2");
  real_cond_clockwait = dlsym(RTLD_NEXT, "pthread_cond_clockwait");
}

__attribute__((constructor)) static void ctor(void) { init(); }

static inline uint64_t now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

static inline uint32_t slot(uintptr_t k)
{
  return (uint32_t)(((k >> 4) * 0x9E3779B97F4A7C15ull) >> (64 - TABLE_BITS));
}

static struct ent *lookup(void *m, int insert)
{
  uintptr_t k = (uintptr_t)m;
  uint32_t h = slot(k);
  for (uint32_t i = 0; i < TABLE_SIZE; i++) {
    struct ent *e = &table[(h + i) & (TABLE_SIZE - 1)];
    uintptr_t cur = atomic_load_explicit(&e->key, memory_order_acquire);
    if (cur == k)
      return e;
    if (cur == 0) {
      if (!insert)
        return NULL;
      uintptr_t z = 0;
      if (atomic_compare_exchange_strong(&e->key, &z, k) || z == k)
        return e;
    }
  }
  return NULL;
}

#define RELAXED memory_order_relaxed

static struct site *site_for(void *m, void **bt, int depth)
{
  uint64_t h = (uintptr_t)m * 0x9E3779B97F4A7C15ull;
  for (int i = 1; i < depth; i++)
    h = (h ^ (uintptr_t)bt[i]) * 0x100000001B3ull;
  if (!h)
    h = 1;
  uint32_t idx = (uint32_t)(h >> (64 - SITE_BITS));
  for (uint32_t i = 0; i < SITE_SIZE; i++) {
    struct site *st = &sites[(idx + i) & (SITE_SIZE - 1)];
    uint64_t cur = atomic_load(&st->hash);
    if (cur == h)
      return st;
    if (cur == 0) {
      uint64_t z = 0;
      if (atomic_compare_exchange_strong(&st->hash, &z, ~0ull)) {
        st->mutex = (uintptr_t)m;
        st->depth = depth;
        memcpy(st->bt, bt, sizeof(void *) * depth);
        atomic_store(&st->hash, h);
        return st;
      }
      if (z == h)
        return st;
    }
  }
  return NULL;
}

// Called before acquiring: returns a site when this acquisition is sampled.
static inline struct site *maybe_sample(void *m)
{
  if (tl_in_bt || ++tl_count % SAMPLE_EVERY)
    return NULL;
  void *bt[BT_DEPTH];
  tl_in_bt = 1;  // the unwinder may take locks of its own
  int d = backtrace(bt, BT_DEPTH);
  tl_in_bt = 0;
  return site_for(m, bt, d);
}

static inline void sample_begin(struct site *st, void *m, uint64_t t, uint64_t wait)
{
  if (!st || tl_nheld >= 8)
    return;
  atomic_fetch_add_explicit(&st->n, 1, RELAXED);
  atomic_fetch_add_explicit(&st->wait_ns, wait, RELAXED);
  tl_held[tl_nheld++] = (struct held){m, t, st};
}

static inline void sample_end(void *m, uint64_t t)
{
  for (int i = tl_nheld - 1; i >= 0; i--) {
    if (tl_held[i].m == m) {
      atomic_fetch_add_explicit(&tl_held[i].s->hold_ns, t - tl_held[i].t, RELAXED);
      tl_held[i] = tl_held[--tl_nheld];
      return;
    }
  }
}

static inline void begin_hold(struct ent *e, uint64_t t, uintptr_t site)
{
  atomic_fetch_add_explicit(&e->acq, 1, RELAXED);
  atomic_store_explicit(&e->t_acq, t, RELAXED);
  uintptr_t z = 0;
  if (atomic_load_explicit(&e->site_first, RELAXED) == 0)
    atomic_compare_exchange_strong(&e->site_first, &z, site);
}

static inline void end_hold(void *m)
{
  struct ent *e = lookup(m, 0);
  if (!e)
    return;
  uint64_t t = atomic_exchange_explicit(&e->t_acq, 0, RELAXED);
  if (t)
    atomic_fetch_add_explicit(&e->hold_ns, now_ns() - t, RELAXED);
}

int pthread_mutex_lock(pthread_mutex_t *m)
{
  if (!real_lock)
    init();
  if (!atomic_load_explicit(&enabled, RELAXED))
    return real_lock(m);
  uintptr_t site = (uintptr_t)__builtin_return_address(0);
  struct site *st = maybe_sample(m);
  int r = real_trylock(m);
  uint64_t t, waited = 0;
  struct ent *e = lookup(m, 1);
  if (r == 0) {
    t = now_ns();
  } else if (r == EBUSY) {
    uint64_t t0 = now_ns();
    r = real_lock(m);
    t = now_ns();
    waited = t - t0;
    if (r == 0 && e) {
      atomic_fetch_add_explicit(&e->cont, 1, RELAXED);
      atomic_fetch_add_explicit(&e->wait_ns, t - t0, RELAXED);
      uintptr_t z = 0;
      if (atomic_load_explicit(&e->site_cont, RELAXED) == 0)
        atomic_compare_exchange_strong(&e->site_cont, &z, site);
    }
  } else {
    return r;
  }
  if (r == 0 && e)
    begin_hold(e, t, site);
  if (r == 0)
    sample_end(m, t), sample_begin(st, m, t, waited);
  return r;
}

int pthread_mutex_trylock(pthread_mutex_t *m)
{
  if (!real_trylock)
    init();
  int r = real_trylock(m);
  if (r == 0 && atomic_load_explicit(&enabled, RELAXED)) {
    struct ent *e = lookup(m, 1);
    if (e)
      begin_hold(e, now_ns(), (uintptr_t)__builtin_return_address(0));
  }
  return r;
}

int pthread_mutex_unlock(pthread_mutex_t *m)
{
  if (!real_unlock)
    init();
  if (atomic_load_explicit(&enabled, RELAXED)) {
    end_hold(m);
    if (tl_nheld)
      sample_end(m, now_ns());
  }
  return real_unlock(m);
}

#define COND_WRAP(call)                                                  \
  do {                                                                   \
    int on = atomic_load_explicit(&enabled, RELAXED);                    \
    struct site *st = NULL;                                              \
    if (on) {                                                            \
      end_hold(m);                                                       \
      if (tl_nheld)                                                      \
        sample_end(m, now_ns());                                         \
      st = maybe_sample(m);                                              \
    }                                                                    \
    int r = call;                                                        \
    if (on && atomic_load_explicit(&enabled, RELAXED)) {                 \
      uint64_t t = now_ns();                                             \
      struct ent *e = lookup(m, 1);                                      \
      if (e)                                                             \
        begin_hold(e, t, (uintptr_t)__builtin_return_address(0));        \
      sample_begin(st, m, t, 0);                                         \
    }                                                                    \
    return r;                                                            \
  } while (0)

int pthread_cond_wait(pthread_cond_t *c, pthread_mutex_t *m)
{
  if (!real_cond_wait)
    init();
  COND_WRAP(real_cond_wait(c, m));
}

int pthread_cond_timedwait(pthread_cond_t *c, pthread_mutex_t *m,
                           const struct timespec *ts)
{
  if (!real_cond_timedwait)
    init();
  COND_WRAP(real_cond_timedwait(c, m, ts));
}

int pthread_cond_clockwait(pthread_cond_t *c, pthread_mutex_t *m,
                           clockid_t clk, const struct timespec *ts)
{
  if (!real_cond_clockwait)
    init();
  COND_WRAP(real_cond_clockwait(c, m, clk, ts));
}

void mutexprof_reset(void)
{
  atomic_store(&enabled, 0);
  for (uint32_t i = 0; i < TABLE_SIZE; i++) {
    struct ent *e = &table[i];
    atomic_store(&e->acq, 0);
    atomic_store(&e->cont, 0);
    atomic_store(&e->wait_ns, 0);
    atomic_store(&e->hold_ns, 0);
    atomic_store(&e->t_acq, 0);
  }
  for (uint32_t i = 0; i < SITE_SIZE; i++) {
    atomic_store(&sites[i].n, 0);
    atomic_store(&sites[i].hold_ns, 0);
    atomic_store(&sites[i].wait_ns, 0);
  }
  t_enable = now_ns();
  atomic_store(&enabled, 1);
}

struct row {
  uintptr_t key, site_first, site_cont;
  uint64_t acq, cont, wait_ns, hold_ns;
};

static int by_hold(const void *a, const void *b)
{
  const struct row *x = a, *y = b;
  return x->hold_ns < y->hold_ns ? 1 : x->hold_ns > y->hold_ns ? -1 : 0;
}

static void site_str(uintptr_t site, char *buf, size_t len)
{
  Dl_info di;
  if (site && dladdr((void *)site, &di) && di.dli_fname)
    snprintf(buf, len, "%s+0x%lx", di.dli_fname,
             (unsigned long)(site - (uintptr_t)di.dli_fbase));
  else
    snprintf(buf, len, "0x%lx", (unsigned long)site);
}

void mutexprof_dump(const char *path)
{
  atomic_store(&enabled, 0);
  uint64_t window = now_ns() - t_enable;
  struct row *rows = calloc(TABLE_SIZE, sizeof(*rows));
  size_t n = 0;
  for (uint32_t i = 0; i < TABLE_SIZE; i++) {
    struct ent *e = &table[i];
    uint64_t acq = atomic_load(&e->acq);
    if (!acq)
      continue;
    rows[n++] = (struct row){atomic_load(&e->key), atomic_load(&e->site_first),
                             atomic_load(&e->site_cont), acq,
                             atomic_load(&e->cont), atomic_load(&e->wait_ns),
                             atomic_load(&e->hold_ns)};
  }
  qsort(rows, n, sizeof(*rows), by_hold);
  FILE *f = fopen(path, "w");
  if (!f) {
    free(rows);
    return;
  }
  fprintf(f, "window_ns %lu\n", (unsigned long)window);
  fprintf(f, "# mutex hold_ns wait_ns acq contended site_first site_contended\n");
  for (size_t i = 0; i < n && i < 40; i++) {
    char s1[512], s2[512];
    site_str(rows[i].site_first, s1, sizeof(s1));
    site_str(rows[i].site_cont, s2, sizeof(s2));
    fprintf(f, "0x%lx %lu %lu %lu %lu %s %s\n", (unsigned long)rows[i].key,
            (unsigned long)rows[i].hold_ns, (unsigned long)rows[i].wait_ns,
            (unsigned long)rows[i].acq, (unsigned long)rows[i].cont, s1, s2);
  }
  // sampled stacks for the mutex with the most hold time
  if (n) {
    fprintf(f, "# sites mutex 0x%lx sample_every %d: n hold_ns wait_ns frames...\n",
            (unsigned long)rows[0].key, SAMPLE_EVERY);
    for (uint32_t i = 0; i < SITE_SIZE; i++) {
      struct site *st = &sites[i];
      uint64_t h = atomic_load(&st->hash);
      if (!h || h == ~0ull || st->mutex != rows[0].key || !atomic_load(&st->n))
        continue;
      fprintf(f, "site %lu %lu %lu", (unsigned long)atomic_load(&st->n),
              (unsigned long)atomic_load(&st->hold_ns), (unsigned long)atomic_load(&st->wait_ns));
      for (int j = 1; j < st->depth; j++) {
        char b[512];
        site_str((uintptr_t)st->bt[j], b, sizeof(b));
        fprintf(f, " %s", b);
      }
      fprintf(f, "\n");
    }
  }
  fclose(f);
  free(rows);
}
