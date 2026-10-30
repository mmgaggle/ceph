// Crash-recovery check for fsync semantics.
//   crashcheck write CONF PATH NWRITES IOSIZE [PUNCH_OFF PUNCH_LEN]
//     creates PATH, appends NWRITES x IOSIZE with ceph_ll_fsync(fh, 0) after
//     each, optionally punches a KEEP_SIZE hole and fsyncs again, prints
//     what it observed, then waits to be killed without unmounting.
//   crashcheck stat CONF PATH
//     prints size/mtime/version from a fresh mount.
#define _GNU_SOURCE
#include <cephfs/libcephfs.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static struct ceph_mount_info *mount_fs(const char *conf)
{
  struct ceph_mount_info *cm;
  if (ceph_create(&cm, "admin") || ceph_conf_read_file(cm, conf) ||
      ceph_conf_set(cm, "admin_socket", "") || ceph_mount(cm, "/")) {
    fprintf(stderr, "mount failed\n");
    exit(1);
  }
  return cm;
}

static void show(const char *tag, struct ceph_statx *stx)
{
  printf("%s size=%llu mtime=%lld.%09ld version=%llu\n", tag,
         (unsigned long long)stx->stx_size, (long long)stx->stx_mtime.tv_sec,
         stx->stx_mtime.tv_nsec, (unsigned long long)stx->stx_version);
  fflush(stdout);
}

int main(int argc, char **argv)
{
  if (argc < 4)
    return 2;
  const unsigned want = CEPH_STATX_SIZE | CEPH_STATX_MTIME | CEPH_STATX_VERSION;
  struct ceph_mount_info *cm = mount_fs(argv[2]);
  UserPerm *perms = ceph_mount_perms(cm);
  struct ceph_statx stx;
  int r;

  if (!strcmp(argv[1], "stat")) {
    r = ceph_statx(cm, argv[3], &stx, want, 0);
    if (r < 0) {
      fprintf(stderr, "statx: %s\n", strerror(-r));
      return 1;
    }
    show("RECOVERED", &stx);
    ceph_unmount(cm);
    return 0;
  }

  int n = atoi(argv[4]), iosize = atoi(argv[5]);
  char *slash = strrchr(argv[3], '/');
  char dirpath[512];
  snprintf(dirpath, sizeof(dirpath), "%.*s", (int)(slash - argv[3]), argv[3]);
  ceph_mkdirs(cm, dirpath, 0755);
  struct Inode *dir, *in;
  struct Fh *fh;
  if ((r = ceph_ll_walk(cm, dirpath, &dir, &stx, CEPH_STATX_INO, 0, perms)) ||
      (r = ceph_ll_create(cm, dir, slash + 1, 0644, O_RDWR | O_CREAT | O_TRUNC,
                          &in, &fh, &stx, CEPH_STATX_INO, 0, perms))) {
    fprintf(stderr, "create: %s\n", strerror(-r));
    return 1;
  }
  char *buf = malloc(iosize);
  memset(buf, 'x', iosize);
  for (int i = 0; i < n; i++) {
    if ((r = ceph_ll_write(cm, fh, (int64_t)i * iosize, iosize, buf)) != iosize ||
        (r = ceph_ll_fsync(cm, fh, 0))) {
      fprintf(stderr, "write/fsync %d: %d\n", i, r);
      return 1;
    }
  }
  ceph_ll_getattr(cm, in, &stx, want, 0, perms);
  show("SEEN_AFTER_WRITES", &stx);
  if (argc >= 8) {
    r = ceph_ll_fallocate(cm, fh, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                          atoll(argv[6]), atoll(argv[7]));
    if (r || (r = ceph_ll_fsync(cm, fh, 0))) {
      fprintf(stderr, "punch/fsync: %d\n", r);
      return 1;
    }
    ceph_ll_getattr(cm, in, &stx, want, 0, perms);
    show("SEEN_AFTER_PUNCH", &stx);
  }
  printf("CLIENT_ID %llu\nREADY\n", (unsigned long long)ceph_get_instance_id(cm));
  fflush(stdout);
  pause();
  return 0;
}
