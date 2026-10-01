/* -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- */
#include "uet_shim.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "uet_api.h"

#define MAX_PEERS 256

struct uet_shim {
  uet_handle_t h;
  struct fi_info* info;
  struct fid_fabric fabric;
  struct fid_domain domain;
  uet_domain_handle_t dom;
  struct fid_ep ep_fid;
  uet_ep_handle_t ep;
  struct fid_cq cq;
  uet_cq_handle_t tx_cq, rx_cq;
  uet_mr_handle_t mr, win_mr;
  char* buf;
  size_t size;
  char* win;
  size_t win_size;
  uint32_t ipv4;
  int npeers;
  uint32_t peer_ip[MAX_PEERS];
  /* the provider's AV entry points at the address it was given rather
   * than copying it, so the addresses must outlive their entries */
  struct uet_addr peer_addr[MAX_PEERS];
  uet_addr_handle_t peer_h[MAX_PEERS];
};

static void eq_cb(uet_handle_t h, struct fi_eq_entry* e) { (void)h; (void)e; }
static void eq_err_cb(uet_handle_t h, struct fi_eq_err_entry* e)
{
  (void)h; (void)e;
}

static char* alloc_region(size_t size)
{
  char* p = aligned_alloc(4096, (size + 4095) & ~(size_t)4095);
  if (p)
    memset(p, 0, size);
  return p;
}

struct uet_shim* uet_shim_open(size_t stage_size, size_t window_size,
                               char* err, size_t errlen)
{
  struct uet_shim* s = calloc(1, sizeof(*s));
  struct fi_info* hints = fi_allocinfo();
  struct uet_addr src;
  struct fi_cq_attr cq_attr;
  const char* what = "";
  int r = -ENOMEM;

  if (!s || !hints) {
    snprintf(err, errlen, "out of memory");
    free(s);
    return NULL;
  }
  hints->caps |= (FI_MSG | FI_RMA);
  memset(&src, 0, sizeof(src));
  src.flags = UET_ADDR_IPV4;
  if ((r = uet_initialize(&s->h))) { what = "uet_initialize"; goto fail; }
  if ((r = uet_getinfo(s->h, &src, hints, &s->info))) {
    what = "uet_getinfo"; goto fail;
  }
  if ((r = uet_domain(s->h, &s->fabric, s->info, &s->domain, NULL,
                      eq_cb, eq_err_cb, &s->dom))) {
    what = "uet_domain"; goto fail;
  }
  s->ipv4 = ((struct uet_addr*)s->info->src_addr)->fa.v4;
  s->info->domain_attr->mr_mode |= FI_MR_PROV_KEY;
  if (stage_size) {
    s->size = stage_size;
    if (!(s->buf = alloc_region(stage_size))) {
      r = -ENOMEM; what = "staging"; goto fail;
    }
    /* a staging source stays local */
    if ((r = uet_mr_reg(s->dom, s->buf, stage_size, FI_WRITE | FI_READ,
                        UET_MR_KEY_NONE, UET_FLAGS_NONE, NULL, &s->mr))) {
      what = "uet_mr_reg (staging)"; goto fail;
    }
  }
  if (window_size) {
    s->win_size = window_size;
    if (!(s->win = alloc_region(window_size))) {
      r = -ENOMEM; what = "window"; goto fail;
    }
    if ((r = uet_mr_reg(s->dom, s->win, window_size,
                        FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                        UET_MR_KEY_IDEMPOTENT_SAFE, UET_FLAGS_NONE, NULL,
                        &s->win_mr))) {
      what = "uet_mr_reg (window)"; goto fail;
    }
  }
  s->info->tx_attr->size = 64;
  s->info->rx_attr->size = 64;
  if ((r = uet_endpoint(s->dom, s->info, &s->ep_fid, NULL, &s->ep))) {
    what = "uet_endpoint"; goto fail;
  }
  if ((stage_size && (r = uet_ep_bind_mr(s->ep, s->mr, UET_FLAGS_NONE))) ||
      (window_size && (r = uet_ep_bind_mr(s->ep, s->win_mr, UET_FLAGS_NONE)))) {
    what = "uet_ep_bind_mr"; goto fail;
  }
  memset(&cq_attr, 0, sizeof(cq_attr));
  cq_attr.format = FI_CQ_FORMAT_DATA;
  cq_attr.size = 128;
  if ((r = uet_ep_bind_cq(s->ep, &cq_attr, &s->cq, FI_SEND, NULL,
                          &s->tx_cq)) ||
      (r = uet_ep_bind_cq(s->ep, &cq_attr, &s->cq, FI_RECV, NULL,
                          &s->rx_cq))) {
    what = "uet_ep_bind_cq"; goto fail;
  }
  if ((stage_size && (r = uet_mr_enable(s->mr))) ||
      (window_size && (r = uet_mr_enable(s->win_mr)))) {
    what = "uet_mr_enable"; goto fail;
  }
  if ((r = uet_ep_enable(s->ep))) { what = "uet_ep_enable"; goto fail; }
  fi_freeinfo(hints);
  return s;

fail:
  snprintf(err, errlen, "%s: %s", what, fi_strerror(-r));
  fi_freeinfo(hints);
  free(s->buf);
  free(s->win);
  free(s);
  return NULL;
}

void uet_shim_close(struct uet_shim* s)
{
  if (!s) return;
  uet_ep_close(s->ep);
  uet_finalize(s->h);
  free(s->buf);
  free(s->win);
  free(s);
}

char* uet_shim_buffer(struct uet_shim* s) { return s->buf; }
char* uet_shim_window(struct uet_shim* s) { return s->win; }
uint64_t uet_shim_key(struct uet_shim* s)
{
  return s->win ? uet_mr_key(s->win_mr) : 0;
}
uint32_t uet_shim_ipv4(struct uet_shim* s) { return s->ipv4; }

int uet_shim_peer(struct uet_shim* s, uint32_t ipv4)
{
  struct uet_addr* ap;
  int i, r;

  for (i = 0; i < s->npeers; i++)
    if (s->peer_ip[i] == ipv4)
      return i;
  if (s->npeers == MAX_PEERS)
    return -ENOSPC;
  /* the peer's fabric endpoint in relative addressing, advertising the
   * HPC profile so writes to it may use RUDI */
  ap = &s->peer_addr[s->npeers];
  memset(ap, 0, sizeof(*ap));
  ap->ver = UET_ADDR_VERSION;
  ap->flags = (UET_ADDR_FEP_CAP_V | UET_ADDR_FA_V | UET_ADDR_PID_ON_FEP_V |
               UET_ADDR_INDEX_V | UET_ADDR_INITIATOR_V |
               UET_ADDR_RELATIVE_MODE | UET_ADDR_IPV4 |
               UET_ADDR_BIG_MSG_SIZE);
  ap->fep_cap = (UET_FEP_CAP_AI_FULL | UET_FEP_CAP_HPC);
  ap->fa.v4 = ipv4;
  ap->pid_on_fep = UET_ADDR_DEF_PID_ON_FEP;
  ap->num_indices = 1;
  ap->start_index = UET_ADDR_DEF_INDEX;
  ap->initiator_id = UET_ADDR_DEF_INITIATOR_ID;
  if ((r = uet_av_insert(s->dom, ap, &s->peer_h[s->npeers])))
    return r;
  s->peer_ip[s->npeers] = ipv4;
  return s->npeers++;
}

int uet_shim_write(struct uet_shim* s, int peer, size_t local_ofs, size_t len,
                   uint64_t remote_addr, uint64_t key)
{
  ssize_t r;

  if (peer < 0 || peer >= s->npeers || local_ofs + len > s->size)
    return -EINVAL;
  r = uet_write(s->ep, UET_DEF_JOB_ID, s->buf + local_ofs, len, NULL, s->mr,
                s->peer_h[peer], remote_addr, key, NULL);
  if (r == -FI_EAGAIN)
    return -EAGAIN;
  return r < 0 ? (int)r : 0;
}

static int poll_cq(uet_cq_handle_t cq)
{
  struct fi_cq_data_entry ce;
  struct fi_cq_err_entry err;
  ssize_t r = uet_cq_read(cq, &ce, 1);

  if (r == 1)
    return 1;
  if (r == 0 || r == -FI_EAGAIN)
    return 0;
  memset(&err, 0, sizeof(err));
  if (r == -FI_EAVAIL && uet_cq_readerr(cq, &err) > 0 && err.err)
    return -err.err;
  return -EIO;
}

int uet_shim_poll_tx(struct uet_shim* s) { return poll_cq(s->tx_cq); }
int uet_shim_poll_rx(struct uet_shim* s) { return poll_cq(s->rx_cq); }

const char* uet_shim_strerror(int err) { return fi_strerror(-err); }
