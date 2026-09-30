/* -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- */
/*
 * Minimal C interface over the UEC reference provider (uet-ref-prov).
 * Its API headers pull in libfabric internals that only compile as C,
 * so C++ code (the OSD, the test client) goes through this shim.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct uet_shim;

/*
 * Open an endpoint on the interface named by UET_IFNAME with one
 * registered region of `size` bytes. `window` makes the region a
 * remotely writable, idempotent-safe target (a client window);
 * otherwise it is a local staging source. Returns NULL and fills err
 * on failure.
 */
struct uet_shim* uet_shim_open(size_t size, int window, char* err,
                               size_t errlen);
void uet_shim_close(struct uet_shim* s);

char* uet_shim_buffer(struct uet_shim* s);
uint64_t uet_shim_key(struct uet_shim* s);
uint32_t uet_shim_ipv4(struct uet_shim* s);  /* host byte order */

/* index of the peer at ipv4 (inserted and cached on first use), or a
 * negative errno */
int uet_shim_peer(struct uet_shim* s, uint32_t ipv4);

/* RMA-write [local_ofs, local_ofs+len) of the region to the peer's
 * remote_addr under key; 0, -EAGAIN when the queue is full, or a
 * negative errno */
int uet_shim_write(struct uet_shim* s, int peer, size_t local_ofs, size_t len,
                   uint64_t remote_addr, uint64_t key);

/* poll once: 1 per completion reaped, 0 when none, negative errno on a
 * failed completion */
int uet_shim_poll_tx(struct uet_shim* s);
int uet_shim_poll_rx(struct uet_shim* s);

const char* uet_shim_strerror(int err);

#ifdef __cplusplus
}
#endif
