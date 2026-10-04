// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/crc64nvme.h"

#include "acconfig.h"
#include "arch/arm.h"
#include "arch/intel.h"
#include "arch/probe.h"
#include "include/buffer.h"

// the ISA-L that src/erasure-code/isa builds is linked into ceph-common.
// Its CRC-64 beats madler only on x86-64 and aarch64. On ppc64le it is a
// byte-at-a-time table; on riscv64 its vclmul kernel depends on the
// assembler ISA-L was built with and, at run time, on Zbb, which
// arch/riscv does not probe
#if defined(WITH_EC_ISA_PLUGIN) && \
    (defined(__x86_64__) || defined(__aarch64__))
#define CRC64NVME_ISAL
#endif

extern "C" {
#include "common/madler/crc64nvme.h"
#ifdef CRC64NVME_ISAL
#include "isa-l/include/crc64.h"
#endif
}

namespace ceph {

namespace {

using crc64nvme_func_t = uint64_t (*)(uint64_t crc, const void* data,
                                      size_t len);

uint64_t crc64nvme_madler(uint64_t crc, const void* data, size_t len)
{
  return crc64nvme_word(crc, data, len);
}

#ifdef CRC64NVME_ISAL
uint64_t crc64nvme_isal(uint64_t crc, const void* data, size_t len)
{
  // CRC-64/NVME is ISA-L's "rocksoft" polynomial, reflected; like
  // madler's, its seed is the previous canonical value (it inverts on
  // entry and on exit)
  return crc64_rocksoft_refl(crc, static_cast<const unsigned char*>(data),
                             len);
}
#endif

crc64nvme_func_t choose_crc64nvme()
{
  ceph_arch_probe();
#ifdef CRC64NVME_ISAL
  // ISA-L picks its fastest folding kernel itself (PCLMUL, AVX2 or
  // AVX-512 VPCLMUL, PMULL), but without carry-less multiply it falls
  // back to a byte-at-a-time table, slower than madler's slicing-by-8
# if defined(__x86_64__)
  if (ceph_arch_intel_pclmul && ceph_arch_intel_sse41) {
    return crc64nvme_isal;
  }
# elif defined(__aarch64__)
  if (ceph_arch_aarch64_pmull) {
    return crc64nvme_isal;
  }
# endif
#endif
  return crc64nvme_madler;
}

crc64nvme_func_t crc64nvme_func()
{
  static const crc64nvme_func_t f = choose_crc64nvme();
  return f;
}

} // anonymous namespace

uint64_t crc64nvme(uint64_t crc, const void* data, size_t len)
{
  if (len == 0) {
    // madler returns 0 for a null buffer, ISA-L the seed
    return crc;
  }
  return crc64nvme_func()(crc, data, len);
}

uint64_t crc64nvme(const ceph::buffer::list& bl)
{
  const auto f = crc64nvme_func();
  uint64_t crc = 0;
  for (const auto& ptr : bl.buffers()) {
    if (ptr.length()) {
      crc = f(crc, ptr.c_str(), ptr.length());
    }
  }
  return crc;
}

uint64_t crc64nvme_combine(uint64_t crc_a, uint64_t crc_b, uint64_t len_b)
{
  return crc64nvme_comb(crc_a, crc_b, len_b);
}

} // namespace ceph
