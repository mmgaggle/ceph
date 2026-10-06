// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

// The libfabric RMA code lives in the ofi-rma library (src/ofi-rma), so
// that clients can lend windows without building Ceph. Ceph code keeps
// including this header and naming the code ceph::ofi.
#include <ofi_rma/ofi_rma.h>

#include "common/ofi_rma_fwd.h"
