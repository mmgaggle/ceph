// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * The life cycle of an EC coroutine op, as PrimaryLogPG::do_op() runs
 * it: coro_resumer owns the coroutine, the coroutine drops that
 * reference from inside when it finishes (on_coroutine_complete()), and
 * a read completion resumes it through a weak reference
 * (ECBackend::objects_read_sync()). Every path must free the
 * coroutine's stack; a stack allocator that counts live stacks checks
 * that.
 */

#include <gtest/gtest.h>

#include <optional>

#include <boost/context/fixedsize_stack.hpp>

#include "osd/Coroutines.h"

namespace {

long live_stacks = 0;

struct counting_stack {
  boost::context::fixedsize_stack s;
  boost::context::stack_context allocate() {
    ++live_stacks;
    return s.allocate();
  }
  void deallocate(boost::context::stack_context& sc) noexcept {
    --live_stacks;
    s.deallocate(sc);
  }
};

struct witness {
  int* destroyed;
  ~witness() { ++*destroyed; }
};

struct FakePG {
  std::shared_ptr<resume_token_t> coro_resumer;
  std::optional<CoroHandles> handles;
  int finished = 0;
  int locals_destroyed = 0;

  // PrimaryLogPG::do_op()
  void start(bool yields) {
    coro_resumer = std::make_shared<resume_token_t>(counting_stack{},
      [this, yields](yield_token_t& yield) {
        witness w{&locals_destroyed};
        handles.emplace(CoroHandles{yield, coro_resumer});
        if (yields) {
          handles->yield();
        }
        ++finished;
        coro_resumer = nullptr;  // on_coroutine_complete()
      });
    resume_coroutine(coro_resumer);
  }
  // the read completion of ECBackend::objects_read_sync()
  void read_completes() {
    if (auto locked = handles->resume.lock(); locked) {
      (*locked)();
    }
  }
};

} // anonymous namespace

TEST(ECCoroutines, FinishWithoutYieldFreesStack)
{
  live_stacks = 0;
  FakePG pg;
  pg.start(false);
  EXPECT_EQ(1, pg.finished);
  EXPECT_EQ(nullptr, pg.coro_resumer);
  EXPECT_EQ(0, live_stacks);
}

TEST(ECCoroutines, FinishAfterYieldFreesStack)
{
  live_stacks = 0;
  FakePG pg;
  pg.start(true);
  EXPECT_EQ(0, pg.finished);
  EXPECT_EQ(1, live_stacks);
  pg.read_completes();
  EXPECT_EQ(1, pg.finished);
  EXPECT_EQ(0, live_stacks);
}

TEST(ECCoroutines, StoppedWhileSuspendedFreesStack)
{
  live_stacks = 0;
  FakePG pg;
  pg.start(true);
  EXPECT_EQ(1, live_stacks);
  pg.coro_resumer = nullptr;  // PrimaryLogPG::stop_coroutine()
  EXPECT_EQ(0, live_stacks);
  EXPECT_EQ(1, pg.locals_destroyed);  // unwound, not abandoned
  pg.read_completes();  // a late completion finds the token gone
  EXPECT_EQ(0, pg.finished);
  EXPECT_EQ(0, live_stacks);
}

TEST(ECCoroutines, ManyOpsKeepNoStacks)
{
  live_stacks = 0;
  FakePG pg;
  for (int i = 0; i < 10000; i++) {
    bool yields = i % 3 == 0;
    pg.start(yields);
    if (yields) {
      pg.read_completes();
    }
  }
  EXPECT_EQ(10000, pg.finished);
  EXPECT_EQ(10000, pg.locals_destroyed);
  EXPECT_EQ(0, live_stacks);
}
