#!/usr/bin/env bash
#
# Out-of-band read pushes on the OSD's transfer threads
# (osd_oob_transfer_threads): an op thread hands the push off and goes
# on, and the reply leaves from the transfer thread once the push is
# over. The client windows are on the libfabric tcp provider, on
# 127.0.0.1, so this needs a build with WITH_OOB_OFI, whose
# ceph_test_osd_oob_read is the client. A libfabric whose memory
# registration cache monitor needs a GPU wants FI_MR_CACHE_MONITOR=disabled
# in the environment.
#
# Each pool has one PG, so that every object of a test shares the PG of
# the read whose push is held (osd_debug_oob_delay_done_ms).
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

# what the client prints in a read's flags
FLAG_DECLINED=8
FLAG_LANDED=16
SIZE=1048576

function run() {
    local dir=$1
    shift

    if ! type ceph_test_osd_oob_read > /dev/null 2>&1 ; then
        echo "ceph_test_osd_oob_read not found (built without WITH_OOB_OFI?), skipping"
        return 0
    fi

    export CEPH_MON="127.0.0.1:7161" # git grep '\<7161\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

##
# Run a mon, a mgr and three OSDs that serve out-of-band reads over the
# libfabric tcp provider, with **threads** transfer threads each. An op
# thread that runs for 3 s makes its OSD unhealthy. The remaining
# arguments go to each OSD.
#
function run_oob_cluster() {
    local dir=$1
    local threads=$2
    shift 2

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 2 ; do
        run_osd $dir $id \
            --oob_client_transports=tcp \
            --oob_memory=268435456 \
            --osd_oob_transfer_threads=$threads \
            --osd_op_thread_timeout=3 \
            "$@" || return 1
    done
    for id in 0 1 2 ; do
        test "$(ofi_stat $id available)" = true || return 1
    done
}

function ofi_stat() {
    local id=$1
    local field=$2
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) ofi status | \
        jq -r ".$field"
}

function xfer_stat() {
    local id=$1
    local field=$2
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) \
        perf dump oob_xfer | jq -r ".oob_xfer.$field"
}

# a replicated pool of one PG, on all three OSDs
function create_repl_pool() {
    local pool=$1
    create_pool $pool 1 1 || return 1
    wait_for_clean || return 1
}

# an erasure-coded pool of one PG, k=2 m=1, with EC optimizations
function create_ec_pool_k2m1() {
    local pool=$1
    ceph osd erasure-code-profile set oobprofile k=2 m=1 \
        crush-failure-domain=osd || return 1
    create_pool $pool 1 1 erasure oobprofile || return 1
    ceph osd pool set $pool allow_ec_overwrites true || return 1
    ceph osd pool set $pool allow_ec_optimizations true || return 1
    wait_for_clean || return 1
}

##
# Print **field** of the line the client printed for its read of **oid**
# in **out**.
#
function read_field() {
    local out="$1"
    local oid=$2
    local field=$3
    echo "$out" | grep "^get $oid: " | grep -o " $field=[^ ]*" | cut -d= -f2
}

function others_field() {
    local out="$1"
    local field=$2
    echo "$out" | grep "^others: " | grep -o " $field=[^ ]*" | cut -d= -f2
}

# the read of oid got its object out of band, landed
function expect_landed() {
    local out="$1"
    local oid=$2
    test "$(read_field "$out" $oid r)" = 0 || return 1
    test "$(read_field "$out" $oid data)" = ok || return 1
    test "$(read_field "$out" $oid crc)" = ok || return 1
    test "$(read_field "$out" $oid bytes)" = $SIZE || return 1
    test "$(read_field "$out" $oid inline)" = 0 || return 1
    local flags=$(read_field "$out" $oid flags)
    (( flags & FLAG_LANDED )) || return 1
}

# the read of oid got its object inline, declined
function expect_declined() {
    local out="$1"
    local oid=$2
    test "$(read_field "$out" $oid r)" = 0 || return 1
    test "$(read_field "$out" $oid data)" = ok || return 1
    test "$(read_field "$out" $oid bytes)" = 0 || return 1
    test "$(read_field "$out" $oid inline)" = $SIZE || return 1
    local flags=$(read_field "$out" $oid flags)
    (( flags & FLAG_DECLINED )) || return 1
}

##
# While the primary holds the push of a read for 3 s on a transfer
# thread, 200 writes, stats and reads of the same PG finish, and no op
# thread of the primary times out. The first is a write to the object
# being read: the op thread let go of the object's lock at the handoff,
# so the write is acknowledged before the read's reply, and the read
# still gets the object as it was before the write.
#
function held_push_frees_op_thread() {
    local dir=$1
    local pool=$2

    ceph_test_osd_oob_read write $pool $SIZE obj || return 1
    local primary=$(get_primary $pool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 3000 || return 1
    local queued=$(xfer_stat $primary queue_lat.avgcount)

    local out
    out=$(ceph_test_osd_oob_read get $pool $SIZE --others 200 obj) || return 1
    echo "$out"
    expect_landed "$out" obj || return 1
    # the read was held, the others were not
    test $(read_field "$out" obj ms) -ge 3000 || return 1
    test "$(others_field "$out" failed)" = 0 || return 1
    test "$(others_field "$out" before_reads)" = 1 || return 1
    test $(others_field "$out" max_ms) -lt 1000 || return 1
    # on a transfer thread, handed off by the op thread
    test $(xfer_stat $primary queue_lat.avgcount) -gt $queued || return 1
    grep -q "deliver_oob .*:::obj:head .* handed off to the transfer threads" \
        $dir/osd.$primary.log || return 1
    # past the 3 s of osd_op_thread_timeout, heartbeat checks included
    sleep 2
    ! grep -q "osd_op_tp.*had timed out" $dir/osd.$primary.log || return 1
}

# (a) a replicated pool
function TEST_held_push_frees_op_thread() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_repl_pool rpool || return 1
    held_push_frees_op_thread $dir rpool || return 1
}

# (b) an erasure-coded pool: the primary's read completes through
# kick_reads
function TEST_held_push_frees_op_thread_ec() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_ec_pool_k2m1 ecpool || return 1
    held_push_frees_op_thread $dir ecpool || return 1
}

# without transfer threads the op thread holds the PG for the push, and
# the other ops of the PG wait for it: what (a) shows is gone
function TEST_inline_push_holds_the_pg() {
    local dir=$1

    run_oob_cluster $dir 0 || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 3000 || return 1

    local out
    out=$(ceph_test_osd_oob_read get rpool $SIZE --others 200 obj) || return 1
    echo "$out"
    expect_landed "$out" obj || return 1
    test "$(others_field "$out" before_reads)" = 0 || return 1
    test $(others_field "$out" max_ms) -ge 2000 || return 1
    test $(xfer_stat $primary queue_lat.avgcount) = 0 || return 1
}

##
# (c) An interval change while the primary holds a push: the op is in no
# list of the PG, so nothing runs it again; the reply leaves once, with
# the object's bytes, and the PG drops the object's context without the
# op (on_flushed asserts it holds none).
#
function TEST_interval_change_during_held_push() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj1 obj2 || return 1
    local primary=$(get_primary rpool obj1)
    local replica=$(get_not_primary rpool obj1)
    local pgid=$(get_pg rpool obj1)
    for id in 0 1 2 ; do
        ceph tell osd.$id config set osd_debug_oob_delay_done_ms 3000 || return 1
    done

    # repeer the PG, then mark a replica down, while the push is held
    local started=$(ofi_stat $primary plans_started)
    timeout 60 ceph_test_osd_oob_read get rpool $SIZE obj1 > $dir/get1.out &
    local pid=$!
    sleep 1
    ceph pg repeer $pgid || return 1
    ceph osd down osd.$replica || return 1
    wait $pid || return 1
    cat $dir/get1.out
    test $(grep -c '^get ' $dir/get1.out) = 1 || return 1
    expect_landed "$(cat $dir/get1.out)" obj1 || return 1
    test $(ofi_stat $primary plans_started) = $((started + 1)) || return 1
    wait_for_clean || return 1

    # mark the primary down while it holds the push: its PG leaves the
    # primary role, and drops its object contexts, while the push runs.
    # The push's reply lands, unless the client learned of the new map
    # first and resent the read to the next primary, which serves it
    # inline; the client then drops the push's reply
    primary=$(get_primary rpool obj2)
    started=$(ofi_stat $primary plans_started)
    timeout 60 ceph_test_osd_oob_read get rpool $SIZE obj2 > $dir/get2.out &
    pid=$!
    sleep 1
    ceph osd down osd.$primary || return 1
    wait $pid || return 1
    cat $dir/get2.out
    test $(grep -c '^get ' $dir/get2.out) = 1 || return 1
    test "$(read_field "$(cat $dir/get2.out)" obj2 r)" = 0 || return 1
    test "$(read_field "$(cat $dir/get2.out)" obj2 data)" = ok || return 1
    grep -q "wrongly marked me down" $dir/osd.$primary.log || return 1
    # the push ran to its end on the old primary
    sleep 3
    test $(ofi_stat $primary plans_started) = $((started + 1)) || return 1
    wait_for_clean || return 1

    # and no OSD ran either read again past its handoff: a re-run would
    # reply without a second push, which the client drops by its tid
    for id in 0 1 2 ; do
        kill -0 $(cat $dir/osd.$id.pid) || return 1
        ! grep -q "obc still alive\|FAILED ceph_assert" $dir/osd.$id.log || return 1
        ! grep -q "already pushed" $dir/osd.$id.log || return 1
    done
}

##
# (d) A slow stop while a push is held and another waits for the only
# transfer thread: the waiting one is shed, which starts nothing, and
# the stop waits for the held one, at most until its land_by. The reads
# get their objects, from the stopping primary or, resent, from the
# next one.
#
function TEST_slow_stop_during_held_push() {
    local dir=$1

    run_oob_cluster $dir 1 --osd_fast_shutdown=false || return 1
    create_repl_pool rpool || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj1 obj2 || return 1
    local primary=$(get_primary rpool obj1)
    # long enough that the stop reaches the transfer threads before the
    # held push ends, and within the lease plus drain
    ceph tell osd.$primary config set osd_debug_oob_delay_done_ms 6000 || return 1

    timeout 60 ceph_test_osd_oob_read get rpool $SIZE obj1 obj2 > $dir/get.out &
    local pid=$!
    local queued
    for i in $(seq 100) ; do
        queued=$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$primary) \
            dump_oob_transfers | jq -r '.oob_transfers.queued // .queued')
        test "$queued" = 1 && break
        sleep 0.1
    done
    test "$queued" = 1 || return 1
    kill_daemons $dir TERM osd.$primary || return 1
    wait $pid || return 1
    cat $dir/get.out
    for oid in obj1 obj2 ; do
        test "$(read_field "$(cat $dir/get.out)" $oid r)" = 0 || return 1
        test "$(read_field "$(cat $dir/get.out)" $oid data)" = ok || return 1
    done

    local log=$dir/osd.$primary.log
    grep -q "stopping: shedding 1 queued jobs, waiting for 1 running" $log || return 1
    grep -q "oob transfer threads stopped" $log || return 1
    ! grep -q "FAILED ceph_assert\|Caught signal" $log || return 1
    # the stop waited for the held push, and not past the pool's lease
    # plus drain (8 s)
    local shed=$(grep -m1 "stopping: shedding" $log | cut -d' ' -f1)
    local stopped=$(grep -m1 "oob transfer threads stopped" $log | cut -d' ' -f1)
    local waited=$(( $(date -d "$stopped" +%s%3N) - $(date -d "$shed" +%s%3N) ))
    echo "the transfer threads stopped ${waited} ms after the shed"
    test $waited -le 8000 || return 1
}

##
# (e) A push that a transfer thread can start only after the pool's
# lease starts nothing: the read is declined, its data inline.
#
function TEST_push_past_lease_declines() {
    local dir=$1

    run_oob_cluster $dir 4 || return 1
    create_repl_pool rpool || return 1
    ceph osd pool set rpool rdma_delivery_lease 1 || return 1
    ceph_test_osd_oob_read write rpool $SIZE obj || return 1
    local primary=$(get_primary rpool obj)
    # each push waits 1.5 s on its transfer thread before it may start
    ceph tell osd.$primary config set osd_debug_oob_delay_post_ms 1500 || return 1
    local started=$(ofi_stat $primary plans_started)
    local expired=$(xfer_stat $primary expired)

    local out
    out=$(ceph_test_osd_oob_read get rpool $SIZE obj) || return 1
    echo "$out"
    expect_declined "$out" obj || return 1
    test $(ofi_stat $primary plans_started) = $started || return 1
    test $(xfer_stat $primary expired) = $((expired + 1)) || return 1

    # within the lease, it goes out of band again
    ceph tell osd.$primary config set osd_debug_oob_delay_post_ms 0 || return 1
    out=$(ceph_test_osd_oob_read get rpool $SIZE obj) || return 1
    echo "$out"
    expect_landed "$out" obj || return 1
    test $(ofi_stat $primary plans_started) = $((started + 1)) || return 1
}

main osd-oob-async "$@"

# Local Variables:
# compile-command: "cd ../../../build ; ninja ceph-osd ceph_test_osd_oob_read && ../qa/run-standalone.sh osd-oob-async.sh"
# End:
