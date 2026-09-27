module System = { Main, Mon, Osd, Client, Env };

// random failures; the cluster settles and must recover
test tcCrash3 [main=TestCrash3]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestCrash3 });
test tcCrashMin1 [main=TestCrash3Min1]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestCrash3Min1 });
test tcRemap [main=TestRemap]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestRemap });
test tcFlap [main=TestFlap]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestFlap });
test tcMinSize [main=TestMinSize]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestMinSize });
// with OSDs marked lost the PG may stay incomplete by design (tcScriptLost*),
// so only safety here; with the override it must recover
test tcLostSafety [main=TestLost]:
  assert AckedWritesSurvive, MapsSettle in (union System, { TestLost });
test tcLostOverride [main=TestLostIgnoreHistoryLes]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestLostIgnoreHistoryLes });

// scripted: incomplete after `ceph osd lost`, and the override
test tcScriptLostIncomplete [main=TestIncompleteAfterLost]:
  assert PgGoesActive in (union System, { TestIncompleteAfterLost });
test tcScriptLostIgnoreLes [main=TestIncompleteAfterLostIgnoreLes]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestIncompleteAfterLostIgnoreLes });

// scripted: a stale notify completes GetInfo without the peer it came from
test tcScriptStaleNotify [main=TestStaleNotify]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestStaleNotify });
test tcScriptMin2StaleNotify [main=TestStaleNotifyMin2]:
  assert AckedWritesSurvive in (union System, { TestStaleNotifyMin2 });
test tcFixScriptStaleNotify [main=TestStaleNotifyFixed]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestStaleNotifyFixed });
test tcFixFlap [main=TestFlapFixed]:
  assert AckedWritesSurvive, PgGoesActive, WritesComplete, MapsSettle in (union System, { TestFlapFixed });

// design elements removed: each must produce a counterexample
test tcBugNoUpThruGate [main=TestNoUpThruGate]:
  assert AckedWritesSurvive in (union System, { TestNoUpThruGate });
test tcBugNoStaleFilter [main=TestNoStaleFilter]:
  assert AckedWritesSurvive in (union System, { TestNoStaleFilter });
