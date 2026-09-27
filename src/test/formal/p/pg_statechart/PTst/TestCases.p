module System = { Main, Mon, Osd, Pg, Client, Env };

test tcCrash2 [main=TestCrash]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestCrash });
test tcCrash3 [main=TestCrash3]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestCrash3 });
test tcRemap [main=TestRemap]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestRemap });
test tcFull [main=TestFull]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFull });
test tcPreempt [main=TestPreempt]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestPreempt });
test tcAsync [main=TestAsync]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestAsync });
test tcCommands [main=TestCommands]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestCommands });
test tcLostSafety [main=TestLost]:
  assert AckedWritesDurable, MapsSettle in (union System, { TestLost });
test tcBfFull [main=TestBfFull]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestBfFull });
test tcBfPreempt [main=TestBfPreempt]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestBfPreempt });
test tcBfCrash [main=TestBfCrash]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestBfCrash });
test tcRecFull [main=TestRecFull]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestRecFull });
test tcRecPreempt [main=TestRecPreempt]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestRecPreempt });
test tcRecCrash [main=TestRecCrash]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestRecCrash });
test tcBf2Full [main=TestBf2Full]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestBf2Full });
test tcBf2Preempt [main=TestBf2Preempt]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestBf2Preempt });
test tcCmdReset [main=TestCmdReset]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestCmdReset });
test tcFixCmdReset [main=TestFixCmdReset]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixCmdReset });
test tcFixCommands [main=TestFixCommands]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixCommands });
test tcRecWaitLocalFull [main=TestRecWaitLocalFull]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestRecWaitLocalFull });
test tcRecWaitRemoteFull [main=TestRecWaitRemoteFull]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestRecWaitRemoteFull });
test tcFixRecWaitLocalFull [main=TestFixRecWaitLocalFull]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixRecWaitLocalFull });
test tcFixRecWaitRemoteFull [main=TestFixRecWaitRemoteFull]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixRecWaitRemoteFull });
test tcBf2StaleGrant [main=TestBf2StaleGrant]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestBf2StaleGrant });
test tcRecSlotLost [main=TestRecSlotLost]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestRecSlotLost });
test tcBfSlotLost [main=TestBfSlotLost]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestBfSlotLost });
test tcFixBf2StaleGrant [main=TestFixBf2StaleGrant]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixBf2StaleGrant });
test tcFixRecSlotLost [main=TestFixRecSlotLost]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixRecSlotLost });
test tcFixBfSlotLost [main=TestFixBfSlotLost]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixBfSlotLost });
test tcFixBf2Chaos [main=TestFixBf2Chaos]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixBf2Chaos });
test tcHalfFixRecSlotLost [main=TestHalfFixRecSlotLost]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestHalfFixRecSlotLost });
test tcHalfFixBfSlotLost [main=TestHalfFixBfSlotLost]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestHalfFixBfSlotLost });
test tcFixRecChaos [main=TestFixRecChaos]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixRecChaos });
test tcDeleteRePriority [main=TestDeleteRePriority]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestDeleteRePriority });
test tcFixDeleteRePriority [main=TestFixDeleteRePriority]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixDeleteRePriority });
test tcMaxPgResume [main=TestMaxPgResume]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestMaxPgResume });
test tcMaxPgBackfillTarget [main=TestMaxPgBackfillTarget]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestMaxPgBackfillTarget });
test tcFixMaxPgBackfillTarget [main=TestFixMaxPgBackfillTarget]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixMaxPgBackfillTarget });
test tcCuts2 [main=TestCuts]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestCuts });
test tcCuts1 [main=TestCuts1]:
  assert AckedWritesDurable, PgGoesActive, PgGoesClean, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestCuts1 });
test tcCutPersist2 [main=TestCutPersist2]:
  assert PgGoesActive, WritesComplete, AckedWritesDurable
  in (union System, { TestCutPersist2 });
test tcCutPersist1 [main=TestCutPersist1]:
  assert PgGoesActive, WritesComplete, AckedWritesDurable
  in (union System, { TestCutPersist1 });
test tcMaxPgSingle [main=TestMaxPgSingle]:
  assert AckedWritesDurable, PgGoesActive, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestMaxPgSingle });
test tcFixMaxPgSingle [main=TestFixMaxPgSingle]:
  assert AckedWritesDurable, PgGoesActive, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixMaxPgSingle });
test tcFixMaxPgChurn [main=TestFixMaxPgChurn]:
  assert AckedWritesDurable, PgGoesActive, WritesComplete, MapsSettle, ReservationsReleased
  in (union System, { TestFixMaxPgChurn });
