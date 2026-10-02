/*
 * CephFS capability protocol model: shared types and helpers.
 *
 * Only the CEPH_LOCK_IFILE ("F") cap bits are modelled. P has no bitwise
 * operators, so a cap mask is a set[tCap] and the helpers below provide the
 * union / intersection / difference operations that the C++ code does with
 * `|`, `&` and `& ~`.
 *
 *   Fs  CEPH_CAP_GSHARED   client can read file metadata (size, mtime, ...)
 *   Fx  CEPH_CAP_GEXCL     client can update file metadata
 *   Fc  CEPH_CAP_GCACHE    client can cache reads
 *   Fr  CEPH_CAP_GRD       client can read
 *   Fw  CEPH_CAP_GWR       client can write
 *   Fb  CEPH_CAP_GBUFFER   client can buffer writes
 *
 * Fl (lazy IO) and Fa (WREXTEND) are not modelled: no lock state issues
 * them unless a client asks for lazy IO.
 */

enum tCap { Fs = 0, Fx = 1, Fc = 2, Fr = 3, Fw = 4, Fb = 5 }
type tCaps = set[tCap];

/* open modes, see CEPH_FILE_MODE_* */
enum tMode { MODE_RD = 0, MODE_WR = 1, MODE_RDWR = 2 }

/* MClientCaps ops used by the MDS -> client direction */
enum tCapOp { OP_GRANT = 0, OP_REVOKE = 1 }

/* ---- client -> MDS ---- */

/* MClientRequest(CEPH_MDS_OP_OPEN) */
type tOpenReq = (client: int, from: machine, mode: tMode);

/*
 * A ceph_mds_request_release embedded in an MClientRequest
 * (Client::encode_inode_release).
 */
type tReqRelease = (cap_id: int, cap_seq: int, issue_seq: int, caps: tCaps, wanted: tCaps);

/* MClientRequest(CEPH_MDS_OP_GETATTR | CEPH_MDS_OP_SETATTR) on the one file */
type tMdsReq = (client: int, from: machine, has_release: bool, release: tReqRelease);

/*
 * MClientCaps(CEPH_CAP_OP_UPDATE | FLUSH).
 *   cap_id     the Capability's id; the MDS ignores messages for an old cap
 *   cap_seq    cap->seq, the seq of the last GRANT/REVOKE the client saw
 *   issue_seq  cap->issue_seq, the seq of the last request-reply issue
 *   caps       cap->implemented after the client dropped what it does not retain
 *   wanted     what the client wants
 *   dirty      metadata the client flushes with this message (Fw and/or Fx)
 *   tid        flush tid, 0 when nothing is flushed
 *   max_size   requested max_size (wanted_max_size), 0 if none
 */
type tCapUpdate = (client: int, cap_id: int, cap_seq: int, issue_seq: int, caps: tCaps, wanted: tCaps,
                   dirty: tCaps, tid: int, max_size: int);

/* MClientCapRelease entry */
type tCapRelease = (client: int, cap_id: int, issue_seq: int);

/* MClientSession(CEPH_SESSION_REQUEST_RENEWCAPS) */
type tRenewCaps = (client: int, renew_seq: int);

event eOpenReq    : tOpenReq;
event eGetattrReq : tMdsReq;
event eSetattrReq : tMdsReq;
event eCapUpdate  : tCapUpdate;
event eCapRelease : tCapRelease;
event eRenewCaps  : tRenewCaps;
/*
 * Modelling artifact: the MDS session timeout fires for this client. It is
 * sent by the client after its own cap_ttl expired, which encodes the
 * protocol's timing assumption that a client stops trusting its caps
 * before the MDS declares the session stale.
 */
event eSessionTimeout : int;

/* ---- MDS -> client ---- */

/* the ceph_mds_reply_cap part of an MClientReply; has_cap is false when the reply carries no cap */
type tCapReply = (has_cap: bool, cap_id: int, caps: tCaps, wanted: tCaps, cap_seq: int, max_size: int);

/* MClientCaps(GRANT | REVOKE) */
type tCapGrant = (op: tCapOp, cap_id: int, cap_seq: int, caps: tCaps, wanted: tCaps, issue_seq: int, max_size: int);

/* MClientCaps(FLUSH_ACK) */
type tFlushAck = (tid: int, dirty: tCaps);

event eOpenReply    : tCapReply;
event eGetattrReply : tCapReply;
event eSetattrReply : tCapReply;
event eCapGrant     : tCapGrant;
event eFlushAck     : tFlushAck;
/* MClientSession(CEPH_SESSION_STALE) */
event eSessionStale;
/* the session was killed and the client blocklisted: the client sees its connection reset */
event eSessionKilled;
/* MClientSession(CEPH_SESSION_RENEWCAPS), carries the renew seq */
event eRenewCapsAck : int;

/* ---- announcements observed by the specs ---- */

type tCapsAnnounce = (client: int, caps: tCaps);
/* the MDS Capability::issued() for a client changed */
event eMdsIssued : tCapsAnnounce;
/*
 * the client's Cap::implemented changed, or the cap was dropped (empty).
 * Caps the client does not consider valid (session cap_gen moved on, or
 * cap_ttl expired) are reported as empty: Inode::caps_issued() ignores them.
 */
event eClientImplemented : tCapsAnnounce;

type tIoDone = (client: int, ver: int);
event eReadStart : int;
event eReadDone  : tIoDone;
event eWriteDone : tIoDone;
event eStatDone  : int;
event eSetattrDone : int;
/* a buffered write that had completed to its application was lost to a blocklist */
event eWriteLost : int;
/* a buffered write was overwritten in the cache before it was flushed: `old` can only reach the
   store through `new`, so if `new` is lost, `old` is lost too */
type tSuperseded = (older: int, newer: int);
event eWriteSuperseded : tSuperseded;

/* ---- set helpers ---- */

fun CapsNone() : tCaps {
  var r: tCaps;
  return r;
}

fun CapsAll() : tCaps {
  var r: tCaps;
  r += (Fs); r += (Fx); r += (Fc); r += (Fr); r += (Fw); r += (Fb);
  return r;
}

fun Caps1(a: tCap) : tCaps {
  var r: tCaps;
  r += (a);
  return r;
}

fun Caps2(a: tCap, b: tCap) : tCaps {
  var r: tCaps;
  r += (a); r += (b);
  return r;
}

fun Caps3(a: tCap, b: tCap, c: tCap) : tCaps {
  var r: tCaps;
  r += (a); r += (b); r += (c);
  return r;
}

fun Caps4(a: tCap, b: tCap, c: tCap, d: tCap) : tCaps {
  var r: tCaps;
  r += (a); r += (b); r += (c); r += (d);
  return r;
}

fun CapsUnion(a: tCaps, b: tCaps) : tCaps {
  var r: tCaps;
  var c: tCap;
  r = a;
  foreach (c in b) { r += (c); }
  return r;
}

fun CapsInter(a: tCaps, b: tCaps) : tCaps {
  var r: tCaps;
  var c: tCap;
  foreach (c in a) { if (c in b) { r += (c); } }
  return r;
}

/* a & ~b */
fun CapsMinus(a: tCaps, b: tCaps) : tCaps {
  var r: tCaps;
  var c: tCap;
  foreach (c in a) { if (!(c in b)) { r += (c); } }
  return r;
}

/* a is a subset of b, i.e. (a & ~b) == 0 */
fun CapsSubset(a: tCaps, b: tCaps) : bool {
  var c: tCap;
  foreach (c in a) { if (!(c in b)) { return false; } }
  return true;
}

fun CapsIntersects(a: tCaps, b: tCaps) : bool {
  var c: tCap;
  foreach (c in a) { if (c in b) { return true; } }
  return false;
}

fun CapsEmpty(a: tCaps) : bool {
  return sizeof(a) == 0;
}

/* CEPH_CAP_ANY_FILE_WR restricted to the F shift */
fun AnyFileWr() : tCaps {
  return Caps3(Fw, Fb, Fx);
}

/* ceph_caps_for_mode(), F bits only */
fun CapsForMode(mode: tMode) : tCaps {
  var r: tCaps;
  if (mode == MODE_RD || mode == MODE_RDWR) {
    r += (Fs); r += (Fr); r += (Fc);
  }
  if (mode == MODE_WR || mode == MODE_RDWR) {
    r += (Fx); r += (Fw); r += (Fb);
  }
  return r;
}
