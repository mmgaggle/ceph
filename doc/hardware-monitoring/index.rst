.. _hardware-monitoring:

===================
Hardware Monitoring
===================

``node-proxy`` is the internal name of the agent which inventories a machine's
hardware, provides different statuses, and enables the operator to perform
some actions.
It gathers details from the Redfish API that is often provided by an
out-of-band management interface present on server systems. The data is then
processed and pushed to an agent endpoint in the Ceph Manager.

.. graphviz::

     digraph G {
         node [shape=record];
         mgr [label="{<mgr> ceph manager}"];
         dashboard [label="<dashboard> ceph dashboard"];
         agent [label="<agent> agent"];
         redfish [label="<redfish> redfish"];
     
         agent -> redfish [label=" 1." color=green];
         agent -> mgr [label=" 2." color=orange];
         dashboard:dashboard -> mgr [label=" 3." color=lightgreen];
         node [shape=plaintext];
         legend [label=<<table border="0" cellborder="1" cellspacing="0">
             <tr><td bgcolor="lightgrey">Legend</td></tr>
             <tr><td align="center">1. Collects data from Redfish API</td></tr>
             <tr><td align="left">2. Pushes data to Ceph Manager</td></tr>
             <tr><td align="left">3. Queries Ceph Manager</td></tr>
         </table>>];
     }


Limitations
===========

For the time being, the ``node-proxy`` agent relies on the Redfish API.
This implies that both ``node-proxy`` agent and the Ceph Manager need to be
able to access the out-of-band network.


How Often It Reports
====================

``node-proxy`` reads the Redfish API every 20 seconds
(``system.refresh_interval``). It sends a report to the Ceph Manager as
soon as something other than a reading changes: health, state,
inventory or firmware. When only readings change (temperatures, fan
speeds, network counters), it sends them at most every 300 seconds
(``reporter.readings_interval``).

When the Redfish service has a server-sent event stream
(``EventService.ServerSentEventUri``), ``node-proxy`` follows it and reads
the API as soon as an event comes, without waiting for the next refresh
(``system.event_stream``, on by default). Without one, it keeps polling
alone.

The Ceph Manager stores each host's report without its readings, and
only when it changes.


.. _hardware-monitoring-metrics:

Metrics
=======

Each ``node-proxy`` serves its host's hardware metrics in the Prometheus
format at ``https://<host>:9456/metrics``:

* ``ceph_hardware_status``: the health of every component, with its
  Redfish state;
* temperatures, fan speeds, drive and memory capacities, CPU cores and
  firmware versions;
* ``ceph_hardware_report_timestamp_seconds``: when ``node-proxy`` last
  read the hardware. A value that stops moving means it cannot read the
  Redfish API.

They come from its last complete read of the Redfish API, so the readings
are at most ``system.refresh_interval`` seconds old, and a scrape does not
wait for the Redfish API. The metrics do not go through the Ceph Manager,
which no longer exports them (``mgr/prometheus/exclude_hardware_metrics``,
on by default).

The Prometheus that cephadm deploys finds every ``node-proxy`` through
cephadm's service discovery. It scrapes them over TLS, checks their
certificates with cephadm's root CA, and logs in with credentials that
cephadm creates once for the cluster. These credentials open
``/metrics`` and nothing else.

To scrape ``node-proxy`` from another Prometheus, give it the root CA and
the credentials:

.. prompt:: bash #

   ceph orch certmgr cert get cephadm_root_ca_cert
   ceph config-key get mgr/cephadm/node_proxy/metrics_username
   ceph config-key get mgr/cephadm/node_proxy/metrics_password


Deploying the Agent
===================

The first step is to provide the out-of-band management tool IP address and
credentials. This can be done when adding the host with a :ref:`service
spec <orchestrator-cli-service-spec>` file:

.. prompt:: bash # auto

  # cat host.yml
  ---
  service_type: host
  hostname: node-10
  addr: 10.10.10.10
  oob:
    addr: 20.20.20.10
    username: admin
    password: p@ssword

Apply the spec:

.. prompt:: bash # auto

  # ceph orch apply -i host.yml
  Added host 'node-10' with addr '10.10.10.10'

Deploy the agent:

.. prompt:: bash # auto

  # ceph config set mgr mgr/cephadm/hw_monitoring true


CLI
===

| **orch** **hardware** **status** [hostname] [--category CATEGORY] [--format plain | json]

Supported categories are:

* summary (default)
* memory
* storage
* processors
* network
* power
* fans
* temperatures
* firmware
* criticals


Examples
--------


Hardware Health Status Summary
______________________________

.. prompt:: bash # auto

  # ceph orch hardware status
  +---------+---------+---------+-----+-----+--------+-------+------+
  |   HOST  |    SN   | STORAGE | CPU | NET | MEMORY | POWER | FANS |
  +---------+---------+---------+-----+-----+--------+-------+------+
  | node-10 | FR8Y5X3 |    ok   |  ok |  ok |   ok   |   ok  |  ok  |
  +---------+---------+---------+-----+-----+--------+-------+------+


Memory Report
_____________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category memory
  +---------+--------+---------+--------+---------+
  |   HOST  | SYS_ID |   NAME  | STATUS |  STATE  |
  +---------+--------+---------+--------+---------+
  | node-10 |   1    | DIMM A1 |   OK   | Enabled |
  | node-10 |   1    | DIMM A2 |   OK   | Enabled |
  | node-10 |   1    | DIMM B1 |   OK   | Enabled |
  | node-10 |   1    | DIMM B2 |   OK   | Enabled |
  +---------+--------+---------+--------+---------+


Storage Devices Report
______________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category storage
  +---------+--------+----------------------------------------------------+-------------------------------------------------------+------------------+----------------+----------+----------------+------+----+------------+--------+---------+
  |   HOST  | SYS_ID |                         ID                         |                          NAME                         |      MODEL       |      SIZE      | PROTOCOL |       SN       | SLOT | FW | ENCRYPTION | STATUS |  STATE  |
  +---------+--------+----------------------------------------------------+-------------------------------------------------------+------------------+----------------+----------+----------------+------+----+------------+--------+---------+
  | node-10 |   1    | disk.bay.8:enclosure.internal.0-1:nonraid.slot.2-1 | Disk 8 in Backplane 1 of Storage Controller in Slot 2 | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT99QLL    |  8   |    |            |   OK   | Enabled |
  | node-10 |   1    | disk.bay.0:enclosure.internal.0-1:nonraid.slot.2-1 | Disk 0 in Backplane 0 of Storage Controller in Slot 2 | MZ7L33T8HBNAAD3  | 3840755981824  |   SATA   | S6M5NE0T800539 |  0   |    |            |   OK   | Enabled |
  +---------+--------+----------------------------------------------------+-------------------------------------------------------+------------------+----------------+----------+----------------+------+----+------------+--------+---------+

The ID of a drive is the device to pass to ``ceph orch hardware light``, for
instance to turn on the identify LED of the first drive above:

.. prompt:: bash # auto

  # ceph orch hardware light drive on node-10 disk.bay.8:enclosure.internal.0-1:nonraid.slot.2-1


Processors Report
_________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category processors
  +---------+--------+--------------+--------------------------------------------+-------+---------+--------+---------+
  |   HOST  | SYS_ID |     NAME     |                   MODEL                    | CORES | THREADS | STATUS |  STATE  |
  +---------+--------+--------------+--------------------------------------------+-------+---------+--------+---------+
  | node-10 |   1    | cpu.socket.1 | Intel(R) Xeon(R) Silver 4314 CPU @ 2.40GHz |   16  |    32   |   OK   | Enabled |
  | node-10 |   1    | cpu.socket.2 | Intel(R) Xeon(R) Silver 4314 CPU @ 2.40GHz |   16  |    32   |   OK   | Enabled |
  +---------+--------+--------------+--------------------------------------------+-------+---------+--------+---------+


Power Supplies Report
_____________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category power
  +---------+------------+----+------------+-------------------------+--------------+--------+---------+
  |   HOST  | CHASSIS_ID | ID |    NAME    |          MODEL          | MANUFACTURER | STATUS |  STATE  |
  +---------+------------+----+------------+-------------------------+--------------+--------+---------+
  | node-10 |     1      | 0  | PS1 Status | PWR SPLY,800W,RDNT,LTON |     DELL     |   OK   | Enabled |
  | node-10 |     1      | 1  | PS2 Status | PWR SPLY,800W,RDNT,LTON |     DELL     |   OK   | Enabled |
  +---------+------------+----+------------+-------------------------+--------------+--------+---------+


Fans Report
___________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category fans
  +---------+------------+----+--------------------+---------+-------+--------+---------+
  |   HOST  | CHASSIS_ID | ID |        NAME        | READING | UNITS | STATUS |  STATE  |
  +---------+------------+----+--------------------+---------+-------+--------+---------+
  | node-10 |     1      | 0  | System Board Fan1A |   4800  |  RPM  |   OK   | Enabled |
  | node-10 |     1      | 1  | System Board Fan1B |   4680  |  RPM  |   OK   | Enabled |
  | node-10 |     1      | 2  | System Board Fan2A |   4920  |  RPM  |   OK   | Enabled |
  | node-10 |     1      | 3  | System Board Fan2B |   4560  |  RPM  |   OK   | Enabled |
  +---------+------------+----+--------------------+---------+-------+--------+---------+


Temperature Sensors Report
__________________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category temperatures
  +---------+------------+----+---------------------------+---------+-------+--------+---------+
  |   HOST  | CHASSIS_ID | ID |            NAME           | READING | UNITS | STATUS |  STATE  |
  +---------+------------+----+---------------------------+---------+-------+--------+---------+
  | node-10 |     1      | 0  |  System Board Inlet Temp  |    24   |  Cel  |   OK   | Enabled |
  | node-10 |     1      | 1  |         CPU1 Temp         |    47   |  Cel  |   OK   | Enabled |
  | node-10 |     1      | 2  |         CPU2 Temp         |    45   |  Cel  |   OK   | Enabled |
  | node-10 |     1      | 3  | System Board Exhaust Temp |    38   |  Cel  |   OK   | Enabled |
  +---------+------------+----+---------------------------+---------+-------+--------+---------+


Firmware Details
________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category firmware
  +------------+----------------------------------------------------------------------------+--------------------------------------------------------------+----------------------+-------------+--------+
  |    HOST    |                                 COMPONENT                                  |                             NAME                             |         DATE         |   VERSION   | STATUS |
  +------------+----------------------------------------------------------------------------+--------------------------------------------------------------+----------------------+-------------+--------+
  |   node-10  |               current-107649-7.03__raid.backplane.firmware.0               |                         Backplane 0                          | 2022-12-05T00:00:00Z |     7.03    |   OK   |


  ... omitted output ...


  |   node-10  |               previous-25227-6.10.30.20__idrac.embedded.1-1                |             Integrated Remote Access Controller              |      00:00:00Z       |  6.10.30.20 |   OK   |
  +------------+----------------------------------------------------------------------------+--------------------------------------------------------------+----------------------+-------------+--------+


Hardware Critical Warnings Report
_________________________________

.. prompt:: bash # auto

  # ceph orch hardware status --category criticals
  +---------+--------+-----------+------------+----------+-----------+
  |   HOST  | SYS_ID | COMPONENT |    NAME    |  STATUS  |   STATE   |
  +---------+--------+-----------+------------+----------+-----------+
  | node-10 |   1    |   power   | PS2 Status | critical | unplugged |
  +---------+--------+-----------+------------+----------+-----------+


Network Report
______________

Each network interface of a host: its ports, and the bonds and VLANs on top
of them. ``node-proxy`` reads the standard Redfish ``EthernetInterface`` of
each one and, when the Redfish service links it, its ``Port`` (link,
transceiver, and the switch port it connects to, from LLDP), the port's
``PortMetrics`` (counters and optics) and ``EnvironmentMetrics``:

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category network
  +---------+----------+------------+-----------+----------+---------+-----------------------------+--------------+---------+---------+
  |   HOST  |  SYS_ID  |    NAME    |    TYPE   |   LINK   |  SPEED  |         SWITCH PORT         | RX/TX ERRORS |  STATUS |  STATE  |
  +---------+----------+------------+-----------+----------+---------+-----------------------------+--------------+---------+---------+
  | node-10 | CS1CAN01 | ens51f0np0 |    port   |  LinkUp  |  100000 |  ceph-perf-top et-0/0/0:1   |     0/0      |    OK   | Enabled |
  | node-10 | CS1CAN01 | ens51f1np1 |    port   | LinkDown | unknown | ceph-perf-bottom et-0/0/0:1 |     0/0      | Warning | Enabled |
  | node-10 | CS1CAN01 |   bond0    |    bond   |  LinkUp  |  100000 |                             |              | Warning | Enabled |
  | node-10 | CS1CAN01 |  vlan1305  | vlan 1305 |  LinkUp  |  100000 |                             |              |    OK   | Enabled |
  +---------+----------+------------+-----------+----------+---------+-----------------------------+--------------+---------+---------+

When the Redfish service says what is wrong (``Status.Conditions``), the
``HARDWARE_*`` health warnings say it too:

.. prompt:: bash # auto

  # ceph health detail
  [WRN] HARDWARE_NETWORK: 2 network members are not ok
      [CS1CAN01]: ethernetinterfaces_ens51f1np1 is warning: Enabled: The connection is no longer active for network adapter 'Slot-51' port 'ens51f1np1' function '1'.
      [CS1CAN01]: ethernetinterfaces_bond0 is warning: Enabled: Only 1 of the 2 member interfaces of team 'bond0' are active. Inactive: ens51f1np1 (no link).

``--format json`` gives every detail: addresses, bond members, the LLDP
neighbor, the transceiver and its receive power per lane, counters and
conditions. In the dashboard, a host's page has a *Network ports* table.


Drive Encryption
________________

Self-encrypting drives report their encryption with the standard Redfish
``Drive`` properties ``EncryptionAbility``, ``EncryptionStatus`` and
``SecurityIDOwned``. The ``ENCRYPTION`` column of the storage report shows
``EncryptionStatus``:

* ``Unencrypted``: the drive is not secured;
* ``Unlocked``: it is secured and readable;
* ``Locked``: it is secured and locked, and the hardware can unlock it;
* ``Foreign``: it is locked, and the hardware cannot unlock it on its own.

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category storage
  +---------+----------+-------------+-------------------+-------+----------------+----------+----------+------+----------+------------+---------+---------+
  |   HOST  |  SYS_ID  |      ID     |        NAME       | MODEL |      SIZE      | PROTOCOL |    SN    | SLOT |    FW    | ENCRYPTION |  STATUS |  STATE  |
  +---------+----------+-------------+-------------------+-------+----------------+----------+----------+------+----------+------------+---------+---------+
  | node-10 | CS1CAN01 | driveslot-5 | Enclosure0:Drive5 |  FCM5 | 19200000000000 |   NVMe   | FCM00005 |  5   | 000A5305 |  Unlocked  |    OK   | Enabled |
  | node-10 | CS1CAN01 | driveslot-6 | Enclosure0:Drive6 |  FCM5 | 19200000000000 |   NVMe   | FCM00006 |  6   | 000A5305 |   Locked   | Warning | Enabled |
  +---------+----------+-------------+-------------------+-------+----------------+----------+----------+------+----------+------------+---------+---------+

The drives' conditions say why one stays locked, in ``HARDWARE_STORAGE``:

.. prompt:: bash # auto

  # ceph health detail
  [WRN] HARDWARE_STORAGE: 1 storage member is not ok
      [CS1CAN01]: driveslot-6 is warning: Enabled: The drive is locked and its credential cannot be read from the key manager (connection refused). It is unlocked once the key manager answers.

The dashboard's *Hardware drives* table has *Encryption* and *Problems*
columns.


Diagnostic Data
---------------

Some hardware collects diagnostic data of a drive, such as a drive dump, for
its support team. ``node-proxy`` uses the standard Redfish
``LogService.CollectDiagnosticData`` action, with ``DiagnosticDataType``
``Device`` and the drive as ``TargetDevice``, on the log service of the
drive's chassis. The data stays with the hardware until it is deleted.

Collect the diagnostic data of a drive, with the drive ID that ``ceph orch
hardware status <host> --category storage`` shows, and follow the
collection:

.. prompt:: bash # auto

  # ceph orch hardware diagnostics collect node-10 driveslot-5
  Collecting the diagnostic data of driveslot-5 on node-10: task 1
  Follow it with 'ceph orch hardware diagnostics status node-10 1'
  # ceph orch hardware diagnostics status node-10 1
  Diagnostic data of Enclosure0:Drive5: Completed (OK), 100%
  entries: 03NK797YS344D57S009_20261106035606

List the diagnostic data of all hosts, or of one, then save or delete it:

.. prompt:: bash # auto

  # ceph orch hardware diagnostics ls
  HOST     ID                                  DRIVE        CREATED                    SIZE   NAME
  node-10  03NK797YS344D57S009_20261106035606  driveslot-5  2026-11-06T03:56:06+00:00  1219k  Diagnostic data of Enclosure0:Drive5
  # ceph orch hardware diagnostics get node-10 03NK797YS344D57S009_20261106035606 --base64 | base64 -d > drive.dump
  # ceph orch hardware diagnostics rm node-10 03NK797YS344D57S009_20261106035606

Without ``--base64``, ``get`` prints the data as text, for reading: command
output is not byte exact, so use ``--base64`` (or the dashboard) to save
the data as collected.

In the dashboard, a host's page offers the same: *Collect diagnostic data*
in its *Hardware drives* table, and a *Diagnostic data* table to download
or delete the data.

For Developers
==============

.. py:currentmodule:: cephadm.agent
.. autoclass:: NodeProxyEndpoint
.. automethod:: NodeProxyEndpoint.__init__
.. automethod:: NodeProxyEndpoint.oob
.. automethod:: NodeProxyEndpoint.data
.. automethod:: NodeProxyEndpoint.fullreport
.. automethod:: NodeProxyEndpoint.summary
.. automethod:: NodeProxyEndpoint.criticals
.. automethod:: NodeProxyEndpoint.memory
.. automethod:: NodeProxyEndpoint.storage
.. automethod:: NodeProxyEndpoint.network
.. automethod:: NodeProxyEndpoint.power
.. automethod:: NodeProxyEndpoint.processors
.. automethod:: NodeProxyEndpoint.fans
.. automethod:: NodeProxyEndpoint.temperatures
.. automethod:: NodeProxyEndpoint.firmware
.. automethod:: NodeProxyEndpoint.led

