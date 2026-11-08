export enum HardwareNameMapping {
  memory = 'Memory',
  storage = 'Drive',
  processors = 'CPU',
  network = 'Network',
  power = 'Power supply',
  fans = 'Fan module',
  temperatures = 'Temperature'
}

export interface HardwareHealthCount {
  total: number;
  ok: number;
  warn: number;
  critical: number;
}

export interface HardwareSummary {
  total: {
    category: Record<string, HardwareHealthCount>;
    total: HardwareHealthCount;
  };
  host: Record<string, { flawed: boolean } | number>;
}

export interface CompressionStats {
  fcm_drive_count: number;
  phy_util_percent: number | null;
  log_util_percent: number | null;
  savings_bytes: number | null;
  compression_ratio: number | null;
  efficiency_percent: number | null;
}

export type HardwareHealth = 'OK' | 'Warning' | 'Critical' | 'Unknown';

/* A hardware component of GET /api/hardware/hosts/{hostname}. */
export interface HardwareComponent {
  id: string;
  chassis_id: string;
  health: HardwareHealth;
  /* Sensors (temperatures, fans) */
  name?: string;
  physical_context?: string;
  reading?: number | 'unknown';
  /* Drives (storage) */
  protocol?: string;
  model?: string;
  firmware_version?: string;
  /* Self-encrypting drives: Unencrypted, Unlocked, Locked or Foreign */
  encryption_ability?: string;
  encryption_status?: string;
  security_id_owned?: boolean | 'unknown';
  /* Network interfaces (network): ports, bonds and VLANs */
  speed_mbps?: number | 'unknown';
  link_status?: string;
  interface_type?: string;
  team_mode?: string;
  mtu_size?: number | 'unknown';
  vlan_id?: number | 'unknown';
  related_interfaces?: string[];
  affiliated_interfaces?: string[];
  conditions?: HardwareCondition[];
  location?: string;
  lldp?: { system_name?: string; port_id?: string; chassis_id?: string };
  sfp?: { type?: string; manufacturer?: string; part_number?: string; health?: string };
  rx_errors?: number | 'unknown';
  tx_errors?: number | 'unknown';
  transceiver?: { rx_power_mw?: number[]; tx_power_mw?: number[]; tx_bias_ma?: number[] };
  transceiver_temperature_c?: number | 'unknown';
}

/* What is wrong with a component, as its Redfish Status.Conditions say. */
export interface HardwareCondition {
  message_id?: string;
  message?: string;
  severity?: string;
  timestamp?: string;
}

/* GET /api/hardware/hosts/{hostname}, with the fields the dashboard uses. */
export interface HardwareHostResponse {
  hostname: string;
  status: Record<
    keyof typeof HardwareNameMapping,
    { total: number; components: HardwareComponent[] }
  >;
  /* Installed firmware: name -> version */
  firmware: Record<string, string>;
  /* null when the host has no FCM drives */
  compression: CompressionStats | null;
  /* When node-proxy read the hardware (Unix time): only in the live report */
  read_at?: number;
}

/* Diagnostic data (a drive dump) kept by a host's hardware: GET /api/hardware/diagnostics */
export interface DiagnosticsEntry {
  hostname: string;
  id: string;
  name: string | null;
  /* ISO 8601 */
  created: string | null;
  message: string | null;
  /* bytes */
  size: number | null;
  /* drive id, as in the storage components; null when unknown */
  drive: string | null;
  diagnostic_data_type: string | null;
}

/* A diagnostic data collection: POST /api/hardware/diagnostics, GET .../diagnostics/task */
export interface DiagnosticsTask {
  hostname: string;
  id: string;
  name: string | null;
  /* Redfish TaskState: New, Running, Completed, Exception... */
  state: string | null;
  /* Redfish TaskStatus: OK, Warning or Critical */
  status: string | null;
  percent_complete: number | null;
  messages: string[];
  /* ids of the entries collected */
  entries: string[];
}

/* Redfish TaskStates of a task that has not ended yet */
export const ACTIVE_TASK_STATES = ['New', 'Starting', 'Running', 'Pending', 'Stopping'];
