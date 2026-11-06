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
