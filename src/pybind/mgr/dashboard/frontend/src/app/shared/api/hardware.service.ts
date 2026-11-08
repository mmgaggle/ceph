import { HttpClient, HttpResponse } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, map, shareReplay } from 'rxjs/operators';
import { MgrModuleService } from './mgr-module.service';
import { AuthStorageService } from '../services/auth-storage.service';
import {
  CompressionStats,
  DiagnosticsEntry,
  DiagnosticsTask,
  HardwareHostResponse,
  HardwareSummary
} from '../enum/hardware.enum';

const EXPERIMENTAL = { Accept: 'application/vnd.ceph.api.v0.1+json' };

@Injectable({
  providedIn: 'root'
})
export class HardwareService {
  baseURL = 'api/hardware';

  private readonly mgrModuleService = inject(MgrModuleService);
  private readonly authStorageService = inject(AuthStorageService);

  constructor(private http: HttpClient) {}

  /**
   * Shared observable — resolves once and replays.
   * True when cephadm hw_monitoring config flag is enabled.
   * Returns false when user lacks configOpt.read permission
   * or when cephadm is not available.
   */
  readonly isHardwareMonitoringEnabled$: Observable<boolean> =
    this.authStorageService.getPermissions().configOpt?.read
      ? this.mgrModuleService.getConfig('cephadm').pipe(
          map((resp: any) => !!resp?.hw_monitoring),
          catchError(() => of(false)),
          shareReplay({ bufferSize: 1, refCount: false })
        )
      : of(false);

  getSummary(category: string[] = []): Observable<HardwareSummary> {
    return this.http.get<HardwareSummary>(`${this.baseURL}/summary`, {
      params: { categories: category },
      headers: { Accept: 'application/vnd.ceph.api.v0.1+json' }
    });
  }

  getCompression(): Observable<CompressionStats> {
    return this.http.get<CompressionStats>(`${this.baseURL}/compression`, {
      headers: { Accept: 'application/vnd.ceph.api.v0.1+json' }
    });
  }

  /* Hardware report of one host */
  getHost(hostname: string): Observable<HardwareHostResponse> {
    return this.http.get<HardwareHostResponse>(
      `${this.baseURL}/hosts/${encodeURIComponent(hostname)}`
    );
  }

  /*
   * Hardware of one host as its node-proxy last read it, fetched through the
   * manager: the report getHost() returns carries new readings less often.
   */
  getHostLive(hostname: string): Observable<HardwareHostResponse> {
    return this.http.get<HardwareHostResponse>(
      `${this.baseURL}/hosts/${encodeURIComponent(hostname)}/live`,
      { headers: EXPERIMENTAL }
    );
  }

  /* Diagnostic data (drive dumps) the hardware of a host keeps */
  listDiagnostics(hostname: string): Observable<DiagnosticsEntry[]> {
    return this.http.get<DiagnosticsEntry[]>(`${this.baseURL}/diagnostics`, {
      params: { hostname },
      headers: EXPERIMENTAL
    });
  }

  /* Start collecting the diagnostic data of a drive; follow the task it returns. */
  collectDiagnostics(hostname: string, device: string): Observable<DiagnosticsTask> {
    return this.http.post<DiagnosticsTask>(
      `${this.baseURL}/diagnostics`,
      { hostname, device },
      { headers: EXPERIMENTAL }
    );
  }

  getDiagnosticsTask(hostname: string, taskId: string): Observable<DiagnosticsTask> {
    return this.http.get<DiagnosticsTask>(`${this.baseURL}/diagnostics/task`, {
      params: { hostname, task_id: taskId },
      headers: EXPERIMENTAL
    });
  }

  /* The data itself; the file name is in the Content-Disposition header. */
  downloadDiagnostics(hostname: string, entryId: string): Observable<HttpResponse<Blob>> {
    return this.http.get(`${this.baseURL}/diagnostics/download`, {
      params: { hostname, entry_id: entryId },
      responseType: 'blob',
      observe: 'response'
    });
  }

  deleteDiagnostics(hostname: string, entryId: string): Observable<void> {
    return this.http.delete<void>(`${this.baseURL}/diagnostics/${encodeURIComponent(entryId)}`, {
      params: { hostname },
      headers: EXPERIMENTAL
    });
  }
}

/* File name of a download, from its Content-Disposition header */
export function attachmentFilename(response: HttpResponse<unknown>, fallback: string): string {
  const disposition = response.headers.get('Content-Disposition') ?? '';
  const match = /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(disposition);
  return match ? decodeURIComponent(match[1]) : fallback;
}
