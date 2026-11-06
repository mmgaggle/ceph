import { HttpParams } from '@angular/common/http';
import {
  Component,
  computed,
  inject,
  OnDestroy,
  OnInit,
  signal,
  TemplateRef,
  viewChild
} from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { saveAs } from 'file-saver';
import { Subscription, timer } from 'rxjs';
import { filter, finalize, mergeMap, switchMap, takeWhile, tap } from 'rxjs/operators';

import { attachmentFilename, HardwareService } from '~/app/shared/api/hardware.service';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import {
  ACTIVE_TASK_STATES,
  DiagnosticsEntry,
  DiagnosticsTask,
  HardwareHealth,
  HardwareHostResponse
} from '~/app/shared/enum/hardware.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { HostOverviewDetails, STATUS_MAP, getStatus } from '~/app/shared/models/host.interface';
import { SEVERITY, SeverityIconMap } from '~/app/shared/models/overview';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';

/* How often a diagnostic data collection in progress is checked */
export const DIAGNOSTICS_POLL_MS = 3000;

/* Drive health, with the same severity icons as the overview page. */
const DRIVE_HEALTH: Record<HardwareHealth, { icon: string; label: string }> = {
  OK: { icon: SeverityIconMap[SEVERITY.ok], label: $localize`OK` },
  Warning: { icon: SeverityIconMap[SEVERITY.warn], label: $localize`Warning` },
  Critical: { icon: SeverityIconMap[SEVERITY.err], label: $localize`Critical` },
  Unknown: { icon: 'infoCircle', label: $localize`Unknown` }
};

/* Readings, in °C, of the temperature sensors with this Redfish PhysicalContext, e.g. 'CPU'. */
function temperatures(report: HardwareHostResponse | null, context: string): number[] {
  return (report?.status.temperatures.components ?? [])
    .filter((sensor) => sensor.physical_context === context)
    .map((sensor) => sensor.reading)
    .filter((reading): reading is number => typeof reading === 'number');
}

/* Hottest sensor with this PhysicalContext, e.g. "52°C". */
function maxTemperature(report: HardwareHostResponse, context: string): string | null {
  const readings = temperatures(report, context);
  return readings.length ? `${Math.round(Math.max(...readings))}°C` : null;
}

@Component({
  selector: 'cd-host-resource-page',
  templateUrl: './host-resource-page.component.html',
  standalone: false
})
export class HostResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  hostname = '';
  section = '';
  permissions: Permissions;
  isOverviewLoading = false;
  hostOverviewFields: OverviewField[] = [];

  private readonly hardwareService = inject(HardwareService);
  /* The host's hardware report; null when hw_monitoring is off or the host has no report. */
  readonly hardware = signal<HardwareHostResponse | null>(null);

  readonly hardwareTitle = $localize`Hardware`;
  readonly hardwareFields = computed(() => this.buildHardwareFields(this.hardware()));
  /* Drive ids are only unique within a chassis. */
  readonly hardwareDrives = computed(() =>
    (this.hardware()?.status.storage.components ?? []).map((drive) => ({
      ...drive,
      uid: `${drive.chassis_id}/${drive.id}`
    }))
  );

  private readonly driveHealthTpl = viewChild.required<TemplateRef<unknown>>('driveHealthTpl');
  readonly hardwareDriveColumns = computed<CdTableColumn[]>(() => [
    { name: $localize`Device`, prop: 'id', flexGrow: 2 },
    { name: $localize`Type`, prop: 'protocol', flexGrow: 2 },
    { name: $localize`Model`, prop: 'model', flexGrow: 2 },
    { name: $localize`Firmware`, prop: 'firmware_version', flexGrow: 1 },
    { name: $localize`Health`, prop: 'health', flexGrow: 1, cellTemplate: this.driveHealthTpl() }
  ]);
  readonly driveHealth = DRIVE_HEALTH;

  private readonly notificationService = inject(NotificationService);
  private readonly modalService = inject(ModalCdsService);

  driveSelection = new CdTableSelection();
  readonly driveActions: CdTableAction[] = [
    {
      name: $localize`Collect diagnostic data`,
      permission: 'create',
      icon: Icons.analyse,
      click: () => this.collectDiagnostics(),
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection ||
        (this.collections()[selection.first().id]
          ? $localize`A collection from this drive is in progress`
          : false)
    }
  ];

  /* Diagnostic data (drive dumps) the host's hardware keeps */
  readonly diagnostics = signal<DiagnosticsEntry[]>([]);
  /* Collections in progress, by drive id */
  readonly collections = signal<Record<string, DiagnosticsTask>>({});
  readonly activeCollections = computed(() =>
    Object.entries(this.collections()).map(([drive, task]) => ({ drive, task }))
  );
  readonly diagnosticsRows = computed(() =>
    this.diagnostics().map((entry) => ({
      ...entry,
      sizeText: entry.size === null ? '-' : this.formatter.formatToBinary(entry.size, false, 1)
    }))
  );
  readonly diagnosticsColumns: CdTableColumn[] = [
    {
      name: $localize`Collected`,
      prop: 'created',
      flexGrow: 2,
      cellTransformation: CellTemplate.timeAgo
    },
    { name: $localize`Drive`, prop: 'drive', flexGrow: 2 },
    { name: $localize`Description`, prop: 'message', flexGrow: 5 },
    { name: $localize`Size`, prop: 'sizeText', flexGrow: 1 }
  ];
  diagnosticsSelection = new CdTableSelection();
  readonly diagnosticsActions: CdTableAction[] = [
    {
      name: $localize`Download`,
      permission: 'read',
      icon: Icons.download,
      click: () => this.downloadDiagnostics(),
      disable: (selection: CdTableSelection) => !selection.hasSingleSelection
    },
    {
      name: $localize`Delete`,
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteDiagnostics(),
      disable: (selection: CdTableSelection) => !selection.hasSingleSelection
    }
  ];

  constructor(
    private route: ActivatedRoute,
    private hostService: HostService,
    private orchService: OrchestratorService,
    private authStorageService: AuthStorageService,
    private formatter: FormatterService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? '';
    this.sub.add(
      this.route.parent?.paramMap.subscribe((pm: ParamMap) => {
        this.hostname = pm.get('hostname') ?? '';
        this.loadOverview();
        // Hardware is only shown on the overview tab.
        if (this.section === 'overview') {
          this.loadHardware();
        }
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private loadOverview(): void {
    if (!this.hostname) {
      this.isOverviewLoading = false;
      this.hostOverviewFields = [];
      return;
    }

    let params = new HttpParams();
    params = params.set('offset', '0');
    params = params.set('limit', '1');
    params = params.set('search', this.hostname);
    params = params.set('sort', '+hostname');
    this.isOverviewLoading = true;

    this.sub.add(
      this.orchService
        .status()
        .pipe(
          mergeMap((orchStatus) => {
            const factsAvailable = this.hostService.checkHostsFactsAvailable(orchStatus);
            return this.hostService.list(params, factsAvailable.toString());
          }),
          finalize(() => {
            this.isOverviewLoading = false;
          })
        )
        .subscribe({
          next: (hosts: object[]) => {
            const hostList = (Array.isArray(hosts) ? hosts : []) as HostOverviewDetails[];
            const host = hostList.find((item) => item.hostname === this.hostname);
            this.hostOverviewFields = this.buildOverviewFields(host);
          },
          error: () => {
            this.hostOverviewFields = this.buildOverviewFields();
          }
        })
    );
  }

  private loadHardware(): void {
    this.hardware.set(null);
    this.sub.add(
      this.hardwareService.isHardwareMonitoringEnabled$
        .pipe(
          filter((enabled) => enabled && !!this.hostname),
          switchMap(() => this.hardwareService.getHost(this.hostname))
        )
        .subscribe({
          next: (report) => {
            this.hardware.set(report);
            this.loadDiagnostics();
          },
          error: (error) => {
            // A host without node-proxy data is expected: show no hardware, not an error.
            if (error?.status === 404) {
              error.preventDefault?.();
            }
          }
        })
    );
  }

  updateDriveSelection(selection: CdTableSelection): void {
    this.driveSelection = selection;
  }

  updateDiagnosticsSelection(selection: CdTableSelection): void {
    this.diagnosticsSelection = selection;
  }

  loadDiagnostics(): void {
    this.sub.add(
      this.hardwareService.listDiagnostics(this.hostname).subscribe({
        next: (entries) => this.diagnostics.set(entries),
        error: (error) => {
          // Not every server's hardware keeps diagnostic data: show none, not an error.
          error?.preventDefault?.();
          this.diagnostics.set([]);
        }
      })
    );
  }

  collectDiagnostics(): void {
    const drive: string | undefined = this.driveSelection.first()?.id;
    if (!drive) {
      return;
    }
    this.sub.add(
      this.hardwareService.collectDiagnostics(this.hostname, drive).subscribe((task) => {
        this.notificationService.show(
          NotificationType.info,
          $localize`Collecting the diagnostic data of ${drive}`
        );
        this.followCollection(drive, task);
      })
    );
  }

  /* Poll a collection until it ends, then report it and list its data. */
  private followCollection(drive: string, task: DiagnosticsTask): void {
    const active = (t: DiagnosticsTask) => ACTIVE_TASK_STATES.includes(t.state ?? '');
    if (!active(task)) {
      this.collectionEnded(drive, task);
      return;
    }
    this.collections.update((tasks) => ({ ...tasks, [drive]: task }));
    this.sub.add(
      timer(DIAGNOSTICS_POLL_MS, DIAGNOSTICS_POLL_MS)
        .pipe(
          switchMap(() => this.hardwareService.getDiagnosticsTask(this.hostname, task.id)),
          takeWhile((current) => active(current), true)
        )
        .subscribe({
          next: (current) => {
            if (active(current)) {
              this.collections.update((tasks) => ({ ...tasks, [drive]: current }));
            } else {
              this.collectionEnded(drive, current);
            }
          },
          error: () => this.forgetCollection(drive)
        })
    );
  }

  private forgetCollection(drive: string): void {
    this.collections.update(({ [drive]: _ended, ...others }) => others);
  }

  private collectionEnded(drive: string, task: DiagnosticsTask): void {
    this.forgetCollection(drive);
    if (task.state === 'Completed') {
      this.notificationService.show(
        NotificationType.success,
        $localize`Diagnostic data of ${drive} collected`
      );
    } else {
      this.notificationService.show(
        NotificationType.error,
        $localize`Failed to collect the diagnostic data of ${drive}`,
        task.messages?.[task.messages.length - 1] ?? task.state ?? ''
      );
    }
    // a failed collection may still have kept a dump found on the drive
    this.loadDiagnostics();
  }

  downloadDiagnostics(): void {
    const entry: DiagnosticsEntry | undefined = this.diagnosticsSelection.first();
    if (!entry) {
      return;
    }
    this.sub.add(
      this.hardwareService.downloadDiagnostics(this.hostname, entry.id).subscribe((response) => {
        saveAs(
          response.body as Blob,
          attachmentFilename(response, `${this.hostname}-${entry.id}.drive_dump`)
        );
      })
    );
  }

  deleteDiagnostics(): void {
    const entry: DiagnosticsEntry | undefined = this.diagnosticsSelection.first();
    if (!entry) {
      return;
    }
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`diagnostic data`,
      itemNames: [entry.name ?? entry.id],
      submitActionObservable: () =>
        this.hardwareService
          .deleteDiagnostics(this.hostname, entry.id)
          .pipe(tap(() => this.loadDiagnostics()))
    });
  }

  private buildHardwareFields(report: HardwareHostResponse | null): OverviewField[] {
    if (!report) {
      return [];
    }
    const fields: OverviewField[] = [
      { label: $localize`CPU temperature`, value: maxTemperature(report, 'CPU') },
      { label: $localize`Drive temperature`, value: maxTemperature(report, 'StorageDevice') },
      { label: $localize`Drives`, value: report.status.storage.total },
      // Installed firmware, under the names the BMC reports, e.g. BIOS or iDRAC.
      ...Object.entries(report.firmware ?? {}).map(([name, version]) => ({
        label: name,
        value: version
      }))
    ];
    const compression = report.compression;
    if (compression) {
      const savings = compression.savings_bytes;
      const ratio = compression.compression_ratio;
      // Both are null until the FCM drives hold enough data.
      const helperText =
        ratio === null
          ? $localize`Needs at least 100 GiB of data on the FCM drives to be shown.`
          : undefined;
      fields.push(
        {
          label: $localize`Savings`,
          value: savings === null ? null : this.formatter.formatToBinary(savings, false, 1),
          helperText
        },
        {
          label: $localize`Compression ratio`,
          value: ratio === null ? null : `${ratio}:1`,
          helperText
        }
      );
    }
    return fields;
  }

  private buildOverviewFields(host: Partial<HostOverviewDetails> = {}): OverviewField[] {
    const hostStatus = STATUS_MAP[getStatus(host)];
    const hostnameWithAddr = host?.addr ? `${this.hostname} (${host.addr})` : this.hostname;
    const totalMemory = this.formatter.formatToBinary(
      this.hostService.getTotalMemoryBytes(host),
      false,
      1
    );
    const rawCapacity = this.formatter.formatToBinary(
      this.hostService.getRawCapacityBytes(host),
      false,
      1
    );

    return [
      {
        label: $localize`Hostname`,
        value: hostnameWithAddr,
        type: 'text'
      },
      {
        label: $localize`Labels`,
        values: host?.labels ?? [],
        type: 'tags'
      },
      {
        label: $localize`Status`,
        value: hostStatus?.label,
        type: 'status',
        status: hostStatus?.icon
      },
      {
        label: $localize`Model`,
        value: host?.model,
        type: 'text'
      },
      {
        label: $localize`CPUs`,
        value: host?.cpu_count,
        type: 'text'
      },
      {
        label: $localize`Cores`,
        value: host?.cpu_cores,
        type: 'text'
      },
      {
        label: $localize`Total Memory`,
        value: totalMemory
      },
      {
        label: $localize`Raw Capacity`,
        value: rawCapacity
      },
      {
        label: $localize`HDDs`,
        value: host?.hdd_count,
        type: 'text'
      },
      {
        label: $localize`Flash`,
        value: host?.flash_count,
        type: 'text'
      },
      {
        label: $localize`NICs`,
        value: host?.nic_count,
        type: 'text'
      }
    ];
  }
}
