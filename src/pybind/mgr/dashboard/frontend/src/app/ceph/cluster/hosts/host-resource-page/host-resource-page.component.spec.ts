import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { HttpHeaders, HttpParams, HttpResponse } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { ActivatedRoute, convertToParamMap, ParamMap } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { saveAs } from 'file-saver';
import { BehaviorSubject, of, throwError } from 'rxjs';

import { HardwareService } from '~/app/shared/api/hardware.service';
import { HostService } from '~/app/shared/api/host.service';
import { DiagnosticsEntry, HardwareHostResponse } from '~/app/shared/enum/hardware.enum';
import { ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DIAGNOSTICS_POLL_MS, HostResourcePageComponent } from './host-resource-page.component';

jest.mock('file-saver', () => ({ saveAs: jest.fn() }));

describe('HostResourcePageComponent', () => {
  let component: HostResourcePageComponent;
  let fixture: ComponentFixture<HostResourcePageComponent>;

  const hostServiceSpy = {
    list: jest.fn(),
    checkHostsFactsAvailable: jest.fn(() => true),
    getTotalMemoryBytes: jest.fn((host?: { memory_total_kb?: number | string }) => {
      const memoryKb = Number(host?.memory_total_kb);
      return Number.isFinite(memoryKb) ? memoryKb * 1024 : undefined;
    }),
    getRawCapacityBytes: jest.fn(
      (host?: { hdd_capacity_bytes?: number | string; flash_capacity_bytes?: number | string }) => {
        const hdd = Number(host?.hdd_capacity_bytes);
        const flash = Number(host?.flash_capacity_bytes);
        return Number.isFinite(hdd) && Number.isFinite(flash) ? hdd + flash : undefined;
      }
    )
  };
  const hardwareEnabled$ = new BehaviorSubject(false);
  const hardwareServiceSpy = {
    isHardwareMonitoringEnabled$: hardwareEnabled$,
    getHost: jest.fn(),
    listDiagnostics: jest.fn(),
    collectDiagnostics: jest.fn(),
    getDiagnosticsTask: jest.fn(),
    downloadDiagnostics: jest.fn(),
    deleteDiagnostics: jest.fn()
  };
  const parentParamMap$ = new BehaviorSubject<ParamMap>(convertToParamMap({ hostname: 'node-1' }));

  configureTestBed({
    declarations: [HostResourcePageComponent],
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
    providers: [
      FormatterService,
      {
        provide: OrchestratorService,
        useValue: { status: jest.fn(() => of({})) }
      },
      {
        provide: ActivatedRoute,
        useValue: {
          parent: { paramMap: parentParamMap$.asObservable() },
          snapshot: { data: { section: 'overview' } }
        }
      },
      {
        provide: HostService,
        useValue: hostServiceSpy
      },
      {
        provide: HardwareService,
        useValue: hardwareServiceSpy
      },
      {
        provide: AuthStorageService,
        useValue: {
          getPermissions: () => new Permissions({ hosts: ['read'], grafana: ['read'] })
        }
      }
    ]
  });

  beforeEach(() => {
    /* Create a fresh component instance for each test */
    fixture = TestBed.createComponent(HostResourcePageComponent);
    component = fixture.componentInstance;

    /* Clear mock call history between tests */
    hostServiceSpy.list.mockClear();
    hostServiceSpy.getTotalMemoryBytes.mockClear();
    hostServiceSpy.getRawCapacityBytes.mockClear();
    hardwareEnabled$.next(false);
    hardwareServiceSpy.getHost.mockReset();
    hardwareServiceSpy.listDiagnostics.mockReset().mockReturnValue(of([]));
    hardwareServiceSpy.collectDiagnostics.mockReset();
    hardwareServiceSpy.getDiagnosticsTask.mockReset();
    hardwareServiceSpy.downloadDiagnostics.mockReset();
    hardwareServiceSpy.deleteDiagnostics.mockReset();
  });

  const getField = (label: string) => {
    return component.hostOverviewFields?.find((field) => field.label === label);
  };

  it('should create', () => {
    hostServiceSpy.list.mockReturnValue(of([]));
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('Overview layout', () => {
    it('should build overview fields from host inventory details', () => {
      hostServiceSpy.list.mockReturnValue(
        of([
          {
            hostname: 'node-1',
            addr: '10.0.0.1',
            labels: ['_admin', 'storage'],
            status: 'maintenance',
            model: 'PowerEdge R750',
            cpu_count: '2',
            cpu_cores: 32,
            memory_total_kb: 1024,
            hdd_capacity_bytes: 1024,
            flash_capacity_bytes: 2048,
            hdd_count: '4',
            flash_count: 2,
            nic_count: '6'
          }
        ])
      );

      fixture.detectChanges();

      expect(hostServiceSpy.list).toHaveBeenCalledWith(expect.any(HttpParams), 'true');
      expect(component.hostname).toBe('node-1');
      expect(component.section).toBe('overview');

      expect(getField('Hostname')).toEqual(expect.objectContaining({ value: 'node-1 (10.0.0.1)' }));
      expect(getField('Labels')).toEqual(
        expect.objectContaining({ values: ['_admin', 'storage'] })
      );
      expect(getField('Status')).toEqual(
        expect.objectContaining({ value: 'Maintenance', status: ICON_TYPE.warning })
      );
      expect(getField('Model')).toEqual(expect.objectContaining({ value: 'PowerEdge R750' }));
      expect(getField('CPUs')).toEqual(expect.objectContaining({ value: '2' }));
      expect(getField('Cores')).toEqual(expect.objectContaining({ value: 32 }));
      expect(getField('Total Memory')?.value).toBe('1 MiB'); // 1024 KB transformed
      expect(getField('Raw Capacity')?.value).toBe('3 KiB'); // 1024 + 2048 Bytes transformed
      expect(getField('HDDs')).toEqual(expect.objectContaining({ value: '4' }));
      expect(getField('Flash')).toEqual(expect.objectContaining({ value: 2 }));
      expect(getField('NICs')).toEqual(expect.objectContaining({ value: '6' }));
    });

    it('should fall back to placeholder overview values when host loading fails', () => {
      hostServiceSpy.list.mockReturnValue(throwError(() => new Error('API Error')));

      fixture.detectChanges();

      expect(getField('Hostname')).toEqual(expect.objectContaining({ value: 'node-1' }));
      expect(getField('Labels')).toEqual(expect.objectContaining({ values: [] }));
      expect(getField('Status')).toEqual(expect.objectContaining({ value: undefined }));
      expect(getField('Model')).toEqual(expect.objectContaining({ value: undefined }));
      expect(getField('Total Memory')).toEqual(expect.objectContaining({ value: '-' }));
      expect(getField('Raw Capacity')).toEqual(expect.objectContaining({ value: '-' }));
    });

    it('should keep capacity values empty if standard fact fields are missing', () => {
      hostServiceSpy.list.mockReturnValue(
        of([
          {
            hostname: 'node-1',
            memory_total_bytes: 2048,
            raw_capacity: 4096
          }
        ])
      );

      fixture.detectChanges();

      expect(getField('Total Memory')?.value).toEqual('-');
      expect(getField('Raw Capacity')?.value).toEqual('-');
    });
  });

  describe('Hardware', () => {
    const sensor = (name: string, physical_context: string, reading: number) => ({
      name,
      physical_context,
      reading
    });
    const drive = (id: string, health: string) => ({ id, chassis_id: '1', health });
    const report = {
      status: {
        storage: { total: 2, components: [drive('nvme0', 'OK'), drive('nvme1', 'Warning')] },
        temperatures: {
          components: [
            sensor('CPU1 Temp', 'CPU', 44),
            sensor('CPU2 Temp', 'CPU', 47.6),
            sensor('Inlet Temp', 'Intake', 24),
            sensor('NVMe 0', 'StorageDevice', 52)
          ]
        }
      },
      firmware: { BMC: '4.86.00', BIOS: '2.14.1', 'Platform firmware': 'v0.50.d57cfd' },
      compression: { fcm_drive_count: 2, savings_bytes: 17.1 * 1024 ** 4, compression_ratio: 4.21 }
    } as unknown as HardwareHostResponse;

    const load = (hardware?: object) => {
      hostServiceSpy.list.mockReturnValue(of([{ hostname: 'node-1' }]));
      if (hardware) {
        hardwareServiceSpy.getHost.mockReturnValue(hardware);
        hardwareEnabled$.next(true);
      }
      fixture.detectChanges();
    };
    const hardwareField = (label: string) =>
      component.hardwareFields().find((field) => field.label === label);
    const text = () => fixture.nativeElement.textContent;

    it('should not fetch or show hardware when hardware monitoring is disabled', () => {
      load();

      expect(hardwareServiceSpy.getHost).not.toHaveBeenCalled();
      expect(text()).not.toContain('Hardware');
    });

    it('should show the hardware of the host in its own card', () => {
      load(of(report));

      expect(hardwareServiceSpy.getHost).toHaveBeenCalledWith('node-1');
      expect(component.hardwareFields().map((f) => [f.label, f.value])).toEqual([
        ['CPU temperature', '48°C'],
        ['Drive temperature', '52°C'],
        ['Drives', 2],
        ['BMC', '4.86.00'],
        ['BIOS', '2.14.1'],
        ['Platform firmware', 'v0.50.d57cfd'],
        ['Savings', '17.1 TiB'],
        ['Compression ratio', '4.21:1']
      ]);
      expect(getField('BMC')).toBeUndefined();
      expect(text()).toContain('Hardware');
    });

    it('should leave out savings when the host has no FCM drives', () => {
      load(of({ ...report, compression: null }));

      expect(hardwareField('BMC')?.value).toBe('4.86.00');
      expect(hardwareField('Savings')).toBeUndefined();
    });

    it('should report errors other than a missing node-proxy report', () => {
      const error = { status: 500, preventDefault: jest.fn() };
      load(throwError(error));

      expect(error.preventDefault).not.toHaveBeenCalled();
      expect(component.hardware()).toBeNull();
    });

    it('should show no hardware and no error without a node-proxy report', () => {
      const error = { status: 404, preventDefault: jest.fn() };
      load(throwError(error));

      expect(error.preventDefault).toHaveBeenCalled();
      expect(getField('Hostname')?.value).toBe('node-1');
      expect(text()).not.toContain('Hardware');
    });

    it('should list the hardware drives below the hardware card', () => {
      load(of(report));

      const sections = fixture.debugElement.queryAll(By.css('section.host-resource-section'));
      expect(sections[1].nativeElement.textContent).toContain('Hardware drives');
      expect(sections[1].nativeElement.textContent).toContain(
        "Drives as reported by the server's BMC: 2 drives."
      );
      expect(component.hardwareDrives().map((d) => d.uid)).toEqual(['1/nvme0', '1/nvme1']);
      expect(sections[1].query(By.css('cd-table'))).toBeTruthy();
    });

    it('should hide the hardware drives when the host reports none', () => {
      load(of({ ...report, status: { ...report.status, storage: { total: 0, components: [] } } }));

      expect(text()).not.toContain('Hardware drives');
    });

    describe('network ports', () => {
      const dropped =
        "The connection is no longer active for network adapter 'Slot-51' port 'ens51f1np1' " +
        "function '1'.";
      const nic = (fields: object) => ({ chassis_id: 'CS1CAN01', health: 'OK', ...fields });
      // as IBM HAL reports a CS1 canister: an 802.3ad bond of two 100G ports, a VLAN on it
      const network = {
        total: 5,
        components: [
          nic({
            id: 'ethernetinterfaces_ens51f0np0',
            name: 'ens51f0np0',
            interface_type: 'Physical',
            link_status: 'LinkUp',
            speed_mbps: 100000,
            affiliated_interfaces: ['bond0'],
            lldp: { system_name: 'ceph-perf-top', port_id: 'et-0/0/0:1' },
            sfp: { type: 'QSFP28' },
            // lanes 3 and 4 are dark
            transceiver: { rx_power_mw: [0.8924, 0.884, 0, 0], tx_bias_ma: [7.0, 6.9, 0, 0] },
            rx_errors: 0,
            tx_errors: 0,
            conditions: []
          }),
          nic({
            id: 'ethernetinterfaces_ens51f1np1',
            name: 'ens51f1np1',
            health: 'Warning',
            interface_type: 'Physical',
            link_status: 'LinkDown',
            speed_mbps: 'unknown',
            affiliated_interfaces: ['bond0'],
            rx_errors: 0,
            tx_errors: 3,
            conditions: [{ message_id: 'NetworkDevice.1.0.ConnectionDropped', message: dropped }]
          }),
          nic({
            id: 'ethernetinterfaces_bond0',
            name: 'bond0',
            interface_type: 'Virtual',
            team_mode: 'IEEE802_3ad',
            related_interfaces: ['ens51f0np0', 'ens51f1np1'],
            link_status: 'LinkUp',
            speed_mbps: 100000
          }),
          nic({
            id: 'ethernetinterfaces_vlan1305',
            name: 'vlan1305',
            interface_type: 'Virtual',
            team_mode: 'None',
            vlan_id: 1305,
            related_interfaces: ['bond0'],
            link_status: 'LinkUp',
            speed_mbps: 100000
          }),
          // a BMC that reports none of the details
          nic({ id: 'NIC.Integrated.1-1-1', name: 'unknown', speed_mbps: 1000 })
        ]
      };

      it('should list the network ports below the hardware drives', () => {
        load(of({ ...report, status: { ...report.status, network } }));

        expect(
          component
            .hardwareNetwork()
            .map((r) => [
              r.interfaceName,
              r.kind,
              r.link,
              r.speed,
              r.neighbor,
              r.optics,
              r.errors,
              r.problems
            ])
        ).toEqual([
          [
            'ens51f0np0',
            'Port in bond0',
            'Up',
            '100 Gb/s',
            'ceph-perf-top et-0/0/0:1',
            'QSFP28 · -0.5, -0.5 dBm',
            '0 / 0',
            '-'
          ],
          ['ens51f1np1', 'Port in bond0', 'Down', '-', '-', '-', '0 / 3', dropped],
          ['bond0', 'Bond of ens51f0np0, ens51f1np1', 'Up', '100 Gb/s', '-', '-', '-', '-'],
          ['vlan1305', 'VLAN 1305 on bond0', 'Up', '100 Gb/s', '-', '-', '-', '-'],
          ['NIC.Integrated.1-1-1', '-', '-', '1 Gb/s', '-', '-', '-', '-']
        ]);
        expect(component.hardwareNetwork()[1].uid).toBe('CS1CAN01/ethernetinterfaces_ens51f1np1');
        const sections = fixture.debugElement.queryAll(By.css('section.host-resource-section'));
        expect(sections[2].nativeElement.textContent).toContain('Network ports');
        expect(sections[2].query(By.css('cd-table'))).toBeTruthy();
      });

      it('should hide the network ports when the host reports none', () => {
        load(of(report));

        expect(text()).not.toContain('Network ports');
      });
    });

    it('should not fetch hardware on the other tabs', () => {
      const route = TestBed.inject(ActivatedRoute);
      route.snapshot.data = { section: 'daemons' };
      try {
        load(of(report));
      } finally {
        route.snapshot.data = { section: 'overview' };
      }

      expect(hardwareServiceSpy.getHost).not.toHaveBeenCalled();
    });
  });

  describe('Diagnostic data', () => {
    const report = {
      hostname: 'node-1',
      status: {
        storage: {
          total: 1,
          components: [{ id: 'driveslot-5', chassis_id: 'C', health: 'OK' }]
        },
        temperatures: { total: 0, components: [] }
      },
      firmware: {},
      compression: null
    } as unknown as HardwareHostResponse;
    const entry: DiagnosticsEntry = {
      hostname: 'node-1',
      id: 'S5_20261106035606',
      name: 'Diagnostic data of Enclosure0:Drive5',
      created: '2026-11-06T03:56:06+00:00',
      message: 'Dump of Enclosure0:Drive5',
      size: 1248895,
      drive: 'driveslot-5',
      diagnostic_data_type: 'Device'
    };
    const task = (state: string, extra: object = {}) => ({
      hostname: 'node-1',
      id: '7',
      name: 'Diagnostic data of Enclosure0:Drive5',
      state,
      status: state === 'Exception' ? 'Critical' : 'OK',
      percent_complete: state === 'Completed' ? 100 : 45,
      messages: [],
      entries: [],
      ...extra
    });
    const select = (row: object) => {
      const selection = new CdTableSelection();
      selection.selected = [row];
      return selection;
    };
    const load = () => {
      hostServiceSpy.list.mockReturnValue(of([{ hostname: 'node-1' }]));
      hardwareServiceSpy.getHost.mockReturnValue(of(report));
      hardwareEnabled$.next(true);
      fixture.detectChanges();
    };
    const text = () => fixture.nativeElement.textContent;
    let notify: jest.SpiedFunction<NotificationService['show']>;

    beforeEach(() => {
      notify = jest.spyOn(TestBed.inject(NotificationService), 'show').mockReturnValue(0);
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    it('should list the diagnostic data kept by the hardware', () => {
      hardwareServiceSpy.listDiagnostics.mockReturnValue(of([entry]));
      load();

      expect(hardwareServiceSpy.listDiagnostics).toHaveBeenCalledWith('node-1');
      expect(text()).toContain('Diagnostic data');
      expect(component.diagnosticsRows()[0].sizeText).toBe('1.2 MiB');
    });

    it('should show no diagnostic data section without data', () => {
      load();

      expect(text()).not.toContain('Diagnostic data');
    });

    it('should not report hardware without diagnostic data support', () => {
      const error = { status: 400, preventDefault: jest.fn() };
      hardwareServiceSpy.listDiagnostics.mockReturnValue(throwError(error));
      load();

      expect(error.preventDefault).toHaveBeenCalled();
      expect(component.diagnostics()).toEqual([]);
    });

    it('should collect the data of the selected drive and follow the collection', () => {
      jest.useFakeTimers();
      load();
      hardwareServiceSpy.collectDiagnostics.mockReturnValue(of(task('New')));
      hardwareServiceSpy.getDiagnosticsTask.mockReturnValue(of(task('Running')));

      component.updateDriveSelection(select({ id: 'driveslot-5' }));
      component.driveActions[0].click();
      expect(hardwareServiceSpy.collectDiagnostics).toHaveBeenCalledWith('node-1', 'driveslot-5');
      jest.advanceTimersByTime(DIAGNOSTICS_POLL_MS);
      fixture.detectChanges();

      expect(text()).toContain('Collecting the diagnostic data of driveslot-5: 45%');
      // one collection per drive at a time
      expect(component.driveActions[0].disable(select({ id: 'driveslot-5' }))).toBeTruthy();

      hardwareServiceSpy.getDiagnosticsTask.mockReturnValue(of(task('Completed')));
      hardwareServiceSpy.listDiagnostics.mockReturnValue(of([entry]));
      jest.advanceTimersByTime(DIAGNOSTICS_POLL_MS);
      fixture.detectChanges();

      expect(component.activeCollections()).toEqual([]);
      expect(component.diagnostics()).toEqual([entry]);
      expect(notify).toHaveBeenLastCalledWith(
        expect.anything(),
        'Diagnostic data of driveslot-5 collected'
      );
    });

    it('should report a failed collection', () => {
      load();
      hardwareServiceSpy.collectDiagnostics.mockReturnValue(
        of(task('Exception', { messages: ['Enclosure0:Drive5: reading the dump failed'] }))
      );

      component.updateDriveSelection(select({ id: 'driveslot-5' }));
      component.collectDiagnostics();

      expect(notify).toHaveBeenLastCalledWith(
        expect.anything(),
        'Failed to collect the diagnostic data of driveslot-5',
        'Enclosure0:Drive5: reading the dump failed'
      );
      expect(component.activeCollections()).toEqual([]);
    });

    it('should download the selected data', () => {
      load();
      hardwareServiceSpy.downloadDiagnostics.mockReturnValue(
        of(
          new HttpResponse({
            body: new Blob(['dump']),
            headers: new HttpHeaders({
              'Content-Disposition': 'attachment; filename="node-1-S5.drive_dump"'
            })
          })
        )
      );

      component.updateDiagnosticsSelection(select(entry));
      component.downloadDiagnostics();

      expect(hardwareServiceSpy.downloadDiagnostics).toHaveBeenCalledWith('node-1', entry.id);
      expect(saveAs).toHaveBeenCalledWith(expect.any(Blob), 'node-1-S5.drive_dump');
    });

    it('should delete the selected data after confirmation', () => {
      load();
      const show = jest.spyOn(TestBed.inject(ModalCdsService), 'show').mockReturnValue(undefined);
      hardwareServiceSpy.deleteDiagnostics.mockReturnValue(of(undefined));

      component.updateDiagnosticsSelection(select(entry));
      component.deleteDiagnostics();
      const options = show.mock.calls[0][1] as { submitActionObservable: () => any };
      hardwareServiceSpy.listDiagnostics.mockClear();
      options.submitActionObservable().subscribe();

      expect(hardwareServiceSpy.deleteDiagnostics).toHaveBeenCalledWith('node-1', entry.id);
      expect(hardwareServiceSpy.listDiagnostics).toHaveBeenCalled();
    });
  });

  describe('Permission-gated template sections', () => {
    it('should show Physical Disks inventory in storage-devices section when hosts.read is allowed', () => {
      hostServiceSpy.list.mockReturnValue(of([]));
      fixture.detectChanges();

      component.section = 'storage-devices';
      component.permissions = new Permissions({ hosts: ['read'], grafana: ['read'] });
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css('cd-inventory'))).toBeTruthy();
    });

    it('should hide Physical Disks inventory in storage-devices section when hosts.read is denied', () => {
      hostServiceSpy.list.mockReturnValue(of([]));
      fixture.detectChanges();

      component.section = 'storage-devices';
      component.permissions = new Permissions({ hosts: [], grafana: ['read'] });
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css('cd-inventory'))).toBeFalsy();
    });

    it('should show daemon list in daemons section when hosts.read is allowed', () => {
      hostServiceSpy.list.mockReturnValue(of([]));
      fixture.detectChanges();

      component.section = 'daemons';
      component.permissions = new Permissions({ hosts: ['read'], grafana: ['read'] });
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css('cd-service-daemon-list'))).toBeTruthy();
    });

    it('should hide daemon list in daemons section when hosts.read is denied', () => {
      hostServiceSpy.list.mockReturnValue(of([]));
      fixture.detectChanges();

      component.section = 'daemons';
      component.permissions = new Permissions({ hosts: [], grafana: ['read'] });
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css('cd-service-daemon-list'))).toBeFalsy();
    });

    it('should show grafana panel in performance section when grafana.read is allowed', () => {
      hostServiceSpy.list.mockReturnValue(of([]));
      fixture.detectChanges();

      component.section = 'performance';
      component.permissions = new Permissions({ hosts: ['read'], grafana: ['read'] });
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css('cd-grafana'))).toBeTruthy();
    });

    it('should hide grafana panel in performance section when grafana.read is denied', () => {
      hostServiceSpy.list.mockReturnValue(of([]));
      fixture.detectChanges();

      component.section = 'performance';
      component.permissions = new Permissions({ hosts: ['read'], grafana: [] });
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css('cd-grafana'))).toBeFalsy();
    });
  });
});
