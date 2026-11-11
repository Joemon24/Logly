import { Link } from 'react-router-dom'
import { ArrowUpRight, ChevronLeft, ChevronRight } from './icons'
import type { ReactNode } from 'react'
import { eventDate, PAGE_SIZE, type Incident, type LogEvent } from './api'
import { Empty } from './resource'
import { incidentProvenance, isIntegrationTestHost } from './provenance'

const numbers = new Intl.NumberFormat()
const dates = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'medium', timeZone: 'UTC' })
export const formatCount = (value: number) => numbers.format(value)
export const formatDate = (value?: string | number | null) => {
  if (value == null) return '—'
  const date = new Date(value)
  return Number.isNaN(date.valueOf()) ? '—' : dates.format(date)
}

export function label(value?: string) {
  if (!value) return '—'
  const names: Record<string, string> = { ssh_brute_force: 'SSH brute force', ssh_password_spraying: 'SSH password spraying', suspicious_login_success: 'Suspicious login success', ssh_login: 'SSH login' }
  return names[value] ?? value.replace(/[_-]/g, ' ').replace(/^./, character => character.toUpperCase())
}

export function Severity({ value }: { value?: string }) {
  if (!value) return <span className="muted">—</span>
  return <span className="severity" data-level={value}><span className="severity-mark" aria-hidden="true" />{label(value)}</span>
}

type Column = string | { label: string; className: string }

export function Table({ name, headers, children, className }: { name: string; headers: Column[]; children: ReactNode; className?: string }) {
  return <div className="table-scroll" role="region" aria-label={name} tabIndex={0}>
    <table className={className}>
      <caption className="sr-only">{name}</caption>
      <thead><tr>{headers.map(header => {
        const column = typeof header === 'string' ? { label: header, className: undefined } : header
        return <th scope="col" key={column.label} className={column.className}>{column.label}</th>
      })}</tr></thead>
      <tbody>{children}</tbody>
    </table>
  </div>
}

const dayFormat = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeZone: 'UTC' })
const clockFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23', timeZone: 'UTC' })

export function TableDate({ value }: { value?: string | number | null }) {
  if (value == null) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.valueOf())) return '—'
  return <time className="table-date" dateTime={date.toISOString()}><span>{dayFormat.format(date)}</span><span>{clockFormat.format(date)}</span></time>
}

export function Provenance({ value }: { value?: string }) {
  return value ? <span className="provenance">{value}</span> : null
}

export function HostLink({ host }: { host: string }) {
  return <div className="record-identity">
    <Link to={'/hosts/' + encodeURIComponent(host)} translate="no">{host}</Link>
    <Provenance value={isIntegrationTestHost(host) ? 'Integration test' : undefined} />
  </div>
}

export function HostList({ hosts }: { hosts?: string[] }) {
  if (!hosts?.length) return <span className="muted">—</span>
  return <details className="host-disclosure">
    <summary>{formatCount(hosts.length)} {hosts.length === 1 ? 'host' : 'hosts'}</summary>
    <ul aria-label="Affected hosts">{hosts.map(host => <li key={host}><HostLink host={host} /></li>)}</ul>
  </details>
}

const incidentColumns: Column[] = [
  { label: 'Classification', className: 'col-classification' },
  { label: 'Severity', className: 'col-severity' },
  { label: 'Status', className: 'col-status' },
  { label: 'Last seen (UTC)', className: 'col-time' },
  { label: 'Evidence', className: 'col-number' },
  { label: 'Affected hosts', className: 'col-hosts' },
]

export function IncidentTable({ incidents, name = 'Incidents' }: { incidents: Incident[]; name?: string }) {
  if (!incidents.length) return <Empty title="No incidents found" detail="Incidents that match this view will appear here." />
  return <Table name={name} headers={incidentColumns} className="incident-table">
    {incidents.map(incident => <tr key={incident.incident_id}>
      <th scope="row"><div className="record-identity">
        <Link className="record-link" to={'/incidents/' + encodeURIComponent(incident.incident_id)}>{label(incident.attack_type)}<ArrowUpRight aria-hidden="true" size={13} /></Link>
        <Provenance value={incidentProvenance(incident.hosts)} />
      </div></th>
      <td><Severity value={incident.severity} /></td>
      <td>{label(incident.status)}</td>
      <td><TableDate value={incident.last_seen} /></td>
      <td className="col-number">{incident.event_ids ? formatCount(incident.event_ids.length) : '—'}</td>
      <td><HostList hosts={incident.hosts} /></td>
    </tr>)}
  </Table>
}

const eventColumns: Column[] = [
  { label: 'Time (UTC)', className: 'col-time' },
  { label: 'Event', className: 'col-event' },
  { label: 'Host', className: 'col-agent' },
  { label: 'Source IP', className: 'col-ip' },
  'User',
  { label: 'Outcome', className: 'col-status' },
  { label: 'Severity', className: 'col-severity' },
]

export function EventTable({ events, name = 'Events' }: { events: LogEvent[]; name?: string }) {
  if (!events.length) return <Empty title="No events found" detail="Change the filters or check whether your agents are sending events." />
  return <Table name={name} headers={eventColumns} className="event-table">
    {events.map((event, i) => <tr key={event.event_id ?? event.storage_id ?? i}>
      <td><TableDate value={eventDate(event)} /></td>
      <th scope="row"><details className="event-record">
        <summary>{label(event.event?.action ?? event.event?.category ?? event.alert_type)}</summary>
        <dl>
          <dt>Event reference</dt><dd translate="no">{event.event_id ?? event.storage_id ?? '—'}</dd>
          <dt>Collection source</dt><dd translate="no">{event.collection?.path ?? event.collection?.source_id ?? '—'}</dd>
          <dt>Record number</dt><dd>{event.collection?.position?.record_number ?? '—'}</dd>
          <dt>MITRE technique</dt><dd>{event.mitre_technique ?? '—'}</dd>
        </dl>
      </details></th>
      <td>{event.agent?.agent_id ? <HostLink host={event.agent.agent_id} /> : '—'}</td>
      <td translate="no">{event.source?.ip ?? event.src_ip ?? '—'}</td>
      <td translate="no">{event.target?.name ?? event.username ?? '—'}</td>
      <td>{label(event.event?.outcome ?? event.outcome)}</td>
      <td><Severity value={event.severity} /></td>
    </tr>)}
  </Table>
}

export function Pagination({ total, shown, page, url }: { total: number; shown: number; page: number; url: (page: number) => string }) {
  const first = (page - 1) * PAGE_SIZE + 1
  return <div className="pagination">
    <span role="status">{shown ? `${formatCount(first)}–${formatCount(first + shown - 1)} of ${formatCount(total)}` : `0 shown · ${formatCount(total)} matching`}</span>
    <nav aria-label="Pagination">
      {page > 1 && <Link className="button" to={url(page - 1)}><ChevronLeft size={15} aria-hidden="true" /> Previous</Link>}
      {first + shown <= total && shown > 0 && <Link className="button" to={url(page + 1)}>Next <ChevronRight size={15} aria-hidden="true" /></Link>}
    </nav>
  </div>
}
