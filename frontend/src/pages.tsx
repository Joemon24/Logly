import { useCallback, useState, type FormEvent, type ReactNode } from 'react'
import { ArrowLeft, ArrowUpRight, RefreshCw, Search } from './icons'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import { ACTIVITY_LIMIT, activitySeries, listEndpoint, request, type EventPage, type Host, type HostHistory, type IncidentDetail, type IncidentPage, type Overview } from './api'
import { DataState, Empty, useResource } from './resource'
import { EventTable, formatCount, formatDate, HostLink, HostList, IncidentTable, label, Pagination, Provenance, Severity, Table, TableDate } from './tables'
import { incidentProvenance, isIntegrationTestHost } from './provenance'

function Heading({ title, description, children }: { title: string; description?: string; children?: ReactNode }) {
  return <header className="page-heading"><div><h1>{title}</h1>{description && <p>{description}</p>}</div>{children}</header>
}

function Section({ title, children, action, className = '' }: { title: string; children: ReactNode; action?: ReactNode; className?: string }) {
  return <section className={'section ' + className}><div className="section-heading"><h2>{title}</h2>{action}</div>{children}</section>
}

const sectionLink = (to: string, text: string) => <Link className="section-link" to={to}>{text}<ArrowUpRight size={14} aria-hidden="true" /></Link>
const severities = ['critical', 'high', 'medium', 'low', 'info']

function Distribution({ stats }: { stats: Overview }) {
  const entries = Object.entries(stats.by_severity).filter(([, count]) => count > 0)
  entries.sort(([a], [b]) => (severities.includes(a) ? severities.indexOf(a) : 99) - (severities.includes(b) ? severities.indexOf(b) : 99))
  if (!entries.length) return <Empty title="No severity data" detail="Classified incidents will appear here." />
  const max = Math.max(...entries.map(([, count]) => count))
  return <ul className="distribution">
    {entries.map(([severity, count]) => <li key={severity}>
      <Severity value={severity} />
      <span className="meter" aria-hidden="true"><span style={{ width: `${100 * count / max}%` }} /></span>
      <strong>{formatCount(count)}</strong>
    </li>)}
  </ul>
}

const hourLabel = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', hourCycle: 'h23', timeZone: 'UTC' })

function Activity() {
  const [end, setEnd] = useState(() => Date.now())
  const load = useCallback((signal: AbortSignal) => request<EventPage>(
    `/events?limit=${ACTIVITY_LIMIT}&since=${encodeURIComponent(new Date(end - 86400000).toISOString())}`, signal,
  ), [end])
  const { state, retry } = useResource(String(end), load)
  return <Section title="Event activity" className="activity-section" action={<div className="section-actions">
    <span className="muted">Past 24h · UTC</span>
    <button type="button" className="icon-button" aria-label="Refresh event activity" onClick={() => setEnd(Date.now())}><RefreshCw size={16} aria-hidden="true" /></button>
  </div>}>
    <DataState state={state} retry={retry}>{data => {
      const buckets = activitySeries(data.events, end)
      const counted = buckets.reduce((total, bucket) => total + bucket.count, 0)
      if (!counted) return <Empty title="No activity in this window" detail="Recorded events will appear in 2-hour intervals." />
      const peak = Math.max(...buckets.map(bucket => bucket.count))
      const tick = Math.ceil(peak / 2)
      const ceiling = tick * 2
      return <>
        <div className="chart-meta"><strong>{formatCount(counted)}</strong><span>events plotted in 2-hour intervals</span></div>
        <div className="chart-frame">
          <div className="chart-scale" aria-hidden="true"><span>{formatCount(ceiling)}</span><span>{formatCount(tick)}</span><span>0</span></div>
          <ol className="chart" aria-label="Event count by two-hour interval, UTC">
            {buckets.map((bucket, index) => {
              const interval = `${hourLabel.format(bucket.start)}–${hourLabel.format(buckets[index + 1]?.start ?? end)} UTC`
              return <li key={bucket.start} title={`${interval}, ${formatCount(bucket.count)} events`}>
                <span className="sr-only">{interval}, {formatCount(bucket.count)} events</span>
                <span className="chart-column" aria-hidden="true"><span style={{ height: `${100 * bucket.count / ceiling}%` }} /></span>
                <span className="chart-time" aria-hidden="true">{hourLabel.format(bucket.start)}</span>
              </li>
            })}
          </ol>
        </div>
        {data.total > data.events.length && <p className="chart-note">Showing the latest {formatCount(data.events.length)} of {formatCount(data.total)} matching records.</p>}
      </>
    }}</DataState>
  </Section>
}

const overviewRequest = (signal: AbortSignal) => request<Overview>('/overview', signal)
const recentRequest = (signal: AbortSignal) => request<IncidentPage>('/incidents?limit=5', signal)

export function OverviewPage() {
  const overview = useResource('overview', overviewRequest)
  const recent = useResource('recent incidents', recentRequest)
  const [refresh, setRefresh] = useState(0)
  return <>
    <Heading title="Overview" description="Monitor telemetry and triage active incidents.">
      <button type="button" className="button" onClick={() => { overview.retry(); recent.retry(); setRefresh(value => value + 1) }}><RefreshCw size={15} aria-hidden="true" /> Refresh overview</button>
    </Heading>
    <DataState {...overview}>{stats => <dl className="metrics" aria-label="Stored telemetry totals">
      {[['Events recorded', stats.events], ['Incidents', stats.incidents], ['Open incidents', stats.open_incidents], ['Recorded hosts', stats.hosts]].map(([name, value]) => <div key={name}><dt>{name}</dt><dd>{formatCount(Number(value))}</dd></div>)}
    </dl>}</DataState>
    <p className="scope-note">Totals include all stored telemetry, including any integration test records.</p>
    <div className="overview-grid">
      <Activity key={refresh} />
      <Section title="Incident distribution">
        <DataState {...overview}>{stats => <Distribution stats={stats} />}</DataState>
      </Section>
    </div>
    <Section title="Recent incidents" action={sectionLink('/incidents', 'View incidents')}>
      <DataState {...recent}>{data => <IncidentTable incidents={data.incidents} name="Recent incidents" />}</DataState>
    </Section>
  </>
}

type Filter = { name: string; label: string; options?: string[]; date?: boolean; example?: string }
const eventFilters: Filter[] = [
  { name: 'source_ip', label: 'Source IP', example: 'e.g. 192.0.2.10' }, { name: 'agent_id', label: 'Host', example: 'e.g. edge-01' },
  { name: 'username', label: 'Username', example: 'e.g. admin' }, { name: 'outcome', label: 'Outcome', options: ['failure', 'success', 'unknown'] },
  { name: 'since', label: 'Since (UTC)', date: true },
]
const incidentFilters: Filter[] = [
  { name: 'source_ip', label: 'Source IP', example: 'e.g. 192.0.2.10' }, { name: 'host', label: 'Host', example: 'e.g. edge-01' },
  { name: 'severity', label: 'Severity', options: severities },
  { name: 'attack_type', label: 'Classification', options: ['ssh_brute_force', 'ssh_password_spraying', 'suspicious_login_success'] },
  { name: 'status', label: 'Status', options: ['open', 'investigating', 'closed'] },
]

function Filters({ fields, params, apply, path }: { fields: Filter[]; params: URLSearchParams; apply: (params: URLSearchParams) => void; path: string }) {
  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const data = new FormData(event.currentTarget)
    const next = new URLSearchParams()
    for (const field of fields) {
      const value = String(data.get(field.name) ?? '').trim()
      if (!value) continue
      const current = params.get(field.name)
      next.set(field.name, field.date ? (current?.slice(0, 10) === value ? current : value + 'T00:00:00Z') : value)
    }
    apply(next)
  }
  const active = fields.filter(field => params.get(field.name)?.trim())
  function removeUrl(name: string) {
    const next = new URLSearchParams(params)
    next.delete(name)
    next.delete('page')
    return path + (next.size ? '?' + next : '')
  }
  return <div className="filter-panel"><form className="filters" key={params.toString()} onSubmit={submit} aria-label="Search filters">
    <div className="filter-fields">{fields.map(field => {
      const current = params.get(field.name) ?? ''
      const options = field.options && current && !field.options.includes(current) ? [...field.options, current] : field.options
      return <label key={field.name}>{field.label}
        {options ? <select name={field.name} defaultValue={current}><option value="">Any {field.label.toLowerCase()}</option>{options.map(option => <option key={option} value={option}>{label(option)}</option>)}</select>
          : <input name={field.name} type={field.date ? 'date' : 'search'} defaultValue={field.date ? current.slice(0, 10) : current} autoComplete="off" spellCheck={false} placeholder={field.example} />}
      </label>
    })}</div>
    <div className="filter-actions"><button type="submit" className="button primary"><Search size={16} aria-hidden="true" /> Apply filters</button><button type="button" className="button quiet" onClick={event => {
      event.currentTarget.form?.reset()
      if (active.length > 0 || params.has('page')) apply(new URLSearchParams())
    }}>Clear all</button></div>
  </form>
    {active.length > 0 && <ul className="active-filters" aria-label="Applied filters">{active.map(field => {
      const value = params.get(field.name) ?? ''
      const display = field.options ? label(value) : field.date ? formatDate(value) : value
      return <li key={field.name}><Link to={removeUrl(field.name)} aria-label={`Remove ${field.label} filter: ${display}`}><span>{field.label}: {display}</span><span aria-hidden="true">×</span></Link></li>
    })}</ul>}
  </div>
}

export function RecordsPage({ kind }: { kind: 'events' | 'incidents' }) {
  const [params, setParams] = useSearchParams()
  const parsed = Number(params.get('page'))
  const page = Number.isSafeInteger(parsed) && parsed > 0 && parsed <= 400 ? parsed : 1
  const fields = kind === 'events' ? eventFilters : incidentFilters
  const endpoint = listEndpoint('/' + kind as '/events' | '/incidents', params, fields.map(field => field.name), page)
  const load = useCallback((signal: AbortSignal) => request<EventPage | IncidentPage>(endpoint, signal), [endpoint])
  const resource = useResource(endpoint, load)
  const path = '/' + kind
  const title = label(kind)
  function pageUrl(number: number) {
    const next = new URLSearchParams(params)
    if (number > 1) next.set('page', String(number))
    else next.delete('page')
    return path + (next.size ? '?' + next : '')
  }
  return <>
    <Heading title={title} description={kind === 'events' ? 'Search collected telemetry by host, user, source and outcome.' : 'Investigate detections and trace their supporting evidence.'} />
    <Filters fields={fields} params={params} apply={setParams} path={path} />
    <Section title={kind === 'events' ? 'Event records' : 'Incident records'}>
      <DataState {...resource}>{data => <>
        {'events' in data ? <EventTable events={data.events} /> : <IncidentTable incidents={data.incidents} />}
        <Pagination total={data.total} shown={'events' in data ? data.events.length : data.incidents.length} page={page} url={pageUrl} />
      </>}</DataState>
    </Section>
  </>
}

function Back({ to, text }: { to: string; text: string }) {
  return <Link className="back-link" to={to}><ArrowLeft size={15} aria-hidden="true" /> {text}</Link>
}

export function IncidentPageView() {
  const { incidentId = '' } = useParams()
  const load = useCallback((signal: AbortSignal) => request<IncidentDetail>('/incidents/' + encodeURIComponent(incidentId), signal), [incidentId])
  const resource = useResource(incidentId, load)
  return <>
    <Back to="/incidents" text="Incidents" />
    {resource.state.status !== 'ready' && <Heading title="Incident details" />}
    <DataState {...resource}>{incident => <>
      <Heading title={label(incident.attack_type)}><div className="heading-status"><Severity value={incident.severity} /><Provenance value={incidentProvenance(incident.hosts)} /></div></Heading>
      <dl className="detail-facts">
        <div><dt>Incident reference</dt><dd translate="no">{incident.incident_id}</dd></div>
        <div><dt>Status</dt><dd>{label(incident.status)}</dd></div>
        <div><dt>Source IP</dt><dd translate="no">{incident.source_ip ?? '—'}</dd></div>
        <div><dt>MITRE technique</dt><dd>{incident.mitre_technique ?? '—'}</dd></div>
        <div><dt>First seen (UTC)</dt><dd>{formatDate(incident.first_seen)}</dd></div>
        <div><dt>Last seen (UTC)</dt><dd>{formatDate(incident.last_seen)}</dd></div>
        <div><dt>Affected hosts</dt><dd><HostList hosts={incident.hosts} /></dd></div>
        <div><dt>Usernames</dt><dd translate="no">{incident.usernames?.length ? incident.usernames.join(', ') : '—'}</dd></div>
      </dl>
      <Section title="Evidence timeline" action={<span className="muted">{formatCount(incident.evidence_event_count)} retrieved</span>}>
        {incident.evidence && <div className="evidence-summary"><span>{formatCount(incident.evidence.failure_count)} failed logins</span><span>{formatCount(incident.evidence.success_count)} successful logins</span></div>}
        <EventTable name="Incident evidence timeline" events={incident.timeline} />
        {incident.missing_event_ids.length > 0 && <details className="missing-evidence"><summary>{formatCount(incident.missing_event_ids.length)} evidence references unavailable</summary><ul>{incident.missing_event_ids.map(id => <li key={id} translate="no">{id}</li>)}</ul></details>}
      </Section>
    </>}</DataState>
  </>
}

const hostsRequest = (signal: AbortSignal) => request<{ hosts: Host[] }>('/hosts?limit=50', signal)

export function HostsPage() {
  const resource = useResource('hosts', hostsRequest)
  return <>
    <Heading title="Hosts" description="Review agent activity and open a host’s investigation history." />
    <Section title="Recorded hosts">
      <DataState {...resource}>{data => data.hosts.length ? <>
        <Table name="Recorded hosts" headers={['Host', { label: 'Recorded events', className: 'col-number' }, 'Last seen (UTC)']} className="hosts-table">
          {data.hosts.map(host => <tr key={host.host}><th scope="row"><HostLink host={host.host} /></th><td className="col-number">{formatCount(host.events)}</td><td><TableDate value={host.last_seen} /></td></tr>)}
        </Table>
        <p className="table-note">{formatCount(data.hosts.length)} hosts shown{data.hosts.length === 50 ? ' · limit 50, ordered by event count' : ''}</p>
      </> : <Empty title="No hosts found" detail="Hosts appear after their first events are collected." />}</DataState>
    </Section>
  </>
}

export function HostHistoryPage() {
  const { host = '' } = useParams()
  const load = useCallback((signal: AbortSignal) => request<HostHistory>('/hosts/' + encodeURIComponent(host) + '?limit=25', signal), [host])
  const resource = useResource(host, load)
  return <>
    <Back to="/hosts" text="Hosts" />
    <Heading title={host} description="Host investigation history."><Provenance value={isIntegrationTestHost(host) ? 'Integration test' : undefined} /></Heading>
    <DataState {...resource}>{data => <>
      <Section title={`Incidents (${formatCount(data.incident_total)})`} action={sectionLink('/incidents?' + new URLSearchParams({ host }), 'All host incidents')}>
        <IncidentTable incidents={data.incidents} name="Host incident history" />
      </Section>
      <Section title={`Recent events (${formatCount(data.event_total)} total)`} action={sectionLink('/events?' + new URLSearchParams({ agent_id: host }), 'All host events')}>
        <EventTable events={data.events} name="Host event history" />
      </Section>
    </>}</DataState>
  </>
}
