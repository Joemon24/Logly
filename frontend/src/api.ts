export type LogEvent = {
  event_id?: string
  storage_id?: string
  timestamp?: string
  '@timestamp'?: string
  agent?: { agent_id?: string }
  event?: { category?: string; action?: string; outcome?: string }
  source?: { ip?: string; name?: string; entity_type?: string }
  target?: { name?: string; path?: string }
  collection?: { source_id?: string; path?: string; position?: { record_number?: number } }
  src_ip?: string
  username?: string
  outcome?: string
  severity?: string
  alert_type?: string
  mitre_technique?: string
}

export type Incident = {
  incident_id: string
  attack_type: string
  severity: string
  status: string
  source_ip?: string
  first_seen?: string
  last_seen?: string
  detected_at?: string
  hosts?: string[]
  usernames?: string[]
  mitre_technique?: string
  evidence?: { failure_count: number; success_count: number }
  mitre?: string
  event_ids?: string[]
}

export type IncidentDetail = Incident & {
  timeline: LogEvent[]
  omitted_event_count?: number
  evidence_event_count: number
  missing_event_ids: string[]
}

export type Investigation = {
  incident_id: string
  mode: 'deterministic' | 'ollama' | 'groq'
  topic: string
  rule: string
  reproduced: boolean
  observations: { text: string; event_ids: string[] }[]
  gaps: string[]
  recommendations: string[]
  related_incident_ids: string[]
  host_history: { host: string; event_total: number; incident_total: number }[]
}

export type Overview = {
  events: number
  incidents: number
  open_incidents: number
  hosts: number
  by_severity: Record<string, number>
  by_attack_type: Record<string, number>
}

export type EventPage = { total: number; limit: number; offset: number; events: LogEvent[] }
export type IncidentPage = { total: number; limit: number; offset: number; incidents: Incident[] }
export type Host = { host: string; events: number; last_seen: string | number | null }
export type HostHistory = { host: string; event_total: number; events: LogEvent[]; incident_total: number; incidents: Incident[] }

export class ApiError extends Error {
  readonly kind: 'disconnected' | 'error'

  constructor(kind: 'disconnected' | 'error', message: string) {
    super(message)
    this.kind = kind
  }
}

const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value)
const number = (value: unknown) => typeof value === 'number' && Number.isFinite(value)
const list = (value: unknown) => Array.isArray(value) && value.every(object)
const counts = (value: unknown) => object(value) && Object.values(value).every(number)

function valid(path: string, body: Record<string, unknown>) {
  if (path === '/overview') return ['events', 'incidents', 'open_incidents', 'hosts'].every(key => number(body[key])) && counts(body.by_severity) && counts(body.by_attack_type)
  if (path === '/events') return number(body.total) && list(body.events)
  if (path === '/incidents') return number(body.total) && list(body.incidents)
  if (path.endsWith('/investigate')) {
    const strings = (items: unknown) => Array.isArray(items) && items.every(item => typeof item === 'string')
    return typeof body.incident_id === 'string' && ['deterministic', 'ollama', 'groq'].includes(String(body.mode))
      && typeof body.topic === 'string' && typeof body.rule === 'string' && typeof body.reproduced === 'boolean'
      && strings(body.gaps) && strings(body.recommendations) && strings(body.related_incident_ids)
      && Array.isArray(body.observations) && body.observations.every(item => object(item) && typeof item.text === 'string' && strings(item.event_ids))
      && Array.isArray(body.host_history) && body.host_history.every(item => object(item) && typeof item.host === 'string' && number(item.event_total) && number(item.incident_total))
  }
  if (path.startsWith('/incidents/')) return typeof body.incident_id === 'string' && list(body.timeline) && number(body.evidence_event_count) && Array.isArray(body.missing_event_ids)
  if (path === '/hosts') return list(body.hosts)
  return typeof body.host === 'string' && number(body.event_total) && number(body.incident_total) && list(body.events) && list(body.incidents)
}

export async function request<T>(endpoint: string, signal?: AbortSignal, json?: { question: string }): Promise<T> {
  let response: Response
  try {
    const timeout = AbortSignal.timeout(15000)
    response = await fetch('/api' + endpoint, {
      signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
      headers: { Accept: 'application/json', ...(json && { 'Content-Type': 'application/json' }) },
      ...(json && { method: 'POST', body: JSON.stringify(json) }),
    })
  } catch {
    throw new ApiError('disconnected', 'Could not reach the Logly API. Check the API connection and retry.')
  }
  if ([502, 503, 504].includes(response.status)) {
    throw new ApiError('disconnected', 'The Logly API is unavailable. Check that the service is running and retry.')
  }
  if (!response.ok) {
    throw new ApiError('error', response.status === 404 ? 'This record could not be found.' : `The API returned HTTP ${response.status}. Retry the request.`)
  }
  const body: unknown = await response.json().catch(() => null)
  if (!object(body) || !valid(endpoint.split('?')[0], body)) {
    throw new ApiError('error', 'The API returned an unexpected response. Check the Logly backend version.')
  }
  return body as T
}

export const PAGE_SIZE = 25
export const MAX_PAGES = 10_000 / PAGE_SIZE
export const ACTIVITY_LIMIT = 500

export function listEndpoint(path: '/events' | '/incidents', filters: URLSearchParams, allowed: readonly string[], page: number) {
  const query = new URLSearchParams({ limit: String(PAGE_SIZE), offset: String((page - 1) * PAGE_SIZE) })
  for (const key of allowed) {
    const value = filters.get(key)?.trim()
    if (value) query.set(key, value)
  }
  return path + '?' + query
}

export function eventDate(event: LogEvent) {
  return event.timestamp ?? event['@timestamp']
}

export function activitySeries(events: LogEvent[], end: number) {
  const hour = 3600000
  const start = end - 24 * hour
  const buckets = Array.from({ length: 12 }, (_, i) => ({ start: start + i * 2 * hour, count: 0 }))
  for (const event of events) {
    const time = Date.parse(eventDate(event) ?? '')
    if (time >= start && time <= end) {
      buckets[Math.min(Math.floor((time - start) / (2 * hour)), buckets.length - 1)].count += 1
    }
  }
  return buckets
}
