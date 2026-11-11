import { afterEach, describe, expect, it, vi } from 'vitest'
import { activitySeries, listEndpoint, request } from './api'

afterEach(() => vi.unstubAllGlobals())

describe('Logly API requests', () => {
  it('encodes permitted filters and paginates on the server', () => {
    const url = listEndpoint('/events', new URLSearchParams({ source_ip: '2001:db8::1', username: 'J & J', unrelated: 'omit' }), ['source_ip', 'username'], 2)
    expect(url).toBe('/events?limit=25&offset=25&source_ip=2001%3Adb8%3A%3A1&username=J+%26+J')
  })

  it('preserves a successful empty result', async () => {
    const fetch = vi.fn().mockResolvedValue(Response.json({ total: 0, events: [] }))
    vi.stubGlobal('fetch', fetch)
    await expect(request('/events?limit=25')).resolves.toEqual({ total: 0, events: [] })
    expect(fetch.mock.calls[0][0]).toBe('/api/events?limit=25')
  })

  it.each([502, 503, 504])('treats HTTP %s as disconnected', async status => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status })))
    await expect(request('/overview')).rejects.toMatchObject({ kind: 'disconnected' })
  })

  it('separates a network failure from an HTTP error', async () => {
    const fetch = vi.fn().mockRejectedValueOnce(new TypeError('Network failure')).mockResolvedValueOnce(new Response(null, { status: 500 }))
    vi.stubGlobal('fetch', fetch)
    await expect(request('/overview')).rejects.toMatchObject({ kind: 'disconnected' })
    await expect(request('/overview')).rejects.toMatchObject({ kind: 'error', message: expect.stringContaining('500') })
  })

  it('rejects HTML and incomplete data returned with HTTP 200', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(new Response('<html></html>')).mockResolvedValueOnce(Response.json({ total: 0 }))
    vi.stubGlobal('fetch', fetch)
    await expect(request('/events')).rejects.toMatchObject({ kind: 'error' })
    await expect(request('/events')).rejects.toMatchObject({ kind: 'error' })
  })
})

describe('event activity', () => {
  it('counts real records in UTC buckets and excludes records outside the window', () => {
    const result = activitySeries([
      { timestamp: '2026-11-10T12:00:00Z' },
      { '@timestamp': '2026-11-10T14:00:00Z' },
      { timestamp: '2026-11-11T12:00:00Z' },
      { timestamp: '2026-11-10T11:59:59Z' },
      { timestamp: '2026-11-11T12:00:01Z' },
      { timestamp: 'invalid' },
    ], Date.parse('2026-11-11T12:00:00Z'))
    expect(result.map(bucket => bucket.count)).toEqual([1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
  })
})
