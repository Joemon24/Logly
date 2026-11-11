import { describe, expect, it } from 'vitest'
import { incidentProvenance, isIntegrationTestHost } from './provenance'

describe('local verification telemetry', () => {
  it('recognizes the exact agent ID reserved by the integration script', () => {
    expect(isIntegrationTestHost('linux-verify-1794372328-esdown')).toBe(true)
    expect(isIntegrationTestHost('linux-verify-1794372328-prod')).toBe(false)
    expect(isIntegrationTestHost('prod-linux-verify-1794372328-esdown')).toBe(false)
    expect(isIntegrationTestHost('linux-verify-backup')).toBe(false)
  })

  it('distinguishes test-only incidents, mixed incidents, and unknown origins', () => {
    const testHost = 'linux-verify-1794372328-esdown'
    expect(incidentProvenance([testHost])).toBe('Integration test')
    expect(incidentProvenance([testHost, 'edge-01'])).toBe('Includes test telemetry')
    expect(incidentProvenance(['edge-01'])).toBeUndefined()
    expect(incidentProvenance([])).toBeUndefined()
  })
})
