// The local verification script reserves these exact agent ID formats.
const verificationAgent = /^linux-verify-\d{10}-(?:one|dup|outage|restart|esdown|capacity|rotation|multi|brute|spray|success|benign|telemetry)$/

export function isIntegrationTestHost(host: string) {
  return verificationAgent.test(host)
}

export function incidentProvenance(hosts: readonly string[] = []) {
  const testCount = hosts.filter(isIntegrationTestHost).length
  if (!testCount) return undefined
  return testCount === hosts.length ? 'Integration test' : 'Includes test telemetry'
}
