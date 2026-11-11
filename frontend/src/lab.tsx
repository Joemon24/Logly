import { useState } from 'react'
import recordings from './lab-recordings.json'
import { label, Table, TableDate } from './tables'

export function SimulationLab() {
  const [scenarioId, setScenarioId] = useState(recordings[0].id)
  const [step, setStep] = useState(0)
  const scenario = recordings.find(record => record.id === scenarioId) ?? recordings[0]
  const observed = scenario.events.slice(0, step)
  const observedIds = new Set(observed.map(event => event.id))
  const findings = scenario.findings.filter(finding => finding.event_ids.every(id => observedIds.has(id)))
  const complete = step === scenario.events.length
  const evidence = new Set(findings.flatMap(finding => finding.event_ids))

  return <>
    <header className="page-heading">
      <div><h1>Simulation Lab</h1>
        <p className="muted">Recorded results from synthetic fixtures replayed through Logly’s SSH rules. This walkthrough uses no live hosts or backend connection.</p>
      </div>
    </header>
    <section className="investigator" aria-labelledby="scenario-heading">
      <h2 id="scenario-heading">Choose a scenario</h2>
      <label htmlFor="scenario">Recorded scenario</label>
      <div className="investigator-controls">
        <select id="scenario" name="scenario" value={scenarioId} onChange={event => {
          setScenarioId(event.target.value)
          setStep(0)
        }}>
          {recordings.map(record => <option key={record.id} value={record.id}>{record.title}</option>)}
        </select>
        <button className="button" type="button" disabled={complete} onClick={() => setStep(step + 1)}>Show next event</button>
        <button className="button" type="button" disabled={step === 0} onClick={() => setStep(0)}>Reset replay</button>
      </div>
      <p role="status">{step} of {scenario.events.length} fixture events shown. {findings.length} rule matches so far.</p>
      {complete && <p>Expected result: {scenario.expected_rule ? label(scenario.expected_rule) : 'no detection'}.</p>}
    </section>
    <section className="section" aria-labelledby="lab-evidence">
      <div className="section-heading"><h2 id="lab-evidence">Recorded evidence</h2></div>
      {observed.length > 0 ? <Table name="Synthetic event timeline" className="lab-table" headers={['Time (UTC)', 'Outcome', 'User', 'Source IP', 'Event reference']}>
        {observed.map(event => <tr key={event.id}>
          <td><TableDate value={event.time} /></td>
          <td>{label(event.outcome)}</td>
          <td translate="no">{event.user}</td>
          <td translate="no">{event.ip}</td>
          <td><code translate="no">{event.id}</code>{evidence.has(event.id) && <div>Cited evidence</div>}</td>
        </tr>)}
      </Table> : <p className="lab-empty muted">Select Show next event to begin.</p>}
    </section>
    {findings.length > 0 && <section className="investigator" aria-labelledby="lab-findings">
      <h2 id="lab-findings">Rule matches</h2>
      <ul>{findings.map(finding => <li key={finding.rule + finding.timestamp}>
        {label(finding.rule)} · <TableDate value={finding.timestamp} />
        <details><summary>{finding.event_ids.length} cited events</summary>
          <ul>{finding.event_ids.map(id => <li key={id}><code translate="no">{id}</code></li>)}</ul>
        </details>
      </li>)}</ul>
    </section>}
  </>
}
