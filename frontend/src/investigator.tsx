import { useEffect, useRef, useState, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import { request, type Investigation } from './api'

export function Investigator({ incidentId, timelineIds }: { incidentId: string; timelineIds: Set<string> }) {
  const [question, setQuestion] = useState('')
  const [answer, setAnswer] = useState<Investigation>()
  const [pending, setPending] = useState(false)
  const [error, setError] = useState('')
  const active = useRef<AbortController | null>(null)
  useEffect(() => () => active.current?.abort(), [])

  async function investigate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    active.current?.abort()
    const controller = new AbortController()
    active.current = controller
    setPending(true)
    setError('')
    try {
      const result = await request<Investigation>(
        '/incidents/' + encodeURIComponent(incidentId) + '/investigate',
        controller.signal, { question: question.trim() },
      )
      if (!controller.signal.aborted) setAnswer(result)
    } catch (cause) {
      if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : 'Investigation failed.')
    } finally {
      if (!controller.signal.aborted) setPending(false)
    }
  }

  return <section className="investigator" aria-labelledby="investigator-heading" aria-busy={pending}>
    <h2 id="investigator-heading">Investigator</h2>
    <form onSubmit={investigate}>
      <label htmlFor="investigator-question">Follow-up question (optional)</label>
      <div className="investigator-controls">
        <input id="investigator-question" name="question" autoComplete="off" maxLength={400} value={question}
          onChange={event => setQuestion(event.target.value)} placeholder="Which hosts were involved?" />
        <button className="button" type="submit" disabled={pending}>{pending ? 'Reviewing evidence…' : 'Investigate'}</button>
      </div>
    </form>
    {error && <p role="alert">{error}</p>}
    {answer && <div className="investigator-answer" aria-live="polite">
      <p className="muted">{answer.mode === 'ollama' ? 'Ollama selected the topic' : 'Deterministic review'}</p>
      <h3>Detection rule</h3>
      <p>{answer.rule}</p>
      {answer.observations.length > 0 && <><h3>Observed evidence</h3>
        <ul>{answer.observations.map(observation => <li key={observation.text}>{observation.text}
          <details><summary>{observation.event_ids.length} cited {observation.event_ids.length === 1 ? 'event' : 'events'}</summary>
            <ul>{observation.event_ids.map(id => <li key={id}><code translate="no">{timelineIds.has(id) ? <a href={'#evidence-' + id}>{id}</a> : id}</code></li>)}</ul>
          </details>
        </li>)}</ul></>}
      {answer.host_history.length > 0 && <><h3>Host history</h3>
        <ul>{answer.host_history.map(row => <li key={row.host}>
          <Link to={'/hosts/' + encodeURIComponent(row.host)}>{row.host}</Link> · {row.event_total} events · {row.incident_total} incidents
        </li>)}</ul></>}
      {answer.topic === 'related' && <><h3>Related incidents</h3>{answer.related_incident_ids.length === 0
        ? <p>No other incidents were returned for this source IP.</p>
        : <ul>{answer.related_incident_ids.map(id => <li key={id}>
          <Link to={'/incidents/' + encodeURIComponent(id)}>{id}</Link>
        </li>)}</ul>}</>}
      <h3>Evidence gaps</h3><ul>{answer.gaps.map(gap => <li key={gap}>{gap}</li>)}</ul>
      <h3>Suggested review</h3><ul>{answer.recommendations.map(item => <li key={item}>{item}</li>)}</ul>
    </div>}
  </section>
}
