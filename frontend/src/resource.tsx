import { useEffect, useState, type ReactNode } from 'react'
import { ApiError } from './api'

type State<T> = { status: 'loading' } | { status: 'ready'; data: T } | { status: 'failed'; error: ApiError }

export function useResource<T>(key: string, load: (signal: AbortSignal) => Promise<T>) {
  const [attempt, setAttempt] = useState(0)
  const [result, setResult] = useState<{ key: string; state: State<T> }>()
  const identity = key + '\n' + attempt
  useEffect(() => {
    const controller = new AbortController()
    void load(controller.signal).then(
      data => { if (!controller.signal.aborted) setResult({ key: identity, state: { status: 'ready', data } }) },
      (error: unknown) => {
        if (!controller.signal.aborted) {
          const failure = error instanceof ApiError ? error : new ApiError('error', 'The request failed. Retry to load the data.')
          setResult({ key: identity, state: { status: 'failed', error: failure } })
        }
      },
    )
    return () => controller.abort()
  }, [identity, load])
  const state: State<T> = result?.key === identity ? result.state : { status: 'loading' }
  return { state, retry: () => setAttempt(previous => previous + 1) }
}

export function DataState<T>({ state, retry, children }: { state: State<T>; retry: () => void; children: (data: T) => ReactNode }) {
  if (state.status === 'ready') return children(state.data)
  if (state.status === 'loading') return <div className="state-panel" role="status"><span className="spinner" /> Loading data…</div>
  return <div className="state-panel" role="status">
    <div><strong>{state.error.kind === 'disconnected' ? 'API disconnected' : 'Unable to load data'}</strong><p>{state.error.message}</p></div>
    <button type="button" className="button" onClick={retry}>Retry</button>
  </div>
}

export function Empty({ title, detail }: { title: string; detail: string }) {
  return <div className="state-panel"><div><strong>{title}</strong><p>{detail}</p></div></div>
}
