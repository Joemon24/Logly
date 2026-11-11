import { useEffect, useRef } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Link, NavLink, Route, Routes, useLocation } from 'react-router-dom'
import { Activity, LayoutGrid, ScanLine, Server, ShieldAlert } from './icons'
import { HostHistoryPage, HostsPage, IncidentPageView, OverviewPage, RecordsPage } from './pages'
import './styles.css'

const navigation = [
  { to: '/', title: 'Overview', icon: LayoutGrid },
  { to: '/events', title: 'Events', icon: Activity },
  { to: '/incidents', title: 'Incidents', icon: ShieldAlert },
  { to: '/hosts', title: 'Hosts', icon: Server },
]

function Dashboard() {
  const { pathname } = useLocation()
  const main = useRef<HTMLElement>(null)
  const previousPath = useRef(pathname)
  const section = navigation.find(item => item.to !== '/' && pathname.startsWith(item.to))?.title ?? 'Overview'
  useEffect(() => {
    document.title = section + ' | Logly'
  }, [section])
  useEffect(() => {
    if (previousPath.current !== pathname) {
      main.current?.focus({ preventScroll: true })
      window.scrollTo(0, 0)
      previousPath.current = pathname
    }
  }, [pathname])

  return <div className="shell">
    <a className="skip-link" href="#main-content">Skip to content</a>
    <aside className="sidebar">
      <Link className="brand" to="/" aria-label="Logly overview">
        <span className="brand-mark"><ScanLine size={20} aria-hidden="true" /></span>
        <span>Logly</span>
      </Link>
      <nav aria-label="Primary navigation" className="primary-nav">
        {navigation.map(({ to, title, icon: Icon }) => <NavLink key={to} to={to} end={to === '/'}><Icon size={17} strokeWidth={1.65} aria-hidden="true" /><span>{title}</span></NavLink>)}
      </nav>
      <div className="sidebar-footer"><span>Security operations</span><span>v2.0</span></div>
    </aside>
    <div className="workspace">
      <div className="topbar"><span>Operations</span><span className="breadcrumb-divider" aria-hidden="true">/</span><strong>{section}</strong><span className="topbar-time">Timestamps in UTC</span></div>
      <main ref={main} id="main-content" className="main-content" tabIndex={-1}>
        <Routes>
          <Route path="/" element={<OverviewPage />} />
          <Route path="/events" element={<RecordsPage kind="events" />} />
          <Route path="/incidents" element={<RecordsPage kind="incidents" />} />
          <Route path="/incidents/:incidentId" element={<IncidentPageView />} />
          <Route path="/hosts" element={<HostsPage />} />
          <Route path="/hosts/:host" element={<HostHistoryPage />} />
          <Route path="*" element={<div className="state-panel"><div><h1>Page not found</h1><p>Open the overview to continue investigating.</p></div><Link className="button" to="/">Open overview</Link></div>} />
        </Routes>
      </main>
    </div>
  </div>
}

createRoot(document.getElementById('root')!).render(<BrowserRouter><Dashboard /></BrowserRouter>)
