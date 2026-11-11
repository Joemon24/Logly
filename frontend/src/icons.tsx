import type { SVGProps } from 'react'

type Props = SVGProps<SVGSVGElement> & { size?: number }

function icon(path: string) {
  return function Glyph({ size = 18, ...props }: Props) {
    return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.7} strokeLinecap="round" strokeLinejoin="round" {...props}><path d={path} /></svg>
  }
}

export const Activity = icon('M3 12h4l3-7 4 14 3-7h4')
export const LayoutGrid = icon('M4 4h6v6H4z M14 4h6v6h-6z M4 14h6v6H4z M14 14h6v6h-6z')
export const ScanLine = icon('M8 4H4v4 M16 4h4v4 M4 16v4h4 M20 16v4h-4 M7 12h10')
export const Server = icon('M4 5h16v5H4z M4 14h16v5H4z M7 7.5h.01 M7 16.5h.01')
export const ShieldAlert = icon('M12 3 4 6v5c0 5 3 8 8 10 5-2 8-5 8-10V6z M12 8v4 M12 16h.01')
export const ArrowLeft = icon('M20 12H4m7-7-7 7 7 7')
export const ArrowUpRight = icon('M6 18 18 6M8 6h10v10')
export const ChevronLeft = icon('m15 18-6-6 6-6')
export const ChevronRight = icon('m9 18 6-6-6-6')
export const RefreshCw = icon('M20 7v5h-5 M4 17v-5h5 M6 9a7 7 0 0 1 12-3l2 3 M4 15l2 3a7 7 0 0 0 12-3')
export const Search = icon('M15.5 15.5 21 21 M17 10a7 7 0 1 1-14 0 7 7 0 0 1 14 0z')
