import { createRoot } from 'react-dom/client'
import { SimulationLab } from './lab'
import './styles.css'

createRoot(document.getElementById('main-content')!).render(<SimulationLab />)
