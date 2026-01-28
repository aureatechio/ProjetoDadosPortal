import { Navigate, Route, Routes } from 'react-router-dom'
import { Layout } from './layout/Layout'
import { HomePage } from './pages/HomePage'
import { PoliticoResumoPage } from './pages/PoliticoResumoPage'
import { PoliticoNoticiasPage } from './pages/PoliticoNoticiasPage'
import { PoliticoInstagramPage } from './pages/PoliticoInstagramPage'
import { PoliticoMencoesPage } from './pages/PoliticoMencoesPage'
import { PoliticoProcessualPage } from './pages/PoliticoProcessualPage'
import { AdminPage } from './pages/AdminPage'
import { NotFoundPage } from './pages/NotFoundPage'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<HomePage />} />
        <Route path="/politicos/:id" element={<PoliticoResumoPage />} />
        <Route path="/politicos/:id/noticias" element={<PoliticoNoticiasPage />} />
        <Route path="/politicos/:id/instagram" element={<PoliticoInstagramPage />} />
        <Route path="/politicos/:id/mencoes" element={<PoliticoMencoesPage />} />
        <Route path="/politicos/:id/processual" element={<PoliticoProcessualPage />} />
        <Route path="/admin" element={<AdminPage />} />

        <Route path="/home" element={<Navigate to="/" replace />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>
    </Routes>
  )
}
