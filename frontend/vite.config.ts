import { defineConfig, loadEnv } from 'vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), 'LOGLY_')
  const proxy = {
    '/api': { target: env.LOGLY_API_TARGET || 'http://127.0.0.1:8000' },
  }
  return { server: { proxy }, preview: { proxy } }
})
