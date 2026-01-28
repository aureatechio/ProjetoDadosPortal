const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'
const DEFAULT_TIMEOUT_MS = Number(import.meta.env.VITE_API_TIMEOUT_MS ?? 10000)

export class ApiError extends Error {
  status: number
  body: unknown

  constructor(message: string, status: number, body: unknown) {
    super(message)
    this.status = status
    this.body = body
  }
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const url = `${API_URL}${path.startsWith('/') ? path : `/${path}`}`
  const controller = new AbortController()
  const timeoutMs = Number.isFinite(DEFAULT_TIMEOUT_MS) ? DEFAULT_TIMEOUT_MS : 10000
  const timer = window.setTimeout(() => controller.abort(), timeoutMs)

  let res: Response
  try {
    res = await fetch(url, {
      ...init,
      signal: controller.signal,
      headers: {
        Accept: 'application/json',
        ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
        ...(init?.headers ?? {}),
      },
    })
  } catch (e) {
    const isAbort = e instanceof DOMException && e.name === 'AbortError'
    const msg = isAbort
      ? `Timeout ao chamar API (${timeoutMs}ms): ${url}`
      : `Falha ao conectar na API: ${url}`
    throw new ApiError(msg, 0, e)
  } finally {
    window.clearTimeout(timer)
  }

  const text = await res.text()
  const body = text ? safeJsonParse(text) : null

  if (!res.ok) {
    throw new ApiError(`Erro na API (${res.status}): ${url}`, res.status, body)
  }

  return body as T
}

function safeJsonParse(text: string) {
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

