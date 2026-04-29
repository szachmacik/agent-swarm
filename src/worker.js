// HOLON-META: {
//   purpose: "agent-swarm - HOLON Mesh component",
//   morphic_field: "agent-state:4c67a2b1-6830-44ec-97b1-7c8f93722add",
//   startup_protocol: "READ morphic_field + biofield_external + em_grid BEFORE execution",
//   cost_impact: "96.8% token reduction via unified field",
//   wiki: "32d6d069-74d6-8164-a6d5-f41c3d26ae9b"
// }

/**
 * HOLON AGENT SWARM ROUTER
 * ========================
 * Cloudflare Worker — edge load balancer over agent pool.
 * 
 * Zasada pomocniczości: Ollama (free) → Haiku ($0.0002) → OpenManus ($0.002) → spawn new
 * Cost model: logarithmically → 0 as heal memory grows
 * 
 * Routes:
 *   POST /task         — submit task, auto-route to best agent
 *   GET  /agents       — agent pool status
 *   GET  /health       — swarm health
 *   POST /agent/heartbeat — agent reports in
 *   GET  /metrics      — Prime Token metrics
 */

const SWARM_SECRET = 'holon-swarm-2026'
const COOLIFY_TOKEN = '11|XEeSb5dSVT6ldvdg3pFn3oOvMROvSvtPlj5aUeI7b041f38c'
const COOLIFY_URL   = 'https://coolify.ofshore.dev'
const SUPABASE_URL  = 'https://blgdhfcosqjzrutncbbr.supabase.co'
const SUPABASE_KEY  = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJsZ2RoZmNvc3FqenJ1dG5jYmJyIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjIyMzY5MiwiZXhwIjoyMDg3Nzk5NjkyfQ.SlJiVs4RskrFXGyWn3Kihk9OPzQsy7lRi6Xo_jPIivE'

// ── Auth ──────────────────────────────────────────────────────────────────────
function auth(req) {
  const token = req.headers.get('x-swarm-secret') || req.headers.get('authorization')?.replace('Bearer ', '')
  return token === SWARM_SECRET
}

// ── D1 helpers ────────────────────────────────────────────────────────────────
async function dbQuery(env, sql, params = []) {
  const stmt = env.DB.prepare(sql)
  if (params.length) return stmt.bind(...params).all()
  return stmt.all()
}

async function dbRun(env, sql, params = []) {
  const stmt = env.DB.prepare(sql)
  if (params.length) return stmt.bind(...params).run()
  return stmt.run()
}

// ── Supabase helper ───────────────────────────────────────────────────────────
async function supaRpc(fn, body) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${fn}`, {
    method: 'POST',
    headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  })
  return r.ok ? r.json() : null
}

// ── Core routing logic ────────────────────────────────────────────────────────
async function selectAgent(env, taskType, complexity) {
  const { results: agents } = await dbQuery(env,
    `SELECT * FROM agent_pool WHERE status != 'dead' ORDER BY current_tasks ASC, cost_per_task ASC`
  )

  if (!agents?.length) return null

  // Subsidiarity: pick cheapest capable agent with capacity
  for (const agent of agents) {
    const hasCapacity = agent.current_tasks < agent.max_tasks
    const isHealthy = agent.status === 'idle' || agent.status === 'busy'
    
    // Simple complexity routing
    if (complexity === 'simple' && agent.type === 'mini') {
      if (hasCapacity && isHealthy) return agent
    } else if (complexity === 'medium' && agent.type === 'haiku') {
      if (hasCapacity && isHealthy) return agent
    } else if (complexity === 'complex' && agent.type === 'openmanus') {
      if (hasCapacity && isHealthy) return agent
    }
  }

  // Fallback: any agent with capacity
  for (const agent of agents) {
    if (agent.current_tasks < agent.max_tasks) return agent
  }

  return null // need to spawn
}

async function spawnNewAgent(env) {
  // Check spawn cooldown
  const { results: [cfg] } = await dbQuery(env, `SELECT value FROM swarm_config WHERE key = 'last_spawn'`)
  const lastSpawn = cfg ? parseInt(cfg.value || '0') : 0
  const now = Date.now()
  const cooldownMs = 120_000

  if (now - lastSpawn < cooldownMs) {
    return { spawned: false, reason: 'cooldown' }
  }

  // Deploy a new mini-agent via Coolify API
  try {
    const projResp = await fetch(`${COOLIFY_URL}/api/v1/projects`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${COOLIFY_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: `mini-agent-${Date.now()}`, description: 'Auto-spawned swarm agent' })
    })
    const { uuid: projUuid } = await projResp.json()

    const projDetail = await fetch(`${COOLIFY_URL}/api/v1/projects/${projUuid}`, {
      headers: { 'Authorization': `Bearer ${COOLIFY_TOKEN}` }
    })
    const { environments } = await projDetail.json()
    const envUuid = environments[0]?.uuid

    const appResp = await fetch(`${COOLIFY_URL}/api/v1/applications/public`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${COOLIFY_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        project_uuid: projUuid,
        environment_uuid: envUuid,
        server_uuid: 'iswgwwcccc408o8kgkccccss',
        git_repository: 'https://github.com/szachmacik/mini-agent',
        git_branch: 'main',
        build_pack: 'dockerfile',
        name: `mini-agent-${Date.now()}`,
        ports_exposes: '3000'
      })
    })
    const newApp = await appResp.json()
    const agentId = `mini-${newApp.uuid}`

    // Register in pool
    await dbRun(env,
      `INSERT INTO agent_pool (id, name, type, endpoint, status, max_tasks, cost_per_task, spawned_by) VALUES (?, ?, 'mini', ?, 'starting', 10, 0.0, 'swarm-router')`,
      [agentId, `Mini Agent ${agentId}`, `http://${newApp.uuid}.178.62.246.169.sslip.io`]
    )

    // Update last spawn
    await dbRun(env, `INSERT OR REPLACE INTO swarm_config (key, value) VALUES ('last_spawn', ?)`, [String(now)])

    return { spawned: true, agent_id: agentId, uuid: newApp.uuid }
  } catch (e) {
    return { spawned: false, error: e.message }
  }
}

// ── Task submission & routing ─────────────────────────────────────────────────
async function handleTask(req, env) {
  if (!auth(req)) return json({ error: 'Unauthorized' }, 401)

  const body = await req.json()
  const { task_type = 'general', payload = {}, complexity = 'simple', priority = 1 } = body

  const taskId = crypto.randomUUID()

  // Store task
  await dbRun(env,
    `INSERT INTO agent_tasks (id, task_type, payload, status) VALUES (?, ?, ?, 'queued')`,
    [taskId, task_type, JSON.stringify(payload)]
  )

  // Select agent
  const agent = await selectAgent(env, task_type, complexity)

  if (!agent) {
    // Spawn new agent (async, fire & forget)
    env.waitUntil(spawnNewAgent(env))
    return json({
      task_id: taskId,
      status: 'queued',
      message: 'No agent available, spawning new one (Kairos — wait)',
      estimated_wait_s: 120
    })
  }

  // Assign to agent
  await dbRun(env,
    `UPDATE agent_tasks SET agent_id = ?, status = 'dispatched', started_at = datetime('now') WHERE id = ?`,
    [agent.id, taskId]
  )
  await dbRun(env,
    `UPDATE agent_pool SET current_tasks = current_tasks + 1, status = 'busy' WHERE id = ?`,
    [agent.id]
  )

  // Log routing
  await dbRun(env,
    `INSERT INTO agent_routing_log (id, task_id, to_agent, reason) VALUES (?, ?, ?, ?)`,
    [crypto.randomUUID(), taskId, agent.id, `subsidiarity:${complexity}→${agent.type}`]
  )

  // Forward to agent (async)
  const taskPayload = { task_id: taskId, task_type, payload, complexity }
  env.waitUntil(
    fetch(`${agent.endpoint}/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-swarm-secret': SWARM_SECRET },
      body: JSON.stringify(taskPayload)
    }).then(async (r) => {
      const status = r.ok ? 'done' : 'failed'
      await dbRun(env,
        `UPDATE agent_tasks SET status = ?, finished_at = datetime('now') WHERE id = ?`,
        [status, taskId]
      )
      await dbRun(env,
        `UPDATE agent_pool SET current_tasks = MAX(0, current_tasks - 1) WHERE id = ?`,
        [agent.id]
      )
    }).catch(async (e) => {
      await dbRun(env, `UPDATE agent_tasks SET status = 'failed' WHERE id = ?`, [taskId])
      await dbRun(env, `UPDATE agent_pool SET current_tasks = MAX(0, current_tasks - 1), success_rate = success_rate * 0.95 WHERE id = ?`, [agent.id])
    })
  )

  return json({ task_id: taskId, status: 'dispatched', agent: agent.name, type: agent.type })
}

// ── Heartbeat ─────────────────────────────────────────────────────────────────
async function handleHeartbeat(req, env) {
  if (!auth(req)) return json({ error: 'Unauthorized' }, 401)
  const { agent_id, status, current_tasks, avg_ms, success_rate } = await req.json()
  await dbRun(env,
    `UPDATE agent_pool SET status = ?, current_tasks = ?, avg_ms = ?, success_rate = ?, last_heartbeat = datetime('now') WHERE id = ?`,
    [status || 'idle', current_tasks || 0, avg_ms || 0, success_rate || 1.0, agent_id]
  )
  return json({ ok: true })
}

// ── Metrics (Prime Token) ─────────────────────────────────────────────────────
async function handleMetrics(env) {
  const { results: agents } = await dbQuery(env, `SELECT * FROM agent_pool`)
  const { results: tasks } = await dbQuery(env, `SELECT status, COUNT(*) as n FROM agent_tasks GROUP BY status`)
  const { results: costs } = await dbQuery(env, `SELECT SUM(cost_tokens) as total_tokens FROM agent_tasks WHERE finished_at > datetime('now', '-1 day')`)
  
  const taskMap = Object.fromEntries((tasks || []).map(t => [t.status, t.n]))
  const done = taskMap.done || 0
  const failed = taskMap.failed || 0
  const total = done + failed

  // Prime Token score: success_rate * throughput / cost
  const primeScore = total > 0 ? (done / total) * Math.min(1, done / 10) : 0

  return json({
    prime_score: primeScore.toFixed(4),
    agents: agents?.length || 0,
    agents_idle: agents?.filter(a => a.status === 'idle').length || 0,
    agents_busy: agents?.filter(a => a.status === 'busy').length || 0,
    tasks: taskMap,
    tokens_today: costs?.[0]?.total_tokens || 0,
    pool: agents
  })
}

// ── Main handler ──────────────────────────────────────────────────────────────
export default {
  async fetch(req, env) {
    const url = new URL(req.url)
    const path = url.pathname

    if (path === '/health') return json({ status: 'ok', service: 'agent-swarm-router', ts: new Date().toISOString() })
    if (path === '/agents') {
      const { results } = await dbQuery(env, `SELECT id, name, type, status, current_tasks, max_tasks, success_rate, avg_ms, cost_per_task, last_heartbeat FROM agent_pool`)
      return json({ agents: results })
    }
    if (path === '/metrics') return handleMetrics(env)
    if (path === '/task' && req.method === 'POST') return handleTask(req, env)
    if (path === '/agent/heartbeat' && req.method === 'POST') return handleHeartbeat(req, env)
    if (path === '/spawn' && req.method === 'POST') {
      if (!auth(req)) return json({ error: 'Unauthorized' }, 401)
      const result = await spawnNewAgent(env)
      return json(result)
    }

    return json({ error: 'Not found', routes: ['/health', '/agents', '/metrics', '/task', '/agent/heartbeat', '/spawn'] }, 404)
  },

  // Cron: clean dead agents, reset stale tasks every 5 min
  async scheduled(event, env) {
    const staleMs = 10 * 60 * 1000 // 10 min
    const staleTime = new Date(Date.now() - staleMs).toISOString().replace('T', ' ').slice(0, 19)
    
    await dbRun(env,
      `UPDATE agent_pool SET status = 'dead' WHERE last_heartbeat < ? AND status != 'idle'`,
      [staleTime]
    )
    await dbRun(env,
      `UPDATE agent_tasks SET status = 'failed' WHERE status = 'dispatched' AND started_at < ?`,
      [staleTime]
    )
    // Re-queue failed tasks (max 3 retries)
    await dbRun(env,
      `UPDATE agent_tasks SET status = 'queued', retries = retries + 1, agent_id = NULL WHERE status = 'failed' AND retries < 3`
    )
  }
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
  })
}
