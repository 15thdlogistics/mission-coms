/**
 * 15D WINGS — MISSION COMMS
 * Enterprise Notification + Event Bus Worker
 * 
 * Receives events from all workers
 * Routes notifications
 * Generates AI messages
 * Dispatches email
 * Writes dashboard notifications
 * Handles worker inbox
 */

export default {
  async fetch(request, env, ctx) {

    const url = new URL(request.url)

    if (request.method === "POST" && url.pathname === "/emit") {
      return emitEvent(request, env)
    }

    if (request.method === "GET" && url.pathname === "/notifications") {
      return getNotifications(request, env)
    }

    if (request.method === "GET" && url.pathname === "/worker/inbox") {
      return getWorkerInbox(request, env)
    }

    return new Response("Not Found", { status: 404 })
  }
}


/* ================================
   EVENT EMIT ENTRYPOINT
================================ */

async function emitEvent(request, env) {

  const event = await request.json()

  const {
    event: eventType,
    actor_id,
    target_id,
    mission_id,
    metadata = {},
    priority = "normal"
  } = event

  if (!eventType) {
    return json({ error: "event required" }, 400)
  }

  const id = crypto.randomUUID()

  const objId = env.ROUTER.idFromName("notification-router")
  const router = env.ROUTER.get(objId)

  await router.fetch("https://router/dispatch", {
    method: "POST",
    body: JSON.stringify({
      id,
      eventType,
      actor_id,
      target_id,
      mission_id,
      metadata,
      priority
    })
  })

  return json({ ok: true, event_id: id })
}



/* ================================
   DURABLE OBJECT ROUTER
================================ */

export class NotificationRouter {

  constructor(state, env) {
    this.state = state
    this.env = env
  }

  async fetch(request) {

    const url = new URL(request.url)

    if (url.pathname === "/dispatch") {
      return this.dispatch(request)
    }

    return new Response("Not Found", { status: 404 })
  }


  async dispatch(request) {

    const event = await request.json()

    const {
      id,
      eventType,
      actor_id,
      target_id,
      mission_id,
      metadata
    } = event


    const channels = EVENT_MAP[eventType] || []

    let user = null

    if (target_id) {
      user = await this.resolveIdentity(target_id)
    }


    if (channels.includes("dashboard")) {
      await this.writeDashboardNotification(
        user,
        eventType,
        metadata
      )
    }

    if (channels.includes("worker")) {
      await this.writeWorkerInbox(event)
    }

    if (channels.includes("email") && user?.email) {
      await this.sendEmail(
        user.email,
        eventType,
        metadata
      )
    }

    return json({ routed: true })
  }



  /* ================================
     IDENTITY RESOLUTION
  ================================ */

  async resolveIdentity(userId) {

    const user = await this.env.schema.prepare(`
      SELECT email, role_id
      FROM users
      WHERE user_id = ?
    `)
    .bind(userId)
    .first()

    return user
  }



  /* ================================
     DASHBOARD NOTIFICATIONS
  ================================ */

  async writeDashboardNotification(user, eventType, metadata) {

    if (!user) return

    const title = eventType.replace(".", " ")

    const body = JSON.stringify(metadata)

    await this.env.schema.prepare(`
      INSERT INTO notifications
      (id,user_id,title,body,type,created_at)
      VALUES (?,?,?,?,?,datetime('now'))
    `)
    .bind(
      crypto.randomUUID(),
      user.user_id,
      title,
      body,
      eventType
    )
    .run()
  }



  /* ================================
     WORKER INBOX
  ================================ */

  async writeWorkerInbox(event) {

    await this.env.schema.prepare(`
      INSERT INTO worker_inbox
      (id,worker_name,event_type,payload,created_at)
      VALUES (?,?,?,?,datetime('now'))
    `)
    .bind(
      crypto.randomUUID(),
      event.target_id,
      event.eventType,
      JSON.stringify(event)
    )
    .run()
  }



  /* ================================
     EMAIL DISPATCH
  ================================ */

  async sendEmail(email, eventType, metadata) {

    let message = ""

    try {

      const ai = await this.env.AI.run(
        "@cf/meta/llama-3.1-8b-instruct",
        {
          messages: [
            {
              role: "system",
              content:
              "You generate professional system notification emails."
            },
            {
              role: "user",
              content: JSON.stringify({
                eventType,
                metadata
              })
            }
          ]
        }
      )

      message = ai.response

    } catch (err) {

      message = JSON.stringify(metadata)

    }


    await fetch(this.env.GAS_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        to: email,
        subject: eventType,
        body: message
      })
    })

  }

}



/* ================================
   DASHBOARD FETCH
================================ */

async function getNotifications(request, env) {

  const userId =
    new URL(request.url).searchParams.get("user_id")

  const rows = await env.schema.prepare(`
    SELECT *
    FROM notifications
    WHERE user_id = ?
    ORDER BY created_at DESC
    LIMIT 50
  `)
  .bind(userId)
  .all()

  return json(rows.results)
}



/* ================================
   WORKER INBOX FETCH
================================ */

async function getWorkerInbox(request, env) {

  const worker =
    new URL(request.url).searchParams.get("worker")

  const rows = await env.schema.prepare(`
    SELECT *
    FROM worker_inbox
    WHERE worker_name = ?
    ORDER BY created_at DESC
    LIMIT 20
  `)
  .bind(worker)
  .all()

  return json(rows.results)
}



/* ================================
   EVENT REGISTRY
================================ */

const EVENT_MAP = {

  "mission.created": ["dashboard","email"],

  "mission.preapproved": ["dashboard","email"],

  "mission.access.granted": ["email"],

  "wallet.debited": ["dashboard"],

  "kyc.approved": ["dashboard","email"],

  "system.alert": ["dashboard","worker"]

}



/* ================================
   JSON HELPER
================================ */

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json"
    }
  })
}