<template>
  <figure class="pg-flow">
    <svg viewBox="0 0 870 330" role="img" aria-labelledby="pg-flow-title">
      <title id="pg-flow-title">
        Signed GitHub App webhooks reach the pulse-github receiver through a TLS reverse proxy, and the collectors poll
        the GitHub REST API. Both write to SQLite first: the inbox, observations and a per-account outbox. The outbox is
        published to Pulse, NATS JetStream, under evt.github, and a row is complete only after the JetStream
        acknowledgement. Consumers replay and follow the events from Pulse.
      </title>
      <defs>
        <marker id="pg-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
          <path d="M0,0 L10,5 L0,10 z" class="pg-flow-arrowhead" />
        </marker>
      </defs>

      <!-- GitHub -->
      <rect x="10" y="56" width="200" height="70" rx="12" class="pg-flow-box" />
      <text x="110" y="85" class="pg-flow-title">GitHub App</text>
      <text x="110" y="108" class="pg-flow-sub">signed webhooks</text>

      <rect x="10" y="180" width="200" height="70" rx="12" class="pg-flow-box" />
      <text x="110" y="209" class="pg-flow-title">GitHub REST API</text>
      <text x="110" y="232" class="pg-flow-sub">notifications · events · repos</text>

      <!-- service -->
      <rect x="270" y="10" width="340" height="300" rx="14" class="pg-flow-box pg-flow-main" />
      <text x="440" y="40" class="pg-flow-title">pulse-github</text>

      <rect x="290" y="56" width="150" height="70" rx="10" class="pg-flow-box" />
      <text x="365" y="85" class="pg-flow-title">Receiver</text>
      <text x="365" y="108" class="pg-flow-sub">HMAC · 2xx after commit</text>

      <rect x="290" y="170" width="150" height="90" rx="10" class="pg-flow-box" />
      <text x="365" y="199" class="pg-flow-title">Collectors</text>
      <text x="365" y="222" class="pg-flow-sub">notifications · events</text>
      <text x="365" y="242" class="pg-flow-sub">reconcile · backfill</text>

      <rect x="460" y="56" width="130" height="204" rx="10" class="pg-flow-box" />
      <text x="525" y="85" class="pg-flow-title">SQLite</text>
      <text x="525" y="120" class="pg-flow-sub">inbox</text>
      <text x="525" y="145" class="pg-flow-sub">observations</text>
      <text x="525" y="170" class="pg-flow-sub">outbox (FIFO)</text>
      <text x="525" y="195" class="pg-flow-sub">checkpoints</text>
      <text x="525" y="220" class="pg-flow-sub">quarantine</text>

      <text x="440" y="292" class="pg-flow-sub">/healthz · /readyz · /metrics</text>

      <line x1="210" y1="91" x2="286" y2="91" class="pg-flow-line" marker-end="url(#pg-arrow)" />
      <text x="248" y="80" class="pg-flow-label">HTTPS</text>
      <line x1="210" y1="215" x2="286" y2="215" class="pg-flow-line" marker-end="url(#pg-arrow)" />
      <text x="248" y="204" class="pg-flow-label">polling</text>
      <line x1="440" y1="91" x2="456" y2="91" class="pg-flow-line" marker-end="url(#pg-arrow)" />
      <line x1="440" y1="215" x2="456" y2="215" class="pg-flow-line" marker-end="url(#pg-arrow)" />

      <!-- Pulse -->
      <rect x="680" y="100" width="180" height="90" rx="12" class="pg-flow-box" />
      <text x="770" y="128" class="pg-flow-title">Pulse</text>
      <text x="770" y="151" class="pg-flow-sub">NATS JetStream</text>
      <text x="770" y="172" class="pg-flow-sub">EVENTS · evt.github.&gt;</text>
      <line x1="590" y1="145" x2="676" y2="145" class="pg-flow-line" marker-end="url(#pg-arrow)" />
      <text x="634" y="134" class="pg-flow-label">publish</text>
      <text x="634" y="164" class="pg-flow-label">ack</text>

      <rect x="680" y="230" width="180" height="70" rx="12" class="pg-flow-box" />
      <text x="770" y="259" class="pg-flow-title">Your tools</text>
      <text x="770" y="282" class="pg-flow-sub">replay · follow live</text>
      <line x1="770" y1="190" x2="770" y2="226" class="pg-flow-line" marker-end="url(#pg-arrow)" />
    </svg>
  </figure>
</template>

<style scoped>
.pg-flow {
  margin: 24px 0;
}
svg {
  width: 100%;
  height: auto;
  font-family: var(--vp-font-family-base);
}
.pg-flow-box {
  fill: var(--vp-c-bg-soft);
  stroke: var(--vp-c-divider);
  stroke-width: 1.5;
}
.pg-flow-main {
  fill: var(--vp-c-brand-soft);
  stroke: var(--vp-c-brand-1);
  stroke-width: 2;
}
.pg-flow-title {
  text-anchor: middle;
  font-size: 18px;
  font-weight: 600;
  fill: var(--vp-c-text-1);
}
.pg-flow-sub {
  text-anchor: middle;
  font-size: 13px;
  fill: var(--vp-c-text-2);
}
.pg-flow-label {
  text-anchor: middle;
  font-size: 13px;
  fill: var(--vp-c-text-2);
}
.pg-flow-line {
  stroke: var(--vp-c-text-3);
  stroke-width: 2;
}
.pg-flow-arrowhead {
  fill: var(--vp-c-text-3);
}
</style>
