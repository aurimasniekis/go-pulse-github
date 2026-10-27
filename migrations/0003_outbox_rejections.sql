-- Consecutive attempts the server rejected (JetStream API errors). outbox.poison_attempts applies to this count only,
-- so outage retries, which also bump attempts, never push a publication into quarantine.
ALTER TABLE outbox ADD COLUMN rejections INTEGER NOT NULL DEFAULT 0;
