-- Accounts each observation was queued for. Outbox rows are swept after publication (7 days by default) but
-- observations live longer (35 days) for deduplication; this table remembers the routing for as long as the
-- observation exists, so re-reading an old item never queues it for the same account again.
CREATE TABLE observation_routes (
    observation_id TEXT NOT NULL REFERENCES observations (observation_id) ON DELETE CASCADE,
    account_key    TEXT NOT NULL,
    PRIMARY KEY (observation_id, account_key)
) WITHOUT ROWID;

INSERT INTO observation_routes (observation_id, account_key)
SELECT DISTINCT o.observation_id, o.account_key FROM outbox o
WHERE EXISTS (SELECT 1 FROM observations s WHERE s.observation_id = o.observation_id);
