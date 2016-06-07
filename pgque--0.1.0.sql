CREATE TABLE pgque_jobs (
  id BIGSERIAL PRIMARY KEY,
  queue TEXT NOT NULL DEFAULT '',
  priority INTEGER NOT NULL DEFAULT 100,
  payload JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE INDEX pgque_jobs_sort ON pgque_jobs
USING btree (priority, id);

CREATE OR REPLACE FUNCTION pgque_enqueue(
  queue TEXT DEFAULT '',
  priority INTEGER DEFAULT 100,
  payload JSONB DEFAULT '{}'::JSONB)
RETURNS pgque_jobs AS $$
INSERT INTO pgque_jobs (queue, priority, payload)
VALUES ($1, $2, $3)
RETURNING *;
$$ LANGUAGE 'sql';

CREATE OR REPLACE FUNCTION pgque_lock(queue TEXT DEFAULT '')
RETURNS pgque_jobs
AS $$
WITH RECURSIVE jobs AS (
  SELECT (pgque_jobs).*, pg_try_advisory_lock((pgque_jobs).id) AS locked
  FROM (
    SELECT pgque_jobs
    FROM pgque_jobs
    WHERE queue = $1
    ORDER BY priority, id
    LIMIT 1
  ) AS tmp
  UNION ALL (
    SELECT (job).*, pg_try_advisory_lock((job).id) AS locked
    FROM (
      SELECT (
        SELECT pgque_jobs
        FROM pgque_jobs
        WHERE queue = $1
        AND (priority, id) > (jobs.priority, jobs.id)
        ORDER BY priority, id
        LIMIT 1
      ) AS job
      FROM jobs
      WHERE jobs.id IS NOT NULL
      LIMIT 1
    ) AS tmp
  )
)
SELECT id, queue, priority, payload
FROM jobs
WHERE locked
LIMIT 1
$$ LANGUAGE 'sql';

CREATE OR REPLACE FUNCTION pgque_unlock(id BIGINT)
RETURNS BOOL AS $$
SELECT pg_advisory_unlock(id);
$$ LANGUAGE 'sql';

CREATE OR REPLACE FUNCTION pgque_destroy(id BIGINT)
RETURNS VOID AS $$
BEGIN
  DELETE FROM pgque_jobs
  WHERE pgque_jobs.id = $1;
END;
$$ LANGUAGE 'plpgsql';
