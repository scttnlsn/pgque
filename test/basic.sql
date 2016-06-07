BEGIN;

DELETE FROM pgque_jobs;

CREATE TEMPORARY TABLE test_results (
  test_name TEXT,
  passed BOOL
);

INSERT INTO test_results
SELECT
  'Basic enqueue',
  COUNT(*) = 1
FROM pgque_enqueue();

INSERT INTO test_results
SELECT
  'Basic enqueue with payload',
  COUNT(*) = 1
FROM pgque_enqueue(payload := '{"foo":123}');

INSERT INTO test_results
SELECT
  'Enqueue with queue name and high priority',
  COUNT(*) = 1
FROM pgque_enqueue(queue := 'foo', priority := 200);

INSERT INTO test_results
SELECT
  'Enqueue with queue name and low priority',
  COUNT(*) = 1
FROM pgque_enqueue(queue := 'foo', priority := 50);

INSERT INTO test_results
SELECT
  'Lock selects lowest priority',
  priority = 50 AND queue = 'foo'
FROM pgque_lock('foo');

INSERT INTO test_results
SELECT
  'Destroy low priority job',
  COUNT(*) = 1
FROM pgque_destroy((SELECT id FROM pgque_jobs WHERE priority = 50 LIMIT 1)::BIGINT);

INSERT INTO test_results
SELECT
  'Three remaining jobs',
  COUNT(*) = 3
FROM pgque_jobs;

INSERT INTO test_results
SELECT
  'Lock selects next lowest priority for given queue',
  priority = 200 AND queue = 'foo'
FROM pgque_lock('foo');

INSERT INTO test_results
SELECT
  'Destroy high priority job',
  COUNT(*) = 1
FROM pgque_destroy((SELECT id FROM pgque_jobs WHERE priority = 200 LIMIT 1)::BIGINT);

INSERT INTO test_results
SELECT
  'Two remaining jobs',
  COUNT(*) = 2
FROM pgque_jobs WHERE priority = 100;

INSERT INTO test_results
SELECT
  'Lock selects job with lower ID',
  priority = 100 AND queue = '' AND payload = '{}'
FROM pgque_lock();

SELECT
  (SELECT COUNT(*) FROM test_results WHERE passed IS true) AS passed,
  (SELECT COUNT(*) FROM test_results WHERE passed IS false) AS failed;

SELECT test_name AS failed FROM test_results WHERE passed IS false;

ROLLBACK;
