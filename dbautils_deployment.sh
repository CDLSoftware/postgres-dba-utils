#!/bin/bash
#File: dbautils_deployment.sh
#Desc:
#
# Amendment History:
# Date:      Who:           Desc:
# 08/04/22   T.Mullen       Initial;


LOGS=/tmp
LOG=$LOGS/$(basename $0 .sh)_$(date +%Y%m%d_%H%M%S).log
export HOST=$1
export DBNAME=$2
export SCHEMA=$3
export PASS=$4

if [ $# -lt 4 ]; then
  echo "Usage: $0  hostname, dbname, schema, password"
  exit 1
fi

if [ ! -d "$LOGS" ]; then
  mkdir $LOGS
  echo "$(date) Created logfile directory $LOGS" | tee -a $LOG
fi


echo "$(date) DBA-Utils install starting on host - $HOST" | tee -a $LOG
echo "$(date) Logfile for run $LOG" | tee -a $LOG

#Get variables for each instance.
PSQL="psql -a -b -e --host=${HOST} --dbname=${DBNAME} --username=${SCHEMA}"
export PGPASSWORD=$PASS
$PSQL <<EOF >>$LOG 2>&1
CREATE SCHEMA IF NOT EXISTS ${SCHEMA};

DROP FUNCTION IF EXISTS ${SCHEMA}.dbautils;
CREATE OR REPLACE FUNCTION ${SCHEMA}.dbautils()
  RETURNS void
AS
\$\$
DECLARE
  DECLARE currentdb name;
  DECLARE dbversion integer;
BEGIN
  --get the current database name and version
  SELECT current_database() INTO currentdb;
  SELECT current_setting('server_version_num') INTO dbversion;

  --common views for all databases
  RAISE NOTICE '==================================== Postgres utility views =================================';
  RAISE NOTICE '======================================== Sessions ===========================================';
  RAISE NOTICE 'select * from sessions;                    - show all sessions in the db order by usename (exclude your connected session)';
  RAISE NOTICE 'select * from active_sessions;             - active sessions in the instance (exclude your connected session)';
  RAISE NOTICE 'select * from non_idle_sessions;           - non idle sessions in the instance (exclude your connected session)';
  RAISE NOTICE 'select * from blocking_detailed;           - detailed blocking session info';
  RAISE NOTICE 'select * from blocking_simple;             - simple blocking session info';
  RAISE NOTICE 'select pg_terminate_backend(pid);          - kill session running against passed pid (process id)';
  RAISE NOTICE '======================================== Performance ========================================';
  RAISE NOTICE 'select * from top_pg_stat_statements;      - top consuming sql ';
  RAISE NOTICE 'select * from pg_stat_statements;          - detailed activity stats ';
  RAISE NOTICE 'select * from top_sql;                     - top consuming sql ';
  RAISE NOTICE 'select * from user_consumers;              - users whose used more time in each database ';
  RAISE NOTICE '======================================== Storage ============================================';
  RAISE NOTICE 'select * from db_size;                     - database sizes ';
  RAISE NOTICE 'select * from schema_size;                 - schema sizes ';
  RAISE NOTICE 'select * from total_table_size;            - total tables size: including TOAST and indexes segments ';
  RAISE NOTICE 'select * from segment_size;                - size of database objects ';
  RAISE NOTICE 'select * from transaction_wrap_around;     - transaction wraparound counts ';
  RAISE NOTICE 'select * from replication_slot_details;    - shows details of replication slots in use ';
  RAISE NOTICE 'select * from drop_replication_slot;       - shows syntax of how to drop a replication slot ';
  RAISE NOTICE '======================================== Vacuum =============================================';
  RAISE NOTICE 'select * from active_vacuum;               - active vacuum in the db ';
  RAISE NOTICE 'select * from auto_vacuum;                 - auto vacuum details on objects ';
  RAISE NOTICE 'select * from vacuum_problems;             - lists any objects with vacuum issues ';
  RAISE NOTICE 'select * from vacuum_progress;             - show vacuum progress ';
  RAISE NOTICE '======================================== Host ===============================================';
  RAISE NOTICE 'select * from cluster_startup;             - shows the cluster startup time ';

  IF (currentdb = 'postgres') THEN
    --POSTGRES DB VIEWS GO HERE!!!

    --check if database version is >= 12.5
    IF (dbversion >= 120005) THEN
      RAISE NOTICE '======================================== Pg_cron ============================================';
      RAISE NOTICE 'select * from pg_cron_scheduled_jobs;      - displays pg_cron scheduled jobs ';
      RAISE NOTICE 'select * from pg_cron_show_failed_jobs;    - displays pg_cron failed jobs for the last week ';
      RAISE NOTICE 'select * from pg_cron_show_last_day;       - displays pg_cron jobs ran in the last day ';
    END IF;

  END IF;

  RAISE NOTICE '=============================================================================================';
END;
\$\$ LANGUAGE plpgsql;

--== Sessions ==
DROP VIEW IF EXISTS ${SCHEMA}.sessions;
create or replace view ${SCHEMA}.sessions as
  SELECT pg_stat_activity.datname,
      pg_stat_activity.pid,
      pg_stat_activity.usename,
      pg_stat_activity.application_name,
      pg_stat_activity.backend_type,
      pg_stat_activity.client_addr,
      pg_stat_activity.query_start,
      pg_stat_activity.state_change,
      pg_stat_activity.state,
      pg_stat_activity.query
  FROM pg_stat_activity
  WHERE pg_backend_pid()<>pid
  ORDER BY pg_stat_activity.usename;

DROP VIEW IF EXISTS ${SCHEMA}.active_sessions;
create or replace view ${SCHEMA}.active_sessions as
  select datname, pid, usename, application_name, client_addr, query_start, state_change, state, query
  from pg_stat_activity
  where state='active' and pg_backend_pid()<>pid;

DROP VIEW IF EXISTS ${SCHEMA}.non_idle_sessions;
create or replace view ${SCHEMA}.non_idle_sessions as
  select datname, pid, usename, application_name, client_addr, query_start, state_change, state, query
  from pg_stat_activity
  where state<>'idle' and pg_backend_pid()<>pid;

DROP VIEW IF EXISTS ${SCHEMA}.blocking_detailed;
create or replace view ${SCHEMA}.blocking_detailed as
  SELECT blocked_locks.pid     AS blocked_pid,
         blocked_activity.usename  AS blocked_user,
         blocking_locks.pid     AS blocking_pid,
         blocking_activity.usename AS blocking_user,
         blocked_activity.query    AS blocked_statement,
         blocking_activity.query   AS current_statement_in_blocking_process
   FROM  pg_catalog.pg_locks         blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks         blocking_locks
        ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
    JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
  WHERE NOT blocked_locks.GRANTED;

DROP VIEW IF EXISTS ${SCHEMA}.blocking_simple;
create or replace view ${SCHEMA}.blocking_simple as
  SELECT * FROM pg_locks WHERE NOT GRANTED;


--== Performance ==
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

DROP VIEW IF EXISTS ${SCHEMA}.top_pg_stat_statements;
create or replace view ${SCHEMA}.top_pg_stat_statements as
  SELECT rolname, round(total_time::numeric, 2) AS total_time, calls, round(mean_time::numeric, 2) AS mean, round((100 * total_time / sum(total_time::numeric) OVER ())::numeric, 2) AS percentage_cpu, query
  FROM pg_stat_statements JOIN pg_roles r ON r.oid = userid
  ORDER BY total_time DESC
  LIMIT 20;

DROP VIEW IF EXISTS ${SCHEMA}.top_sql;
create or replace view ${SCHEMA}.top_sql as
  SELECT rolname, substring(query, 1, 50) AS short_query, round(total_time::numeric, 2) AS total_time, calls, round(mean_time::numeric, 2) AS mean, round((100 * total_time / sum(total_time::numeric) OVER ())::numeric, 2) AS percentage_cpu
  FROM pg_stat_statements
    JOIN pg_roles r ON r.oid = userid
  ORDER BY total_time DESC
  LIMIT 20;

DROP VIEW IF EXISTS ${SCHEMA}.user_consumers;
create or replace view ${SCHEMA}.user_consumers as
  select u.usename, d.datname, sum(s.calls), round(sum(s.total_time)::decimal,2) total_time
  from pg_stat_statements s join pg_user u on (s.userid=u.usesysid) join pg_database d on (s.dbid=d.oid)
  group by u.usename, d.datname order by sum(s.total_time);


--== Storage ==
DROP VIEW IF EXISTS ${SCHEMA}.db_size;
create or replace view ${SCHEMA}.db_size as
  select datname as db, pg_size_pretty(pg_database_size(datname)) as size
  from pg_database
  order by pg_database_size(datname) desc;

DROP VIEW IF EXISTS ${SCHEMA}.schema_size;
create or replace view ${SCHEMA}.schema_size as
  SELECT pg_catalog.pg_namespace.nspname AS schema_name, sum(pg_relation_size(pg_catalog.pg_class.oid)/1024/1024) AS schema_size_mb
  FROM pg_catalog.pg_class
    JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
  group by 1
  order by schema_size_mb desc;

DROP VIEW IF EXISTS ${SCHEMA}.total_table_size;
create or replace view ${SCHEMA}.total_table_size as
  SELECT N.nspname AS schema, relname AS table, pg_total_relation_size (C.oid) as bytes, pg_size_pretty ( pg_total_relation_size (C.oid) ) AS "total_size", C.oid AS oid
  FROM
      pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
  WHERE
      N.nspname NOT IN ( 'pg_catalog', 'information_schema' )
      AND C.relkind <> 'i'
      AND N.nspname !~ '^pg_toast'
  ORDER BY pg_total_relation_size (C.oid) DESC;

DROP VIEW IF EXISTS ${SCHEMA}.segment_size;
create or replace view ${SCHEMA}.segment_size as
  SELECT
  pg_namespace.nspname AS schema,
      pg_class.relname AS segment_name,
      CASE
          WHEN pg_class.relkind = 'r' THEN CAST( 'TABLE' AS VARCHAR( 18 ) )
          WHEN pg_class.relkind = 'i' THEN CAST( 'INDEX' AS VARCHAR( 18 ) )
          WHEN pg_class.relkind = 'f' THEN CAST( 'FOREIGN TABLE' AS VARCHAR( 18 ) )
          WHEN pg_class.relkind = 'S' THEN CAST( 'SEQUENCE' AS VARCHAR( 18 ) )
          WHEN pg_class.relkind = 's' THEN CAST( 'SPECIAL' AS VARCHAR( 18 ) )
          WHEN pg_class.relkind = 't' THEN CAST( 'TOAST TABLE' AS VARCHAR( 18 ) )
          WHEN pg_class.relkind = 'v' THEN CAST( 'VIEW' AS VARCHAR( 18 ) )
          ELSE CAST( pg_class.relkind AS VARCHAR( 18 ) )
      END AS segment_type,
      pg_relation_size( pg_class.oid ) BYTES,
      pg_size_pretty( pg_relation_size( pg_class.oid ) ) AS "pretty_size",
      pg_tablespace.spcname AS tablespace_name,
      pg_class.oid AS oid
  FROM
    pg_class INNER JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
    LEFT OUTER JOIN pg_tablespace ON pg_class.reltablespace = pg_tablespace.oid
  WHERE
      pg_class.relkind not in ('f','S','v') and
      pg_namespace.nspname !~ '^pg_toast'
  ORDER BY bytes DESC;

DROP VIEW IF EXISTS ${SCHEMA}.transaction_wrap_around;
create or replace view ${SCHEMA}.transaction_wrap_around as
  select datname, age(datfrozenxid)
  from pg_database
  order by age(datfrozenxid) desc limit 20;

DROP VIEW IF EXISTS ${SCHEMA}.replication_slot_details;
create or replace view ${SCHEMA}.replication_slot_details as
  select ps.slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)) as replicationSlotLag, ps.active, ps.active_pid, psa.usename, psa.client_addr
  from pg_replication_slots ps, pg_stat_activity psa
  where ps.active_pid=psa.pid;

DROP VIEW IF EXISTS ${SCHEMA}.drop_replication_slot;
create or replace view ${SCHEMA}.drop_replication_slot as
  select 'select pg_drop_replication_slot('''||slot_name||''');' from pg_replication_slots order by active;


--== Vacuum ==
DROP VIEW IF EXISTS ${SCHEMA}.active_vacuum;
create or replace view ${SCHEMA}.active_vacuum as
  SELECT datname, usename, pid, current_timestamp - xact_start AS xact_runtime, query
  FROM pg_stat_activity WHERE upper(query) like '%VACUUM%'
  ORDER BY xact_start;

DROP VIEW IF EXISTS ${SCHEMA}.auto_vacuum;
create or replace view ${SCHEMA}.auto_vacuum as
  SELECT relname, n_live_tup, n_dead_tup, trunc(100*n_dead_tup/(n_live_tup+1))::float "ratio%", to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI:SS') as autovacuum_date, to_char(last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS') as autoanalyze_date
  FROM pg_stat_all_tables
  ORDER BY last_autovacuum;

DROP VIEW IF EXISTS ${SCHEMA}.vacuum_problem;
create or replace view ${SCHEMA}.vacuum_problem as
  WITH vbt AS (SELECT setting AS autovacuum_vacuum_threshold FROM pg_settings WHERE name = 'autovacuum_vacuum_threshold'),
    vsf AS (SELECT setting AS autovacuum_vacuum_scale_factor FROM pg_settings WHERE name = 'autovacuum_vacuum_scale_factor'),
    fma AS (SELECT setting AS autovacuum_freeze_max_age FROM pg_settings WHERE name = 'autovacuum_freeze_max_age'),
    sto AS (select opt_oid, split_part(setting, '=', 1) as param,
    split_part(setting, '=', 2) as value from (select oid opt_oid, unnest(reloptions) setting from pg_class) opt)
  SELECT
    '"'||ns.nspname||'"."'||c.relname||'"' as relation , pg_size_pretty(pg_table_size(c.oid)) as table_size , age(relfrozenxid) as xid_age, coalesce(cfma.value::float, autovacuum_freeze_max_age::float) autovacuum_freeze_max_age,
    (coalesce(cvbt.value::float, autovacuum_vacuum_threshold::float) + coalesce(cvsf.value::float,autovacuum_vacuum_scale_factor::float) * c.reltuples) as autovacuum_vacuum_tuples , n_dead_tup as dead_tuples
  FROM pg_class c
    join pg_namespace ns on ns.oid = c.relnamespace
    join pg_stat_all_tables stat on stat.relid = c.oid
    join vbt on (1=1) join vsf on (1=1) join fma on (1=1)
    left join sto cvbt on cvbt.param = 'autovacuum_vacuum_threshold' and c.oid = cvbt.opt_oid
    left join sto cvsf on cvsf.param = 'autovacuum_vacuum_scale_factor' and c.oid = cvsf.opt_oid
    left join sto cfma on cfma.param = 'autovacuum_freeze_max_age' and c.oid = cfma.opt_oid
  WHERE c.relkind = 'r' and nspname <> 'pg_catalog'
    and ( age(relfrozenxid) >= coalesce(cfma.value::float, autovacuum_freeze_max_age::float)
    or  coalesce(cvbt.value::float, autovacuum_vacuum_threshold::float) + coalesce(cvsf.value::float,autovacuum_vacuum_scale_factor::float) * c.reltuples <= n_dead_tup )
  ORDER BY age(relfrozenxid) DESC LIMIT 50;

DROP VIEW IF EXISTS ${SCHEMA}.vacuum_progress;
create or replace view ${SCHEMA}.vacuum_progress as
  SELECT heap_blks_scanned/cast(heap_blks_total as numeric)*100 as heap_blks_percent, progress.*, activity.query
  FROM pg_stat_progress_vacuum AS progress
  INNER JOIN pg_stat_activity AS activity ON activity.pid = progress.pid;


--== Host ==
DROP VIEW IF EXISTS ${SCHEMA}.cluster_startup;
create or replace view ${SCHEMA}.cluster_startup as
  select pg_postmaster_start_time() as startup_time;


--database specific views
DO \$\$
  DECLARE currentdb name;
  DECLARE dbversion integer;
BEGIN
  --get the current database name and version
  SELECT current_database() INTO currentdb;
  SELECT current_setting('server_version_num') INTO dbversion;

  IF (currentdb = 'postgres') THEN
    --check if database version is >= 12.5
    IF (dbversion >= 120005) THEN
      --== Pg_cron ==
      DROP VIEW IF EXISTS ${SCHEMA}.pg_cron_scheduled_jobs;
      IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'cron' AND table_name = 'job') THEN
        create or replace view ${SCHEMA}.pg_cron_scheduled_jobs as
          select * from cron.job order by jobname;
      END IF;

      DROP VIEW IF EXISTS ${SCHEMA}.pg_cron_show_failed_jobs;
      IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'cron' AND table_name = 'job_run_details') THEN
        create or replace view ${SCHEMA}.pg_cron_show_failed_jobs as
          select *,ROUND(EXTRACT(EPOCH FROM end_time - start_time)) "duration_seconds" from cron.job_run_details where status!='succeeded' AND start_time > NOW() - INTERVAL '1 week' order by start_time desc;
      END IF;

      DROP VIEW IF EXISTS ${SCHEMA}.pg_cron_show_last_day;
      IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'cron' AND table_name = 'job_run_details') THEN
        create or replace view ${SCHEMA}.pg_cron_show_last_day as
          select *,ROUND(EXTRACT(EPOCH FROM end_time - start_time)) "duration_seconds" from cron.job_run_details where start_time > NOW() - INTERVAL '1 day' order by start_time desc;
      END IF;
    END IF;

  END IF;
END \$\$

EOF

#Check for errors
chk_errors=$(cat $LOG | grep -E "ERROR|error|refused" | wc -l)
if [ $chk_errors -lt 1 ]; then
  echo "$(date) DBA-Utils deployment complete"
  echo "$(date) Connect to database - psql -h ${HOST} -U ${SCHEMA} --dbname ${DBNAME}"
  echo "$(date) Run the follwing sql to see all the views - select * from dbautils();"
PSQL="psql -a -b -e --host=${HOST} --dbname=${DBNAME} --username=${SCHEMA}"
else
  echo "$(date)****ERROR - Problems with the deployment.  Please check $LOG"
  exit 1
fi

echo "$(date) Clearing down old logfiles"
find $LOGS -name "$(basename $0 .sh)[-_]*" -mtime +7 -exec rm -vf {} \; >>$LOG
rm -f ${INST_LIST}
