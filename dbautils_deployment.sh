#/bin/bash
#$Id:
#File: dbautils_deployment.sh
#Desc:
#
# Amendment History:
# Date:      Who:           Desc:
# 19/09/19   T.Mullen       Initial;
# 03/03/25   D.Chapman      Updated with new views and support for more local databases

notify() {
  vTYPE=$1
  vMSG=$2
  vTIME=$(date "+%F %T")

  echo "${vTIME} [${vTYPE}]: ${vMSG}" | tee -a $LOG
  if [ $vTYPE == "ERROR" ]; then
    echo "${vTIME} [${vTYPE}]: EXITING..." | tee -a $LOG
    echo ""
    exit 1
  elif [ $vTYPE == "ERRORLOOP" ]; then
    # Error inside a loop, so exits the loop, not the program
    echo "${vTIME} [${vTYPE}]: Skipping this iteration" | tee -a $LOG
    echo ""
    continue
  fi
}

check_exit() {
  # Purpose: Check last command's return code and if returned error, then write error message and leaves the program. It must be right after the command you want to test
  # Parameters: [IN: message type (can be NOTE or ERROR. If ERROR, will stop execution and leave the program); OUT: Formatted message ]
  # Example: check_exit $?
  if [ $1 != 0 ]; then
    notify ERROR "Unexpected error occurred with the last statement. I can't continue..."
  fi
}

check_exit_loop() {
  # Purpose: Check last command's return code and if returned error, then write error message and leaves the program. It must be right after the command you want to test
  # Parameters: [IN: message type (can be NOTE or ERROR. If ERROR, will stop execution and leave the program); OUT: Formatted message ]
  # Example: check_exit $?
  sleep 3
  if [ $1 != 0 ]; then
    notify ERRORLOOP "Unexpected error occurred with the last statement, on the last iteration..."
  fi
}

#aws='/var/lib/pgsql/awscli/bin/aws-cli'
vExclude="nomon|clone|-rr|nightly" # -rr is to skip read-replicas
LOGDIR="/tmp"
DATE=$(date +%Y%m%d%H%M%S)
RUNID=$(
  tr </dev/urandom -dc '123456789' | head -c5
  echo ""
)

if [ $# -lt 1 ]; then
  echo "Usage: $0  aws-profile [instance-name]"
  exit 1
elif [ $# -eq 1 ]; then
  # Apply to all instances
  vAWSProfile=$1
  LOG="${LOGDIR}/dbautils_deployment-${vAWSProfile}-${RUNID}-${DATE}.log"
  INST_LIST="${LOGDIR}/list_RDS_instances_deploy-${vAWSProfile}-${RUNID}-${DATE}.log"
  touch ${INST_LIST}
  DB_LIST="${LOGDIR}/db_list-${vAWSProfile}-${RUNID}-${DATE}.log"
  touch ${DB_LIST}

  aws rds describe-db-instances --query 'DBInstances[?Engine==`'postgres'` && DBInstanceStatus!=`stopped`].[DBInstanceIdentifier]' --output text --profile ${vAWSProfile} | grep -ivE "${vExclude}" > ${INST_LIST}
  check_exit $?
  sleep 1
elif [ $# -eq 2 ]; then
  # Apply only for this instance
  vAWSProfile=$1
  vInstanceName=$2
  LOG="${LOGDIR}/dbautils_deployment-${vAWSProfile}-${RUNID}-${DATE}.log"
  INST_LIST="${LOGDIR}/list_RDS_instances_deploy-${vAWSProfile}-${RUNID}-${DATE}.log"
  touch ${INST_LIST}
  DB_LIST="${LOGDIR}/db_list-${vAWSProfile}-${RUNID}-${DATE}.log"
  touch ${DB_LIST}

  aws rds describe-db-instances --db-instance-identifier ${vInstanceName} --query 'DBInstances[?Engine==`'postgres'` && DBInstanceStatus!=`stopped`].[DBInstanceIdentifier]' --output text --profile ${vAWSProfile} | grep -ivE "${vExclude}" > ${INST_LIST}
  check_exit $?
  sleep 1
fi

if [ ! -d "${LOGDIR}" ]; then
  mkdir ${LOGDIR}
  echo "$(date) Created logfile directory ${LOGDIR}" | tee -a $LOG
fi

notify NOTE "Logfile created: ${LOG}"
notify NOTE "DBAUTILS deployment will be attempted on the following instances:"
cat ${INST_LIST} | tee -a ${LOG}

#Get variables for each instance.
for line in $(cat ${INST_LIST} | awk '{print $1}'); do
  tempInstData=$(aws rds describe-db-instances --db-instance-identifier ${line} --output text --query "DBInstances[*].[Endpoint.Address,DBInstanceIdentifier,DBInstanceStatus,EngineVersion]" --profile ${vAWSProfile} 2>/dev/null)
  export host=$(echo $tempInstData|awk '{print $1}')
  export dbidentifer=$(echo $tempInstData|awk '{print $2}')
  export dbstatus=$(echo $tempInstData|awk '{print $3}')
  export dbversion=$(echo $tempInstData|awk '{print $4}')
  export port=5432
  export user=$(aws rds describe-db-instances --db-instance-identifier ${dbidentifer} --profile ${vAWSProfile} --query 'DBInstances[*].MasterUsername' --output text)

  notify NOTE "=== Deploying DBAUTILS on: ${host} ==="

  # Will attempt to fetch the password 5 times. If it can't, then leaves the loop and continue on the next...
  for times in $(seq 1 5); do
    export pass=$(aws ssm get-parameter --name /dba/${vAWSProfile}/${dbidentifer}/${user} --with-decryption --output text --query Parameter.Value --profile ${vAWSProfile})

    # Extra layer of validation for the password as it is the part which is failing the most
    if [ $(echo ${pass} | wc -w) -le 0 ]; then
      if [ $times -eq 5 ]; then
        notify ERRORLOOP "   ! The password could not be retrieved after ${times} attempts, so can't continue with the current iteration."
      fi
      notify NOTE "   ! Attempt #${times} to retrieve password..."
      sleep 10
    else
      export PGPASSWORD=${pass}
      break 1
    fi
  done

notify NOTE "Looping through all local databases and setting up dbutils in each one"
# loop through local databases setting up dbautils on each one
    psql -a -b -t --host=${host} --dbname=postgres --username=${user} << EOF > ${DB_LIST}
        select datname from pg_database where datname not in ('rdsadmin','template0','template1');
EOF

  sed '$d' ${DB_LIST} | while read dbname
    do
    vLocalLog="${LOGDIR}/${dbidentifer}-${dbname}-${RUNID}-${DATE}.log"
    echo "==============================================================================" >>${LOG}
    notify NOTE "  - ${dbidentifer}:${port}/${dbname}"

    # Test if database accessible
    psql -q --host=${host} --dbname=${dbname} --username=${user} -c '\c' >>/dev/null 2>&1
    if [ $? -ne 0 ]; then
      notify NOTE "   ! Skipping deployment on ${host}:${port}/${dbname} due to connection issue"
    else
      PSQL="psql -a -b --host=${host} --dbname=${dbname} --username=${user}"
      $PSQL <<EOF 1>${vLocalLog} 2>&1
CREATE SCHEMA IF NOT EXISTS dbadmin;

DROP FUNCTION IF EXISTS dbadmin.dbautils;
CREATE OR REPLACE FUNCTION dbadmin.dbautils()
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
  RAISE NOTICE 'select * from sessions_count_by_host;      - shows the count of sessions per host (exclude your connected session)';
  RAISE NOTICE 'select dbadmin.dynamic_sql_kill_idle_session_over_x_hours(''strata'',10); - shows dynamic SQL to kill sessions over x hours, example is strata and 10 hours';
  RAISE NOTICE 'select * from non_idle_sessions;           - non idle sessions in the instance (exclude your connected session)';
  RAISE NOTICE 'select * from blocking_detailed;           - detailed blocking session info';
  RAISE NOTICE 'select * from blocking_simple;             - simple blocking session info';
  RAISE NOTICE 'select * from prepared_statements;         - shows you if there is any incomplete prepared_statements';
  RAISE NOTICE 'select * from long_running_queries;        - reports active queries running over than 15 minutes';
  RAISE NOTICE 'select pg_terminate_backend(pid);          - kill session running against passed pid (process id)';
  RAISE NOTICE '======================================== Performance ========================================';
  RAISE NOTICE 'select * from top_pg_stat_statements;      - top consuming sql ';
  RAISE NOTICE 'select * from pg_stat_statements;          - detailed activity stats ';
  RAISE NOTICE 'select * from top_sql;                     - top consuming sql ';
  RAISE NOTICE 'select * from user_consumers;              - users whose used more time in each database ';
  RAISE NOTICE 'select * from reindex_progress;            - keep track of [re]-index creation on the fly. Issue a \\watch 5 to keep updating the status';
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

  ELSIF (currentdb = 'strata') THEN
    --STRATA DB VIEWS GO HERE!!!
    RAISE NOTICE '======================================== Strata =============================================';
    RAISE NOTICE 'select * from strata_version;              - strata version ';
    RAISE NOTICE 'select * from sql_runner_scripts;          - sql runner scripts ';

  ELSIF (currentdb = 'strata_si') THEN
    --STRATA_SI DB VIEWS GO HERE!!!
    RAISE NOTICE '======================================== Strata_si ==========================================';
    RAISE NOTICE 'select * from recent_si_counts;            - si counts for last 2 hours ';

  END IF;

  RAISE NOTICE '=============================================================================================';
END;
\$\$ LANGUAGE plpgsql;

--== Sessions ==
DROP VIEW IF EXISTS dbadmin.sessions_count_by_host;
create or replace view dbadmin.sessions_count_by_host as
(select usename,client_addr,count(*)
from pg_stat_activity where usename is not null
and usename not in ('rdsadmin')
and pg_backend_pid() <> pg_stat_activity.pid
group by usename,client_addr order by 3);

DROP FUNCTION IF EXISTS dbadmin.dynamic_sql_kill_idle_session_over_x_hours(schema character varying(200), hours integer);
create or replace function dbadmin.dynamic_sql_kill_idle_session_over_x_hours(schema character varying(200), hours integer)
RETURNS table (pid text) as
\$body\$
  SELECT 'select pg_terminate_backend('||pid||');' from pg_stat_activity
  where usename=\$1
  and state='idle'
  and query_start<now() - \$2 * INTERVAL '1 HOUR';
\$body\$
language sql;


DROP VIEW IF EXISTS dbadmin.sessions;
create or replace view dbadmin.sessions as
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

DROP VIEW IF EXISTS dbadmin.active_sessions;
create or replace view dbadmin.active_sessions as
  select datname, pid, usename, application_name, client_addr, query_start, state_change, state, query
  from pg_stat_activity
  where state='active' and pg_backend_pid()<>pid;

DROP VIEW IF EXISTS dbadmin.non_idle_sessions;
create or replace view dbadmin.non_idle_sessions as
  select datname, pid, usename, application_name, client_addr, query_start, state_change, state, query
  from pg_stat_activity
  where state<>'idle' and pg_backend_pid()<>pid;

DROP VIEW IF EXISTS dbadmin.blocking_detailed;
create or replace view dbadmin.blocking_detailed as
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

DROP VIEW IF EXISTS dbadmin.blocking_simple;
create or replace view dbadmin.blocking_simple as
  SELECT * FROM pg_locks WHERE NOT GRANTED;


DROP VIEW IF EXISTS dbadmin.prepared_statements;
create or replace view dbadmin.prepared_statements as
  select 'ROLLBACK PREPARED '''|| gid ||''';'FROM pg_prepared_xacts where database='strata';


--== Performance ==
DROP EXTENSION IF EXISTS pg_stat_statements cascade;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;


-- pg_stat_statements changed slightly on PG 13+
DO \$\$
  DECLARE
    dbversion integer;
  BEGIN
    SELECT current_setting('server_version_num') into dbversion;

    IF (dbversion >= 130001) THEN
      DROP VIEW IF EXISTS dbadmin.top_pg_stat_statements;
      create or replace view dbadmin.top_pg_stat_statements as
        SELECT rolname, round(total_exec_time::numeric, 2) AS total_time, calls, round(mean_exec_time::numeric, 2) AS mean, round((100 * total_exec_time / sum(total_exec_time::numeric) OVER ())::numeric, 2) AS percentage_cpu, query
        FROM pg_stat_statements JOIN pg_roles r ON r.oid = userid
        ORDER BY total_time DESC
        LIMIT 20;

      DROP VIEW IF EXISTS dbadmin.top_sql;
      create or replace view dbadmin.top_sql as
        SELECT rolname, substring(query, 1, 50) AS short_query, round(total_exec_time::numeric, 2) AS total_time, calls, round(mean_exec_time::numeric, 2) AS mean, round((100 * total_exec_time / sum(total_exec_time::numeric) OVER ())::numeric, 2) AS percentage_cpu
        FROM pg_stat_statements
          JOIN pg_roles r ON r.oid = userid
        ORDER BY total_time DESC
        LIMIT 20;

      DROP VIEW IF EXISTS dbadmin.user_consumers;
      create or replace view dbadmin.user_consumers as
        select u.usename, d.datname, sum(s.calls), round(sum(s.total_exec_time)::decimal,2) total_time
        from pg_stat_statements s join pg_user u on (s.userid=u.usesysid) join pg_database d on (s.dbid=d.oid)
        group by u.usename, d.datname order by sum(s.total_exec_time);


      -- fix of create_pgstat_history_snap.py
      CREATE TABLE IF NOT EXISTS dbadmin.pg_stats_history(
        timeinserted    timestamp without time zone default current_timestamp,
        rolname         character varying(30),
        total_time      numeric,
        calls           bigint,
        mean            numeric,
        max             numeric,
        percentage_cpu  numeric,
        query           text);

      CREATE INDEX IF NOT EXISTS pg_stats_history_timeinserted_idx ON dbadmin.pg_stats_history USING btree (timeinserted ASC NULLS LAST);
      DROP INDEX IF EXISTS pg_stats_history_query_idx;
      CREATE INDEX IF NOT EXISTS pg_stats_history_query_idx ON dbadmin.pg_stats_history (md5(left(query,2500)));

      INSERT INTO dbadmin.pg_stats_history (rolname,total_time,calls,mean,max,percentage_cpu,query)
        (SELECT left(r.rolname, 30) as rolname, round(st.total_exec_time::numeric, 2) AS total_time, st.calls, round(st.mean_exec_time::numeric, 2) AS mean, round(st.max_exec_time::numeric, 2) AS max,
          round((100::double precision * st.total_exec_time / sum(st.total_exec_time::numeric) OVER ()::double precision)::numeric, 2) AS percentage_cpu, st.query
        FROM pg_stat_statements st
          JOIN pg_roles r ON r.oid = st.userid
        ORDER BY (round(st.total_exec_time::numeric, 2)) DESC LIMIT 20);

      DROP VIEW IF EXISTS dbadmin.long_running_queries;
      create or replace view dbadmin.long_running_queries as
      SELECT leader_pid, pid, query_start,now() - pg_stat_activity.query_start AS duration, query, client_addr, usename
      FROM pg_stat_activity
      WHERE (now() - pg_stat_activity.query_start) > interval '15 minutes' and state = 'active' order by duration desc, usename, leader_pid, pid;
    ELSE
      DROP VIEW IF EXISTS dbadmin.top_pg_stat_statements;
      create or replace view dbadmin.top_pg_stat_statements as
        SELECT rolname, round(total_time::numeric, 2) AS total_time, calls, round(mean_time::numeric, 2) AS mean, round((100 * total_time / sum(total_time::numeric) OVER ())::numeric, 2) AS percentage_cpu, query
        FROM pg_stat_statements JOIN pg_roles r ON r.oid = userid
        ORDER BY total_time DESC
        LIMIT 20;

      DROP VIEW IF EXISTS dbadmin.top_sql;
      create or replace view dbadmin.top_sql as
        SELECT r.rolname, substring(st.query, 1, 50) AS short_query, round(st.total_time::numeric, 2) AS total_time, calls, round(st.mean_time::numeric, 2) AS mean, round((100 * st.total_time / sum(st.total_time::numeric) OVER ())::numeric, 2) AS percentage_cpu
        FROM pg_stat_statements st
          JOIN pg_roles r ON r.oid = st.userid
        ORDER BY total_time DESC
        LIMIT 20;

      DROP VIEW IF EXISTS dbadmin.user_consumers;
      create or replace view dbadmin.user_consumers as
        select u.usename, d.datname, sum(s.calls), round(sum(s.total_time)::decimal,2) total_time
        from pg_stat_statements s join pg_user u on (s.userid=u.usesysid) join pg_database d on (s.dbid=d.oid)
        group by u.usename, d.datname order by sum(s.total_time);


      -- fix of create_pgstat_history_snap.py
      CREATE TABLE IF NOT EXISTS dbadmin.pg_stats_history(
        timeinserted    timestamp without time zone default current_timestamp,
        rolname         character varying(30),
        total_time      numeric,
        calls           bigint,
        mean            numeric,
        max             numeric,
        percentage_cpu  numeric,
        query           text);

      CREATE INDEX IF NOT EXISTS pg_stats_history_timeinserted_idx ON dbadmin.pg_stats_history USING btree (timeinserted ASC NULLS LAST);
      DROP INDEX IF EXISTS pg_stats_history_query_idx;
      CREATE INDEX IF NOT EXISTS pg_stats_history_query_idx ON dbadmin.pg_stats_history (md5(left(query,2500)));

      INSERT INTO dbadmin.pg_stats_history (rolname,total_time,calls,mean,max,percentage_cpu,query)
        (SELECT left(r.rolname, 30) as rolname, round(st.total_time::numeric, 2) AS total_time, st.calls, round(st.mean_time::numeric, 2) AS mean, round(st.max_time::numeric, 2) AS max,
          round((100::double precision * st.total_time / sum(st.total_time::numeric) OVER ()::double precision)::numeric, 2) AS percentage_cpu, st.query
        FROM pg_stat_statements st
          JOIN pg_roles r ON r.oid = st.userid
        ORDER BY (round(st.total_time::numeric, 2)) DESC LIMIT 20);

      DROP VIEW IF EXISTS dbadmin.long_running_queries;
      create or replace view dbadmin.long_running_queries as
      SELECT pid, query_start,now() - pg_stat_activity.query_start AS duration, query, client_addr, usename
      FROM pg_stat_activity
      WHERE (now() - pg_stat_activity.query_start) > interval '15 minutes' and state = 'active' order by duration desc, usename, pid;
    END IF;

    PERFORM pg_stat_statements_reset();

    DROP VIEW IF EXISTS dbadmin.reindex_progress;
    create or replace view dbadmin.reindex_progress as
      select
        now(),
        query_start as started_at,
        now() - query_start as query_duration,
        format('[%s] %s', a.pid, a.query) as pid_and_query,
        index_relid::regclass as index_name,
        relid::regclass as table_name,
        (pg_size_pretty(pg_relation_size(relid))) as table_size,
        nullif(wait_event_type, '') || ': ' || wait_event as wait_type_and_event,
        phase,
        format(
          '%s (%s of %s)',
          coalesce((round(100 * blocks_done::numeric / nullif(blocks_total, 0), 2))::text || '%', 'N/A'),
          coalesce(blocks_done::text, '?'),
          coalesce(blocks_total::text, '?')
        ) as blocks_progress,
        format(
          '%s (%s of %s)',
          coalesce((round(100 * tuples_done::numeric / nullif(tuples_total, 0), 2))::text || '%', 'N/A'),
          coalesce(tuples_done::text, '?'),
          coalesce(tuples_total::text, '?')
        ) as tuples_progress,
        current_locker_pid,
        (select nullif(left(query, 150), '') || '...' from pg_stat_activity a where a.pid = current_locker_pid) as current_locker_query,
        format(
          '%s (%s of %s)',
          coalesce((round(100 * lockers_done::numeric / nullif(lockers_total, 0), 2))::text || '%', 'N/A'),
          coalesce(lockers_done::text, '?'),
          coalesce(lockers_total::text, '?')
        ) as lockers_progress,
        format(
          '%s (%s of %s)',
          coalesce((round(100 * partitions_done::numeric / nullif(partitions_total, 0), 2))::text || '%', 'N/A'),
          coalesce(partitions_done::text, '?'),
          coalesce(partitions_total::text, '?')
        ) as partitions_progress,
        (
          select
            format(
              '%s (%s of %s)',
              coalesce((round(100 * n_dead_tup::numeric / nullif(reltuples::numeric, 0), 2))::text || '%', 'N/A'),
              coalesce(n_dead_tup::text, '?'),
              coalesce(reltuples::int8::text, '?')
            )
          from pg_stat_all_tables t, pg_class tc
          where t.relid = p.relid and tc.oid = p.relid
        ) as table_dead_tuples
      from pg_stat_progress_create_index p
      left join pg_stat_activity a on a.pid = p.pid
      order by p.index_relid;

  END;
\$\$;

--== Storage ==
DROP VIEW IF EXISTS dbadmin.db_size;
create or replace view dbadmin.db_size as
  select datname as db, pg_size_pretty(pg_database_size(datname)) as size
  from pg_database
  order by pg_database_size(datname) desc;

DROP VIEW IF EXISTS dbadmin.schema_size;
create or replace view dbadmin.schema_size as
  SELECT pg_catalog.pg_namespace.nspname AS schema_name, sum(pg_relation_size(pg_catalog.pg_class.oid)/1024/1024) AS schema_size_mb
  FROM pg_catalog.pg_class
    JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
  group by 1
  order by schema_size_mb desc;

DROP VIEW IF EXISTS dbadmin.total_table_size;
create or replace view dbadmin.total_table_size as
  SELECT N.nspname AS schema, relname AS table, pg_total_relation_size (C.oid) as bytes, pg_size_pretty ( pg_total_relation_size (C.oid) ) AS "total_size", C.oid AS oid
  FROM
      pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
  WHERE
      N.nspname NOT IN ( 'pg_catalog', 'information_schema' )
      AND C.relkind <> 'i'
      AND N.nspname !~ '^pg_toast'
  ORDER BY pg_total_relation_size (C.oid) DESC;

DROP VIEW IF EXISTS dbadmin.segment_size;
create or replace view dbadmin.segment_size as
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

DROP VIEW IF EXISTS dbadmin.transaction_wrap_around;
create or replace view dbadmin.transaction_wrap_around as
  select datname, age(datfrozenxid)
  from pg_database
  order by age(datfrozenxid) desc limit 20;

DROP VIEW IF EXISTS dbadmin.replication_slot_details;
create or replace view dbadmin.replication_slot_details as
select slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)) as replicationSlotLag, active from pg_replication_slots;

DROP VIEW IF EXISTS dbadmin.drop_replication_slot;
create or replace view dbadmin.drop_replication_slot as
  select 'select pg_drop_replication_slot('''||slot_name||''');' from pg_replication_slots order by active;


--== Vacuum ==
DROP VIEW IF EXISTS dbadmin.active_vacuum;
create or replace view dbadmin.active_vacuum as
  SELECT datname, usename, pid, current_timestamp - xact_start AS xact_runtime, query
  FROM pg_stat_activity WHERE upper(query) like '%VACUUM%'
  ORDER BY xact_start;

DROP VIEW IF EXISTS dbadmin.auto_vacuum;
create or replace view dbadmin.auto_vacuum as
  SELECT relname, n_live_tup, n_dead_tup, trunc(100*n_dead_tup/(n_live_tup+1))::float "ratio%", to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI:SS') as autovacuum_date, to_char(last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS') as autoanalyze_date
  FROM pg_stat_all_tables
  ORDER BY last_autovacuum;

DROP VIEW IF EXISTS dbadmin.vacuum_problem;
create or replace view dbadmin.vacuum_problem as
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

DROP VIEW IF EXISTS dbadmin.vacuum_progress;
create or replace view dbadmin.vacuum_progress as
  SELECT heap_blks_scanned/cast(heap_blks_total as numeric)*100 as heap_blks_percent, progress.*, activity.query
  FROM pg_stat_progress_vacuum AS progress
  INNER JOIN pg_stat_activity AS activity ON activity.pid = progress.pid;


--== Host ==
DROP VIEW IF EXISTS dbadmin.cluster_startup;
create or replace view dbadmin.cluster_startup as
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
      DROP VIEW IF EXISTS dbadmin.pg_cron_scheduled_jobs;
      IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'cron' AND table_name = 'job') THEN
        create or replace view dbadmin.pg_cron_scheduled_jobs as
          select * from cron.job order by jobname;
      END IF;

      DROP VIEW IF EXISTS dbadmin.pg_cron_show_failed_jobs;
      IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'cron' AND table_name = 'job_run_details') THEN
        create or replace view dbadmin.pg_cron_show_failed_jobs as
          select *,ROUND(EXTRACT(EPOCH FROM end_time - start_time)) "duration_seconds" from cron.job_run_details where status!='succeeded' AND start_time > NOW() - INTERVAL '1 week' order by start_time desc;
      END IF;

      DROP VIEW IF EXISTS dbadmin.pg_cron_show_last_day;
      IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'cron' AND table_name = 'job_run_details') THEN
        create or replace view dbadmin.pg_cron_show_last_day as
          select *,ROUND(EXTRACT(EPOCH FROM end_time - start_time)) "duration_seconds" from cron.job_run_details where start_time > NOW() - INTERVAL '1 day' order by start_time desc;
      END IF;
    END IF;

  ELSIF (currentdb = 'strata') THEN
    --== Strata ==
    DROP VIEW IF EXISTS dbadmin.strata_version;
    IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'strata' AND table_name = 'strata_version') THEN
      create or replace view dbadmin.strata_version as
        select * from strata.strata_version where created=(select max(created) from strata.strata_version);
    END IF;

    DROP VIEW IF EXISTS dbadmin.sql_runner_scripts;
    IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'strata' AND table_name = 'sqlupdsc') THEN
      create or replace view dbadmin.sql_runner_scripts as
        select sqlupdatescriptlog_id, updated, version, description, whoby from strata.sqlupdsc order by 2 desc limit 20;
    END IF;

    CREATE TABLE IF NOT EXISTS dbadmin.cdl_database_parameters (
      CATEGORY varchar(100) not null,
      PARAMETER varchar(100) not null,
      VALUE varchar(1000) not null,
      COMMENTS text not null,
      PRIMARY KEY (CATEGORY, PARAMETER),
      CONSTRAINT CATEGORY_PARAMETER_UNQ UNIQUE (CATEGORY, PARAMETER)
    );

    COMMENT ON TABLE dbadmin.cdl_database_parameters IS 'Holds metadata used for a number of services and settings required to be consumed by our activities';
    COMMENT ON COLUMN dbadmin.cdl_database_parameters.PARAMETER IS 'The parameter name';
    COMMENT ON COLUMN dbadmin.cdl_database_parameters.CATEGORY IS 'The category the parameter belongs to like memory, pruning, etc';
    COMMENT ON COLUMN dbadmin.cdl_database_parameters.VALUE IS 'The actual value';
    COMMENT ON COLUMN dbadmin.cdl_database_parameters.COMMENTS IS 'Any comments';

    -- UPSERT statements to avoid erroring on a daily execution
     --= Category: pruning
    INSERT INTO dbadmin.cdl_database_parameters(CATEGORY, PARAMETER, VALUE, COMMENTS) VALUES
      ('pruning', 'xmltype_pruning', '31', 'Number of days to keep data. The pruning script will delete any data after that threshold')
      ON CONFLICT ON CONSTRAINT CATEGORY_PARAMETER_UNQ
      DO NOTHING;

  ELSIF (currentdb = 'strata_si') THEN
    --== Strata_si ==
    DROP VIEW IF EXISTS dbadmin.recent_si_counts;
    IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'strata_si' AND table_name = 'si_aud_req_xmltype') THEN
      create or replace view dbadmin.recent_si_counts as
        select count(*), to_char(audit_date,'dd-MON-yyyy HH24:MI')
        from strata_si.si_aud_req_xmltype
        where audit_date > now() - interval  '2 hours'
        group by to_char(audit_date,'dd-MON-yyyy HH24:MI')
        order by 2;
    END IF;

  END IF;
END \$\$
EOF

      # Saving each individual log to the main logfile
      cat ${vLocalLog} &>>${LOG}

      # Checking for error on each logfile individually. Will delete logs if NO errors found.
      vErrorCount=$(grep -iEc "error|refused|fatal" ${vLocalLog} | xargs)
      if [ ${vErrorCount} -gt 0 ]; then
        notify WARNING "Found ${vErrorCount} error on ${vLocalLog} ."
        grep -iE -B1 "error|refused|fatal" ${vLocalLog}
      else
        rm -f ${vLocalLog}
      fi

      sleep 0.1
    fi
  done
done

#Check for errors
chk_errors=$(cat ${LOG} | grep -E "ERROR|error|refused" | wc -l)
if [ $chk_errors -lt 1 ]; then
  notify NOTE "=== DBAUTILS deployment complete ==="
else
  notify ERROR "****ERROR - Problems with the deployment.  Please check $LOG"
  exit 1
fi

notify NOTE "Clearing down old logfiles"
find ${LOGDIR} -name "$(basename $0 .sh)[-_]*" -mtime +3 -exec rm -vf {} \; >>$LOG
rm -f ${INST_LIST}
rm -f ${DB_LIST}
