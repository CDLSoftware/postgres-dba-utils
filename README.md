# Postgres DBA Utils

This project was spawned from our team migrating from oracle to postgres and we wanted to try and make the move from oracle to postgres as simple as possible and this has made our life much easier so we wanted to share and hopefully make other people life's that bit easier.  It enables quick easy use of views inside the postgres database to make administration of the database just that little bit easier.

Sample output of the views 

``` bash
postgres=# select * from dbautils();
NOTICE:  ==================================== Postgres utility views =================================
NOTICE:  ======================================== Sessions ===========================================
NOTICE:  select * from sessions;                    - show all sessions in the db order by usename (exclude your connected session)
NOTICE:  select * from active_sessions;             - active sessions in the instance (exclude your connected session)
NOTICE:  select * from non_idle_sessions;           - non idle sessions in the instance (exclude your connected session)
NOTICE:  select * from blocking_detailed;           - detailed blocking session info
NOTICE:  select * from blocking_simple;             - simple blocking session info
NOTICE:  select pg_terminate_backend(pid);          - kill session running against passed pid (process id)
NOTICE:  ======================================== Performance ========================================
NOTICE:  select * from top_pg_stat_statements;      - top consuming sql
NOTICE:  select * from pg_stat_statements;          - detailed activity stats
NOTICE:  select * from top_sql;                     - top consuming sql
NOTICE:  select * from user_consumers;              - users whose used more time in each database
NOTICE:  ======================================== Storage ============================================
NOTICE:  select * from db_size;                     - database sizes
NOTICE:  select * from schema_size;                 - schema sizes
NOTICE:  select * from total_table_size;            - total tables size: including TOAST and indexes segments
NOTICE:  select * from segment_size;                - size of database objects
NOTICE:  select * from transaction_wrap_around;     - transaction wraparound counts
NOTICE:  select * from replication_slot_details;    - shows details of replication slots in use
NOTICE:  select * from drop_replication_slot;       - shows syntax of how to drop a replication slot
NOTICE:  ======================================== Vacuum =============================================
NOTICE:  select * from active_vacuum;               - active vacuum in the db
NOTICE:  select * from auto_vacuum;                 - auto vacuum details on objects
NOTICE:  select * from vacuum_problems;             - lists any objects with vacuum issues
NOTICE:  select * from vacuum_progress;             - show vacuum progress
NOTICE:  ======================================== Host ===============================================
NOTICE:  select * from cluster_startup;             - shows the cluster startup time
NOTICE:  ======================================== Pg_cron ============================================
NOTICE:  select * from pg_cron_scheduled_jobs;      - displays pg_cron scheduled jobs
NOTICE:  select * from pg_cron_show_failed_jobs;    - displays pg_cron failed jobs for the last week
NOTICE:  select * from pg_cron_show_last_day;       - displays pg_cron jobs ran in the last day
NOTICE:  ============================================================================================= 

```

## Installing the views

The views can be installed in any schema there is 4 inputs that are needed when running the install script, hostname, dbname, schema & schema password, this will then install the scripts in the database and schema inputted, example below.

``` bash
cd postgres-dba-utils
$$ postgres-dba-utils $ ./dbautils_deployment.sh localhost postgres postgres password123
Fri  8 Apr 2022 15:11:02 BST DBA-Utils install starting on host - localhost
Fri  8 Apr 2022 15:11:02 BST Logfile for run /tmp/dbautils_deployment_20220408_151102.log
Fri  8 Apr 2022 15:11:02 BST DBA-Utils deployment complete
Fri  8 Apr 2022 15:11:02 BST Connect to database - psql -h localhost -U postgres --dbname postgres
Fri  8 Apr 2022 15:11:02 BST Run the follwing sql to see all the views - select * from dbautils();
Fri  8 Apr 2022 15:11:02 BST Clearing down old logfiles

```

## Testing the views

Now you have the views installed you can connect to the instance and run the dbautils function that will show you all the views you can run.

``` bash
$$ :postgres-dba-utils $ psql -h localhost -U postgres
psql (12.1, server 12.5 (Debian 12.5-1.pgdg100+1))
Type "help" for help.

postgres=# select * from dbautils();
NOTICE:  ==================================== Postgres utility views =================================
NOTICE:  ======================================== Sessions ===========================================
NOTICE:  select * from sessions;                    - show all sessions in the db order by usename (exclude your connected session)
NOTICE:  select * from active_sessions;             - active sessions in the instance (exclude your connected session)
NOTICE:  select * from non_idle_sessions;           - non idle sessions in the instance (exclude your connected session)
NOTICE:  select * from blocking_detailed;           - detailed blocking session info
NOTICE:  select * from blocking_simple;             - simple blocking session info
NOTICE:  select pg_terminate_backend(pid);          - kill session running against passed pid (process id)
NOTICE:  ======================================== Performance ========================================
NOTICE:  select * from top_pg_stat_statements;      - top consuming sql
NOTICE:  select * from pg_stat_statements;          - detailed activity stats
NOTICE:  select * from top_sql;                     - top consuming sql
NOTICE:  select * from user_consumers;              - users whose used more time in each database
NOTICE:  ======================================== Storage ============================================
NOTICE:  select * from db_size;                     - database sizes
NOTICE:  select * from schema_size;                 - schema sizes
NOTICE:  select * from total_table_size;            - total tables size: including TOAST and indexes segments
NOTICE:  select * from segment_size;                - size of database objects
NOTICE:  select * from transaction_wrap_around;     - transaction wraparound counts
NOTICE:  select * from replication_slot_details;    - shows details of replication slots in use
NOTICE:  select * from drop_replication_slot;       - shows syntax of how to drop a replication slot
NOTICE:  ======================================== Vacuum =============================================
NOTICE:  select * from active_vacuum;               - active vacuum in the db
NOTICE:  select * from auto_vacuum;                 - auto vacuum details on objects
NOTICE:  select * from vacuum_problems;             - lists any objects with vacuum issues
NOTICE:  select * from vacuum_progress;             - show vacuum progress
NOTICE:  ======================================== Host ===============================================
NOTICE:  select * from cluster_startup;             - shows the cluster startup time
NOTICE:  ======================================== Pg_cron ============================================
NOTICE:  select * from pg_cron_scheduled_jobs;      - displays pg_cron scheduled jobs
NOTICE:  select * from pg_cron_show_failed_jobs;    - displays pg_cron failed jobs for the last week
NOTICE:  select * from pg_cron_show_last_day;       - displays pg_cron jobs ran in the last day
NOTICE:  =============================================================================================
 dbautils
----------

(1 row)

postgres=# select * from db_size;
    db     |  size
-----------+---------
 postgres  | 8233 kB
 template1 | 7801 kB
 template0 | 7801 kB
(3 rows)


```

