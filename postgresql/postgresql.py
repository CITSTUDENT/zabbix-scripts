#!/usr/bin/python

import sys
import psycopg2
import json
import time

dbhost = 'localhost'
dbuser = 'zabbix_agent'
dbpassword = 'ln17OJ47Wlp9565'

# ...

def execute(sql, args=[], dbname="postgres"):

    try:
        conn = psycopg2.connect("host='%s' user='%s' password='%s' dbname='%s'" % (dbhost, dbuser, dbpassword, dbname))
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)
    except:
        print "Unexpected error"
        sys.exit(1)

    cur = conn.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall()
    
    return rows

# ...

key = sys.argv[1]

# ...

# General info

if key == "pgsql.uptime":
    print execute("select date_part('epoch', now() - pg_postmaster_start_time())::int")[0][0]
    sys.exit(0)

if key == "pgsql.ping":
    start_time = time.time()
    result = execute("select 1")[0][0]
    assert(result == 1)
    print round((time.time() - start_time) * 1000, 2)
    sys.exit(0)
    
# Connections

if key == "pgsql.connections.active":
    print execute("select count(*) from pg_stat_activity where state = 'active'")[0][0]
    sys.exit(0)
    
if key == "pgsql.connections.idle":
    print execute("select count(*) from pg_stat_activity where state = 'idle'")[0][0]
    sys.exit(0)

if key == "pgsql.connections.idle_in_transaction":
    print execute("select count(*) from pg_stat_activity where state = 'idle in transaction'")[0][0]
    sys.exit(0)

if key == "pgsql.connections.total":
    print execute("select count(*) from pg_stat_activity")[0][0]
    sys.exit(0) 

if key == "pgsql.connections.total_pct":
    print execute("select count(*)*100/(select current_setting('max_connections')::int) from pg_stat_activity")[0][0]
    sys.exit(0) 

if key == "pgsql.connections.waiting":
    print execute("select count(*) from pg_stat_activity where waiting")[0][0]
    sys.exit(0)   

if key == "pgsql.connections.prepared":
    print execute("select count(*) from pg_prepared_xacts")[0][0]
    sys.exit(0) 

    
# Background writer statistics. http://www.postgresql.org/docs/9.3/static/monitoring-stats.html#PG-STAT-BGWRITER-VIEW

if key == "pgsql.bgwriter.checkpoints_timed":
    print execute("select checkpoints_timed from pg_stat_bgwriter")[0][0]
    sys.exit(0)

if key == "pgsql.bgwriter.checkpoints_req":
    print execute("select checkpoints_req from pg_stat_bgwriter")[0][0]
    sys.exit(0)

if key == "pgsql.bgwriter.checkpoint_write_time":
    print execute("select checkpoint_write_time from pg_stat_bgwriter")[0][0]
    sys.exit(0)

if key == "pgsql.bgwriter.checkpoint_sync_time":
    print execute("select checkpoint_sync_time from pg_stat_bgwriter")[0][0]
    sys.exit(0)
    
if key == "pgsql.bgwriter.buffers_checkpoint":
    print execute("select buffers_checkpoint from pg_stat_bgwriter")[0][0]
    sys.exit(0)

if key == "pgsql.bgwriter.buffers_clean":
    print execute("select buffers_clean from pg_stat_bgwriter")[0][0]
    sys.exit(0)
    
if key == "pgsql.bgwriter.maxwritten_clean":
    print execute("select maxwritten_clean from pg_stat_bgwriter")[0][0]
    sys.exit(0)

if key == "pgsql.bgwriter.buffers_backend":
    print execute("select buffers_backend from pg_stat_bgwriter")[0][0]
    sys.exit(0)

if key == "pgsql.bgwriter.buffers_backend_fsync":
    print execute("select buffers_backend_fsync from pg_stat_bgwriter")[0][0]
    sys.exit(0)

if key == "pgsql.bgwriter.buffers_alloc":
    print execute("select buffers_alloc from pg_stat_bgwriter")[0][0]
    sys.exit(0)


# Size of database, table or indexes of specified table

if key == "pgsql.db.size":
    print execute("select pg_database_size(%s)", [sys.argv[2]])[0][0]
    sys.exit(0)

#if key == "pgsql.table.size":
#    print execute("select pg_relation_size(%s)", [sys.argv[2]])[0][0]
#    sys.exit(0)
        
#if key == "pgsql.index.size":
#    print execute("select pg_total_relation_size(%s) - pg_relation_size(%s)", [sys.argv[2], sys.argv[2]])[0][0]
#    sys.exit(0)

# Summary database statistics

if key == "pgsql.dbstat.sum.numbackends":
    print execute("select sum(numbackends) from pg_stat_database")[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.sum.xact_commit":
    print execute("select sum(xact_commit) from pg_stat_database")[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.sum.xact_rollback":
    print execute("select sum(xact_rollback) from pg_stat_database")[0][0]
    sys.exit(0) 

if key == "pgsql.dbstat.sum.blks_read":
    print execute("select sum(blks_read) from pg_stat_database")[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.sum.blks_hit":
    print execute("select sum(blks_hit) from pg_stat_database")[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.sum.tup_returned":
    print execute("select sum(tup_returned) from pg_stat_database")[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.sum.tup_fetched":
    print execute("select sum(tup_fetched) from pg_stat_database")[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.sum.tup_inserted":
    print execute("select sum(tup_inserted) from pg_stat_database")[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.sum.tup_updated":
    print execute("select sum(tup_updated) from pg_stat_database")[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.sum.tup_deleted":
    print execute("select sum(tup_deleted) from pg_stat_database")[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.sum.conflicts":
    print execute("select sum(conflicts) from pg_stat_database")[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.sum.temp_files":
    print execute("select sum(temp_files) from pg_stat_database")[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.sum.temp_bytes":
    print execute("select sum(temp_bytes) from pg_stat_database")[0][0]
    sys.exit(0)    

if key == "pgsql.dbstat.sum.deadlocks":
    print execute("select sum(deadlocks) from pg_stat_database")[0][0]
    sys.exit(0)

# Specified database statistics

if key == "pgsql.dbstat.numbackends":
    print execute("select numbackends from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
        
if key == "pgsql.dbstat.xact_commit":
    print execute("select xact_commit from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
                      
if key == "pgsql.dbstat.xact_rollback":
    print execute("select xact_rollback from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)

if key == "pgsql.dbstat.blks_read":
    print execute("select blks_read from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.blks_hit":
    print execute("select blks_hit from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.tup_returned":
    print execute("select tup_returned from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.tup_fetched":
    print execute("select tup_fetched from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.tup_inserted":
    print execute("select tup_inserted from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.tup_updated":
    print execute("select tup_updated from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.tup_deleted":
    print execute("select tup_deleted from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.conflicts":
    print execute("select conflicts from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.temp_files":
    print execute("select temp_files from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.temp_bytes":
    print execute("select temp_bytes from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)
    
if key == "pgsql.dbstat.deadlocks":
    print execute("select deadlocks from pg_stat_database where datname = %s", [sys.argv[2]])[0][0]
    sys.exit(0)

# Transactions

if key == "pgsql.transactions.idle":
    print execute("select coalesce(extract(epoch from max(age(now(), query_start))), 0) from pg_stat_activity where state='idle in transaction'")[0][0]
    sys.exit(0)    

if key == "pgsql.transactions.active":
    print execute("select coalesce(extract(epoch from max(age(now(), query_start))), 0) from pg_stat_activity where state <> 'idle in transaction' and state <> 'idle'")[0][0]
    sys.exit(0)
        
if key == "pgsql.transactions.waiting":
    print execute("select coalesce(extract(epoch from max(age(now(), query_start))), 0) from pg_stat_activity where waiting")[0][0]
    sys.exit(0)   

if key == "pgsql.transactions.prepared":
    print execute("select coalesce(extract(epoch from max(age(now(), prepared))), 0) from pg_prepared_xacts")[0][0]
    sys.exit(0)


# pgbuffercache

if key == "pgsql.buffercache.clear":
    print execute("select count(*) from pg_buffercache where not isdirty")[0][0]
    sys.exit(0)

if key == "pgsql.buffercache.dirty":
    print execute("select count(*) from pg_buffercache where isdirty")[0][0]
    sys.exit(0)

if key == "pgsql.buffercache.used":
    print execute("select count(*) from pg_buffercache where reldatabase is not null")[0][0]
    sys.exit(0)              

if key == "pgsql.buffercache.total":
    print execute("select count(*) from pg_buffercache")[0][0]
    sys.exit(0)

# Others

if key == "pgsql.setting":
    print execute("select current_setting(%s)", [sys.argv[2]])[0][0]
    sys.exit(0)

if key == "pgsql.wal.write":
    print execute("select pg_xlog_location_diff(pg_current_xlog_location(),'0/00000000')")[0][0]
    sys.exit(0)
        
if key == "pgsql.wal.count":
    print execute("select count(*) from pg_ls_dir('pg_xlog')")[0][0]
    sys.exit(0)

#UserParameter=pgsql.table.tuples[*],psql -qAtX $1 -c "select count(*) from $2"
#UserParameter=pgsql.trigger[*],psql -qAtX $1 -c "select count(*) from pg_trigger where tgenabled='O' and tgname='$2'"

# Discovery

if key == "pgsql.db.discovery":
    result = {"data":[]}
    rows = execute("select datname from pg_database where not datistemplate and datallowconn and datname != 'postgres'")
    for row in rows:
        result["data"].append({"{#DBNAME}":row[0]})
    print json.dumps(result)
    sys.exit(0)
    
if key == "pgsql.table.discovery":
    result = {"data":[]}
    databases = execute("select datname from pg_database where not datistemplate and datallowconn and datname != 'postgres'")
    for database in databases:
        dbname = database[0]
        rows = execute("select schemaname, tablename from pg_tables where schemaname not in ('pg_catalog','information_schema')", dbname=dbname)
        for row in rows:
            result["data"].append({"{#DBNAME}":dbname, "{#SCHEMANAME}":row[0], "{#TABLENAME}":row[1]})
    print json.dumps(result)
    sys.exit(0)
    
if key == "pgsql.streaming.discovery":
    result = {"data":[]}
    rows = execute("select client_addr from pg_stat_replication")
    for row in rows:
        result["data"].append({"{#HOTSTANDBY}":row[0]})
    print json.dumps(result)
    sys.exit(0)
