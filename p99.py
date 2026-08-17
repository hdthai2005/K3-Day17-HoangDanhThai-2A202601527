import duckdb
con = duckdb.connect('warehouse.duckdb')
res = con.sql("select quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) as p99_ngay from bronze_events;").fetchall()
print("P99:", res[0][0])
