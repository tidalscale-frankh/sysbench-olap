-- OLAP.LUA
--
-- Sysbench options:
-- 
-- olap_verbose - output detail, such as insertion progress (default: 0)
-- olap_tables_count - number of tables to fill (default: 1)
-- olap_table_size - number of rows to fill per table (default: 10000)
-- olap_max_key - range of integer values in the key columns (default: 1000000)
-- olap_range_fraction - the fraction of the key values to select per query (default: 1000)
-- olap_range_size - the number of key values to select per query (default: olap_max_key / olap_range_faction)
-- olap_simple_indexed_ranges - number of queries per event (default: 0)
-- olap_simple_unindexed_ranges - number of queries per event (default: 0)
-- olap_count_indexed_ranges - number of aggregate queries per event, lookup by index (default: 1)
-- olap_count_unindexed_ranges - number of aggregate queries per event, lookup by non-index (default: 1)
-- olap_sample_type - choose starting point randomly per query ("random") or cycle through table iteratively ("tiled") (default: "random")
-- olap_range_start - starting key value per thread (default: random value between 1 and olap_max_key)

pathtest = string.match(test, "(.*/)") or ""

function create_insert(table_id)
--[=====[
   print("olap create_insert(): entered")
--]=====]

   local table_name
   local query
   local i
   local j
   local stmt
   local start
   local now
   local end_estimated

   table_name = string.format("olaptest%d", table_id)

   --
   -- This table is designed to fit exactly two rows in one 16KB InnoDB page.
   --
   print(string.format("Creating table %s...", table_name))
   query = string.format([[CREATE TABLE IF NOT EXISTS %s (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    key1 BIGINT UNSIGNED NOT NULL,
    key2 BIGINT UNSIGNED NOT NULL,
    key3 BIGINT UNSIGNED NOT NULL,
    nonkey01 CHAR(255) NOT NULL,
    nonkey02 CHAR(255) NOT NULL,
    nonkey03 CHAR(255) NOT NULL,
    nonkey04 CHAR(255) NOT NULL,
    nonkey05 CHAR(255) NOT NULL,
    nonkey06 CHAR(255) NOT NULL,
    nonkey07 CHAR(255) NOT NULL,
    nonkey08 CHAR(255) NOT NULL,
    nonkey09 CHAR(255) NOT NULL,
    nonkey10 CHAR(255) NOT NULL,
    nonkey11 CHAR(255) NOT NULL,
    nonkey12 CHAR(255) NOT NULL,
    nonkey13 CHAR(255) NOT NULL,
    nonkey14 CHAR(255) NOT NULL,
    nonkey15 CHAR(255) NOT NULL,
    nonkey16 CHAR(255) NOT NULL,
    nonkey17 CHAR(255) NOT NULL,
    nonkey18 CHAR(255) NOT NULL,
    nonkey19 CHAR(255) NOT NULL,
    nonkey20 CHAR(255) NOT NULL,
    nonkey21 CHAR(255) NOT NULL,
    nonkey22 CHAR(255) NOT NULL,
    nonkey23 CHAR(255) NOT NULL,
    nonkey24 CHAR(255) NOT NULL,
    nonkey25 CHAR(255) NOT NULL,
    nonkey26 CHAR(255) NOT NULL,
    nonkey27 CHAR(255) NOT NULL,
    nonkey28 CHAR(255) NOT NULL,
    nonkey29 CHAR(255) NOT NULL,
    nonkey30 CHAR(255) NOT NULL,
    pad TEXT,
    KEY k_%d (key1, key2, key3)
) ENGINE=%s %s]], table_name, table_id, mysql_table_engine, (mysql_table_options or ""))

   db_query(query)

   print(string.format("Inserting %d records into '%s'", olap_table_size, table_name))

   db_query(begin_query)
   stmt = db_prepare(string.format("INSERT INTO %s SET key1=?, key2=?, key3=?, nonkey01=?, nonkey02=?, nonkey03=?, nonkey04=?, nonkey05=?, nonkey06=?, nonkey07=?, nonkey08=?, nonkey09=?, nonkey10=?, nonkey11=?, nonkey12=?, nonkey13=?, nonkey14=?, nonkey15=?, nonkey16=?, nonkey17=?, nonkey18=?, nonkey19=?, nonkey20=?, nonkey21=?, nonkey22=?, nonkey23=?, nonkey24=?, nonkey25=?, nonkey26=?, nonkey27=?, nonkey28=?, nonkey29=?, nonkey30=?", table_name))
   params = {}
   for i = 1, 3 do
      params[i] = 1
   end
   for i = 4, 33 do
      params[i] = 'x'
   end
   db_bind_param(stmt, params)

   start = os.time()
   db_query(begin_query)
   for i = 1, olap_table_size do
      for j = 1, 3 do
            params[j] = sb_rand(1, olap_max_key)
      end
      for j = 4, 33 do
         params[j] = sb_rand_str(
         "################" ..
	 "@@@@@@@@@@@@@@@@" ..
	 "################" ..
	 "@@@@@@@@@@@@@@@@" ..
	 "################" ..
	 "@@@@@@@@@@@@@@@@" ..
	 "################" ..
	 "@@@@@@@@@@@@@@@@" ..
	 "################" ..
	 "@@@@@@@@@@@@@@@@" ..
	 "################" ..
	 "@@@@@@@@@@@@@@@@" ..
	 "################" ..
	 "@@@@@@@@@@@@@@@@" ..
	 "################" ..
	 "@@@@@@@@@@@@@@@"
	 )
      end
      rs = db_execute(stmt)
      if i % 10000 == 0 then
         db_query(commit_query)
	 if (olap_verbose) then
	    now = os.time()
	    end_estimated = start + (now - start) * olap_table_size / i
	    print(string.format("%s: %d/%d rows (start=%s, now=%s, end=%s)",
	       table_name, i, olap_table_size,
	       os.date("%X", start),
	       os.date("%X", now),
	       os.date("%X", end_estimated)))
	 end
         db_query(begin_query)
      end
   end
   db_query(commit_query)
end

function prepare()
--[=====[
   print("olap prepare(): entered")
--]=====]

   local query
   local i

   set_vars()

   db_connect()

   for i = 1, olap_tables_count do
      create_insert(i)
   end

   return 0
end

function cleanup()
--[=====[
   print("olap cleanup(): entered")
--]=====]
   local i
   local table_name

   set_vars()

   for i = 1, olap_tables_count do
      table_name = string.format("olaptest%d", i)
      print(string.format("Dropping table '%s'", table_name))
      db_query(string.format("DROP TABLE %s", table_name))
   end
end

function set_vars()
--[=====[
   print("olap set_vars(): entered")
--]=====]
   begin_query = "BEGIN"
   commit_query = "COMMIT"
   olap_verbose = olap_verbose or 0
   olap_table_size = olap_table_size or 10000
   olap_max_key = olap_max_key or 1000000
   olap_range_fraction = olap_range_pct or 1000
   olap_range_size = olap_range_size or math.floor(olap_max_key / olap_range_fraction)
   olap_tables_count = olap_tables_count or 1
   olap_simple_indexed_ranges = olap_simple_ranges or 0
   olap_simple_unindexed_ranges = olap_simple_ranges or 0
   olap_count_indexed_ranges = olap_count_indexed_ranges or 1
   olap_count_unindexed_ranges = olap_count_unindexed_ranges or 1
   olap_sample_type = olap_sample_type or "random"
   if olap_sample_type ~= "random" and olap_sample_type ~= "tiled" then
     os.exit()
   end
   if olap_sample_type == "random" then
     olap_range_start = olap_range_start or sb_rand_uniform(1, olap_max_key)
   else
     olap_range_start = olap_range_start or 1
   end
   olap_tile_size = olap_max_key / 37
end

function thread_init(thread_id)
--[=====[
   print("olap thread_init(): entered")
--]=====]
   set_vars()
end

--[=====[
function event(thread_id)
   print("olap event(): entered")
   local rs
   local i
   local table_name
   local range_start
   local range_end

   table_name = string.format("olaptest%d", sb_rand_uniform(1, olap_tables_count))

   db_query(begin_query)

   for i = 1, olap_simple_indexed_ranges do
      if olap_sample_type == "random" then
         range_start = sb_rand(1, olap_max_key)
      elseif olap_sample_type == "tiled" then
         range_start = olap_range_start
         olap_range_start = (olap_range_start + olap_tile_size * 8) % olap_max_key
      end
      range_end = range_start + olap_range_size
      rs = db_query(string.format("SELECT * FROM %s WHERE key1 BETWEEN %d AND %d", table_name, range_start, range_end))
   end

   for i = 1, olap_simple_unindexed_ranges do
      if olap_sample_type == "random" then
         range_start = sb_rand(1, olap_max_key)
      elseif olap_sample_type == "tiled" then
         range_start = olap_range_start
         olap_range_start = (olap_range_start + olap_tile_size * 8) % olap_max_key
      end
      range_end = range_start + olap_range_size
      rs = db_query(string.format("SELECT * FROM %s WHERE key2 BETWEEN %d AND %d", table_name, range_start, range_end))
   end

   for i = 1, olap_count_indexed_ranges do
      if olap_sample_type == "random" then
         range_start = sb_rand(1, olap_max_key)
      elseif olap_sample_type == "tiled" then
         range_start = olap_range_start
         olap_range_start = (olap_range_start + olap_tile_size * 8) % olap_max_key
      end
      range_end = range_start + olap_range_size
      rs = db_query(string.format("SELECT COUNT(*) FROM %s WHERE key1 BETWEEN %d AND %d", table_name, range_start, range_end))
   end

   for i = 1, olap_count_unindexed_ranges do
      if olap_sample_type == "random" then
         range_start = sb_rand(1, olap_max_key)
      elseif olap_sample_type == "tiled" then
         range_start = olap_range_start
         olap_range_start = (olap_range_start + olap_tile_size * 8) % olap_max_key
      end
      range_end = range_start + olap_range_size
      rs = db_query(string.format("SELECT COUNT(*) FROM %s WHERE key2 BETWEEN %d AND %d", table_name, range_start, range_end))
   end

   db_query(commit_query)
end
--]=====]

function event(thread_id)
--[=====[
   print("olap event(): entered")
--]=====]
   local rs
   local table_name
   local range_start
   local range_end

   print("olap-randtile event(): entered")
   table_name = string.format("olaptest%d", sb_rand_uniform(1, olap_tables_count))

   db_query(begin_query)

   range_start = 1
   range_end = olap_range_size
--[=====[
   rs = db_query(string.format("SELECT * FROM %s WHERE key2 BETWEEN %d AND %d", table_name, range_start, range_end))
   rs = db_query(string.format("SELECT COUNT(%s.id) AS tbl_row_count FROM %s WHERE %s.key2 = %d", table_name, table_name, table_name, range_end))
   rs = db_query(string.format("SELECT COUNT(%s.id) AS tbl_row_count FROM %s WHERE %s.nonkey02 = %d", table_name, table_name, table_name, range_end))
   rs = db_query(string.format("SELECT * FROM %s LIMIT %d", table_name, range_end))
   rs = db_query(string.format("SELECT COUNT(*) FROM %s WHERE %s.nonkey02 = %d LIMIT %d", table_name, table_name, range_end, range_end))
--]=====]

   print(string.format("SELECT COUNT(*) FROM %s WHERE %s.nonkey02 < 4294967295 AND id < %d", table_name, table_name, range_end))
   rs = db_query(string.format("SELECT COUNT(*) FROM %s WHERE %s.nonkey02 < 4294967295 AND id < %d", table_name, table_name, range_end))

   db_query(commit_query)

   print("olap-randtile event(): returning")
   return
end
