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

   table_name = "olaptest" .. table_id

   print("Creating table " .. table_name .. "...")
   query = [[CREATE TABLE ]] .. table_name .. [[ (
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
    pad TEXT
) ENGINE=]] .. mysql_table_engine ..  (mysql_table_options or "")

    db_query(query)

    db_query("CREATE INDEX k_" .. table_id .. " on " .. table_name .. "(key1,key2,key3)")

    print("Inserting " .. olap_table_size .. " records into " .. table_name .. "'")

    stmt = db_prepare([[INSERT INTO ]] .. table_name .. [[ SET key1=?, key2=?, key3=?, nonkey01=?, nonkey02=?, nonkey03=?, nonkey04=?, nonkey05=?, nonkey06=?, nonkey07=?, nonkey08=?, nonkey09=?, nonkey10=?, nonkey11=?, nonkey12=?, nonkey13=?, nonkey14=?, nonkey15=?, nonkey16=?, nonkey17=?, nonkey18=?, nonkey19=?, nonkey20=?, nonkey21=?, nonkey22=?, nonkey23=?, nonkey24=?, nonkey25=?, nonkey26=?, nonkey27=?, nonkey28=?, nonkey29=?, nonkey30=?]])
    params = {}
    for i = 1,33 do
        params[i] = 1
    end
    db_bind_param(stmt, params)
 
    for i = 1, olap_table_size do
        for j = 1, 3 do
            params[j] = sb_rand(1, olap_table_size)
            params[j] = sb_rand(1, olap_table_size)
            params[j] = sb_rand(1, olap_table_size)
        end
        for j = 4, 33 do
            params[j] = sb_rand_str([[###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###########-###]])
        end
        rs = db_execute(stmt)
    end
end

function prepare()
--[=====[
   print("olap prepare(): entered")
--]=====]

   local query
   local i
   local j

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

   set_vars()

   for i = 1, olap_tables_count do
       print("Dropping table 'olaptest" .. i .. "'...")
       db_query("DROP TABLE olaptest".. i )
   end
end

function set_vars()
--[=====[
   print("olap set_vars(): entered")
--]=====]
   olap_table_size = olap_table_size or 10000
   olap_range_size = olap_range_size or 100
   olap_tables_count = olap_tables_count or 1
   olap_simple_indexed_ranges = olap_simple_ranges or 1
   olap_simple_unindexed_ranges = olap_simple_ranges or 0
   olap_count_indexed_ranges = olap_count_indexed_ranges or 1
   olap_count_unindexed_ranges = olap_count_unindexed_ranges or 1
end

function thread_init(thread_id)
   print("olap thread_init(): entered")
--[=====[
   set_vars()
   begin_query = "BEGIN"
   commit_query = "COMMIT"
--]=====]
end

function event(thread_id)
   print("olap event(): entered")
--[=====[
   local rs
   local i
   local table_name
   local range_start
   local range_end

   table_name = "olaptest" .. sb_rand_uniform(1, olap_tables_count)
   db_query(begin_query)

   for i = 1, olap_simple_indexed_ranges do
      range_start = sb_rand(1, olap_table_size)
      range_end = range_start + olap_range_size
      rs = db_query("SELECT * FROM " .. table_name .. " WHERE key1 BETWEEN " .. range_start .. " AND " .. range_end)
   end

   for i = 1, olap_simple_unindexed_ranges do
      range_start = sb_rand(1, olap_table_size)
      range_end = range_start + olap_range_size
      rs = db_query("SELECT * FROM " .. table_name .. " WHERE key2 BETWEEN " .. range_start .. " AND " .. range_end)
   end

   for i = 1, olap_count_indexed_ranges do
      range_start = sb_rand(1, olap_table_size)
      range_end = range_start + olap_range_size
      rs = db_query("SELECT COUNT(*) FROM " .. table_name .. " WHERE key1 BETWEEN " .. range_start .. " AND " .. range_end)
   end

   for i = 1, olap_count_unindexed_ranges do
      range_start = sb_rand(1, olap_table_size)
      range_end = range_start + olap_range_size
      rs = db_query("SELECT COUNT(*) FROM " .. table_name .. " WHERE key2 BETWEEN " .. range_start .. " AND " .. range_end)
   end

   db_query(commit_query)
--]=====]
end
