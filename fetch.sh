
# download / extract data
for i in $(seq -f "%02g" 1 12)
  do
    echo "Downloading 2016-$i trips"
    wget --directory-prefix=data 2016$i-citibike-tripdata.zip https://s3.amazonaws.com/tripdata/2016$i-citibike-tripdata.zip
    echo "Unzipping 2016-$i trips"
    unzip ./data/2016$i-citibike-tripdata.zip -d data
  done

# upload to storage
gsutil mb gs://citi-bike-trips
gsutil -o GSUtil:parallel_composite_upload_threshold=50M -m cp ./data/*.csv gs://citi-bike-trips

# put into bigquery
bq mk trips
read -d '' schema <<- EOF
  tripduration:integer,
  starttime:string,
  stoptime:string,
  start_station_id:integer,
  start_station_name:string,
  start_station_latitude:float,
  start_station_longitude:float,
  end_station_id:integer,
  end_station_name:string,
  end_station_latitude:float,
  end_station_longitude:float,
  bikeid:integer,
  usertype:string,
  birth_year:integer,
  gender:integer
EOF
schema=`echo $schema | tr -d '[[:space:]]'`
bq load --skip_leading_rows=1 trips.trips gs://citi-bike-trips/*.csv $schema

# format start time to timestamp (tricky because multiple formats)
bq query --use_legacy_sql=false --destination_table=trips.trips2 "
  SELECT *, TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S', starttime)) AS starttime_ts
  FROM trips.trips
  WHERE REGEXP_CONTAINS(starttime, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')
  UNION ALL
  SELECT *, TIMESTAMP(starttime) AS starttime_ts
  FROM trips.trips
  WHERE NOT REGEXP_CONTAINS(starttime, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')"

# pull station-to-station rebalances
bq query --use_legacy_sql=false --destination_table=trips.rebalances "
  SELECT last_end_station_id AS from_station, start_station_id AS to_station, COUNT(*) AS n FROM (
    SELECT bikeid, start_station_id, end_station_id, starttime_ts,
      LAG(end_station_id, 1) OVER (PARTITION BY bikeid ORDER BY starttime_ts ASC) last_end_station_id
    FROM trips.trips2
    ORDER BY bikeid, starttime_ts ASC
  ) AS x
  WHERE start_station_id != last_end_station_id
  GROUP BY 1,2
  ORDER BY n DESC"

# pull list of stations with inflows and outflows
bq query --use_legacy_sql=false --destination_table=trips.stations "
  SELECT id, name, lat, lon, SUM(outflow) AS outflow, SUM(inflow) AS inflow
  FROM (
    SELECT id,
      FIRST_VALUE(name) OVER (PARTITION BY id ORDER BY starttime_ts DESC) AS name,
      FIRST_VALUE(lat) OVER (PARTITION BY id ORDER BY starttime_ts DESC) AS lat,
      FIRST_VALUE(lon) OVER (PARTITION BY id ORDER BY starttime_ts DESC) AS lon,
      outflow, inflow
    FROM (
      SELECT start_station_id AS id, start_station_name AS name, start_station_latitude AS lat, start_station_longitude AS lon,
        COUNT(*) AS outflow, 0 AS inflow, MAX(starttime_ts) AS starttime_ts
      FROM trips.trips2 GROUP BY 1,2,3,4
      UNION DISTINCT
      SELECT end_station_id AS id, end_station_name AS name, end_station_latitude AS lat, end_station_longitude AS lon,
        0 AS outflow, COUNT(*) AS inflow, MAX(starttime_ts) AS starttime_ts
      FROM trips.trips2 GROUP BY 1,2,3,4
    ) AS x
  ) AS x
  GROUP BY 1,2,3,4"

# download two new tables
bq extract trips.rebalances gs://citi-bike-trips/outputs/rebalances.csv
bq extract trips.stations gs://citi-bike-trips/outputs/stations.csv
gsutil cp gs://citi-bike-trips/outputs/*.csv ./data/outputs/
