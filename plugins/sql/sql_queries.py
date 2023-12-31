class SqlQueries:
    create_staging_tables = [
        '''
        CREATE TABLE IF NOT EXISTS staging_trips (
            vendor_id bigint,
            tpep_pickup_datetime timestamp,
            tpep_dropoff_datetime timestamp,
            passenger_count float,
            trip_distance float,
            rate_code_id float,
            store_and_fwd_flag char(10),
            pu_location_id bigint,
            do_location_id bigint,
            payment_type bigint,
            fare_amount float,
            extra float,
            mta_tax float,
            tip_amount float,
            tolls_amount float,
            improvement_surcharge float,
            total_amount float,
            congestion_surcharge float,
            airport_fee float
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS staging_locations (
            location_id integer,
            borough varchar(256),
            zone varchar(256),
            service_zone varchar(256)
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS staging_rate_codes (
            rate_code_id integer,
            rate_code_desc varchar(256)
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS staging_payment_types (
            payment_type_id integer,
            payment_type_desc varchar(256)
            );
        '''
    ]
    create_dim_tables = [
        '''
        CREATE TABLE IF NOT EXISTS dim_rate_codes (
            rate_code_id integer,
            rate_code_desc varchar(256)
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS dim_payment_types (
            payment_type_id integer,
            payment_type_desc varchar(256)
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS dim_locations (
            location_id integer,
            borough varchar(256),
            zone varchar(256),
            service_zone varchar(256)
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS dim_time (
            timestamp timestamp,
            date date,
            hour integer,
            day integer,
            week integer,
            month integer,
            year integer,
            weekday varchar(30)
        );
        '''
    ]
    create_fact_table = '''
        CREATE TABLE IF NOT EXISTS fact_trips (
            fct_id varchar(256),
            vendor_id integer,
            pickup_timestamp timestamp,
            dropoff_timestamp timestamp,
            passenger_count integer,
            trip_distance numeric(12,2),
            rate_code_id integer,
            store_and_fwd_flag varchar(1),
            pickup_location_id integer,
            dropoff_location_id integer,
            payment_type integer,
            if_refund boolean,
            fare_amount numeric(12,2),
            extra_amount numeric(12,2),
            mta_tax_amount numeric(12,2),
            tip_amount numeric(12,2),
            tolls_amount numeric(12,2),
            improvement_surcharge_amount numeric(12,2),
            total_amount numeric(12,2),
            congestion_surcharge_amount numeric(12,2),
            airport_fee numeric(12,2)
        );
        '''
    create_mart_table = '''
        CREATE TABLE IF NOT EXISTS mart_trips_hourly (
            vendor varchar(256),
            pickup_date date,
            pickup_hour integer,
            dropoff_date date,
            dropoff_hour integer,
            rate_code varchar(256),
            store_and_fwd_flag varchar(1),
            pickup_borough varchar(256),
            pickup_zone varchar(256),
            pickup_service_zone varchar(256),
            dropoff_borough varchar(256),
            dropoff_zone varchar(256),
            dropoff_service_zone varchar(256),
            payment_type varchar(256),
            if_refund boolean,
            passenger_count integer,
            trip_distance numeric(30,2),
            fare_amount numeric(30,2),
            extra_amount numeric(30,2),
            mta_tax_amount numeric(30,2),
            tip_amount numeric(30,2),
            tolls_amount numeric(30,2),
            improvement_surcharge_amount numeric(30,2),
            total_amount numeric(30,2),
            congestion_surcharge_amount numeric(30,2),
            airport_fee numeric(30,2)
        );
        '''
    insert_dim_tables = {
        "dim_rate_codes":
            '''
                SELECT
                    rate_code_id
                    ,rate_code_desc
                FROM staging_rate_codes;
            '''
        ,
        "dim_payment_types":
        '''
            SELECT
                payment_type_id
                ,payment_type_desc
            FROM staging_payment_types;
        '''
        ,
        "dim_locations":
        '''
        SELECT
            location_id
            ,borough
            ,zone
            ,service_zone
        FROM staging_locations;
        '''
        ,
        "dim_time":
        '''
            SELECT DISTINCT
                tpep_pickup_datetime
                ,date(tpep_pickup_datetime)
                ,extract(hour from tpep_pickup_datetime)
                ,extract(day from tpep_pickup_datetime)
                ,extract(week from tpep_pickup_datetime)
                ,extract(month from tpep_pickup_datetime)
                ,extract(year from tpep_pickup_datetime)
                ,extract(dayofweek from tpep_pickup_datetime)
            FROM staging_trips
            
            UNION
            
            SELECT DISTINCT
                tpep_dropoff_datetime
                ,date(tpep_dropoff_datetime)
                ,extract(hour from tpep_dropoff_datetime)
                ,extract(day from tpep_dropoff_datetime)
                ,extract(week from tpep_dropoff_datetime)
                ,extract(month from tpep_dropoff_datetime)
                ,extract(year from tpep_dropoff_datetime)
                ,extract(dayofweek from tpep_dropoff_datetime)
            FROM staging_trips;
        '''
        
    }
    
    insert_fact_table = '''
        SELECT DISTINCT
            md5(vendor_id || tpep_pickup_datetime || tpep_dropoff_datetime || pu_location_id || do_location_id 
                || total_amount || trip_distance || passenger_count || rate_code_id || store_and_fwd_flag || payment_type || case
                when total_amount < 0 then 1
                else 0
            end)
            ,vendor_id
            ,tpep_pickup_datetime
            ,tpep_dropoff_datetime
            ,passenger_count
            ,trip_distance
            ,rate_code_id
            ,store_and_fwd_flag
            ,pu_location_id
            ,do_location_id
            ,payment_type
            ,case
                when total_amount < 0 then 1
                else 0
            end
            ,fare_amount
            ,extra
            ,mta_tax
            ,tip_amount
            ,tolls_amount
            ,improvement_surcharge
            ,total_amount
            ,congestion_surcharge
            ,airport_fee
        FROM staging_trips
        WHERE tpep_pickup_datetime >= '{interval_start}'
        AND tpep_dropoff_datetime < '{interval_end}'
        AND passenger_count is not null
        and rate_code_id is not null
        and store_and_fwd_flag is not null;
        '''
    insert_mart_table = """
        SELECT
            case
                when fact_trips.vendor_id = 1 then 'Creative Mobile Technologies, LCC'
                when fact_trips.vendor_id = 2 then 'VeriFone Inc.'
            else 'Unknown'
             end
            ,pu_time.date
            ,pu_time.hour
            ,do_time.date
            ,do_time.hour
            ,dim_rate_codes.rate_code_desc
            ,fact_trips.store_and_fwd_flag
            ,pu.borough
            ,pu.zone
            ,pu.service_zone
            ,dof.borough
            ,dof.zone
            ,dof.service_zone
            ,dim_payment_types.payment_type_desc
            ,fact_trips.if_refund
            ,sum(fact_trips.passenger_count)
            ,sum(fact_trips.trip_distance)
            ,sum(fact_trips.fare_amount)
            ,sum(fact_trips.extra_amount)
            ,sum(fact_trips.mta_tax_amount)
            ,sum(fact_trips.tip_amount)
            ,sum(fact_trips.tolls_amount)
            ,sum(fact_trips.improvement_surcharge_amount)
            ,sum(fact_trips.total_amount)
            ,sum(fact_trips.congestion_surcharge_amount)
            ,sum(fact_trips.airport_fee)
        FROM fact_trips
        JOIN dim_rate_codes ON fact_trips.rate_code_id = dim_rate_codes.rate_code_id
        JOIN dim_payment_types ON fact_trips.payment_type = dim_payment_types.payment_type_id
        JOIN dim_locations pu ON fact_trips.pickup_location_id = pu.location_id
        JOIN dim_locations dof ON fact_trips.dropoff_location_id = dof.location_id
        JOIN dim_time pu_time ON fact_trips.pickup_timestamp = pu_time.timestamp
        JOIN dim_time do_time ON fact_trips.dropoff_timestamp = do_time.timestamp
        WHERE fact_trips.pickup_timestamp >= '{interval_start}'
        AND fact_trips.dropoff_timestamp < '{interval_end}'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;
        """
    