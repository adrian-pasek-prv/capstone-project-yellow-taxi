class SqlQueries:
    create_staging_tables = [
        '''
        CREATE TABLE IF NOT EXISTS staging_trips (
            vendor_id bigint NOT NULL,
            tpep_pickup_datetime timestamp NOT NULL,
            tpep_dropoff_datetime timestamp NOT NULL,
            passenger_count bigint NOT NULL,
            trip_distance float NOT NULL,
            rate_code_id bigint NOT NULL,
            store_and_fwd_flag char(10) NOT NULL,
            pu_location_id bigint NOT NULL,
            do_location_id bigint NOT NULL,
            payment_type bigint NOT NULL,
            fare_amount float NOT NULL,
            extra float NOT NULL,
            mta_tax float NOT NULL,
            tip_amount float NOT NULL,
            tolls_amount float NOT NULL,
            improvement_surcharge float NOT NULL,
            total_amount float NOT NULL,
            congestion_surcharge float,
            airport_fee float
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS staging_locations (
            location_id integer NOT NULL,
            borough varchar(256) NOT NULL,
            zone varchar(256) NOT NULL,
            service_zone varchar(256) NOT NULL
            );
        '''
    ]
    create_dim_tables = [
        '''
        CREATE TABLE IF NOT EXISTS dim_rate_codes (
            rate_code_id integer NOT NULL,
            rate_code_desc varchar(256) NOT NULL
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS dim_payment_types (
            payment_type_id integer NOT NULL,
            payment_type_desc varchar(256) NOT NULL
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS dim_locations (
            location_id integer NOT NULL,
            borough varchar(256) NOT NULL,
            zone varchar(256) NOT NULL,
            service_zone varchar(256) NOT NULL
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS dim_time (
            timestamp timestamp NOT NULL,
            hour integer NOT NULL,
            date date NOT NULL,
            day integer NOT NULL,
            week integer NOT NULL,
            month integer NOT NULL,
            year integer NOT NULL,
            weekday varchar(30) NOT NULL
        );
        '''
    ]
    create_fact_table = [
        '''
        CREATE TABLE IF NOT EXISTS fact_trips (
            fct_id varchar(256) NOT NULL,
            vendor_id integer NOT NULL,
            pickup_timestamp timestamp NOT NULL,
            dropoff_timestamp timestamp NOT NULL,
            passenger_count integer NOT NULL,
            trip_distance numeric(12,2) NOT NULL,
            rate_code_id integer NOT NULL,
            store_and_fwd_flag varchar(1) NOT NULL,
            pickup_location_id integer NOT NULL,
            dropoff_location_id integer NOT NULL,
            payment_type integer NOT NULL,
            fare_amount numeric(12,2) NOT NULL,
            extra_amount numeric(12,2) NOT NULL,
            mta_tax_amount numeric(12,2) NOT NULL,
            tip_amount numeric(12,2) NOT NULL,
            tolls_amount numeric(12,2) NOT NULL,
            improvement_surcharge_amount numeric(12,2) NOT NULL,
            total_amount numeric(12,2) NOT NULL,
            congestion_surcharge_amount numeric(12,2),
            airport_fee numeric(12,2)
        );
        '''
    ]
    create_mart_table = [
        '''
        CREATE TABLE IF NOT EXISTS mart_trips_hourly (
            vendor varchar(256) NOT NULL,
            pickup_date date NOT NULL,
            pickup_hour integer NOT NULL,
            dropoff_date date NOT NULL,
            dropoff_hour integer NOT NULL,
            rate_code varchar(256) NOT NULL,
            store_and_fwd_flag varchar(1) NOT NULL,
            pickup_borough varchar(256) NOT NULL,
            pickup_zone varchar(256) NOT NULL,
            pickup_service_zone varchar(256) NOT NULL,
            dropoff_borough varchar(256) NOT NULL,
            dropoff_zone varchar(256) NOT NULL,
            dropoff_service_zone varchar(256) NOT NULL,
            payment_type varchar(256) NOT NULL,
            passenger_count integer NOT NULL,
            trip_distance numeric(30,2) NOT NULL,
            fare_amount numeric(30,2) NOT NULL,
            extra_amount numeric(30,2) NOT NULL,
            mta_tax_amount numeric(30,2) NOT NULL,
            tip_amount numeric(30,2) NOT NULL,
            tolls_amount numeric(30,2) NOT NULL,
            improvement_surcharge_amount numeric(30,2) NOT NULL,
            total_amount numeric(30,2) NOT NULL,
            congestion_surcharge_amount numeric(30,2),
            airport_fee numeric(30,2)
        );
        '''
        ]
    insert_dim_tables = [
        {"dim_rate_codes":
            '''
                ('1', 'Standard rate'),
                ('2', 'JFK'),
                ('3', 'Newark'),
                ('4', 'Nassau or Westchester'),
                ('5', 'Negotiated fare'),
                ('6', 'Group ride');
            '''
        },
        {"dim_payment_types":
        '''
            ('1', 'Credit card'),
            ('2', 'Cash'),
            ('3', 'No charge'),
            ('4', 'Dispute'),
            ('5', 'Unknown'),
            ('6', 'Voided trip');
        '''
        },
        {"dim_locations":
        '''
        SELECT
            locationid
            ,borough
            ,zone
            ,service_zone
        FROM staging_locations;
        '''
        },
        {"dim_time":
        '''
        SELECT DISTINCT
            tpep_pickup_datetime
            ,extract(hour from tpep_pickup_datetime)
            ,date_trunc('day', tpep_pickup_datetime)
            ,extract(day from tpep_pickup_datetime)
            ,extract(week from tpep_pickup_datetime)
            ,extract(month from tpep_pickup_datetime)
            ,extract(year from tpep_pickup_datetime)
            ,extract(dayofweek from tpep_pickup_datetime)
        FROM staging_trips
        
        UNION
        
        SELECT DISTINCT
            tpep_dropoff_datetime
            ,extract(hour from tpep_dropoff_datetime)
            ,date_trunc('day', tpep_dropoff_datetime)
            ,extract(day from tpep_dropoff_datetime)
            ,extract(week from tpep_dropoff_datetime)
            ,extract(month from tpep_dropoff_datetime)
            ,extract(year from tpep_dropoff_datetime)
            ,extract(dayofweek from tpep_dropoff_datetime)
        FROM staging_trips;
        '''
        }
    ]
    insert_fact_table = [
        '''
        SELECT
            md5(vendor_id || tpep_pickup_datetime || pu_location_id || do_location_id)
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
            ,fare_amount
            ,extra
            ,mta_tax
            ,tip_amount
            ,tolls_amount
            ,improvement_surcharge
            ,total_amount
            ,congestion_surcharge
            ,airport_fee
        FROM staging_trips;
        '''
    ]
    insert_mart_table = [
        '''
        SELECT
            case
                when fact_trips.vendor_id = '1' then 'Creative Mobile Technologies, LCC'
                when fact_trips.vendor_id = '2' then 'VeriFone Inc.'
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
            ,do.borough
            ,do.zone
            ,do.service_zone
            ,dim_payment_types.payment_type_desc
            ,sum(fact_trips.passenger_count)
            ,sum(fact_trips.trip_distance)
            ,sum(fact_trips.fare_amount)
            ,sum(fact_trips.extra)
            ,sum(fact_trips.mta_tax)
            ,sum(fact_trips.tip_amount)
            ,sum(fact_trips.tolls_amount)
            ,sum(fact_trips.improvement_surcharge)
            ,sum(fact_trips.total_amount)
            ,sum(fact_trips.congestion_surcharge)
            ,sum(fact_trips.airport_fee)
        FROM fact_trips
        LEFT JOIN dim_rate_codes ON fact_trips.rate_code_id = dim_rate_codes.rate_code_id
        LEFT JOIN dim_payment_types ON fact_trips.payment_type = dim_payment_types.payment_type_id
        LEFT JOIN dim_locations pu ON fact_trips.pickup_location_id = pu.location_id
        LEFT JOIN dim_locations do ON fact_trips.dropoff_location_id = do.location_id
        LEFT JOIN dim_time pu_time ON fact_trips.pickup_timestamp = pu_time.timestamp
        LEFT JOIN dim_time do_time ON fact_trips.dropoff_timestamp = do_time.timestamp
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;
        '''
    ]
    