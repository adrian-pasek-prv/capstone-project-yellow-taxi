class SqlQueries:
    create_staging_tables = [
        '''
        CREATE TABLE IF NOT EXISTS staging_trips (
            vendor_id integer NOT NULL,
            tpep_pickup_datetime timestamp NOT NULL,
            tpep_dropoff_datetime timestamp NOT NULL,
            passenger_count integer NOT NULL,
            trip_distance numeric(12,2) NOT NULL,
            rate_code_id integer NOT NULL,
            store_and_fwd_flag varchar(1) NOT NULL,
            pu_location_id integer NOT NULL,
            do_location_id integer NOT NULL,
            payment_type integer NOT NULL,
            fare_amount numeric(12,2) NOT NULL,
            extra numeric(12,2) NOT NULL,
            mta_tax numeric(12,2) NOT NULL,
            tip_amount numeric(12,2) NOT NULL,
            tolls_amount numeric(12,2) NOT NULL,
            improvement_surcharge numeric(12,2) NOT NULL,
            total_amount numeric(12,2) NOT NULL,
            congestion_surcharge numeric(12,2),
            airport_fee numeric(12,2)
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS staging_locations (
            location_id integer NOT NULL,
            borough varchar(30) NOT NULL,
            zone varchar(30) NOT NULL,
            service_zone varchar(30) NOT NULL
            );
        '''
    ]
    create_dim_tables = [
        '''
        CREATE TABLE IF NOT EXISTS dim_rate_codes (
            rate_code_id integer NOT NULL,
            rate_code_desc varchar(30) NOT NULL
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS dim_payment_types (
            payment_type_id integer NOT NULL,
            payment_type_desc varchar(30) NOT NULL
            );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS dim_locations (
            location_id integer NOT NULL,
            borough varchar(30) NOT NULL,
            zone varchar(30) NOT NULL,
            service_zone varchar(30) NOT NULL
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
            mart_record_id varchar(256) NOT NULL,
            vendor varchar(30) NOT NULL,
            pickup_datetime timestamp NOT NULL,
            pickup_date date NOT NULL,
            pickup_hour integer NOT NULL,
            dropoff_datetime timestamp NOT NULL,
            dropoff_date date NOT NULL,
            dropoff_hour integer NOT NULL,
            passenger_count integer NOT NULL,
            trip_distance numeric(30,2) NOT NULL,
            rate_code varchar(30) NOT NULL,
            store_and_fwd_flag varchar(1) NOT NULL,
            pickup_borough varchar(30) NOT NULL,
            pickup_zone varchar(30) NOT NULL,
            pickup_service_zone varchar(30) NOT NULL,
            dropoff_borough varchar(30) NOT NULL,
            dropoff_zone varchar(30) NOT NULL,
            dropoff_service_zone varchar(30) NOT NULL,
            payment_type varchar(30) NOT NULL,
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