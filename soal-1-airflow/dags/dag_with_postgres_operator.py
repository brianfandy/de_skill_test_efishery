from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

#time zone WIB
local_tz = pendulum.timezone("Asia/Jakarta")

#airflow provider postgres hook config for postgres_dwh
"""
host: host.docker.internal
schema: dwh
password: airflow
port: 5432
"""

#airflow provider postgres hook config for postgres_localhost
"""
host: host.docker.internal
schema: test
password: airflow
port: 5432
"""

#config for dags
default_args = {
    'owner': 'brian',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'tzinfo': local_tz
}

def postgresql_to_tuples(p_hook, p_query):
    """
    Function to transform select query to tupple
    :param p_hook: connection string for airflow provider postgres
    :param p_query: sql query
    return tuple. if db error, it will return 1
    """
    conn = p_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(p_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    #create tuple list of data
    result = tuple([str(r[0]) for r in cursor.fetchall()])
    cursor.close()

    return result


def postgresql_to_dataframe(p_hook, p_query, p_cols):
    """
    Function to transform select query to pandas dataframe
    :param p_hook: connection string for airflow provider postgres
    :param p_query: sql query
    :param p_cols: columns table in db
    return dataframe. if db error, it will return 1
    """
    conn = p_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(p_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    #insert data to tupples
    tupples = cursor.fetchall()
    cursor.close()

    #insert data to pandas from tupples
    df = pd.DataFrame(tupples, columns=p_cols)
    return df


def execute_mogrify(p_hook, df, p_config):
    """
    Function to bulk insert query
    :param p_hook: connection string for airflow provider postgres
    :param df: dataframe for inserting data to db
    :param p_config: config for table and column name
    if db error, it will return 1
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    
    conn = p_hook.get_conn()
    cursor = conn.cursor()

    values = [cursor.mogrify(f"({p_config['cols']})", tup).decode('utf8') for tup in tuples]
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES " % (p_config['table_name'], cols) + ",".join(values)

    try:
        cursor.execute(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute done")
    cursor.close()


def records_exists(p_hook, p_query):
    """
        Function to check if record exist
       :param p_hook: connection string for airflow provider postgres
       :param p_query: sql query
       :return True if record exist, false if record not exist
    """
    conn = p_hook.get_conn()
    try:
        cur = conn.cursor()
        query = "select exists(%s)" % (p_query)

        print(query)
        cur.execute(query)
        exists = cur.fetchone()[0]
    except psycopg2.Error as e:
        exists = False
    return exists


def delete_all_value(p_hook, p_query):
    """
    Function to perform delete data from table
    :param p_hook: connection string for airflow provider postgres
    :param p_query: sql query
    :param p_config: config for table and column name
    if db error, it will return 1
    """
    conn = p_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(p_query)
        rows_deleted = cursor.rowcount
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

def load_data_to_dim_table():
    """
    Function to load dim_customer and dim_table
    """

    # start load data to dim_customer
    #init connection string for airflow provider postgres
    hook_reader = PostgresHook(postgres_conn_id="postgres_localhost") #postgres for transaction db
    hook_writer = PostgresHook(postgres_conn_id="postgres_dwh") #postgres for dwh db

    # get data exist from dim customer and store into tupple
    query = "select id from dim_customer"
    cust_tuple = postgresql_to_tuples(hook_writer, query)

    cols = ["id", "name"]
    # if data exist in tuple, it will execute query with ids in tupple. else execute without ids in tupple
    if len(cust_tuple) > 0:
        query = "select id ,name from customers cc where id not in %s" % str(cust_tuple)
    else:
        query = "select id ,name from customers cc"
    dfs = postgresql_to_dataframe(hook_reader, query, cols)

    #table config for dim_customer
    table_config = {
        "table_name": "dim_customer",
        "cols": "%s,%s"
    }
    execute_mogrify(hook_writer, dfs, table_config)
    # end load data to dim_customer

    # start load data to dim_customer
    cols = ["id", "date", "month", "quarter_of_year", "year", "is_weekend"]
    # get date series in one year on current year
    query = """
    SELECT 
        to_char(day::date , 'YYYYMMDD'), day::date ,to_char(day::date , 'MM') as month,
        EXTRACT (QUARTER FROM day::date) as quarter_of_year,
        to_char(day::date , 'YYYY') as year,
        EXTRACT(ISODOW FROM day::date) IN (6, 7) as is_weekend
    FROM 
	    generate_series(date_trunc('year', now()), date_trunc('year', now()) + interval '1 year' - interval '1 day', '1 day') day;;
    """
    dfs = postgresql_to_dataframe(hook_reader, query, cols)

    # checking if date series exist
    query="""
        select * from dim_date dd where "date" between date_trunc('year', now()) and date_trunc('year', now()) + interval '1 year' - interval '1 day'
    """
    #exist = True
    result_query = records_exists(hook_writer, query)

    #table config for dim_date
    table_config = {
        "table_name": "dim_date",
        "cols": "%s,%s,%s,%s,%s,%s"
    }

    # get date series if result False
    if not result_query:
        execute_mogrify(hook_writer, dfs, table_config)

    # end load data to dim_customer

def load_data_to_fact_table():
    """
    Function to load all fact order accumulating
    """

    #start load data to fact_order_accumulating
    #init connection string for airflow provider postgres
    hook_reader = PostgresHook(postgres_conn_id="postgres_localhost") #postgres for transaction db
    hook_writer = PostgresHook(postgres_conn_id="postgres_dwh") #postgres for dwh db

    # get data exist from fact_order_accumulating
    query = "select order_number  from fact_order_accumulating"
    ids_tupple = postgresql_to_tuples(hook_writer, query)
    
    # if data exist in tuple, it will execute query with ids in tupple. else execute without ids in tupple
    cols = ["order_date_id", "invoice_date_id","payment_date_id","customer_id","order_number","invoice_number","payment_number","total_order_quantity","total_order_usd_amount","order_to_invoice_lag_days","invoice_to_payment_lag_days"]
    if len(ids_tupple) > 0:
        #query = "select id ,name from customers cc where id not in %s" % str(cust_tuple)
        query = """
        select 
            to_char(o."date", 'YYYYMMDD') as order_date_id 
            ,to_char(i."date" , 'YYYYMMDD') as invoice_date_id 
            ,to_char(p."date" , 'YYYYMMDD') as payment_date_id 
            ,c.id as customer_id 
            ,o.order_number
            ,i.invoice_number 
            ,p.payment_number 
            ,coalesce(sum(ol.quantity),0) as total_order_quantity
            ,coalesce(sum(ol.quantity * ol.usd_amount),0) as total_order_usd_amount
            ,coalesce(DATE_PART('day', i."date"::timestamp - o."date"::timestamp),0) as order_to_invoice_lag_days
            ,coalesce(DATE_PART('day', i."date"::timestamp - o."date"::timestamp),0) as invoice_to_payment_lag_days
        from orders o 
        left join invoices i on i.order_number = o.order_number 
        left join payments p on p.invoice_number = i.invoice_number 
        left join customers c on c.id = o.customer_id 
        left join order_lines ol on ol.order_number = o.order_number 
        where o.order_number not in %s
        group by 
            c.id
            ,o.order_number 
            ,i.invoice_number 
            ,p.payment_number 
        """ % str(ids_tupple)
    else:
        #query = "select id ,name from customers cc"
        query = """
        select 
            to_char(o."date", 'YYYYMMDD') as order_date_id 
            ,to_char(i."date" , 'YYYYMMDD') as invoice_date_id 
            ,to_char(p."date" , 'YYYYMMDD') as payment_date_id 
            ,c.id as customer_id 
            ,o.order_number
            ,i.invoice_number 
            ,p.payment_number 
            ,coalesce(sum(ol.quantity),0) as total_order_quantity
            ,coalesce(sum(ol.quantity * ol.usd_amount),0) as total_order_usd_amount
            ,coalesce(DATE_PART('day', i."date"::timestamp - o."date"::timestamp),0) as order_to_invoice_lag_days
            ,coalesce(DATE_PART('day', i."date"::timestamp - o."date"::timestamp),0) as invoice_to_payment_lag_days
        from orders o 
        left join invoices i on i.order_number = o.order_number 
        left join payments p on p.invoice_number = i.invoice_number 
        left join customers c on c.id = o.customer_id 
        left join order_lines ol on ol.order_number = o.order_number 
        group by 
            c.id
            ,o.order_number 
            ,i.invoice_number 
            ,p.payment_number 
        """
    dfs = postgresql_to_dataframe(hook_reader, query, cols)

    #if dataframe is not empty, it will load data to fact_order_accumulating
    if dfs.shape[0] == 0:
        print("no data inserted")
    else:
        #table config for fact_order_accumulating
        table_config = {
            "table_name": "fact_order_accumulating",
            "cols": "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        }
        execute_mogrify(hook_writer, dfs, table_config)
    
    #end load data to fact_order_accumulating
    
def sync_fact_order():
    """
    Function to sync uncompleted data (data without invoice and payment) from fact order accumulating
    """

    #init connection string for airflow provider postgres
    hook_reader = PostgresHook(postgres_conn_id="postgres_localhost") #postgres for transaction db
    hook_writer = PostgresHook(postgres_conn_id="postgres_dwh") #postgres for dwh db

    #get order numbers of uncompleted data from fact_order_accumulating, store to tupple
    query = "select order_number from fact_order_accumulating foa where invoice_date_id is null or payment_date_id is null"
    ids_tupple = postgresql_to_tuples(hook_writer, query)

    # if data exist in tuple, it will delete and reload data from transaction to fact_order_accumulating
    if len(ids_tupple) > 0:
        query = "delete from fact_order_accumulating where order_number in %s" % str(ids_tupple)
        delete_all_value(hook_writer, query)
        cols = ["order_date_id", "invoice_date_id","payment_date_id","customer_id","order_number","invoice_number","payment_number","total_order_quantity","total_order_usd_amount","order_to_invoice_lag_days","invoice_to_payment_lag_days"]
        query = """
        select 
            to_char(o."date", 'YYYYMMDD') as order_date_id 
            ,to_char(i."date" , 'YYYYMMDD') as invoice_date_id 
            ,to_char(p."date" , 'YYYYMMDD') as payment_date_id 
            ,c.id as customer_id 
            ,o.order_number
            ,i.invoice_number 
            ,p.payment_number 
            ,coalesce(sum(ol.quantity),0) as total_order_quantity
            ,coalesce(sum(ol.quantity * ol.usd_amount),0) as total_order_usd_amount
            ,coalesce(DATE_PART('day', i."date"::timestamp - o."date"::timestamp),0) as order_to_invoice_lag_days
            ,coalesce(DATE_PART('day', i."date"::timestamp - o."date"::timestamp),0) as invoice_to_payment_lag_days
        from orders o 
        left join invoices i on i.order_number = o.order_number 
        left join payments p on p.invoice_number = i.invoice_number 
        left join customers c on c.id = o.customer_id 
        left join order_lines ol on ol.order_number = o.order_number 
        where o.order_number in %s
        group by 
            c.id
            ,o.order_number 
            ,i.invoice_number 
            ,p.payment_number 
        """ % str(ids_tupple)
        #table config for fact_order_accumulating
        table_config = {
            "table_name": "fact_order_accumulating",
            "cols": "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        }
        dfs = postgresql_to_dataframe(hook_reader, query, cols)
        execute_mogrify(hook_writer, dfs, table_config)

    

with DAG(
    dag_id='dag_load_to_dwh_v1',
    default_args=default_args,
    start_date=datetime(2022, 8, 11),
    #set schedule for 7AM WIB
    schedule_interval='0 7 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id='create_tb_fact_order_accumulating',
        postgres_conn_id='postgres_dwh', #postgres for dwh db
        #create table if table fact_order_accumulating not exist
        sql="""
            CREATE TABLE IF NOT EXISTS fact_order_accumulating (
                order_date_id int4 NULL,
                invoice_date_id int4 NULL,
                payment_date_id int4 NULL,
                customer_id int4 NULL,
                order_number varchar NULL,
                invoice_number varchar NULL,
                payment_number varchar NULL,
                total_order_quantity int4 NULL,
                total_order_usd_amount numeric NULL,
                order_to_invoice_lag_days int4 NULL,
                invoice_to_payment_lag_days int4 NULL
            );
        """
    )

    task2 = PostgresOperator(
        task_id='create_tb_dim_customer',
        postgres_conn_id='postgres_dwh', #postgres for dwh db
        sql="""
            CREATE TABLE IF NOT EXISTS public.dim_customer (
                id int4 NULL,
                "name" varchar NULL
            );
        """
    )

    task3 = PostgresOperator(
        task_id='create_tb_dim_date',
        postgres_conn_id='postgres_dwh', #postgres for dwh db
        sql="""
            CREATE TABLE IF NOT EXISTS public.dim_date (
                id int4 NULL,
                "date" date NULL,
                "month" int4 NULL,
                quater_of_year int4 NULL,
                "year" int4 NULL,
                is_weekend bool NULL
            );
        """
    )

    task4 = PythonOperator(
        task_id='load_data_to_dim_table',
        python_callable=load_data_to_dim_table
    )
    
    task5 = PythonOperator(
        task_id='load_data_to_fact_table',
        python_callable=load_data_to_fact_table
    )

    task6 = PythonOperator(
        task_id='sync_fact_order',
        python_callable=sync_fact_order
    )
    
    [task2, task3] >> task1 >> task4 >> task5 >> task6