import psycopg2
import sys
from datetime import datetime, timedelta
from trx_order_detail_query import query_pararelize_services
import sys
import time


start_time = time.time()  # to check time execute


# environment variable call
today_date = (datetime.today().date() - timedelta(days = 0)).strftime("%Y-%m-%d")
stagging_table = 'put here'
destination_table = 'put here'
s3path = 'put here'
iam_role = 'put here'
destination_column_primary = 'put here'
stagging_column_primary = 'put here'
db_host = 'put here'
db_users = 'put here'
db_password = 'put here'
db_name = 'put here'


# define connection to db function
def execute_query(sql_statement):
    try:
        conn = psycopg2.connect(
                host=db_host,
                port='5439',
                user=db_users,
                password=db_password,
                database=db_name
            )
        cur = conn.cursor()
        cur.execute("""{sql_statement}""".format(sql_statement=sql_statement))
        conn.commit()
        cur.close()
        print(f'process is done')

    except psycopg2.DatabaseError as error:
        print("something error = ", error)
        conn.rollback()
    finally:
        if conn is not None:
            conn.close()    


# define execution function
def unload_data ():
    try:
        execute_query(
            sql_statement= query_pararelize_services.query_unloadData_to_s3.format(
                ingestion_date=today_date,
                s3path=s3path,
                iam_role=iam_role
            )        
        )
    except:
        print ( 'Error in process unload data to s3 \n', sys.exc_info() )

    else:
        print ( 'Success unload data to s3' )


def create_table_stagging ():
    try:
        execute_query(
            sql_statement= query_pararelize_services.query_create_table_stagging.format(
                stagging_table=stagging_table,
                destination_table=destination_table
            )        
        )
    except:
        print ( 'Error in process create_table_stagging \n', sys.exc_info() )

    else:
        print ( 'Success create_table_stagging' )


def loadData_to_stagging ():
    try:
        execute_query(
            sql_statement= query_pararelize_services.query_loadData_to_stagging.format(
                ingestion_date=today_date,
                stagging_table=stagging_table,
                s3path=s3path,
                iam_role=iam_role
            )        
        )
    except:
        print ( 'Error in process loadData_to_stagging \n', sys.exc_info() )

    else:
        print ( 'Success loadData_to_stagging' )


def delete_conflict_dest_stagging ():
    try:
        execute_query(
            sql_statement= query_pararelize_services.query_delete_conflict_dest_stagging.format(
                destination_table=destination_table,
                stagging_table=stagging_table,
                destination_column_primary=destination_column_primary,
                stagging_column_primary=stagging_column_primary
            )        
        )
    except:
        print ( 'Error in process delete_conflict_dest_stagging \n', sys.exc_info() )

    else:
        print ( 'Success delete_conflict_dest_stagging' )


def insert_dest_from_stagging ():
    try:
        execute_query(
            sql_statement= query_pararelize_services.query_insert_dest_from_stagging.format(
                destination_table=destination_table,
                stagging_table=stagging_table
            )        
        )
    except:
        print ( 'Error in process insert_dest_from_stagging \n', sys.exc_info() )

    else:
        print ( 'Success insert_dest_from_stagging' )


def drop_stagging ():
    try:
        execute_query(
            sql_statement= query_pararelize_services.query_drop_stagging.format(
                stagging_table=stagging_table
            )        
        )
    except:
        print ( 'Error in process drop_stagging \n', sys.exc_info() )

    else:
        print ( 'Success drop_stagging' )


# call all function to running
def running_allfunction_deduplication():

    try:

        unload_data ()
        create_table_stagging ()
        loadData_to_stagging ()
        delete_conflict_dest_stagging ()
        insert_dest_from_stagging ()
        # drop_stagging ()

    except:
        print( 'Error ingestion covering data in : \n', sys.exc_info() )
    else:
        print( 'Success ingestion data covering all process' )



if __name__ == '__main__':
    
    try:
        # final running function
        running_allfunction_deduplication()

    except:
        print('Error ingestion in : \n', sys.exc_info())
    else:
        print('Success ingestion data')

    print("--- %s seconds ---" % (time.time() - start_time))
