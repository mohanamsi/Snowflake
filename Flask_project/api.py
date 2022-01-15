import pandas as pd
import snowflake.connector as sf
import json, os
from flask import Flask, request

app = Flask(__name__)

snowflake_username ='Mohanamsi'
snowflake_password ='M0hanamsi'
snowflake_account ='KT12090.us-east-2.aws'

snowflake_warehouse = 'COMPUTE_WH'
database_name = 'SNOWFLAKE_SAMPLE_DATA'
schema_name = 'TPCH_SF1'
snowflake_role = 'ACCOUNTADMIN'


# connection with snowflake
conn = sf.connect(user=snowflake_username,
                password=snowflake_password,
                account=snowflake_account)


def run_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


try:
    warehouse_sql = 'use warehouse {}'.format(snowflake_warehouse)
    run_query(conn, warehouse_sql)

    try:
        sql = 'alter warehouse {} resume'.format(snowflake_warehouse)
        run_query(conn, sql)
    except:
        pass

    sql = 'use database {}'.format(database_name)
    run_query(conn, sql)

    sql = 'use role {}'.format(snowflake_role)
    run_query(conn, sql)

    sql = f'use schema {schema_name}'
    run_query(conn, sql)

except Exception as e:
    print(e)

def lineitem_data(shipmode, conn):
    global df
    sql = f"""select * 
            from LINEITEM
            where upper(L_SHIPMODE) = upper('{shipmode}') limit 200 ;"""
    df = pd.read_sql(sql, conn)
    # data processing
    # Fetch only those records who have Quantity greater than 30
    df_processed = df[df['L_QUANTITY'] > 30]
    # print(df_processed)
    return {"data":json.loads(df_processed.to_json(orient='records'))}

#get
def all_records():
    query = f"""select * from LINEITEM limit 10 ;"""
    df = pd.read_sql(query,conn)
    return {"records":json.loads(df.to_json(orient='records'))}



@app.route('/linedata' ,methods=['GET','POST'])
def api_fun():
    if request.method == 'POST':
        input_condition = request.get_json()
        if 'mode' in input_condition.keys():
            value = str(input_condition['mode'])
            result = lineitem_data(value, conn)
        else:
            return "Error : No shipmode field is provided. Please specify a shipmode to filter the data."

        return result
    elif request.method == 'GET':
        all = all_records()
        return all



if __name__ == '__main__':
    app.run(debug=True)