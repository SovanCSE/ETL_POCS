from flask import Flask, jsonify, request 
from dotenv import load_dotenv
import snowflake.connector as sf
import os 

## Load parameters from .env file
load_dotenv()

# Create an flask app
app = Flask(__name__)

snowflake_account = 'ri36021.ap-south-1'
snowflake_warehouse = 'COMPUTE_WH'
snowflake_database = 'SAMPLE_DB'
snowflake_schema ='SAMPLE_SCHEMA'
snowflake_user = os.getenv('snowflake_user')
snowflake_password = os.getenv('snowflake_password')
# snowflake_role = os.getenv('snowflake_role')

def snowflake_connector():
  # Connect to Snowflake
    return  sf.connect(
      user=snowflake_user,
      password=snowflake_password,
      account=snowflake_account,
    #   role=snowflake_role,
      warehouse=snowflake_warehouse,
      database=snowflake_database,
      schema=snowflake_schema
    )

def query_to_snowflake(sql_query):
    #Query to snowflake
    try:
        connection = snowflake_connector()
        cursor = connection.cursor()
        cursor.execute(sql_query)

        # Fetch all rows as a list of dictionaries
        rows = cursor.fetchall()
        column_names = [column[0] for column in cursor.description]
        result = [dict(zip(column_names, row)) for row in rows]

        return result
    
    except Exception as e:
        print(f'ERROR While invoking query to snowflake:: {e}')
        return jsonify({'Message': 'Internal server error', 'Status': 500})
    finally:
        if connection:
            connection.close()


@app.route('/posts', methods = ['GET', 'POST']) 
def home(): 
    try:
        if request.method == 'GET':
            extract_type = request.args.get('extract_type')
            user_id = request.args.get('user_id')
            query = "SELECT USER_ID, POST_ID, POST_MESSAGE, CURRENT_FLAG FROM SAMPLE_DB.SAMPLE_SCHEMA.USER_POST_TARGET "
            add_on_query = ''
            if user_id:
                add_on_query += f"WHERE USER_ID = {int(user_id)} "

            if extract_type and extract_type.lower() == 'latest':
                if add_on_query:
                    add_on_query += "AND CURRENT_FLAG='Y' "
                else:
                    add_on_query += "WHERE CURRENT_FLAG='Y' "
            final_query = query + add_on_query + "ORDER BY CURRENT_FLAG DESC;"
            print(f'final_query:: {final_query}')
            query_results = query_to_snowflake(final_query)
            return jsonify({'result': query_results, 'Status': 200})
    except Exception as e:
        return jsonify({'Message': 'Internal server error', 'Status': 500})
    

# driver function 
if __name__ == '__main__': 
    app.run(debug = True) ## http://127.0.0.1:5000