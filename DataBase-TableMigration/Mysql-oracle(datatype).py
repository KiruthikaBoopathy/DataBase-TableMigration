import mysql.connector
import cx_Oracle


def migrate_table(mysql_config, oracle_config, table_name, schema_name, oracle_table_name):
    try:
        # Connect to MySQL
        with mysql.connector.connect(**mysql_config) as mysql_conn, mysql_conn.cursor(dictionary=True) as mysql_cursor:
            # Connect to Oracle
            oracle_conn = cx_Oracle.connect(**oracle_config)
            oracle_cursor = oracle_conn.cursor()

            # Fetch column information from MySQL
            db_schema = mysql_config['database']
            mysql_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{db_schema}' AND TABLE_NAME = '{table_name}'
            """)
            columns_info = mysql_cursor.fetchall()

            # Extract column names, data types, and max lengths
            columns = [col_info['COLUMN_NAME'] for col_info in columns_info]
            data_types = [col_info['DATA_TYPE'] for col_info in columns_info]
            max_lengths = [col_info['CHARACTER_MAXIMUM_LENGTH'] for col_info in columns_info]

            # Connect to Oracle as SYSDBA to grant quota on 'USERS' tablespace
            sys_conn = cx_Oracle.connect(user=oracle_config['user'],
                                         password=oracle_config['password'],
                                         dsn=oracle_config['dsn'],
                                         mode=cx_Oracle.SYSDBA)
            sys_cursor = sys_conn.cursor()

            # Grant quota on 'USERS' tablespace
            sys_cursor.execute(f"ALTER USER {schema_name} QUOTA UNLIMITED ON USERS")
            sys_conn.commit()

            # Create Oracle table with correct data types
            column_definitions = []
            for col, data_type, max_length in zip(columns, data_types, max_lengths):
                oracle_data_type = data_type.upper()
                if 'CHAR' in oracle_data_type:
                    oracle_data_type = 'VARCHAR'

                elif 'INT' in oracle_data_type:
                    oracle_data_type = 'NUMBER'

                elif 'FLOAT' in oracle_data_type:
                    oracle_data_type = 'FLOAT'

                column_definitions.append(f'"{col}" {oracle_data_type}')

            create_table_query = f"CREATE TABLE {schema_name}.{oracle_table_name} ({', '.join(column_definitions)})"
            print("Debugging: CREATE TABLE Query:", create_table_query)
            oracle_cursor.execute(create_table_query)

            # Fetch all rows in MySQL
            mysql_cursor.execute(f"SELECT * FROM {table_name}")
            data_to_insert = mysql_cursor.fetchall()
            print('data_to_insert:', data_to_insert)

            placeholders = ','.join([':' + col.upper() for col in columns])
            column_names_quoted = [f'"{col}"' for col in columns]
            insert_query = f"INSERT INTO {schema_name}.{oracle_table_name} ({','.join(column_names_quoted)}) " \
                           f"VALUES ({placeholders})"

            print("Debugging: INSERT Query:", insert_query)

            for row in data_to_insert:
                oracle_cursor.execute(insert_query, row)

            oracle_conn.commit()
            print("Data inserted successfully")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Oracle DatabaseError:", error.message)
    except Exception as e:
        print(f"Error: {e}")


# Your configurations
mysql_config = {
    'user': 'root',
    'password': 'vrdella!6',
    'host': 'localhost',
    'database': 'ats',
}

oracle_config = {
    'user': 'system',
    'password': 'vrdella!6',
    'dsn': 'localhost:1521/ORCL'
}

mysql_table_name = ['time_sheet']
schema_name = 'C##NEWSHCEMA'

for table_name in mysql_table_name:
    oracle_table_name = table_name
    migrate_table(mysql_config, oracle_config, table_name, schema_name, oracle_table_name)

