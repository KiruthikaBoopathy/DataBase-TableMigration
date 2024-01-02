import psycopg2
import mysql.connector


def migrate_table(mysql_config, postgresql_config, table_name, schema_name, postgresql_table_name):
    try:
        # connect to mysql
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # Connect to PostgreSQL
        postgresql_conn = psycopg2.connect(**postgresql_config)
        postgresql_cursor = postgresql_conn.cursor()

        # Fetch column information from MySQL using CHARACTER_OCTET_LENGTH
        db_schema = mysql_config['database']
        mysql_cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_OCTET_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{db_schema}' AND TABLE_NAME = '{table_name}'
        """)
        DB_info = mysql_cursor.fetchall()

        # Extract column names, data types, and max lengths
        columns = [col_info['COLUMN_NAME'] for col_info in DB_info]
        data_types = [col_info['DATA_TYPE'] for col_info in DB_info]
        max_lengths = [col_info['CHARACTER_OCTET_LENGTH'] for col_info in DB_info]

        # Create PostgreSQL table with correct data types
        column_definitions = []
        for col, data_type, max_length in zip(columns, data_types, max_lengths):
            postgresql_data_type = data_type.upper()
            if 'CHAR' in postgresql_data_type:
                postgresql_data_type = 'VARCHAR'

            elif 'INT' in postgresql_data_type:
                postgresql_data_type = 'INTEGER'

            elif 'FLOAT' in postgresql_data_type:
                postgresql_data_type = 'FLOAT'

            column_definitions.append(f'"{col}" {postgresql_data_type}')

        create_table_query = f"CREATE TABLE {schema_name}.\"{postgresql_table_name}\" ({', '.join(column_definitions)})"
        postgresql_cursor.execute(create_table_query)

        # Fetch all rows in MySQL
        mysql_cursor.execute(f"SELECT * FROM {table_name}")
        data_to_insert = mysql_cursor.fetchall()

        placeholders = ','.join(['%s' for _ in columns])
        insert_query = f"INSERT INTO {schema_name}.\"{postgresql_table_name}\" ({','.join(columns)}) " \
                       f"VALUES ({placeholders})"

        for row in data_to_insert:
            row_values = tuple(row[col] for col in columns)
            postgresql_cursor.execute(insert_query, row_values)

        postgresql_conn.commit()
        print("Data inserted successfully")
    except psycopg2.Error as e:
        print("PostgreSQL DatabaseError:", e)
    except Exception as e:
        print(f"Error: {e}")


# Your configurations
mysql_config = {
    'user': 'root',
    'password': 'vrdella!6',
    'host': 'localhost',
    'database': 'fastapi',
}

postgresql_config = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'vrdella!6',
    'host': 'localhost',
    'port': '5432'
}

mysql_table_name = ['numbers']
schema_name = 'table_1'

for table_name in mysql_table_name:
    postgresql_table_name = table_name
    migrate_table(mysql_config, postgresql_config, table_name, schema_name, postgresql_table_name)
