#!/usr/bin/env python3
"""
Migrate data from Neon PostgreSQL to Railway PostgreSQL
"""

import psycopg2
import psycopg2.extras
import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Source (Neon) database connection
NEON_DATABASE_URL = os.getenv('DATABASE_URL')

# Target (Railway) database connection
RAILWAY_DATABASE_URL = "postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway"

def get_table_schema(cursor, table_name):
    """Get the CREATE TABLE statement for a table"""
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position
    """, (table_name,))
    
    columns = cursor.fetchall()
    if not columns:
        return None
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    column_defs = []
    
    for col_name, data_type, is_nullable, col_default in columns:
        col_def = f"    {col_name} {data_type}"
        
        if is_nullable == 'NO':
            col_def += " NOT NULL"
        
        # Handle sequences by converting to SERIAL
        if col_default and 'nextval' in col_default:
            if data_type == 'integer':
                col_def = f"    {col_name} SERIAL"
            elif data_type == 'bigint':
                col_def = f"    {col_name} BIGSERIAL"
        elif col_default and col_default != 'NULL':
            col_def += f" DEFAULT {col_default}"
        
        column_defs.append(col_def)
    
    create_sql += ",\n".join(column_defs)
    create_sql += "\n);"
    
    return create_sql

def migrate_data():
    """Migrate data from Neon to Railway"""
    
    # Connect to source database (Neon)
    print("Connecting to Neon database...")
    neon_conn = psycopg2.connect(NEON_DATABASE_URL)
    neon_cursor = neon_conn.cursor()
    
    # Connect to target database (Railway)
    print("Connecting to Railway database...")
    railway_conn = psycopg2.connect(RAILWAY_DATABASE_URL)
    railway_cursor = railway_conn.cursor()
    
    try:
        # Get list of tables from Neon
        neon_cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
        """)
        
        tables = [row[0] for row in neon_cursor.fetchall()]
        print(f"Found tables: {tables}")
        
        for table_name in tables:
            print(f"\nMigrating table: {table_name}")
            
            # Get table schema
            schema_sql = get_table_schema(neon_cursor, table_name)
            if schema_sql:
                print(f"Creating table {table_name} in Railway...")
                railway_cursor.execute(schema_sql)
                railway_conn.commit()
            
            # Get data from Neon
            neon_cursor.execute(f"SELECT * FROM {table_name}")
            rows = neon_cursor.fetchall()
            
            if rows:
                # Get column names
                neon_cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
                column_names = [desc[0] for desc in neon_cursor.description]
                
                # Prepare insert statement with ON CONFLICT DO NOTHING to avoid duplicates
                placeholders = ",".join(["%s"] * len(column_names))
                insert_sql = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                
                print(f"Inserting {len(rows)} rows into {table_name}...")
                
                # Insert data in batches
                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    
                    # Convert any dict/list objects to JSON strings
                    processed_batch = []
                    for row in batch:
                        processed_row = []
                        for value in row:
                            if isinstance(value, (dict, list)):
                                processed_row.append(json.dumps(value))
                            else:
                                processed_row.append(value)
                        processed_batch.append(tuple(processed_row))
                    
                    railway_cursor.executemany(insert_sql, processed_batch)
                    railway_conn.commit()
                    print(f"  Inserted batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1}")
            
            print(f"Completed migration of {table_name}")
        
        print("\nMigration completed successfully!")
        
    except Exception as e:
        print(f"Error during migration: {e}")
        railway_conn.rollback()
        raise
    
    finally:
        neon_cursor.close()
        neon_conn.close()
        railway_cursor.close()
        railway_conn.close()

if __name__ == "__main__":
    migrate_data()