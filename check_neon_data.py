#!/usr/bin/env python3
import psycopg2
import os
import json
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cur = conn.cursor()
    
    # Check total count
    cur.execute('SELECT COUNT(*) FROM listings')
    total_count = cur.fetchone()[0]
    print(f'Total listings in NEON database: {total_count}')
    
    # Get sample data
    cur.execute('SELECT listing_key, data FROM listings LIMIT 5')
    rows = cur.fetchall()
    
    print('\nSample listings:')
    for row in rows:
        listing_key = row[0]
        data = row[1]
        city = data.get('City', 'N/A')
        price = data.get('ListPrice', 'N/A')
        street = data.get('StreetName', 'N/A')
        print(f'- {listing_key}: {street}, {city} - ${price}')
    
    # Check for real vs sample data
    cur.execute("SELECT COUNT(*) FROM listings WHERE listing_key LIKE 'MA%'")
    sample_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM listings WHERE listing_key NOT LIKE 'MA%'")
    real_count = cur.fetchone()[0]
    
    print(f'\nData breakdown:')
    print(f'- Sample data (MA* keys): {sample_count}')
    print(f'- Real data (non-MA* keys): {real_count}')
    
    conn.close()
    
except Exception as e:
    print(f'Error connecting to database: {e}')