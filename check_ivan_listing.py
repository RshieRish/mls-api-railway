import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Use DATABASE_URL from .env
database_url = os.getenv('DATABASE_URL')
conn = psycopg2.connect(database_url)

cur = conn.cursor()

# Check the specific listing
print("Searching for 38 Ivan St listing...")
cur.execute("""
    SELECT 
        listingstatus, 
        data->>'ListingStatus', 
        data->>'ListPrice', 
        data->>'StreetNumber', 
        data->>'StreetName',
        data->>'MLSNumber'
    FROM listings 
    WHERE data->>'StreetName' ILIKE '%Ivan%' 
    AND data->>'StreetNumber' = '38'
""")

result = cur.fetchone()
if result:
    print(f'Table Status: {result[0]}')
    print(f'JSON Status: {result[1]}')
    print(f'Price: {result[2]}')
    print(f'Address: {result[3]} {result[4]}')
    print(f'MLS Number: {result[5]}')
else:
    print('Listing not found, searching for any Ivan St listings...')
    cur.execute("""
        SELECT 
            listingstatus, 
            data->>'ListingStatus', 
            data->>'ListPrice', 
            data->>'StreetNumber', 
            data->>'StreetName',
            data->>'MLSNumber'
        FROM listings 
        WHERE data->>'StreetName' ILIKE '%Ivan%'
        LIMIT 5
    """)
    
    results = cur.fetchall()
    for result in results:
        print(f'Table Status: {result[0]}, JSON Status: {result[1]}, Price: {result[2]}, Address: {result[3]} {result[4]}, MLS: {result[5]}')

# Also check for listings with price around 6800
print("\nSearching for listings with price around $6,800...")
cur.execute("""
    SELECT 
        listingstatus, 
        data->>'ListingStatus', 
        data->>'ListPrice', 
        data->>'StreetNumber', 
        data->>'StreetName',
        data->>'MLSNumber'
    FROM listings 
    WHERE CAST(data->>'ListPrice' AS NUMERIC) BETWEEN 6000 AND 7000
    LIMIT 10
""")

results = cur.fetchall()
print(f"Found {len(results)} listings with price between $6,000-$7,000:")
for result in results:
    print(f'Status: {result[0]}/{result[1]}, Price: ${result[2]}, Address: {result[3]} {result[4]}, MLS: {result[5]}')

cur.close()
conn.close()

print("\n=== ANALYSIS COMPLETE ===")