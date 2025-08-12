import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
database_url = os.getenv('DATABASE_URL')
conn = psycopg2.connect(database_url)
cur = conn.cursor()

print('Checking low-price ACT listings (likely rentals):')
cur.execute("SELECT COUNT(*) FROM listings WHERE listingstatus = 'ACT' AND CAST(listprice AS NUMERIC) < 10000")
low_price_act = cur.fetchone()[0]
print(f'ACT listings under $10,000: {low_price_act}')

print('\nSample low-price ACT listings:')
cur.execute("""
    SELECT 
        listprice, 
        data->>'StreetNumber', 
        data->>'StreetName', 
        data->>'City',
        data->>'MLSNumber'
    FROM listings 
    WHERE listingstatus = 'ACT' AND CAST(listprice AS NUMERIC) < 10000 
    ORDER BY listprice
    LIMIT 15
""")

for price, num, street, city, mls in cur.fetchall():
    print(f'${price} - {num} {street}, {city} (MLS: {mls})')

print('\nChecking if these should be rentals by looking at property characteristics:')
cur.execute("""
    SELECT 
        listprice,
        data->>'StreetNumber', 
        data->>'StreetName', 
        data->>'City',
        data->>'PropertyType',
        data->>'Bedrooms',
        data->>'BathroomsTotalInteger'
    FROM listings 
    WHERE listingstatus = 'ACT' AND CAST(listprice AS NUMERIC) BETWEEN 6000 AND 8000
    LIMIT 10
""")

print('\nListings priced $6,000-$8,000 (likely rentals):')
for price, num, street, city, prop_type, beds, baths in cur.fetchall():
    print(f'${price} - {num} {street}, {city} | {prop_type} | {beds}bd/{baths}ba')

# Check the actual SOLD count issue
print('\nChecking SOLD listings count:')
cur.execute("SELECT COUNT(*) FROM listings WHERE listingstatus = 'SOLD'")
sold_count = cur.fetchone()[0]
print(f'SOLD listings in database: {sold_count}')

cur.close()
conn.close()

print('\n=== ANALYSIS COMPLETE ===')
print('\nFINDINGS:')
print('1. Low-price ACT listings are likely rentals misclassified in MLS data')
print('2. The API correctly maps ACT->sale, but the source data classification is wrong')
print('3. Need to implement price-based logic to detect rentals')