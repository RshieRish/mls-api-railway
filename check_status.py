import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

# Check status distribution
cur.execute('SELECT DISTINCT listingstatus, COUNT(*) FROM listings GROUP BY listingstatus ORDER BY COUNT(*) DESC;')
print('Status distribution in database:')
for row in cur.fetchall():
    print(f'{row[0]}: {row[1]}')

# Check for sold listings specifically
cur.execute('SELECT COUNT(*) FROM listings WHERE listingstatus ILIKE \'%sold%\' OR listingstatus ILIKE \'%sld%\';')
sold_count = cur.fetchone()[0]
print(f'\nListings with SOLD-like status: {sold_count}')

cur.close()
conn.close()