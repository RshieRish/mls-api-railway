import psycopg2

conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

# Search for CN222505 listings
cur.execute("SELECT data FROM listings WHERE data::text LIKE '%CN222505%' LIMIT 10")
results = cur.fetchall()

print(f'CN222505 listings found in Railway database: {len(results)}')
print('='*60)

for i, row in enumerate(results):
    if row[0]:
        data = row[0]
        address = data.get('STREET_NAME', 'Unknown Address')
        city = data.get('CITY', '')
        agent_id = data.get('LIST_AGENT', 'Unknown Agent')
        list_price = data.get('LIST_PRICE', 'Unknown Price')
        bedrooms = data.get('NO_BEDROOMS', 'Unknown')
        bathrooms = data.get('NO_FULL_BATHS', 'Unknown')
        sqft = data.get('SQUARE_FEET', 'Unknown')
        
        print(f'{i+1}. {address}, {city}')
        print(f'   Agent: {agent_id}')
        print(f'   Price: ${list_price}')
        print(f'   Beds: {bedrooms}, Baths: {bathrooms}, SqFt: {sqft}')
        print()

cur.close()
conn.close()