#!/usr/bin/env python3
import csv
import io

# Read the first file to check structure
with open('idx_sf.txt', 'r', encoding='utf-8', errors='ignore') as f:
    content = f.read()
    
print("First 500 characters:")
print(repr(content[:500]))
print("\n" + "="*50 + "\n")

# Check first few lines
lines = content.split('\n')[:5]
for i, line in enumerate(lines):
    print(f"Line {i}: {repr(line[:100])}...")

print("\n" + "="*50 + "\n")

# Try CSV parsing
fh = io.StringIO(content)
reader = csv.DictReader(fh, delimiter="|")
rows = []
for i, row in enumerate(reader):
    if i >= 3:  # Only check first 3 rows
        break
    print(f"Row {i}: ListingKey = {row.get('ListingKey', 'NOT_FOUND')}")
    print(f"Row {i}: Keys = {list(row.keys())[:10]}...")  # First 10 keys
    if row.get('ListingKey') or row.get('LIST_NO'):
        rows.append(row)
        
print(f"\nTotal rows with ListingKey: {len(rows)}")
print(f"Total lines in file: {len(lines)}")