import json
import mysql.connector
from datetime import datetime

# MySQL Connection
def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Wealth3visual%",
        database="set50"
    )

# Function: Get Company ID
def get_company_id(cursor, symbol, name):
    cursor.execute("SELECT id FROM Company WHERE symbol = %s", (symbol,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("INSERT INTO Company (symbol, name) VALUES (%s, %s)", (symbol, name))
    return cursor.lastrowid

# Function: Get Period ID
def get_period_id(cursor, year, quarter, date_asof):
    cursor.execute("SELECT id FROM Period WHERE year = %s AND quarter = %s", (year, quarter))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("INSERT INTO Period (year, quarter, date) VALUES (%s, %s, %s)", (year, quarter, date_asof))
    return cursor.lastrowid

# Insert Financial Data
def insert_financial_data(cursor, data):
    for record in data:
        symbol = record['symbol']
        date_asof = record['dateAsof']
        year, quarter = datetime.strptime(date_asof, '%Y-%m-%d').year, (datetime.strptime(date_asof, '%Y-%m-%d').month - 1) // 3 + 1

        company_id = get_company_id(cursor, symbol)
        period_id = get_period_id(cursor, year, quarter, date_asof)

        cursor.execute("""
        INSERT INTO FinancialMetrics (company_id, period_id, total_assets, total_liabilities, shareholder_equity,
            total_equity, total_revenue_quarter, total_revenue_accum)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            company_id, period_id, record.get('totalAssets'), record.get('totalLiabilities'),
            record.get('shareholderEquity'), record.get('totalEquity'), record.get('totalRevenueQuarter'),
            record.get('totalRevenueAccum')
        ))

# Insert Market Data
def insert_market_data(cursor, data):
    for record in data:
        company_id = get_company_id(cursor, record['symbol'], record['symbol'])
        year, quarter = datetime.strptime(record['date'], '%Y-%m-%d').year, (datetime.strptime(record['date'], '%Y-%m-%d').month - 1) // 3 + 1
        period_id = get_period_id(cursor, year, quarter)

        cursor.execute("""
        INSERT INTO MarketData (company_id, period_id, prior, open, high, low, close, average, aom_volume, aom_value,
            tr_volume, tr_value, total_volume, total_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            company_id, period_id, record.get('prior'), record.get('open'), record.get('high'), record.get('low'),
            record.get('close'), record.get('average'), record.get('aomVolume'), record.get('aomValue'),
            record.get('trVolume'), record.get('trValue'), record.get('totalVolume'), record.get('totalValue')
        ))

# Insert Financial Ratios
def insert_ratios(cursor, data):
    for record in data:
        company_id = get_company_id(cursor, record['symbol'], record['symbol'])
        period_id = get_period_id(cursor, record['year'], record['quarter'])

        ratios = {
            'ROE': record.get('roe'),
            'ROA': record.get('roa'),
            'NetProfitMarginQuarter': record.get('netProfitMarginQuarter'),
            'NetProfitMarginAccum': record.get('netProfitMarginAccum'),
            'DE': record.get('de'),
            'FixedAssetTurnover': record.get('fixedAssetTurnover'),
            'TotalAssetTurnover': record.get('totalAssetTurnover'),
        }

        for ratio_type, value in ratios.items():
            if value is not None:
                cursor.execute("""
                INSERT INTO Ratios (company_id, period_id, type, value) 
                VALUES (%s, %s, %s, %s)
                """, (company_id, period_id, ratio_type, value))

# Main Execution
try:
    connection = connect_db()
    cursor = connection.cursor()

    # Load JSON Data
    with open('FilteredFinancialData.json') as fin_file:
        financial_data = json.load(fin_file)
        insert_financial_data(cursor, financial_data)
        insert_ratios(cursor, financial_data)

    with open('FilteredEODData.json') as eod_file:
        market_data = json.load(eod_file)
        insert_market_data(cursor, market_data)

    connection.commit()
    print("Data inserted successfully!")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    cursor.close()
    connection.close()
