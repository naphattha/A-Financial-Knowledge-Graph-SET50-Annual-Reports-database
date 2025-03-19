import json
import mysql.connector
from datetime import datetime

# Mapping Company Symbols to Names
company_names = {
    "ADVANC": "บริษัท แอดวานซ์ อินโฟร์ เซอร์วิส จำกัด (มหาชน)",
    "AOT": "บริษัท ท่าอากาศยานไทย จำกัด (มหาชน)",
    "AWC": "บริษัท แอสเสท เวิรด์ คอร์ป จำกัด (มหาชน)",
    "BBL": "ธนาคารกรุงเทพ จำกัด (มหาชน)",
    "BCP": "บริษัท บางจาก คอร์ปอเรชั่น จำกัด (มหาชน)",
    "BDMS": "บริษัท กรุงเทพดุสิตเวชการ จำกัด (มหาชน)",
    "BEM": "บริษัท ทางด่วนและรถไฟฟ้ากรุงเทพ จำกัด (มหาชน)",
    "BGRIM": "บริษัท บี.กริม เพาเวอร์ จำกัด (มหาชน)",
    "BH": "บริษัท โรงพยาบาลบำรุงราษฎร์ จำกัด (มหาชน)",
    "BJC": "บริษัท เบอร์ลี่ ยุคเกอร์ จำกัด (มหาชน)",
    "BTS": "บริษัท บีทีเอส กรุ๊ป โฮลดิ้งส์ จำกัด (มหาชน)",
    "CBG": "บริษัท คาราบาวกรุ๊ป จำกัด (มหาชน)",
    "CENTEL": "บริษัท โรงแรมเซ็นทรัลพลาซา จำกัด (มหาชน)",
    "CPALL": "บริษัท ซีพี ออลล์ จำกัด (มหาชน)",
    "CPF": "บริษัท เจริญโภคภัณฑ์อาหาร จำกัด (มหาชน)",
    "CPN": "บริษัท เซ็นทรัลพัฒนา จำกัด (มหาชน)",
    "CRC": "บริษัท เซ็นทรัล รีเทล คอร์ปอเรชั่น จำกัด (มหาชน)",
    "DELTA": "บริษัท เดลต้า อีเลคโทรนิคส์ (ประเทศไทย) จำกัด (มหาชน)",
    "EA": "บริษัท พลังงานบริสุทธิ์ จำกัด (มหาชน)",
    "EGCO": "บริษัท ผลิตไฟฟ้า จำกัด (มหาชน)",
    "GLOBAL": "บริษัท สยามโกลบอลเฮ้าส์ จำกัด (มหาชน)",
    "GPSC": "บริษัท โกลบอล เพาเวอร์ ซินเนอร์ยี่ จำกัด (มหาชน)",
    "GULF": "บริษัท กัลฟ์ เอ็นเนอร์จี ดีเวลลอปเมนท์ จำกัด (มหาชน)",
    "HMPRO": "บริษัท โฮม โปรดักส์ เซ็นเตอร์ จำกัด (มหาชน)",
    "INTUCH": "บริษัท อินทัช โฮลดิ้งส์ จำกัด (มหาชน)",
    "ITC": "บริษัท ไอ-เทล คอร์ปอเรชั่น จำกัด (มหาชน)",
    "IVL": "บริษัท อินโดรามา เวนเจอร์ส จำกัด (มหาชน)",
    "KBANK": "ธนาคารกสิกรไทย จำกัด (มหาชน)",
    "KTB": "ธนาคารกรุงไทย จำกัด (มหาชน)",
    "KTC": "บริษัท บัตรกรุงไทย จำกัด (มหาชน)",
    "LH": "บริษัท แลนด์ แอนด์ เฮ้าส์ จำกัด (มหาชน)",
    "MINT": "บริษัท ไมเนอร์ อินเตอร์เนชั่นแนล จำกัด (มหาชน)",
    "MTC": "บริษัท เมืองไทย แคปปิตอล จำกัด (มหาชน)",
    "OR": "บริษัท ปตท. น้ำมันและการค้าปลีก จำกัด (มหาชน)",
    "OSP": "บริษัท โอสถสภา จำกัด (มหาชน)",
    "PTT": "บริษัท ปตท. จำกัด (มหาชน)",
    "PTTEP": "บริษัท ปตท. สำรวจและผลิตปิโตรเลียม จำกัด (มหาชน)",
    "PTTGC": "บริษัท พีทีที โกลบอล เคมิคอล จำกัด (มหาชน)",
    "RATCH": "บริษัท ราช กรุ๊ป จำกัด (มหาชน)",
    "SCB": "ธนาคารไทยพาณิชย์ จำกัด (มหาชน)",
    "SCC": "บริษัท ปูนซิเมนต์ไทย จำกัด (มหาชน)",
    "SCGP": "บริษัท เอสซีจี แพคเกจจิ้ง จำกัด (มหาชน)",
    "TIDLOR": "บริษัท เงินติดล้อ จำกัด (มหาชน)",
    "TISCO": "บริษัท ทิสโก้ไฟแนนเชียลกรุ๊ป จำกัด (มหาชน)",
    "TLI": "บริษัท ไทยประกันชีวิต จำกัด (มหาชน)",
    "TOP": "บริษัท ไทยออยล์ จำกัด (มหาชน)",
    "TRUE": "บริษัท ทรู คอร์ปอเรชั่น จำกัด (มหาชน)",
    "TTB": "ธนาคารทหารไทยธนชาต จำกัด (มหาชน)",
    "TU": "บริษัท ไทยยูเนี่ยน กรุ๊ป จำกัด (มหาชน)",
    "WHA": "บริษัท ดับบลิวเอชเอ คอร์ปอเรชั่น จำกัด (มหาชน)"
}

# Function to Get the Last Date of a Given Quarter
def get_last_date_of_quarter(year, quarter):
    last_dates = {
        1: f"{year}-03-31",
        2: f"{year}-06-30",
        3: f"{year}-09-30",
        4: f"{year}-12-31",
    }
    return last_dates[quarter]

# MySQL Connection
def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Wealth3visual%",
        database="set50"
    )

# Function to Create Tables (Only Runs Once)
def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Company (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(10) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Period (
            id INT AUTO_INCREMENT PRIMARY KEY,
            year INT NOT NULL,
            quarter INT NOT NULL,
            date DATE NOT NULL,
            UNIQUE(year, quarter, date) 
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS FinancialMetrics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT,
            period_id INT,
            total_assets DECIMAL(18,2),
            total_liabilities DECIMAL(18,2),
            shareholder_equity DECIMAL(18,2),
            total_equity DECIMAL(18,2),
            total_revenue_quarter DECIMAL(18,2),
            total_revenue_accum DECIMAL(18,2),
            total_expenses_quarter DECIMAL(18,2),
            total_expenses_accum DECIMAL(18,2),
            operating_cash_flow DECIMAL(18,2),
            investing_cash_flow DECIMAL(18,2),
            financing_cash_flow DECIMAL(18,2),
            FOREIGN KEY (company_id) REFERENCES Company(id) ON DELETE CASCADE,
            FOREIGN KEY (period_id) REFERENCES Period(id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MarketData (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT,
            period_id INT,
            prior DECIMAL(18,2),
            open DECIMAL(18,2),
            high DECIMAL(18,2),
            low DECIMAL(18,2),
            close DECIMAL(18,2),
            average DECIMAL(18,2),
            aom_volume DECIMAL(18,2),
            aom_value DECIMAL(18,2),
            tr_volume DECIMAL(18,2),
            tr_value DECIMAL(18,2),
            total_volume DECIMAL(18,2),
            total_value DECIMAL(18,2),
            FOREIGN KEY (company_id) REFERENCES Company(id) ON DELETE CASCADE,
            FOREIGN KEY (period_id) REFERENCES Period(id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Ratios (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT,
            period_id INT,
            type VARCHAR(50),
            value DECIMAL(18,2),
            FOREIGN KEY (company_id) REFERENCES Company(id) ON DELETE CASCADE,
            FOREIGN KEY (period_id) REFERENCES Period(id) ON DELETE CASCADE
        )
    """)

# Function: Get Company ID (with name update)
def get_company_id(cursor, symbol):
    company_name = company_names.get(symbol)  # Get name or use symbol as fallback

    cursor.execute("SELECT id FROM Company WHERE symbol = %s", (symbol,))
    result = cursor.fetchone()

    if result:
        return result[0]

    cursor.execute("INSERT INTO Company (symbol, name) VALUES (%s, %s)", (symbol, company_name))
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
        company_id = get_company_id(cursor, record['symbol'])
        year, quarter = datetime.strptime(record['date'], '%Y-%m-%d').year, (datetime.strptime(record['date'], '%Y-%m-%d').month - 1) // 3 + 1
        period_id = get_period_id(cursor, year, quarter,get_last_date_of_quarter(year, quarter))

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
        company_id = get_company_id(cursor, record['symbol'])
        period_id = get_period_id(cursor, record['year'], record['quarter'],record['dateAsof'])

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

    # Create tables if they do not exist
    create_tables(cursor)
    
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
