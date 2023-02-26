import sqlite3
import yfinance as yf
from flask import Flask, jsonify, request
import configparser

# Load configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Define database file and table name
DB_FILE = 'stock_data.db'
TABLE_NAME = 'finance_data'

# Download finance data for each company and store it in a dictionary
data_dict = {}
for company in config['COMPANIES']:
    ticker = config['COMPANIES'][company]
    data = yf.download(ticker)
    data_dict[company] = data

# Create or connect to database and create finance_data table if it doesn't exist
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (Company TEXT, Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume INTEGER, PRIMARY KEY (Company, Date))")

# Insert or update finance data into finance_data table
for company in data_dict:
    data = data_dict[company]
    for index, row in data.iterrows():
        values = (company, str(index.date()), row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
        c.execute(f"INSERT OR REPLACE INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?)", values)

# Commit changes and close database connection
conn.commit()
conn.close()

app = Flask(__name__)


# API to get all companies' stock data for a particular day
@app.route('/stocks/<date>', methods=['GET'])
def get_all_stocks_by_date(date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"SELECT * FROM {TABLE_NAME} WHERE Date = ?", (date,))
    rows = c.fetchall()
    conn.close()

    if len(rows) == 0:
        return jsonify({'error': f"No data found for date {date}"}), 404

    data = []
    for row in rows:
        stock_data = {
            'company': row[0],
            'date': row[1],
            'open': row[2],
            'high': row[3],
            'low': row[4],
            'close': row[5],
            'volume': row[6]
        }
        data.append(stock_data)

    return jsonify(data), 200

# API to get all stock data for a particular company for a particular day
@app.route('/stocks/<company>/<date>', methods=['GET'])
def get_company_stocks_by_date(company, date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"SELECT * FROM {TABLE_NAME} WHERE Company = ? AND Date = ?", (company, date))
    rows = c.fetchall()
    conn.close()

    if len(rows) == 0:
        return jsonify({'error': f"No data found for company {company} on date {date}"}), 404

    stock_data = {
        'company': rows[0][0],
        'date': rows[0][1],
        'open': rows[0][2],
        'high': rows[0][3],
        'low': rows[0][4],
        'close': rows[0][5],
        'volume': rows[0][6]
    }

    return jsonify(stock_data), 200

# API to get all stock data for a particular company
@app.route('/stocks/:<company>', methods=['GET'])
def get_company_stocks(company):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"SELECT * FROM {TABLE_NAME} WHERE Company = ?", (company,))
    rows = c.fetchall()
    conn.close()

    if len(rows) == 0:
        return jsonify({'error': f"No data found for company {company}"}), 404

    data = []
    for row in rows:
        stock_data = {
            'company': row[0],
            'date': row[1],
            'open': row[2],
            'high': row[3],
            'low': row[4],
            'close': row[5],
            'volume': row[6]
        }
        data.append(stock_data)

    return jsonify(data), 200


# API to update stock data for a company by date
@app.route('/stocks/<company>/<date>', methods=['POST'])
def update_company_stocks_by_date(company, date):
    # Get request data
    request_data = request.json
    open_price = request_data.get('open', None)
    high_price = request_data.get('high', None)
    low_price = request_data.get('low', None)
    close_price = request_data.get('close', None)
    volume = request_data.get('volume', None)

    # Check if request data is valid
    if not any([open_price, high_price, low_price, close_price, volume]):
        return jsonify({'error': 'At least one stock data field is required'}), 400

    # Update data in database
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"SELECT * FROM {TABLE_NAME} WHERE Company = ? AND Date = ?", (company, date))
    rows = c.fetchall()
    if len(rows) == 0:
        return jsonify({'error': f"No data found for company {company} on date {date}"}), 404
    else:
        if open_price:
            c.execute(f"UPDATE {TABLE_NAME} SET Open = ? WHERE Company = ? AND Date = ?", (open_price, company, date))
        if high_price:
            c.execute(f"UPDATE {TABLE_NAME} SET High = ? WHERE Company = ? AND Date = ?", (high_price, company, date))
        if low_price:
            c.execute(f"UPDATE {TABLE_NAME} SET Low = ? WHERE Company = ? AND Date = ?", (low_price, company, date))
        if close_price:
            c.execute(f"UPDATE {TABLE_NAME} SET Close = ? WHERE Company = ? AND Date = ?", (close_price, company, date))
        if volume:
            c.execute(f"UPDATE {TABLE_NAME} SET Volume = ? WHERE Company = ? AND Date = ?", (volume, company, date))
    conn.commit()
    conn.close()

    return jsonify({'message': 'Stock data updated successfully'}), 200

if __name__ == '__main__':
    app.run(debug=True)