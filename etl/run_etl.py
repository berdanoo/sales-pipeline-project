import pandas as pd
import sqlite3

# CSV laden
df = pd.read_csv('../data/sales_data.csv')


month_map = {
    'January': 1,
    'February': 2,
    'March': 3,
    'April': 4,
    'May': 5,
    'June': 6,
    'July': 7,
    'August': 8,
    'September': 9,
    'October': 10,
    'November': 11,
    'December': 12
}

# Mapping anwenden
df['Month'] = df['Month'].map(month_map)

# Neue Felder vorbereiten
df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])
df['YearMonth'] = df['Date'].dt.to_period('M').astype(str)
df['Total_Revenue'] = df['Revenue']
df['Total_Cost'] = df['Cost']
df['Total_Profit'] = df['Profit']

# Verbindung zur SQLite-Datenbank
conn = sqlite3.connect('../database/sales.db')
cursor = conn.cursor()

# Tabelle erstellen
cursor.execute('''
CREATE TABLE IF NOT EXISTS sales (
    e TEXT,
    Day INTEGER,
    Month INTEGER,
    Year INTEGER,
    Customer_Age INTEGER,
    Age_Group TEXT,
    Customer_Gender TEXT,
    Country TEXT,
    State TEXT,
    Product_Category TEXT,
    Sub_Category TEXT,
    Product TEXT,
    Order_Quantity INTEGER,
    Unit_Cost REAL,
    Unit_Price REAL,
    Profit REAL,
    Cost REAL,
    Revenue REAL,
    Date TEXT,
    YearMonth TEXT,
    Total_Revenue REAL,
    Total_Cost REAL,
    Total_Profit REAL
)
''')

# In Datenbank schreiben
df.to_sql('sales', conn, if_exists='replace', index=False)

conn.commit()
conn.close()

print("✅ ETL abgeschlossen – neue Datenbank gespeichert.")
