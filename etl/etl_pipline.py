from prefect import flow, task
import pandas as pd
import sqlite3
from datetime import datetime

@task
def load_data():
    df = pd.read_csv("../data/sales_data.csv")
    return df

@task
def clean_data(df):
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
    df['Month'] = df['Month'].map(month_map)
    df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])
    df['YearMonth'] = df['Date'].dt.to_period('M').astype(str)
    df['Total_Revenue'] = df['Revenue']
    df['Total_Cost'] = df['Cost']
    df['Total_Profit'] = df['Profit']
    return df

@task
def save_to_db(df):
    conn = sqlite3.connect('../database/sales.db')
    df.to_sql('sales', conn, if_exists='replace', index=False)
    conn.close()

@flow
def etl_pipeline():
    df = load_data()
    df_clean = clean_data(df)
    save_to_db(df_clean)

if __name__ == '__main__':
    etl_pipeline()
