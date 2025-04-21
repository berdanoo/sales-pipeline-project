import sqlite3
import pandas as pd
import streamlit as st


conn=sqlite3.connect("../database/sales.db")
df=pd.read_sql("select * from sales",conn)

total_revenue=df["Total_Revenue"].sum()
total_profit=df["Total_Profit"].sum()
total_orders=df["Order_Quantity"].sum()



st.title("Sales Dashbord")
st.markdown("Analyse der Verkaufsdaten mit Umsatz, Gewinn und Bestellungen.")

col1, col2, col3 =st.columns(3)
col1.metric(" Gesamtumsatz", f"${total_revenue:,.0f}")
col2.metric(" Gesamtgewinn", f"${total_profit:,.0f}")
col3.metric(" Bestellungen", f"{total_orders:,}")


#Umsatz pro Monat
st.subheader("Monatlicher Umsatz")
monthly_revenue= df.groupby("YearMonth")["Total_Revenue"].sum().reset_index()
st.bar_chart(monthly_revenue.set_index("YearMonth"))

#Top 5 Produktkategorien
st.subheader("Produktkategorien nach Umsatz")
top_category= df.groupby("Product_Category")["Total_Revenue"].sum().sort_values()


#Filterfunktion
st.subheader("Länderbasierte Analyse")
selected_country= st.selectbox("Wähle ein Land",df["Country"].unique())
filtered_df=df[df["Country"]== selected_country]


# Gefilterte Tabelle anzeigen
st.write(f"Top Verkäufe in **{selected_country}**:")
st.dataframe(filtered_df[['Date', 'Product', 'Total_Revenue', 'Order_Quantity']].head(10))

#Verbindung schließen
conn.close()
