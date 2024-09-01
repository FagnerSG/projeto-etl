from socket import create_connection
import requests
import pandas as pd
import mysql.connector
from datetime import datetime
from mysql.connector import errorcode

#configurar o banco de dados
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "admin",
    "database": "database_adventureworks"
}

try:
    conn = mysql.connector.connect(**db_config)
    print("Conexão estabelecida com sucesso!")
except mysql.connector.Error as err:
    print(f"Erro ao conectar: {err}")
    conn = None
finally:
    if conn and conn.is_connected():
        conn.close()
        print("Conexão encerrada.")

def extract_data_from_api(endpoint):
    demo_url = "https://demodata.grapecity.com/swagger/index.html?urls.primaryName=Restful%20AdventureWorks)para"
    url = f"{demo_url}{endpoint}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Dados extraídos com sucesso de {endpoint}")
        return pd.DataFrame(data)
    else:
        print(f"Falha ao extrair dados de {endpoint}")
        return pd.DataFrame()

# Extrair dados dos paises
countries_df = extract_data_from_api("/adventureworks/api/v1/countries")

# Extrair dados de Products
products_df = extract_data_from_api("/northwind/api/products")

# Extrair dados de Territories
territories_df = extract_data_from_api("/northwind/api/territories")

# Extrair dados de Orders
orders_df = extract_data_from_api("/northwind/api/orders")

# Extrair dados de Order Details
order_details_df = extract_data_from_api("/northwind/api/orderdetails")


