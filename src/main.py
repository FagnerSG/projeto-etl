from socket import create_connection
import psycopg2
import requests 
import json
import pandas as pd
from psycopg2 import pool
from datetime import datetime

try:
    conn = psycopg2.connect(
                        database = "postgresdatabase",                         
                        host= 'localhost',
                        user = "admin",
                        password = "admin",
                        port = 5432
                        )
    print("Conexão estabelecida com sucesso!")
    print(conn.status)

except psycopg2.Error as err:
    print(f"Erro ao conectar: {err}")
    conn = None

cur = conn.cursor()

def extract_data_from_api(endpoints):
    demo_url = 'https://demodata.grapecity.com'
    url = f"{demo_url}{endpoints}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Dados extraídos com sucesso de ")
        return pd.DataFrame(data)
    else:
        print(f"Falha ao extrair dados de")
        return pd.DataFrame()

# Extrair dados costomers
def customers_df (): 
    result_customers = extract_data_from_api('/adventureworks/api/v1/customers?PageNumber=1&PageSize=500')
    return result_customers;

# Transform dados customers
def transform_customers(customers_df):
    customers_df = customers_df.rename(columns={
       "customerId": "customerId",
       "accountNumber": "accountNumber",
    })   
    customers_df = customers_df[["customerId", "accountNumber"]]
    return customers_df

transformed_customers = transform_customers(customers_df())

# Carregar dados customers e inserindo no DW
for index, row in transformed_customers.iterrows():

    insert_query = """
        INSERT INTO CustomerDTO (customerId, accountNumber)
        VALUES (%s, %s)
        ON CONFLICT (customerId) DO UPDATE
        SET accountNumber = EXCLUDED.accountNumber;
    """
    data = (row["customerId"], row["accountNumber"])
    print(f"Inserindo dados: {data}")  
    cursor = conn.cursor()
    cursor.execute(insert_query, data)   
    conn.commit()
    print("CustomerDTO Inserido")


# Extrair dados de Products
def products_df ():
    result_products_df = extract_data_from_api('/adventureworks/api/v1/products?PageNumber=1&PageSize=500')
    return result_products_df

# Transformar dados products
def transform_products(products_df):
    products_df = products_df.rename(columns={
        "productId": "productId",
        "name": "name",
        "standardCost": "standardCost",
        "category": "category", 
        "model": "model",
        "sellStartDate": "sellStartDate",
        "sellEndDate": "sellEndDate",
        
    })
    products_df = products_df[["productId", "name", "standardCost", "category", "model", "sellStartDate", "sellEndDate"]]
    return products_df

transformed_products = transform_products(products_df())

# Carregar dados products e inserindo no DW
for index, row in transformed_products.iterrows():
    insert_query = """
        INSERT INTO ProductDTO (productId, name, standardCost, category, model, sellStartDate, sellEndDate)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (productId) DO UPDATE
        SET name = EXCLUDED.name,
            standardCost = EXCLUDED.standardCost,
            category = EXCLUDED.category,
            model = EXCLUDED.model,
            sellStartDate = EXCLUDED.sellStartDate,
            sellEndDate = EXCLUDED.sellEndDate;
    """
    data = (row["productId"], row["name"], row["standardCost"], row ["category"], row["model"], row["sellStartDate"], row["sellEndDate"])
    print(f"Inserindo dados: {data}")  
    cursor = conn.cursor()
    cursor.execute(insert_query, data)   
    conn.commit()
    print("ProductDTO Inserido")

# Extrair dados de Order
def orders_df ():
    result_order_df = extract_data_from_api('/adventureworks/api/v1/salesOrders?pageNumber=1&PageSize=500')
    return result_order_df

# Transformar dados extraidos da order
def transform_order(orders_df):
    orders_df = orders_df.rename(columns={
        "salesOrderId": "salesOrderId",
        "orderDate": "orderDate",
        "shipDate": "shipDate",
        "status": "status",
        "salesOrderNumber": "salesOrderNumber",
    })
    orders_df = orders_df[["salesOrderId", "orderDate", "shipDate", "status", "salesOrderNumber"]]
    return orders_df

transformed_orders = transform_order(orders_df())

# Carregar dados orders e inserindo no DW
for index, row in transformed_orders.iterrows():
    insert_query = """
        INSERT INTO OrderDTO (salesOrderId, orderDate, shipDate, status, salesOrderNumber)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (salesOrderId) DO UPDATE
        SET orderDate = EXCLUDED.orderDate,
            shipDate = EXCLUDED.shipDate,
            status = EXCLUDED.status,
            salesOrderNumber = EXCLUDED.salesOrderNumber;
    """
    data = (row["salesOrderId"], row["orderDate"], row["shipDate"], row["status"], row["salesOrderNumber"])
    print(f"Inserindo dados: {data}")  
    cursor = conn.cursor()
    cursor.execute(insert_query, data)   
    conn.commit()
    print("OrderDTO Inserido")

# Extrair dados de Vendas
def vendors_df():
    result_vendor_df = extract_data_from_api('/adventureworks/api/v1/vendors')
    return result_vendor_df

def transform_vendor(vendors_df):
    vendors_df = vendors_df.rename(columns={
        "vendorId": "vendorId",
        "accountNumber": "accountNumber",
        "name": "vendorName",
        "creditRating": "creditRating",
    })
    vendors_df = vendors_df[["vendorId", "accountNumber", "vendorName", "creditRating"]]
    return vendors_df

transformed_vendors = transform_vendor(vendors_df())

for index, row in transformed_vendors.iterrows():
    insert_query = """
       INSERT INTO VendorsDTO (vendorId, accountNumber, vendorName, creditRating)
       VALUES (%s, %s, %s, %s)
       ON CONFLICT (vendorId) DO UPDATE
       SET accountNumber = EXCLUDED.accountNumber,
           vendorName = EXCLUDED.vendorName,
           creditRating = EXCLUDED.creditRating;
    """
    data = (row["vendorId"], row["accountNumber"], row["vendorName"], row["creditRating"])
    print(f"Inserindo dados: {data}")  
    cursor = conn.cursor()
    cursor.execute(insert_query, data)   
    conn.commit()
    print("VendorsDTO Inserido")

    
       

if conn is not None:
    cursor.close
    conn.close()
    print("Conexão encerrada.")