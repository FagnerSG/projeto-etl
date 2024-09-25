import psycopg2
import requests 
import pandas as pd

# Conexão com o banco de dados PostgreSQL
try:
    conn = psycopg2.connect(
                        database = "postgresdatabase",                         
                        host= 'localhost',
                        user = "admin",
                        password = "admin",
                        port = 5432
                        )
    print("Conexão estabelecida com sucesso!")
except psycopg2.Error as err:
    print(f"Erro ao conectar: {err}")
    conn = None

cur = conn.cursor()

# Função para criar as tabelas se não existirem
def create_tables_if_not_exist():
    create_customer_table = """
    CREATE TABLE IF NOT EXISTS CustomerDTO (
        customerId INT PRIMARY KEY NOT NULL,
        accountNumber VARCHAR(50)
    );
    """
    
    create_product_table = """
    CREATE TABLE IF NOT EXISTS ProductDTO (
        productId INT PRIMARY KEY NOT NULL,
        name VARCHAR(255),
        standardCost DECIMAL,
        category VARCHAR(100),
        model VARCHAR(100),
        sellStartDate DATE
    );
    """
    
    create_vendor_table = """
    CREATE TABLE IF NOT EXISTS VendorsDTO (
        vendorId INT PRIMARY KEY NOT NULL,
        accountNumber VARCHAR(50),
        vendorName VARCHAR(255),
        creditRating VARCHAR(50)
    );
    """
    drop_calendar_table = "DROP TABLE IF EXISTS CalendarDTO CASCADE;"
    cur.execute(drop_calendar_table)
    
    create_calendar_table = """
    CREATE TABLE IF NOT EXISTS CalendarDTO (
        dateId SERIAL PRIMARY KEY NOT NULL,
        date DATE NOT NULL UNIQUE,
        year INT NOT NULL,
        month INT NOT NULL,
        day INT NOT NULL,
        quarter INT NOT NULL,
        week INT NOT NULL
    );
    """
    
    create_sales_fact_table = """
    CREATE TABLE IF NOT EXISTS SalesFact (
        salesFactId SERIAL PRIMARY KEY,
        salesOrderId INT,
        orderDate DATE,
        customerId INT,
        productId INT,
        vendorId INT,
        dateId INT, -- Nova coluna para relacionamento com CalendarDTO
        orderQuantity INT,
        totalAmount DECIMAL,
        FOREIGN KEY (customerId) REFERENCES CustomerDTO (customerId),
        FOREIGN KEY (productId) REFERENCES ProductDTO (productId),
        FOREIGN KEY (vendorId) REFERENCES VendorsDTO (vendorId),
        FOREIGN KEY (dateId) REFERENCES CalendarDTO (dateId) -- Relacionamento com CalendarDTO
    );
    """
    
    cur.execute(create_customer_table)
    cur.execute(create_product_table)
    cur.execute(create_vendor_table)
    cur.execute(create_calendar_table)  # Criação da tabela CalendarDTO
    cur.execute(create_sales_fact_table)  # Atualização da tabela SalesFact com relação a CalendarDTO
    conn.commit()
    print("Tabelas criadas ou já existentes.")

create_tables_if_not_exist()

def extract_data_from_api(endpoints):
    demo_url = 'https://demodata.grapecity.com'
    url = f"{demo_url}{endpoints}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Dados extraídos com sucesso de {url}")
        return pd.DataFrame(data)
    else:
        print(f"Falha ao extrair dados de {url}")
        return pd.DataFrame()


def customers_df(): 
    result_customers = extract_data_from_api('/adventureworks/api/v1/customers?PageNumber=1&PageSize=500')
    return result_customers

def transform_customers(customers_df):
    customers_df = customers_df.rename(columns={
       "customerId": "customerId",
       "accountNumber": "accountNumber",
    })   
    return customers_df[["customerId", "accountNumber"]]

transformed_customers = transform_customers(customers_df())


for index, row in transformed_customers.iterrows():
    insert_query = """
        INSERT INTO CustomerDTO (customerId, accountNumber)
        VALUES (%s, %s)
        ON CONFLICT (customerId) DO UPDATE
        SET accountNumber = EXCLUDED.accountNumber;
    """
    data = (row["customerId"], row["accountNumber"])
    print(f"Inserindo dados: {data}")  
    cur.execute(insert_query, data)   
    conn.commit()
    print("CustomerDTO Inserido")


def products_df():
    result_products_df = extract_data_from_api('/adventureworks/api/v1/products?PageNumber=1&PageSize=500')
    return result_products_df

def transform_products(products_df):
    products_df = products_df.rename(columns={
        "productId": "productId",
        "name": "name",
        "standardCost": "standardCost",
        "category": "category", 
        "model": "model",
        "sellStartDate": "sellStartDate",
    })
    return products_df[["productId", "name", "standardCost", "category", "model", "sellStartDate"]]

transformed_products = transform_products(products_df())


for index, row in transformed_products.iterrows():
    insert_query = """
        INSERT INTO ProductDTO (productId, name, standardCost, category, model, sellStartDate)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (productId) DO UPDATE
        SET name = EXCLUDED.name,
            standardCost = EXCLUDED.standardCost,
            category = EXCLUDED.category,
            model = EXCLUDED.model,
            sellStartDate = EXCLUDED.sellStartDate;
    """
    data = (row["productId"], row["name"], row["standardCost"], row["category"], row["model"], row["sellStartDate"])
    print(f"Inserindo dados: {data}")  
    cur.execute(insert_query, data)   
    conn.commit()
    print("ProductDTO Inserido")


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
    return vendors_df[["vendorId", "accountNumber", "vendorName", "creditRating"]]

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
    cur.execute(insert_query, data)   
    conn.commit()
    print("VendorsDTO Inserido")

def insert_calendar_dates(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date)
    
    for single_date in date_range:
        year = single_date.year
        month = single_date.month
        day = single_date.day
        quarter = (month - 1) // 3 + 1  # Calcula o trimestre
        week = single_date.isocalendar()[1]  # Número da semana
        
        insert_query = """
            INSERT INTO CalendarDTO (date, year, month, day, quarter, week)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
        """
        
        data = (single_date.date(), year, month, day, quarter, week)
        cur.execute(insert_query, data)
        conn.commit()
    print("CalendarDTO preenchido.")
    
# Preencher CalendarDTO de 2020 a 2024
insert_calendar_dates('2001-01-01', '2024-12-31')

def fetch_all_ids(table_name, column_name, cur):
    query = f"SELECT {column_name} FROM {table_name};"
    cur.execute(query)
    results = cur.fetchall()
    return [row[0] for row in results]  # Retorna uma lista com todos os IDs

def generate_sales_fact_data(vendor_ids, customer_ids, product_ids):
    sales_data = {
        "salesOrderId": [],  # Inicializa listas vazias
        "orderDate": [],
        "customerId": [],
        "productId": [],
        "vendorId": [],
        "orderQuantity": [],
    }

    # Determinar o número de entradas que queremos gerar
    num_entries = min(len(vendor_ids), len(customer_ids), len(product_ids))

    for i in range(num_entries):
        sales_data["salesOrderId"].append(i + 1)  # IDs de vendas sequenciais
        sales_data["orderDate"].append(pd.to_datetime('2024-01-01') + pd.DateOffset(days=i))  # Datas sequenciais
        sales_data["customerId"].append(customer_ids[i])  # IDs dos clientes
        sales_data["productId"].append(product_ids[i])  # IDs dos produtos
        sales_data["vendorId"].append(vendor_ids[i])  # IDs dos fornecedores
        sales_data["orderQuantity"].append(10)  # Quantidade padrão para cada entrada

    return pd.DataFrame(sales_data)

# Buscar todos os IDs das tabelas correspondentes
vendor_ids = fetch_all_ids("VendorsDTO", "vendorId", cur)
customer_ids = fetch_all_ids("CustomerDTO", "customerId", cur)
product_ids = fetch_all_ids("ProductDTO", "productId", cur)

# Gerar os dados de vendas usando os IDs das tabelas
sales_fact_data = generate_sales_fact_data(vendor_ids, customer_ids, product_ids)

def fetch_date_id(order_date, cur):
    query = "SELECT dateId FROM CalendarDTO WHERE date = %s;"
    cur.execute(query, (order_date,))
    result = cur.fetchone()
    return result[0] if result else None

# Loop para inserir os dados gerados
for index, row in sales_fact_data.iterrows():
    # Buscar o dateId na CalendarDTO usando o orderDate
    date_id = fetch_date_id(row["orderDate"].date(), cur)
    
    if date_id is None:
        print(f"Erro: dateId não encontrado para a data {row['orderDate']}")
        continue

    # Query de inserção na SalesFact
    insert_query = """
        INSERT INTO SalesFact (salesOrderId, orderDate, customerId, productId, vendorId, dateId, orderQuantity, totalAmount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """    

    try:
        # Tentar obter o custo padrão do produto
        product_cost = transformed_products.loc[transformed_products['productId'] == row['productId'], 'standardCost'].values[0]
    except IndexError:
        print(f"Erro: Produto com ID {row['productId']} não encontrado no dataframe.")
        continue

    # Calcular o valor total
    total_amount = float(row['orderQuantity']) * float(product_cost)

    # Montar os dados para inserção
    data = (
        int(row["salesOrderId"]), 
        row["orderDate"].to_pydatetime(),  
        int(row["customerId"]), 
        int(row["productId"]), 
        int(row["vendorId"]), 
        date_id,  # Usar o dateId obtido da tabela CalendarDTO
        int(row["orderQuantity"]), 
        total_amount
    )

    # Executar a inserção
    print(f"Inserindo dados: {data}")  
    cur.execute(insert_query, data)   
    conn.commit()
    print("SalesFact Inserido")


cur.close()
conn.close()
