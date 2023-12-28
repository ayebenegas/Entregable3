from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import kaggle
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

# funcion de transformacion de datos
def conexion():
    url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    data_base ="data-engineer-database"
    user ="ayelenbenegas_coderhouse"
    pwd = "Q8TYsj8380"
    dataset_name = 'fekihmea/fashion-retail-sales'
    download_path = 'C:/Users/dell/OneDrive/Escritorio/kaggle'
    table_name='fashionretail'
    dataframe=data

    try:
        conn = psycopg2.connect(
            host = url,
            dbname = data_base,
            user = user,
            password = pwd,
            port = '5439'
        )
        print("Connected to Redshift successfully!")
        
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
    
    kaggle.api.dataset_download_files(dataset_name, path=download_path, unzip=True)
    cursor = conn.cursor()
    data = pd.read_csv('C:/Users/dell/OneDrive/Escritorio/kaggle/Fashion_Retail_Sales.csv', encoding='latin-1', skiprows=1)
    data.drop_duplicates(inplace=True)
    try:
        # Combine column definitions into the CREATE TABLE statement
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                Customer_Reference_ID VARCHAR(255),
                Item_Purchased VARCHAR(255),
                Purchase_Amount_USD VARCHAR(255),
                Date_Purchase VARCHAR(255),
                Review_Rating VARCHAR(255),
                Payment_Method VARCHAR(255)
            );
            """
        # Crear la tabla
        cur = conn.cursor()
        cur.execute(table_schema)
        # Generar los valores a insertar
        values = [tuple(x) for x in dataframe.to_numpy()]
        # Definir el INSERT
        insert_sql = f"""INSERT INTO {table_name} (Customer_Reference_ID, Item_Purchased, Purchase_Amount_USD,
        Date_Purchase, Review_Rating, Payment_Method) VALUES %s"""
        # Execute the transaction to insert the data
        cur.execute("BEGIN")    
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        print('Proceso terminado')
        cur.close()

    except Exception as e:
        conn.rollback()
        print(f"Error: {str(e)}")


ingestion_dag = DAG(
    dag_id='ingestion_data',
    description='Desafio 3',
    schedule_interval=None,
    start_date = None
)

task_1 = PythonOperator(
    task_id='conexion',
    python_callable=conexion,
    dag=ingestion_dag,
)


task_1