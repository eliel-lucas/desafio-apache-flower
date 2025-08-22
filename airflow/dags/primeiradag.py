import fitz  # PyMuPDF

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


dag =   DAG('teste_comparador_arquivos', description="Bora testar o python operator", 
            schedule=None, start_date=datetime(2023, 3, 5), 
            catchup=False)

def extract_text_from_pdf(pdf_path):
    pdf_document = fitz.open(pdf_path)
    text = ""
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text += page.get_text()
    return text

def comparar():
    file1_text = extract_text_from_pdf("/opt/airflow/dags/file1.pdf")
    file2_text = extract_text_from_pdf("/opt/airflow/dags/file2.pdf")

    if file1_text != file2_text:
        print("Arquivos Diferentes!")

task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task2 = PythonOperator(task_id="tsk2", python_callable=comparar, dag=dag)

task1 >> task2

