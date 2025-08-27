import os
import gdown
import shutil
import logging
import requests
import mimetypes
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo 
from weasyprint import HTML
from utils.utils import (
    get_last_file_name, 
    extract_text_from_pdf, 
    download_pdf, 
    generate_html_diff, 
    bump_name
)

FILE_URL = Variable.get("FILE_URL")
DATA_DIR = Variable.get("DATA_DIR")
REPORT_DIR = Variable.get("REPORT_DIR")
CANAL_SLACK = Variable.get("CANAL_SLACK")
API_KEY_SLACK = Variable.get("API_KEY_SLACK")
RETRIES = Variable.get("RETRIES")

with DAG(
    dag_id='file_comparator',
    schedule=timedelta(minutes=1),
    start_date=datetime(2025, 8, 22),
    catchup=False,
    max_active_runs=1,
    description='Verifica se o arqui' \
    'vo ren20211000 teve alguma alteração e, se teve, ' \
    'baixa esse arquivo e gera relatório de diferenças.',
) as dag:
    
    def compute_paths():
        """
        Build and return absolute paths used by the DAG.

        Returns:
            Dict[str, str]: {
                "file1_path": temp path for the freshly downloaded PDF,
                "file2_path": latest stored PDF in DATA_DIR,
                "last_report_path": latest report PDF in REPORT_DIR
            }
        """
        return {
            "file1_path": "/tmp/file_att.pdf",
            "file2_path": os.path.join(DATA_DIR, get_last_file_name(DATA_DIR)),
            "last_report_path": os.path.join(REPORT_DIR, get_last_file_name(REPORT_DIR)),
        }
    
    def download_file(ti):
        """
        Download the target PDF to the temporary comparison path.
        """
        paths = ti.xcom_pull(task_ids="compute_path_tsk")

        logging.info("Baixando o arquivo para comparação.")
        download_pdf(FILE_URL, paths["file1_path"])

        if not os.path.exists(paths["file1_path"]):
            logging.warning("Arquivo não foi baixado!")

    def compare_files(ti):
        """
        Extract text from both PDFs and check if they differ.

        Returns:
            Dict[str, object]: {
                "is_diff": bool,
                "old_file_text": str,
                "new_file_text": str
            }
        """
        paths = ti.xcom_pull(task_ids="compute_path_tsk")

        old_file_text = extract_text_from_pdf(pdf_path=paths["file1_path"])
        new_file_text = extract_text_from_pdf(pdf_path=paths["file2_path"])
 
        return {
            "is_diff": old_file_text != new_file_text,
            "old_file_text": old_file_text,
            "new_file_text": new_file_text
        }

    def branch_on_file_difference(ti):
        """
        Branch the DAG depending on whether a diff exists.

        Returns:
            list[str] | str: downstream task id(s) to follow.
        """
        is_diff = ti.xcom_pull(task_ids="compare_files_tsk")["is_diff"]

        if is_diff:
            return ["rename_and_move_file_tsk", "generate_report_tsk"]

        return "tsk_end"

    def rename_and_move_file(ti):
        """
        Version the new PDF and move it into DATA_DIR.
        """
        paths = ti.xcom_pull(task_ids="compute_path_tsk")
        new_pdf_path = bump_name(paths["file2_path"], new_ext=".pdf")
        new_pdf_base_name = os.path.basename(new_pdf_path)
        os.rename(paths["file1_path"], new_pdf_base_name)
        shutil.move(new_pdf_base_name, DATA_DIR)

    def generate_report(ti):
        """
        Build a styled HTML diff and write a PDF report into REPORT_DIR.
        """
        paths = ti.xcom_pull(task_ids="compute_path_tsk")
        compare_files_result = ti.xcom_pull(task_ids="compare_files_tsk")

        html_content = generate_html_diff(
           compare_files_result["old_file_text"].splitlines(keepends=True),
           compare_files_result["new_file_text"].splitlines(keepends=True)
        )
        
        next_pdf = bump_name(paths["last_report_path"], new_ext=".pdf")
        HTML(string=html_content, base_url=os.getcwd()).write_pdf(next_pdf)
        ti.xcom_push(key="report_pdf_path", value=str(next_pdf))
        
    def send_notification_on_slack(ti):
        """
        Send a notification to Slack with the generated PDF report attached.
        """
        report_pdf_path = ti.xcom_pull(task_ids="generate_report_tsk", key="report_pdf_path")

        if not report_pdf_path or not os.path.exists(report_pdf_path):
            raise FileNotFoundError(f"Report PDF not found: {report_pdf_path!r}")

        report_filename = os.path.basename(report_pdf_path)
        report_size_bytes = os.path.getsize(report_pdf_path)

        resp_get_url = requests.get(
            "https://slack.com/api/files.getUploadURLExternal",
            headers={"Authorization": f"Bearer {API_KEY_SLACK}"},
            params={"filename": report_filename, "length": report_size_bytes},
            timeout=30,
        )

        data_get_url = resp_get_url.json()
        if not data_get_url.get("ok"):
            raise ValueError({"step": "getUploadURLExternal", "response": data_get_url})

        upload_url = data_get_url["upload_url"]
        external_file_id = data_get_url["file_id"]

        with open(report_pdf_path, "rb") as file_handle:
            resp_upload = requests.post(
                upload_url,
                files={"filename": (report_filename, file_handle, "application/pdf")},
                timeout=120,
            )
        if resp_upload.status_code != 200:
            raise ValueError({
                "step": "upload_to_presigned_url",
                "status": resp_upload.status_code,
                "text": resp_upload.text
            })

        now = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M:%S")
        initial_comment = (
            f"Atualização detectada em {now}.\n"
            "Arquivo base: <https://www2.aneel.gov.br/cedoc/ren20211000.pdf|link original>.\n"
        )
        resp_complete = requests.post(
            "https://slack.com/api/files.completeUploadExternal",
            headers={"Authorization": f"Bearer {API_KEY_SLACK}"},
            data={
                "files": f'[{{"id":"{external_file_id}","title":"Relatório de diferenças"}}]',
                "channel_id": CANAL_SLACK,
                "initial_comment": initial_comment,
            },
            timeout=30,
        )
        if resp_complete.status_code == 200 and resp_complete.json().get("ok"):
            print(f'Notificação enviada com sucesso!')
        else:
            print(f'Erro ao enviar notificação: {resp_complete.status_code}. Tentativa {ti.try_number}/{RETRIES+1}... ')
            raise ValueError(resp_complete.text)


    compute_paths_task = PythonOperator(
        task_id="compute_path_tsk", 
        python_callable=compute_paths, 
        dag=dag
    )
    download_file_task = PythonOperator(
        task_id="download_tsk", 
        python_callable=download_file, 
        retries=RETRIES, 
        retry_delay=timedelta(minutes=1), 
        dag=dag
    )
    compare_files_task = PythonOperator(
        task_id="compare_files_tsk", 
        python_callable=compare_files, 
        dag=dag
    )
    branch_on_file_difference_task = BranchPythonOperator(
        task_id="branch_on_file_difference_tsk", 
        python_callable=branch_on_file_difference, 
        dag=dag
    )
    rename_and_move_file_task = PythonOperator(
        task_id="rename_and_move_file_tsk",
        python_callable=rename_and_move_file, 
        dag=dag
    )
    generate_report_task = PythonOperator(
        task_id="generate_report_tsk",
        python_callable=generate_report, 
        dag=dag
    )
    send_notification_on_slack_task = PythonOperator(
        task_id="send_notification_on_slack_tsk",
        python_callable=send_notification_on_slack, 
        retries=RETRIES, 
        retry_delay=timedelta(minutes=1), 
        dag=dag
    )
    task_end = EmptyOperator(task_id="tsk_end", dag=dag)

    compute_paths_task >> download_file_task
    download_file_task >> compare_files_task
    compare_files_task >> branch_on_file_difference_task
    branch_on_file_difference_task >> [rename_and_move_file_task, generate_report_task]
    branch_on_file_difference_task >> task_end
    generate_report_task >> send_notification_on_slack_task

