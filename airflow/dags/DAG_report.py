import os
import sys
import shutil
import smtplib
import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
# from airflow.operators.email import EmailOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.models import Variable

from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo 
from weasyprint import HTML
sys.path.append('/opt/airflow/scripts')
from utils_pdf import (
    get_last_file_name, 
    extract_text_from_pdf, 
    download_pdf, 
    generate_html_diff, 
    bump_name
)
from utils_slack import (
    get_upload_url_external,
    upload_to_presigned_url,
    complete_upload_external,
)

FILE_URL = Variable.get("FILE_URL")
DATA_DIR = Variable.get("DATA_DIR")
REPORT_DIR = Variable.get("REPORT_DIR")
CANAL_SLACK = Variable.get("CANAL_SLACK")
CANAL_SLACK_NOTIFICAR_FALHA = Variable.get("CANAL_SLACK_NOTIFICAR_FALHA")
API_KEY_SLACK = Variable.get("API_KEY_SLACK")
RETRIES = int(Variable.get("RETRIES", default_var=3))
TEMP_DIR = Variable.get("TEMP_DIR", default_var="/tmp")

with DAG(
    dag_id='file_comparator',
    schedule=timedelta(minutes=1),
    start_date=datetime(2025, 8, 22),
    catchup=False,
    max_active_runs=1,
    description='Verifica se o arquivo ren20211000 teve alguma alteração e, se teve, ' \
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
            "file1_path": os.path.join(TEMP_DIR, "file_att.pdf"),
            "file2_path": os.path.join(DATA_DIR, get_last_file_name(DATA_DIR)),
            "last_report_path": os.path.join(REPORT_DIR, get_last_file_name(REPORT_DIR)),
        }
    
    compute_paths_task = PythonOperator(
        task_id="compute_paths", 
        python_callable=compute_paths, 
        dag=dag,
        doc_md="Calcula os caminhos absolutos (arquivo novo, último versionado e último relatório) e publica via XCom.",
    )

    def download_file(ti):
        """
        Download the target PDF to the temporary comparison path.
        """
        paths = ti.xcom_pull(task_ids="compute_paths")

        logging.info("Baixando o arquivo para comparação.")
        download_pdf(FILE_URL, paths["file1_path"])

        if not os.path.exists(paths["file1_path"]):
            raise FileNotFoundError(f"Falha ao baixar: {paths['file1_path']} não existe.")

    download_file_task = PythonOperator(
        task_id="download_file", 
        python_callable=download_file, 
        retries=RETRIES, 
        retry_delay=timedelta(minutes=1), 
        # retry_exponential_backoff=True,
        # max_retry_delay=timedelta(minutes=15),
        dag=dag,
        doc_md="Baixa o PDF de FILE_URL para TEMP_DIR; falha se o arquivo não existir/for inválido (aciona retries).",
    )

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
        paths = ti.xcom_pull(task_ids="compute_paths")
        
        new_file_text = extract_text_from_pdf(pdf_path=paths["file1_path"])
        old_file_text = extract_text_from_pdf(pdf_path=paths["file2_path"])

        return {
            "is_diff": old_file_text != new_file_text,
            "old_file_text": old_file_text,
            "new_file_text": new_file_text
        }

    task_compare_files = PythonOperator(
        task_id="compare_files", 
        python_callable=compare_files, 
        dag=dag,
        doc_md="Extrai texto dos PDFs e indica se tem diferença; retorna is_diff e textos para o relatório.",
    )

    def branch_on_file_difference(ti):
        """
        Branch the DAG depending on whether a diff exists.

        Returns:
            list[str] | str: downstream task id(s) to follow.
        """
        is_diff = ti.xcom_pull(task_ids="compare_files")["is_diff"]

        if is_diff:
            return ["rename_and_move_file", "generate_report"]

        return "task_end"

    branch_on_file_difference_task = BranchPythonOperator(
        task_id="branch_on_file_difference", 
        python_callable=branch_on_file_difference, 
        dag=dag,
        doc_md="Desvia o fluxo: se tem diferença, segue para versionar e gerar relatório; caso contrário, encerra em tsk_end.",
    )

    def rename_and_move_file(ti):
        """
        Version the new PDF and move it into DATA_DIR.
        """
        paths = ti.xcom_pull(task_ids="compute_paths")
        new_pdf_path = bump_name(paths["file2_path"], new_ext=".pdf")
        new_pdf_base_name = os.path.basename(new_pdf_path)
        shutil.move(paths["file1_path"], os.path.join(DATA_DIR, new_pdf_base_name))

    task_rename_and_move_file = PythonOperator(
        task_id="rename_and_move_file",
        python_callable=rename_and_move_file, 
        dag=dag,
        doc_md="Versiona o PDF novo e move para DATA_DIR.",
    )

    def generate_report(ti):
        """
        Build a styled HTML diff and write a PDF report into REPORT_DIR.
        """
        paths = ti.xcom_pull(task_ids="compute_paths")
        compare_files_result = ti.xcom_pull(task_ids="compare_files")

        html_content = generate_html_diff(
           compare_files_result["old_file_text"].splitlines(keepends=True),
           compare_files_result["new_file_text"].splitlines(keepends=True)
        )
        
        next_pdf = bump_name(paths["last_report_path"], new_ext=".pdf")
        HTML(string=html_content, base_url=os.getcwd()).write_pdf(next_pdf)

        if not next_pdf or not os.path.exists(next_pdf):
            raise FileNotFoundError(f"Report PDF not found: {next_pdf}")
        
        ti.xcom_push(key="report_pdf_path", value=str(next_pdf))
        
    task_generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report, 
        dag=dag,
        doc_md="Gera diff HTML e renderiza PDF em REPORT_DIR; publica report_pdf_path via XCom.",
    )

    def send_notification_on_slack(ti):
        """
        Send the generated diff report to Slack using the external upload flow.
        """
        report_pdf_path = ti.xcom_pull(task_ids="generate_report", key="report_pdf_path")
        report_filename = os.path.basename(report_pdf_path)
        report_size = os.path.getsize(report_pdf_path)

        logging.info("Solicitando URL pré-assinada ao Slack…")
        data = get_upload_url_external(
            token=API_KEY_SLACK,
            filename=report_filename,
            length=report_size,
            timeout=30,
        )
        upload_url = data["upload_url"]
        external_file_id = data["file_id"]

        logging.info("Enviando arquivo para a URL pré-assinada…")
        upload_to_presigned_url(
            upload_url=upload_url,
            local_path=report_pdf_path,
            filename=report_filename,
            timeout=120,
        )

        now = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M:%S")
        initial_comment = (
            f"Atualização detectada em {now}.\n"
            f"Arquivo base: <{FILE_URL}|link original>.\n"
        )

        logging.info("Finalizando upload no Slack e compartilhando no canal…")
        complete_upload_external(
            token=API_KEY_SLACK,
            external_file_id=external_file_id,
            channel_id=CANAL_SLACK,
            title="Relatório de diferenças",
            initial_comment=initial_comment,
            timeout=30,
        )

        logging.info("Notificação enviada com sucesso.")

    task_send_notification_on_slack = PythonOperator(
        task_id="send_notification_on_slack",
        python_callable=send_notification_on_slack, 
        retries=RETRIES, 
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=15),
        dag=dag,
        doc_md="Publica o relatório no Slack",
    )

    def notify_failure_slack(ti):
        """Post a Slack alert for a DAG failure."""
        text = (
            f"Houve uma falha de execução!\n"
            f"*DAG*: `{ti.dag_id}`\n"
            f"*Logs*: {ti.log_url}"
        )
        payload = {"channel": CANAL_SLACK_NOTIFICAR_FALHA, "text": text}
        response = requests.post("https://slack.com/api/chat.postMessage", json=payload, headers={"Authorization": f"Bearer {API_KEY_SLACK}"})
        if response.status_code == 200 and response.json().get("ok"):
            logging.info("Notificação de falha enviada para o slack!")
        else:
            logging.info(f"Erro ao enviar mensagem: {response.status_code}. Tentativa {ti.try_number}/{RETRIES+1}")
            raise ValueError(response.text)

    task_notify_failure_slack = PythonOperator(
        task_id="notify_failure_slack",
        python_callable=notify_failure_slack,
        trigger_rule="one_failed",
        doc_md="Envia uma notificação para o slack em caso de falha no pipeline."
    )
   
    task_end = EmptyOperator(
        task_id="task_end", 
        trigger_rule="none_failed_min_one_success",
        dag=dag,
        doc_md="Fim do fluxo quando não tem diferenças detectadas entre as versões do PDF.",
    )

    def cleanup_tmp():
        """"Deletes the temporary PDF file file_att.pdf."""
        try:
            os.remove(os.path.join(TEMP_DIR, "file_att.pdf"))
        except FileNotFoundError:
            pass

    task_cleanup = PythonOperator(
        task_id="cleanup_tmp",
        python_callable=cleanup_tmp,
        trigger_rule="all_done",
    )

    compute_paths_task >> download_file_task
    download_file_task >> task_compare_files
    task_compare_files >> branch_on_file_difference_task
    branch_on_file_difference_task >> [task_rename_and_move_file, task_generate_report]
    branch_on_file_difference_task >> task_end
    task_generate_report >> task_send_notification_on_slack
    [download_file_task, task_compare_files, task_rename_and_move_file, task_generate_report, task_send_notification_on_slack] >> task_notify_failure_slack
    task_send_notification_on_slack >> task_end
    task_end >> task_cleanup






