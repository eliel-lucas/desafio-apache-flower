import os
import fitz  # PyMuPDF
import logging
import difflib, re
import requests
from zoneinfo import ZoneInfo 
from datetime import datetime, timezone

def bump_name(path: str, new_ext: str) -> str:
    """
    Generate a new file name by incrementing a numeric suffix.

    If the file name already ends with a number, that number is incremented.
    Otherwise, the suffix "1" is appended.
    The original extension is preserved unless `new_ext` is provided.

    Args:
        path (str): Full path of the original file.
        new_ext (str): New extension (including the dot, ex., ".pdf").

    Returns:
        str: Full path of the file with the updated name.
    """
    dir = os.path.dirname(path)
    base = os.path.basename(path)
    name, ext = os.path.splitext(base)

    i = len(name) - 1
    while i >= 0 and not name[i].isdigit():
        i -= 1
    if i < 0:
        new_name = f"{name}1{new_ext or ext}"
    else:
        j = i
        while j >= 0 and name[j].isdigit():
            j -= 1
        num = int(name[j+1:i+1]) + 1
        new_name = f"{name[:j+1]}{num}{new_ext or ext}"

    return os.path.join(dir, new_name)

def download_pdf(url: str, save_path: str) -> None:
    """
    Download a PDF from a given URL and save it locally.

    Args:
        url (str): The URL of the PDF to download.
        save_path (str): The local file path where the PDF will be saved.

    Returns:
        None

    Raises:
        ValueError: If the request or file write fails.
    """
    try:
        response = requests.get(url)
        with open(save_path, 'wb') as f:
            f.write(response.content)
    except Exception as e:
        print("Falha no Download!")
        raise ValueError(e)
    
def get_last_modified_date(url: str) -> datetime | None:
    try:
        response = requests.head(url, allow_redirects=True)
        last_modified_str = response.headers.get('Last-Modified')
        if last_modified_str:
            return datetime.strptime(last_modified_str, '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=timezone.utc)
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao acessar a URL para checar data: {e}")
        return None

def extract_text_from_pdf(pdf_path: str) -> str:
    """
    Extract all text content from a PDF file.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        str: Extracted text from all pages in the PDF.
    """
    pdf_document = fitz.open(pdf_path)
    text = ""
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text += page.get_text()
    return text

def get_last_file_name(dirpath: str) -> str:
    """
    Return the filename in `dirpath` that has the largest numeric suffix
    immediately before the extension.

    Args:
        dirpath (str): Directory path to search.
    
    Returns:
        str: The filename with the highest trailing number.
    """
    files = [f for f in os.listdir(dirpath) if os.path.isfile(os.path.join(dirpath, f))]
    if not files:
        raise FileNotFoundError(f"Diretório vazio: {dirpath!r}")

    def num_sufixo(fname: str) -> int:
        m = re.search(r'(\d+)(?=\.[^.]+$)', fname)  # pega os dígitos antes da extensão
        return int(m.group(1)) if m else -1

    return max(files, key=num_sufixo)

def generate_html_diff(file1_lines: list[str], file2_lines: list[str]) -> str:
    """
    Generate an HTML diff report between two versions of text files.

    Args:
        file1_lines (list[str]): Lines of the original file.
        file2_lines (list[str]): Lines of the modified file.

    Returns:
        str: Full HTML string containing the diff report.
    """
    differ = difflib.HtmlDiff(tabsize=2, wrapcolumn=90, charjunk=difflib.IS_CHARACTER_JUNK)
    table = differ.make_table(
                file1_lines, file2_lines, 
                fromdesc="Versão Antiga", todesc="Versão Nova", 
                context=True, numlines=3
            )

    colgroup = (
        "<colgroup>"
        "<col class='c-a-l'><col class='c-n-l'><col class='c-t-l'>"
        "<col class='c-a-r'><col class='c-n-r'><col class='c-t-r'>"
        "</colgroup>"
    )
    table = re.sub(r'(<table[^>]*class="diff"[^>]*>)', r'\1' + colgroup, table, count=1)

    css = """
    <style>
      @page{margin:8mm 10mm} html,body{margin:0;padding:0}
      .report-title{
            margin:6px 0 8px;
            text-align:center;
            font:700 12px system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
            color:#3f1f8f
        }
      table.diff{
            width:100%;
            border-collapse:collapse;
            table-layout:fixed
        }
      .diff th,.diff td{
            font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,"Liberation Mono",monospace;
            font-size:10px;
            line-height:1.25;
            border:1px solid #ddd;
            padding:1px 2px;
            vertical-align:top;
            box-sizing:border-box
        }

      .diff col.c-a-l,.diff col.c-a-r{width:1.5%}
      .diff col.c-n-l,.diff col.c-n-r{width:4.5%}
      .diff col.c-t-l,.diff col.c-t-r{width:44%}

      thead .diff_header{
            font-weight:700;
            text-align:center;
            border-bottom:2px solid #ddd
        }
      tbody .diff_header{
            font-weight:700;
            text-align:right;
            white-space:nowrap;
            color:#6b21a8
        }

      td.diff_next{padding:0;text-align:center;white-space:nowrap}
      td.diff_next a{
            display:inline-block;
            width:100%;
            font-weight:700;
            font-size:9px;
            line-height:1;
            text-decoration:none;
            color:#6b7280
        }

      tbody td:nth-child(3),tbody td:nth-child(6){
            white-space:pre-wrap;
            overflow-wrap:anywhere;
            word-break:break-all
        }

      .diff_add{background:#D4FBB2} 
      .diff_sub{background:#FFB47F} 
      .diff_chg{background:#D5A6FF}
      .diff tr{page-break-inside:avoid;break-inside:avoid}
    </style>
    """

    now = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M:%S")
    return f"""<!doctype html>
            <html lang="pt-BR">
            <head><meta charset="utf-8">{css}</head>
            <body>
            <h1 class="report-title">Comparação ren20211000 gerada em {now}</h1>
            {table}
            </body></html>"""