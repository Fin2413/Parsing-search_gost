import os, re, sys, html, shutil, logging, mimetypes
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

# ---------- НАСТРОЙКИ ----------
URL = "https://new-shop.ksm.kz/egfntd/ntdgo/kds/4.php"
BASE_DIR = os.path.join(os.getcwd(), "downloads")
REQUEST_TIMEOUT = (10, 30)
MAX_RETRIES = 3
VERIFY_SSL = True
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/128.0.0.0 Safari/537.36")
ONLY_PDF = True  # скачивать только PDF
# --------------------------------

def make_session():
    s = requests.Session()
    r = Retry(total=MAX_RETRIES, backoff_factor=1.0,
              status_forcelist=(429, 500, 502, 503, 504),
              allowed_methods=frozenset(["GET", "HEAD"]))
    s.mount("http://", HTTPAdapter(max_retries=r))
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    return s

def today_folder(base_dir: str) -> str:
    d = datetime.now().strftime("%d.%m.%Y")
    path = os.path.join(base_dir, d)
    os.makedirs(path, exist_ok=True)
    return path

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    # заменяем неразрывные пробелы и невидимые символы на обычный пробел
    s = s.replace("\u00A0", " ").replace("\u200b", " ").replace("\ufeff", " ")
    s = re.sub(r"\s+", " ", s.strip())
    return s.lower()

def sanitize_filename(name: str) -> str:
    name = html.unescape(name).strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = re.sub(r"\s+", " ", name)
    return name[:180] or "document"

def is_pdf_href(href: str) -> bool:
    # учитываем как путь, так и параметры (?file=...pdf)
    parsed = urlparse(href)
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()
    return ".pdf" in path or ".pdf" in query or href.lower().endswith(".pdf")

def guess_ext(content_type: str | None, url: str) -> str:
    if is_pdf_href(url):
        return ".pdf"
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct == "application/pdf":
            return ".pdf"
        ext = (mimetypes.guess_extension(ct) or "").lower()
        if ext:
            return ext
    ext = os.path.splitext(urlparse(url).path)[1].lower()
    return ext if ext else ".bin"

def unique_path(folder: str, stem: str, ext: str) -> str:
    p = os.path.join(folder, f"{stem}{ext}")
    if not os.path.exists(p):
        return p
    i = 1
    while True:
        cand = os.path.join(folder, f"{stem} ({i}){ext}")
        if not os.path.exists(cand):
            return cand
        i += 1

def download(session: requests.Session, file_url: str, dest_path: str) -> str:
    with session.get(file_url, stream=True, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL) as r:
        r.raise_for_status()
        ext = guess_ext(r.headers.get("Content-Type"), file_url)
        stem, _ = os.path.splitext(dest_path)
        dest_path = f"{stem}{ext}"
        tmp = dest_path + ".part"
        with open(tmp, "wb") as f:
            shutil.copyfileobj(r.raw, f)
        os.replace(tmp, dest_path)
    return dest_path

# --------- Парсинг таблицы ---------

def pick_pdf_link_from_cell(cell):
    # 1) предпочитаем .pdf внутри ячейки
    for a in cell.find_all("a", href=True):
        if is_pdf_href(a["href"]):
            return a
    # 2) любая ссылка в ячейке
    return cell.find("a", href=True)

def detect_designation_col(table) -> int | None:
    """
    Находит индекс колонки «Обозначение».
    Сначала по заголовкам (подстрока 'обозн'),
    затем — по эвристике (колонка с максимумом ссылок).
    """
    # Пытаемся взять строку заголовков
    header_row = None
    # приоритет thead
    thead = table.find("thead")
    if thead:
        header_row = thead.find("tr")
    if not header_row:
        # первая строка с th или первая строка вообще
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            if tr.find("th"):
                header_row = tr
                break
            if header_row is None:
                header_row = tr  # fallback
        # header_row может быть и строкой данных — это не страшно
    headers = []
    if header_row:
        headers = [normalize_text(c.get_text(" ", strip=True)) for c in header_row.find_all(["th", "td"])]

    # 1) По заголовкам
    for idx, h in enumerate(headers):
        if "обозн" in h:  # покрывает «обозначение», «обозн.», «обозн»
            return idx

    # 2) Эвристика: выбираем колонку с наибольшим числом ссылок
    link_counts = []
    rows = table.find_all("tr")
    max_cols = max((len(r.find_all(["td", "th"])) for r in rows), default=0)
    for col in range(max_cols):
        cnt = 0
        # пропускаем header_row
        for r in rows[1:]:
            cells = r.find_all(["td", "th"])
            if len(cells) <= col:
                continue
            if cells[col].find("a", href=True):
                cnt += 1
        link_counts.append(cnt)
    if any(c > 0 for c in link_counts):
        return max(range(len(link_counts)), key=lambda i: link_counts[i])

    return None

def extract_rows(table, designation_idx: int):
    rows_out = []
    all_tr = table.find_all("tr")
    for tr in all_tr:
        cells = tr.find_all(["td", "th"])
        if len(cells) <= designation_idx:
            continue
        cell = cells[designation_idx]
        designation_raw = normalize_text(cell.get_text(" ", strip=True))
        # пропускаем строку-заголовок
        if "обозн" in designation_raw or designation_raw in ("", "№"):
            continue

        # ссылка приоритетно .pdf внутри ячейки
        a = pick_pdf_link_from_cell(cell)
        if not a:
            continue
        href = a.get("href", "").strip()
        if ONLY_PDF and not is_pdf_href(href):
            continue

        # оригинальное имя без нормализации для файла — сохраняем регистр/символы
        designation_for_name = sanitize_filename(cell.get_text(" ", strip=True))
        rows_out.append((designation_for_name, href))
    return rows_out

def find_target_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    logging.info("Найдено таблиц на странице: %d", len(tables))
    for i, t in enumerate(tables, 1):
        idx = detect_designation_col(t)
        if idx is not None:
            logging.info("Выбрана таблица #%d, индекс колонки 'Обозначение' = %d", i, idx)
            return t, idx
    return None, None

# ------------------- main -------------------

def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    sess = make_session()
    logging.info("Открываю: %s", URL)
    try:
        resp = sess.get(URL, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        resp.raise_for_status()
        # корректируем кодировку
        resp.encoding = resp.apparent_encoding or resp.encoding
    except Exception as e:
        logging.error("Не удалось загрузить страницу: %s", e)
        sys.exit(1)

    soup = BeautifulSoup(resp.text, "lxml")

    table, designation_idx = find_target_table(soup)
    if table is None:
        logging.error("Не найдена таблица с колонкой 'Обозначение' (даже по эвристике).")
        sys.exit(2)

    rows = extract_rows(table, designation_idx)
    if not rows:
        logging.error("В выбранной таблице не найдено PDF-ссылок в колонке 'Обозначение'.")
        sys.exit(3)

    target_dir = today_folder(BASE_DIR)
    logging.info("Каталог: %s", target_dir)

    ok = err = 0
    for designation, href in rows:
        file_url = urljoin(URL, href)
        dest = unique_path(target_dir, designation, ".pdf")
        try:
            saved = download(sess, file_url, dest)
            logging.info("Скачано: %s -> %s", file_url, os.path.basename(saved))
            ok += 1
        except Exception as e:
            logging.warning("Ошибка: %s (%s)", file_url, e)
            err += 1

    logging.info("Готово. Успешно: %d, ошибок: %d. Папка: %s", ok, err, target_dir)

if __name__ == "__main__":
    main()
