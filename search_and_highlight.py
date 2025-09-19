import os
import re
import sys
import argparse
import logging
import subprocess  # <-- исправлено
from datetime import datetime
from pathlib import Path

import fitz  # PyMuPDF
from tqdm import tqdm

# --------- НАСТРОЙКИ ПО УМОЛЧАНИЮ ---------
DEFAULT_DOCS_DIR = Path.cwd() / "downloads"
DEFAULT_OUT_ROOT = Path.cwd() / "search_output"
OPEN_AFTER_SAVE = True
OPEN_LIMIT = 10
HILIGHT_COLOR_RGB = (1, 1, 0)
# Попробуем взять доступные флаги из текущей версии PyMuPDF
TEXT_DEHYPHENATE = getattr(fitz, "TEXT_DEHYPHENATE", 0)
# ------------------------------------------

def sanitize_for_fs(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name[:120] or "query"

def iter_pdfs(root: Path):
    for p in root.rglob("*.pdf"):
        if p.is_file():
            yield p

def open_file(path: Path):
    try:
        if sys.platform.startswith("win"):
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        pass

def _dedup_rects(rects):
    seen = set()
    out = []
    for r in rects:
        key = (round(r.x0, 2), round(r.y0, 2), round(r.x1, 2), round(r.y1, 2))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def _search_rects(page, query: str):
    """
    Универсальный поиск: пытается использовать доступные флаги PyMuPDF,
    иначе ищет несколько вариантов регистра и объединяет результаты.
    """
    rects = []
    variants = {query, query.lower(), query.upper(), query.capitalize()}
    for q in variants:
        try:
            # если флаг де-дефиса поддерживается — применим
            rects.extend(page.search_for(q, flags=TEXT_DEHYPHENATE))
        except TypeError:
            # старые версии: search_for(flags=...) может не поддерживаться
            rects.extend(page.search_for(q))
    return _dedup_rects(rects)

def highlight_file(pdf_path: Path, query: str, out_dir: Path, color=(1, 1, 0)):
    """
    Возвращает (hits, pages_set, out_path | None).
    Создаёт копию PDF с подсветкой всех вхождений query.
    """
    hits = 0
    pages = set()
    out_path = None

    doc = fitz.open(pdf_path)
    try:
        for page in doc:  # type: ignore[assignment]
            rects = _search_rects(page, query)
            if not rects:
                continue
            pages.add(page.number + 1)
            for r in rects:
                annot = page.add_highlight_annot(r)
                # set_colors может отсутствовать в очень старых версиях — не критично
                try:
                    annot.set_colors(stroke=color)
                    annot.update()
                except Exception:
                    pass
                hits += 1

        if hits > 0:
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / pdf_path.name
            doc.save(out_path, deflate=True, garbage=4)
    finally:
        doc.close()

    return hits, pages, out_path

def build_out_dir(out_root: Path, query: str) -> Path:
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    q = sanitize_for_fs(query)
    return out_root / f"{stamp}__{q}"

def main():
    parser = argparse.ArgumentParser(
        description="Поиск фразы во всех PDF с подсветкой совпадений."
    )
    parser.add_argument("query", nargs="?", help="Искомое слово/фраза")
    parser.add_argument("--dir", dest="docs_dir", default=str(DEFAULT_DOCS_DIR),
                        help=f"Каталог с PDF (по умолчанию: {DEFAULT_DOCS_DIR})")
    parser.add_argument("--out", dest="out_root", default=str(DEFAULT_OUT_ROOT),
                        help=f"Каталог для результатов (по умолчанию: {DEFAULT_OUT_ROOT})")
    parser.add_argument("--no-open", action="store_true",
                        help="Не открывать найденные файлы после сохранения")
    args = parser.parse_args()

    query = args.query or input("Введите слово/фразу для поиска: ").strip()
    if not query:
        print("Пустой запрос. Завершение.")
        sys.exit(1)

    docs_dir = Path(args.docs_dir)
    out_dir = build_out_dir(Path(args.out_root), query)
    auto_open = not args.no_open and OPEN_AFTER_SAVE

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not docs_dir.exists():
        logging.error("Каталог с документами не найден: %s", docs_dir)
        sys.exit(2)

    pdfs = list(iter_pdfs(docs_dir))
    if not pdfs:
        logging.error("PDF-файлы не найдены в: %s", docs_dir)
        sys.exit(3)

    logging.info("PDF для проверки: %d; запрос: %r", len(pdfs), query)
    out_dir.mkdir(parents=True, exist_ok=True)

    total_hits = 0
    matched_files = []

    for pdf in tqdm(pdfs, desc="Поиск"):
        try:
            hits, pages, saved = highlight_file(pdf, query, out_dir, color=HILIGHT_COLOR_RGB)
            if hits > 0 and saved:
                matched_files.append((saved, hits, sorted(pages)))
                total_hits += hits
        except Exception as e:
            logging.warning("Ошибка обработки %s: %s", pdf, e)

    if not matched_files:
        logging.info("Совпадений не найдено.")
        print(f"\nИТОГО: совпадений нет. Запрос: {query!r}")
        print(f"Проверено файлов: {len(pdfs)}. Каталог: {docs_dir}")
        sys.exit(0)

    print("\nНАЙДЕНО СОВПАДЕНИЙ:", total_hits)
    for i, (p, hits, pages) in enumerate(matched_files, 1):
        print(f"{i:>3}. {p.name} — {hits} совп., стр.: {pages}")

    if auto_open:
        for p, _, _ in matched_files[:OPEN_LIMIT]:
            open_file(p)

    print(f"\nФайлы с подсветкой сохранены в: {out_dir}")

if __name__ == "__main__":
    main()
