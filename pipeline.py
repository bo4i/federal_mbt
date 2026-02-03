import argparse
import subprocess
import sys
import logging
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Tuple, Optional


def parse_ddmmyyyy(s: str) -> date:
    return datetime.strptime(s, "%d.%m.%Y").date()


def friday_to_friday_range(ref: Optional[date] = None) -> Tuple[date, date]:
    """Возвращает (start, end) для диапазона ПТ→ПТ.

    Логика:
    - start = ближайшая прошедшая пятница (включая сегодня, если сегодня пятница)
    - end = start + 7 дней (следующая пятница)
    Пример: 05.12 → 12.12 → 19.12 ...
    """
    if ref is None:
        ref = date.today()

    # weekday(): Monday=0 ... Sunday=6, Friday=4
    days_since_friday = (ref.weekday() - 4) % 7
    start = ref - timedelta(days=days_since_friday)
    end = start + timedelta(days=7)
    return start, end


def setup_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # File handler
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def run_step(cmd: list[str], title: str, logger: logging.Logger):
    sep = "=" * 80
    logger.info(sep)
    logger.info(title)
    logger.info("CMD: %s", " ".join(cmd))
    logger.info(sep)

    # Потоковый вывод: и в консоль, и в лог
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert proc.stdout is not None
    for line in proc.stdout:
        # строка уже содержит \n
        logger.info(line.rstrip("\n"))

    rc = proc.wait()
    if rc != 0:
        raise SystemExit(f"Шаг упал с кодом {rc}: {title}")


def main():
    parser = argparse.ArgumentParser(
        description="Единый пайплайн: download -> OCR -> morph -> classify"
    )
    parser.add_argument("--date-from", help="Переопределить дату начала (DD.MM.YYYY)")
    parser.add_argument("--date-to", help="Переопределить дату конца (DD.MM.YYYY)")
    parser.add_argument("--base-dir", default=".", help="Базовая директория (по умолчанию текущая)")
    parser.add_argument("--dpi", type=int, default=300, help="DPI для OCR")
    parser.add_argument("--page-size", type=int, default=30, help="PageSize для API скачивания")
    args = parser.parse_args()

    base_dir = Path(args.base_dir).resolve()

    if args.date_from and args.date_to:
        start_d = parse_ddmmyyyy(args.date_from)
        end_d = parse_ddmmyyyy(args.date_to)
    else:
        start_d, end_d = friday_to_friday_range()

    # Папка недели: runs/YYYY-MM-DD__YYYY-MM-DD/
    run_dir = base_dir / "runs" / f"{start_d.isoformat()}__{end_d.isoformat()}"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Лог: в папке недели + уникальный суффикс по времени запуска
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = run_dir / f"pipeline_{ts}.log"
    logger = setup_logger(log_path)

    # Стандартные папки пайплайна внутри папки недели
    pdf_dir = run_dir / "downloaded_documents"
    ocr_dir = run_dir / "text_output_ocr"
    norm_dir = run_dir / "text_output_ocr_normalized"
    out_budget = run_dir / "бюджетные_документы"
    out_cfo = run_dir / "документы_цфо"

    date_from = start_d.strftime("%d.%m.%Y")
    date_to = end_d.strftime("%d.%m.%Y")

    logger.info("Пайплайн запущен.")
    logger.info("Диапазон дат: %s — %s", date_from, date_to)
    logger.info("BASE_DIR: %s", base_dir)
    logger.info("RUN_DIR: %s", run_dir)
    logger.info("LOG_FILE: %s", log_path)

    py = sys.executable

    # 1) Download PDFs
    run_step(
        [py, str(base_dir / "download_pdf_cli.py"),
         "--date-from", date_from, "--date-to", date_to,
         "--download-dir", str(pdf_dir),
         "--page-size", str(args.page_size)],
        "ШАГ 1/4: Скачивание PDF",
        logger,
    )

    # 2) OCR PDFs -> TXT
    run_step(
        [py, str(base_dir / "minepdf_cli.py"),
         "--input-folder", str(pdf_dir),
         "--output-folder", str(ocr_dir),
         "--dpi", str(args.dpi)],
        "ШАГ 2/4: OCR (PDF -> TXT)",
        logger,
    )

    # 3) Normalize (morphy)
    run_step(
        [py, str(base_dir / "morphy_cli.py"),
         "--input-folder", str(ocr_dir),
         "--output-suffix", "_normalized"],
        "ШАГ 3/4: Нормализация (pymorphy3)",
        logger,
    )

    # 4) Classify / filter
    run_step(
        [py, str(base_dir / "classifier_cli.py"),
         "--source-folder", str(ocr_dir),
         "--normalized-folder", str(norm_dir),
         "--output-budget", str(out_budget),
         "--output-cfo", str(out_cfo)],
        "ШАГ 4/4: Классификация/отбор",
        logger,
    )

    logger.info("ГОТОВО ✅")
    logger.info("Результаты:")
    logger.info("  PDF: %s", pdf_dir)
    logger.info("  OCR txt: %s", ocr_dir)
    logger.info("  Normalized txt: %s", norm_dir)
    logger.info("  Бюджетные документы: %s", out_budget)
    logger.info("  Документы ЦФО: %s", out_cfo)


if __name__ == "__main__":
    main()
