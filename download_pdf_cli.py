import requests
import json
import os
from tqdm import tqdm
import time
import argparse
from datetime import datetime


def download_documents(
    date_from: str,
    date_to: str,
    download_dir: str = "downloaded_documents",
    page_size: int = 30,
    sleep_between_pages: float = 0.5,
    sleep_between_files: float = 1.0,
):
    """Скачивает PDF-документы с publication.pravo.gov.ru за диапазон дат.

    date_from/date_to: строки в формате DD.MM.YYYY (как ожидает API).
    """

    base_api_url = "http://publication.pravo.gov.ru/api/Documents"

    params = {
        "DocumentTypes": "7ff5b3b5-3757-44f1-bb76-3766cabe3593",
        "SignatoryAuthorityId": "8005d8c9-4b6d-48d3-861a-2a37e69fccb3",
        "PublishDateSearchType": "0",
        "NumberSearchType": "0",
        "DocumentDateSearchType": "0",
        "DocumentDateFrom": date_from,
        "DocumentDateTo": date_to,
        "JdRegSearchType": "0",
        "SortedBy": "6",
        "SortDestination": "0",
        "PageSize": str(int(page_size)),
        "index": "1",
    }

    print("Параметры запроса:")
    for key, value in params.items():
        print(f"  {key}: {value}")

    os.makedirs(download_dir, exist_ok=True)
    all_items = []

    try:
        print("\nПолучаем информацию о количестве страниц...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }

        first_page_response = requests.get(base_api_url, params=params, headers=headers, timeout=30)
        first_page_response.raise_for_status()
        first_page_data = first_page_response.json()

        total_count = first_page_data.get("totalCount", 0)
        page_size_api = first_page_data.get("pageSize", page_size)
        pages_total_count = first_page_data.get("pagesTotalCount", 1)

        print(f"Всего документов: {total_count}")
        print(f"Размер страницы (API): {page_size_api}")
        print(f"Всего страниц: {pages_total_count}")

        print(f"\nСобираем данные со всех {pages_total_count} страниц...")
        for index in range(1, int(pages_total_count) + 1):
            print(f"Обрабатываем страницу {index}/{pages_total_count}...")
            params["index"] = str(index)

            response = requests.get(base_api_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "items" in data:
                items = data["items"]
                all_items.extend(items)
                print(f"Страница {index}: добавлено {len(items)} документов")
            else:
                print(f"Страница {index}: ключ 'items' не найден")

            time.sleep(float(sleep_between_pages))

        print(f"\nВсего собрано документов: {len(all_items)}")

        if not all_items:
            print("Документы не найдены по заданным критериям.")
            return 0, 0, 0

        print("\nПервые несколько документов:")
        for i, item in enumerate(all_items[:3], 1):
            print(f"\nДокумент {i}:")
            print(f"  eoNumber: {item.get('eoNumber', 'N/A')}")
            print(f"  Название: {item.get('title', 'N/A')}")
            print(f"  Дата: {item.get('documentDate', 'N/A')}")
            print(f"  Номер: {item.get('number', 'N/A')}")
            if "signatoryAuthority" in item:
                print(f"  Орган: {item['signatoryAuthority'].get('name', 'N/A')}")

        download_base_url = "http://publication.pravo.gov.ru/file/pdf?eoNumber="
        successful_downloads = 0
        failed_downloads = 0

        print(f"\nНачинаем скачивание {len(all_items)} документов...")

        for i, item in enumerate(all_items, 1):
            eo_number = item.get("eoNumber")
            if not eo_number:
                print(f"\n[{i}/{len(all_items)}] Отсутствует eoNumber, пропускаем")
                failed_downloads += 1
                continue

            download_url = f"{download_base_url}{eo_number}"

            title = item.get("title", "document")
            number = item.get("number", "number")
            doc_date = item.get("documentDate", "documentDate")
            doc_date = doc_date[:10] if isinstance(doc_date, str) else "date"

            safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "-", "_")).strip()[:50]
            filename = f"{number}_{safe_title}_{doc_date}.pdf".replace(" ", "_")
            filepath = os.path.join(download_dir, filename)

            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                print(f"[{i}/{len(all_items)}] ✓ Уже скачан: {filename} ({file_size} bytes)")
                successful_downloads += 1
                continue

            print(f"\n[{i}/{len(all_items)}] Скачиваю: {eo_number}")
            print(f"Название: {title}")

            try:
                file_response = requests.get(download_url, stream=True, timeout=30, headers=headers)
                file_response.raise_for_status()

                content_type = file_response.headers.get("content-type", "")
                if "pdf" not in content_type.lower():
                    print(f"Внимание: файл может не быть PDF (Content-Type: {content_type})")

                total_size = int(file_response.headers.get("content-length", 0))
                with open(filepath, "wb") as f, tqdm(
                    desc=filename,
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    disable=total_size == 0,
                ) as pbar:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

                file_size = os.path.getsize(filepath)
                print(f"✓ Успешно сохранен: {filename} ({file_size} bytes)")
                successful_downloads += 1

            except requests.exceptions.RequestException as e:
                print(f"✗ Ошибка при скачивании {eo_number}: {e}")
                failed_downloads += 1

            time.sleep(float(sleep_between_files))

        print(f"\n{'=' * 60}")
        print("ЗАВЕРШЕНО!")
        print(f"Диапазон дат: {date_from} — {date_to}")
        print(f"Всего документов (в выдаче): {len(all_items)}")
        print(f"Успешно скачано: {successful_downloads}")
        print(f"Не удалось скачать: {failed_downloads}")
        print(f"Документы сохранены в папке: '{download_dir}'")
        print(f"{'=' * 60}")

        return len(all_items), successful_downloads, failed_downloads

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении API запроса: {e}")
    except json.JSONDecodeError as e:
        print(f"Ошибка при парсинге JSON ответа: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")

    return 0, 0, 0


def _validate_date(s: str) -> str:
    # DD.MM.YYYY
    datetime.strptime(s, "%d.%m.%Y")
    return s


def main():
    parser = argparse.ArgumentParser(description="Скачивание документов с publication.pravo.gov.ru")
    parser.add_argument("--date-from", required=True, type=_validate_date, help="Дата начала (DD.MM.YYYY)")
    parser.add_argument("--date-to", required=True, type=_validate_date, help="Дата конца (DD.MM.YYYY)")
    parser.add_argument("--download-dir", default="downloaded_documents", help="Папка для PDF")
    parser.add_argument("--page-size", type=int, default=30, help="PageSize для API")
    parser.add_argument("--sleep-pages", type=float, default=0.5, help="Пауза между страницами API (сек)")
    parser.add_argument("--sleep-files", type=float, default=1.0, help="Пауза между скачиваниями файлов (сек)")
    args = parser.parse_args()

    print("=" * 60)
    print("СКРИПТ ДЛЯ СКАЧИВАНИЯ ДОКУМЕНТОВ С PRAVO.GOV.RU")
    print("=" * 60)
    download_documents(
        date_from=args.date_from,
        date_to=args.date_to,
        download_dir=args.download_dir,
        page_size=args.page_size,
        sleep_between_pages=args.sleep_pages,
        sleep_between_files=args.sleep_files,
    )


if __name__ == "__main__":
    main()
