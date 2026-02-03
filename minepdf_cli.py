import os
import pytesseract
from pdf2image import convert_from_path
from pathlib import Path
import time
import argparse


# При необходимости укажите путь к tesseract.exe:
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'


def ocr_pdf_to_text(pdf_path, output_path, dpi=300):
    """Конвертирует PDF в текст с помощью OCR."""
    try:
        print(f"Начинаю OCR обработку: {os.path.basename(pdf_path)}")
        start_time = time.time()

        images = convert_from_path(
            pdf_path,
            dpi=dpi,
            fmt="jpeg",
            thread_count=4
        )

        print(f"PDF конвертирован в {len(images)} изображений")

        full_text = ""
        for i, image in enumerate(images, 1):
            print(f"Обрабатываю страницу {i}...")

            text = pytesseract.image_to_string(
                image,
                lang="rus+eng",
                config="--psm 6"
            )

            full_text += f"--- Страница {i} ---\n{text}\n\n"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(full_text)

        processing_time = time.time() - start_time
        print(f"Успешно: {os.path.basename(pdf_path)} -> {len(full_text)} символов, время: {processing_time:.1f} сек")

        return True

    except Exception as e:
        print(f"Ошибка OCR при обработке {os.path.basename(pdf_path)}: {str(e)}")
        return False


def process_pdf_folder(input_folder, output_folder, dpi=300):
    """Обрабатывает все PDF файлы в папке."""
    Path(output_folder).mkdir(parents=True, exist_ok=True)

    pdf_files = [f for f in os.listdir(input_folder) if f.lower().endswith(".pdf")]
    if not pdf_files:
        print("PDF файлы не найдены в указанной папке!")
        return 0, 0

    print(f"Найдено {len(pdf_files)} PDF файлов для обработки")

    success_count = 0
    for filename in pdf_files:
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(output_folder, f"{Path(filename).stem}.txt")

        if ocr_pdf_to_text(input_path, output_path, dpi):
            success_count += 1

    print(f"\nОбработка завершена! Успешно: {success_count}/{len(pdf_files)}")
    return success_count, len(pdf_files)


def main():
    parser = argparse.ArgumentParser(description="OCR PDF документов в текст")
    parser.add_argument("--input-folder", default="downloaded_documents", help="Папка с PDF")
    parser.add_argument("--output-folder", default="text_output_ocr", help="Папка для txt после OCR")
    parser.add_argument("--dpi", type=int, default=300, help="DPI (300-400 обычно оптимально)")
    args = parser.parse_args()

    print("=== OCR обработка PDF документов ===")
    print(f"Входная папка: {args.input_folder}")
    print(f"Выходная папка: {args.output_folder}")
    print(f"Разрешение: {args.dpi} DPI")
    print("=" * 50)

    process_pdf_folder(args.input_folder, args.output_folder, args.dpi)


if __name__ == "__main__":
    main()
