import pymorphy3
from pathlib import Path
import time
import argparse


class TextNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_text(self, text: str) -> str:
        """Приводит все слова в тексте к начальной форме."""
        words = text.split()
        normalized_words = []

        for word in words:
            clean_word = "".join(char for char in word if char.isalpha() or char == "-")
            if clean_word:
                try:
                    parsed = self.morph.parse(clean_word)[0]
                    normalized_words.append(parsed.normal_form)
                except Exception:
                    normalized_words.append(clean_word)
            else:
                normalized_words.append(word)

        return " ".join(normalized_words)


def setup_directories(input_path: str, output_suffix: str = "_normalized"):
    input_dir = Path(input_path)
    if not input_dir.exists():
        raise FileNotFoundError(f"Исходная папка не существует: {input_path}")

    output_dir = input_dir.parent / f"{input_dir.name}{output_suffix}"
    output_dir.mkdir(exist_ok=True)
    return input_dir, output_dir


def process_files(input_dir: Path, output_dir: Path, normalizer: TextNormalizer):
    text_extensions = {".txt", ".text", ".md", ".rtf", ""}
    processed_count = 0
    error_count = 0

    for input_file_path in input_dir.rglob("*"):
        if input_file_path.is_file() and input_file_path.suffix.lower() in text_extensions:
            try:
                relative_path = input_file_path.relative_to(input_dir)
                output_file_path = output_dir / relative_path
                output_file_path.parent.mkdir(parents=True, exist_ok=True)

                content = input_file_path.read_text(encoding="utf-8", errors="ignore")
                normalized_content = normalizer.normalize_text(content)
                output_file_path.write_text(normalized_content, encoding="utf-8")

                processed_count += 1
                if processed_count % 100 == 0:
                    print(f"Обработано файлов: {processed_count}")

            except Exception as e:
                error_count += 1
                print(f"Ошибка при обработке {input_file_path}: {e}")

    return processed_count, error_count


def main():
    parser = argparse.ArgumentParser(description="Нормализация текстов (pymorphy3) в начальную форму")
    parser.add_argument("--input-folder", default="text_output_ocr", help="Папка с txt после OCR")
    parser.add_argument("--output-suffix", default="_normalized", help="Суффикс папки вывода")
    args = parser.parse_args()

    print("Запуск нормализации текстовых файлов...")
    print(f"Исходная папка: {args.input_folder}")

    try:
        normalizer = TextNormalizer()
        input_dir, output_dir = setup_directories(args.input_folder, args.output_suffix)

        print(f"Выходная папка: {output_dir}")
        print("Начинаю обработку...")

        start_time = time.time()
        processed, errors = process_files(input_dir, output_dir, normalizer)
        end_time = time.time()

        print("\n" + "=" * 50)
        print("ОБРАБОТКА ЗАВЕРШЕНА")
        print("=" * 50)
        print(f"Обработано файлов: {processed}")
        print(f"Ошибок: {errors}")
        print(f"Затраченное время: {end_time - start_time:.2f} секунд")
        print(f"Нормализованные файлы сохранены в: {output_dir}")

    except Exception as e:
        print(f"Критическая ошибка: {e}")


if __name__ == "__main__":
    main()
