import re
import shutil
from pathlib import Path
import argparse


class DocumentFilter:
    def __init__(self, source_folder, normalized_folder, output_budget, output_cfo):
        self.source_folder = Path(source_folder)
        self.normalized_folder = Path(normalized_folder)
        self.output_budget = Path(output_budget)
        self.output_cfo = Path(output_cfo)

        self.keywords_phase1 = [
            "резервный фонд",
            "бюджетный ассигнование",
            "субсидия",
            "межбюджетный трансферт",
            "дотация",
        ]

        self.subject_pattern = re.compile(r"^(.*?)\s+(\d+[\d\s,.]*)\s*$", re.MULTILINE)
        self.subject_title_pattern = re.compile(r"наименование.*субъекта", re.IGNORECASE)
        self.sum_title_pattern = re.compile(r"(размер|сумма|тыс.*руб)", re.IGNORECASE)

        self.cfo_keywords = [
            "белгородский область",
            "брянский область",
            "владимирский область",
            "воронежский область",
            "ивановский область",
            "калужский область",
            "костромской область",
            "курский область",
            "липецкий область",
            "московский область",
            "орловский область",
            "рязанский область",
            "смоленский область",
            "тамбовский область",
            "тверской область",
            "тульский область",
            "ярославский область",
            "г. москва",
        ]

        self.output_budget.mkdir(exist_ok=True)
        self.output_cfo.mkdir(exist_ok=True)

    def read_file_content(self, file_path: Path) -> str:
        encodings = ["utf-8", "cp1251", "iso-8859-1"]
        for encoding in encodings:
            try:
                return file_path.read_text(encoding=encoding)
            except UnicodeDecodeError:
                continue
            except Exception:
                break
        return ""

    def contains_keywords(self, text: str, keywords) -> bool:
        text_lower = text.lower()
        return any(keyword.lower() in text_lower for keyword in keywords)

    def contains_table_data(self, text: str) -> bool:
        has_subject_title = bool(self.subject_title_pattern.search(text))
        has_sum_title = bool(self.sum_title_pattern.search(text))
        table_lines = self.subject_pattern.findall(text)
        return (has_subject_title and has_sum_title and len(table_lines) > 3)

    def phase1_filter(self, file_path: Path) -> bool:
        content = self.read_file_content(file_path)
        return self.contains_keywords(content, self.keywords_phase1) or self.contains_table_data(content)

    def phase2_filter(self, file_path: Path) -> bool:
        content = self.read_file_content(file_path)
        return self.contains_keywords(content, self.cfo_keywords)

    def find_matching_source_file(self, normalized_file: Path):
        file_stem = normalized_file.stem
        for source_file in self.source_folder.glob("*.txt"):
            if file_stem in source_file.stem or source_file.stem in file_stem:
                return source_file
        return None

    def process_documents(self):
        budget_docs = []
        cfo_docs = []

        for normalized_file in self.normalized_folder.glob("*.txt"):
            print(f"Обработка файла: {normalized_file.name}")

            if self.phase1_filter(normalized_file):
                source_file = self.find_matching_source_file(normalized_file)
                if source_file:
                    budget_docs.append(source_file)
                    if self.phase2_filter(normalized_file):
                        cfo_docs.append(source_file)

        self.copy_files(budget_docs, self.output_budget)
        self.copy_files(cfo_docs, self.output_cfo)

        print("\nРезультаты обработки:")
        print(f"Найдено бюджетных документов: {len(budget_docs)}")
        print(f"Найдено документов ЦФО: {len(cfo_docs)}")
        print(f"Скопировано в {self.output_budget}: {len(budget_docs)}")
        print(f"Скопировано в {self.output_cfo}: {len(cfo_docs)}")

        return len(budget_docs), len(cfo_docs)

    def copy_files(self, files, destination_folder: Path):
        for file_path in files:
            try:
                shutil.copy2(file_path, destination_folder / file_path.name)
            except Exception as e:
                print(f"Ошибка при копировании {file_path.name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Фильтрация/классификация документов по ключевым словам")
    parser.add_argument("--source-folder", default="text_output_ocr", help="Папка с исходными txt (OCR)")
    parser.add_argument("--normalized-folder", default="text_output_ocr_normalized", help="Папка с нормализованными txt")
    parser.add_argument("--output-budget", default="бюджетные_документы", help="Выходная папка для бюджетных документов")
    parser.add_argument("--output-cfo", default="документы_цфо", help="Выходная папка для документов ЦФО")
    args = parser.parse_args()

    flt = DocumentFilter(args.source_folder, args.normalized_folder, args.output_budget, args.output_cfo)
    flt.process_documents()


if __name__ == "__main__":
    main()
