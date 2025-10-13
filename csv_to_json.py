import csv
import json
from pathlib import Path


def auto_cast(value: str):
    """Best-effort type conversion for numeric and boolean fields."""
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned == "":
        return None

    lowered = cleaned.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"

    # Keep integers as ints so large values remain precise.
    if cleaned.isdigit() or (cleaned.startswith("-") and cleaned[1:].isdigit()):
        try:
            return int(cleaned)
        except ValueError:
            pass

    try:
        return float(cleaned)
    except ValueError:
        return value


def csv_to_json(csv_path: Path, json_path: Path) -> None:
    with csv_path.open(mode="r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = []
        for row in reader:
            converted = {key: auto_cast(value) for key, value in row.items()}
            rows.append(converted)

    # Write pretty-printed JSON for readability, preserving unicode characters.
    with json_path.open(mode="w", encoding="utf-8") as json_file:
        json.dump(rows, json_file, indent=2, ensure_ascii=False)


def main() -> None:
    csv_path = Path(__file__).with_name("final_dataset.csv")
    json_path = csv_path.with_suffix(".json")

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    csv_to_json(csv_path, json_path)
    print(f"Converted {csv_path.name} -> {json_path.name}")


if __name__ == "__main__":
    main()
