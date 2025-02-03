from pathlib import Path
# -----------------------------------------------------------------------------/


def check_xlsx(file: Path, _class_name: str):
    """
    """
    if not file.exists():
        raise ValueError(f"指定的 '{_class_name}' 不存在, Current path: '{file}'")
    
    if file.suffix != ".xlsx":
        raise ValueError(f"指定的 '{_class_name}' 不是 Excel 檔, Current path: '{file}'")
    # -------------------------------------------------------------------------/


def check_dir(dir: Path):
    """
    """
    if not dir.is_dir():
        raise ValueError(f"指定的存放路徑 不存在 或 不是資料夾, Current path: '{dir}'")
    # -------------------------------------------------------------------------/
