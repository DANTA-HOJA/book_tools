from pathlib import Path
# -----------------------------------------------------------------------------/


def check_xlsx(file: Path, _class_name: str):
    """
    """
    if not file.exists():
        raise ValueError(f"指定的 '{_class_name}' 不存在: '{file}'")
    
    if file.suffix != ".xlsx":
        raise ValueError(f"指定的 '{_class_name}' 不是 Excel 檔: '{file}'")
    # -------------------------------------------------------------------------/
