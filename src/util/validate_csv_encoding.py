from pathlib import Path


def validate_csv_encoding(csvfile_path: str | Path | list[str] | list[Path]) -> str | None:
    """
    驗證csv檔案是否為UTF-8編碼，並回傳csv檔案路徑；如果驗證失敗則刪除該檔案並回傳None。
    Parameters:
        csvfile_path (str | Path | list[str] | list[Path]): 要驗證的csv檔案路徑或路徑列表。
    Returns:
        None | str: 如果驗證成功，回傳csv檔案路徑；如果驗證失敗，刪除該檔案並回傳None。
    """
    try:
        if isinstance(csvfile_path, list):
            for path in csvfile_path:
                validate_csv_encoding(str(path))
        else:
            csvfile_name = str(csvfile_path).split("/")[-1]  # 取得檔名，方便後續印出錯誤訊息

        with open(csvfile_path, 'rb') as f:
            sample = f.read(1024)  # Read first 1KB for quick check
            sample.decode('utf-8')
    except UnicodeDecodeError as e:
        print(f"編碼驗證失敗: 檔案 {csvfile_name}不是有效的utf-8編碼。錯誤: {e}")
        csvfile_path.unlink(missing_ok=True)  # Delete invalid file
        return None
    except Exception as e:
        print(f"驗證過程中發生錯誤: {e}")
        return None
    else:
        print(f"驗證成功: 檔案 {csvfile_name} 為UTF-8編碼。")
        return str(csvfile_path)
