from pathlib import Path


def clean_dir(target_dir: Path) -> (int, int):
    num_files = 0
    num_bytes = 0
    for path in target_dir.iterdir():
        num_files += 1
        stat = path.stat()
        num_bytes += stat.st_size

        path.unlink()

    return num_files, num_bytes
