from pathlib import Path


def clean_dir(target_dir: Path):
    for path in target_dir.iterdir():
        path.unlink()
