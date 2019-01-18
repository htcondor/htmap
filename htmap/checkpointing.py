import os
from pathlib import Path
import shutil


def checkpoint(*paths):
    transfer_dir = Path(os.getenv('_CONDOR_SCRATCH_DIR')) / '_htmap_transfer'

    prep_dir = transfer_dir / 'prep'
    curr_dir = transfer_dir / 'current_checkpoint'
    old_dir = transfer_dir / 'old_checkpoint'

    for d in (prep_dir, curr_dir, old_dir):
        d.mkdir(parents = True, exist_ok = True)

    for path in paths:
        path.rename(prep_dir / path.name)

    curr_dir.rename(old_dir)
    prep_dir.rename(curr_dir)
    shutil.rmtree(old_dir)
