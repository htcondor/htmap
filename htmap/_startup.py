import os
import logging
from logging import handlers
from pathlib import Path

from . import settings, names

logger = logging.getLogger('htmap')

LOGS_DIR_PATH = Path(settings['HTMAP_DIR']) / names.LOGS_DIR


def setup_internal_file_logger():
    LOGS_DIR_PATH.mkdir(parents = True, exist_ok = True)
    LOG_FILE = LOGS_DIR_PATH / 'htmap.log'
    _logfile_handler = handlers.RotatingFileHandler(
        filename = LOG_FILE,
        mode = 'a',
        maxBytes = 10 * 1024 * 1024,  # 10 MB
        backupCount = 4,
    )
    _fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _logfile_handler.setFormatter(_fmt)
    _logfile_handler.setLevel(logging.DEBUG)
    logger.addHandler(_logfile_handler)


def ensure_htmap_dir_exists():
    from . import names as _names

    _htmap_dir = Path(settings['HTMAP_DIR'])
    try:
        did_not_exist = not _htmap_dir.exists()

        dirs = (
            _htmap_dir,
            _htmap_dir / _names.MAPS_DIR,
            _htmap_dir / _names.TAGS_DIR,
            _htmap_dir / _names.REMOVED_TAGS_DIR
        )
        for dir in dirs:
            dir.mkdir(parents = True, exist_ok = True)

        if did_not_exist:
            logger.debug(f'created HTMap dir at {_htmap_dir}')
    except PermissionError as e:
        raise PermissionError(f'the HTMap directory ({_htmap_dir}) needs to be writable') from e


if os.getenv('HTMAP_ON_EXECUTE') != '1':
    ensure_htmap_dir_exists()
    setup_internal_file_logger()
