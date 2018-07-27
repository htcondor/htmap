import pytest

import htcmap

EXPECTED_DIR_NAMES = [
    'inputs',
    'outputs',
    'job_logs',
    'cluster_logs',
    'cluster_hashes',
]


@pytest.mark.parametrize('dir', EXPECTED_DIR_NAMES)
def test_dir_exists(dir, mapped_doubler):
    assert (htcmap.settings.HTCMAP_DIR / mapped_doubler.name / dir).exists()


@pytest.mark.parametrize('dir', EXPECTED_DIR_NAMES)
def test_dir_exists_when_given_name(dir):
    @htcmap.htcmap(name = 'joe')
    def func(x):
        return x

    assert (htcmap.settings.HTCMAP_DIR / func.name / dir).exists()
