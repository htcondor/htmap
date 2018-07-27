import pytest

import htcmap

EXPECTED_DIR_NAMES = [
    'inputs',
    'outputs',
    'job_logs',
    'cluster_logs',
    'hashes_by_clusterid',
]


@pytest.mark.parametrize('dir', EXPECTED_DIR_NAMES)
def test_dir_exists(dir, mapped_doubler):
    assert (htcmap.settings.HTCMAP_DIR / mapped_doubler.name / dir).exists()


@pytest.mark.parametrize('dir', EXPECTED_DIR_NAMES)
def test_dir_exists_when_given_name(dir, doubler):
    n = 'joe'
    htcmap.htcmap(name = n)(doubler)

    assert (htcmap.settings.HTCMAP_DIR / n / dir).exists()
