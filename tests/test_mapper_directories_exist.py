import pytest

import htmap

EXPECTED_DIR_NAMES = [
    'inputs',
    'outputs',
    'job_logs',
    'cluster_logs',
    'hashes_by_clusterid',
]


@pytest.mark.parametrize('dir', EXPECTED_DIR_NAMES)
def test_dir_exists(dir, mapped_doubler):
    assert (htmap.settings.HTMAP_DIR / mapped_doubler.name / dir).exists()


@pytest.mark.parametrize('dir', EXPECTED_DIR_NAMES)
def test_dir_exists_when_given_name(dir, doubler):
    n = 'joe'
    htmap.htmap(name = n)(doubler)

    assert (htmap.settings.HTMAP_DIR / n / dir).exists()
