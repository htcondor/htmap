import pytest


@pytest.mark.usefixtures('mock_pool')
def test_reconstruct(mapped_doubler):
    result = mapped_doubler.map(range(10))

    connected = mapped_doubler.reconstruct(result.clusterid)

    assert result.clusterid == connected.clusterid
    assert result.hashes == connected.hashes
