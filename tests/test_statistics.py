from dicomindex.statistics import PathStatuses, Statistics


def test_statistics():
    stats = Statistics()
    paths = [f"/tmp/path{x}" for x in range(20)]
    for _ in range(15):
        stats.add(paths.pop(), PathStatuses.PROCESSED)
    for _ in range(3):
        stats.add(paths.pop(), PathStatuses.SKIPPED_ALREADY_VISITED)
    for _ in range(2):
        stats.add(paths.pop(), PathStatuses.SKIPPED_FAILED)

    output = stats.summary()
    assert "Visited 20, processed 15" in output  # enough. looks fine
