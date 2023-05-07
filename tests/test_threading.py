from dicomindex.threading import EagerIterator


def test_eager_iterator():
    iterator = EagerIterator(iter(range(100)))
    assert len(iterator) == 0
    items = [x for x in iterator]
    assert len(iterator) == 100
    assert len(items) == 100


def test_eager_iterator_context():
    with EagerIterator(iter(range(100))) as iterator:
        assert len(iterator) == 0
        items = [x for x in iterator]
        assert len(iterator) == 100
        assert len(items) == 100
