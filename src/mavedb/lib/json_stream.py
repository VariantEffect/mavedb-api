"""Building one JSON document out of pieces encoded separately, so a large response can stream."""

from collections.abc import Iterable, Iterator


def stream_json_array(batches: Iterable[Iterable[bytes]]) -> Iterator[bytes]:
    """Write already-encoded JSON values as one array, a chunk per batch.

    Callers encode each element themselves, so the output is byte for byte what encoding every element as one list
    gives with compact separators, and a batch's elements arrive together as one chunk rather than one per element.

    If ``batches`` raises after the first chunk, the exception propagates and the closing ``]`` is never sent, so a
    truncated stream fails to parse rather than reading as a complete array.
    """
    yield b"["
    separator = b""
    for batch in batches:
        elements = b",".join(batch)
        # An empty batch adds nothing, not a stray comma.
        if not elements:
            continue
        yield separator + elements
        separator = b","
    yield b"]"
