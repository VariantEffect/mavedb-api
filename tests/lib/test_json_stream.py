"""Tests for writing separately encoded JSON values as one streamed array."""

import json

import pytest

from mavedb.lib.json_stream import stream_json_array


@pytest.mark.parametrize(
    "batches",
    [
        [],
        [[]],
        [[b"1", b"2", b"3"]],
        [[b"1"], [b"2", b"3"]],
        [[], [b"1"], [], [], [b"2"], []],
        [[b'{"a":[1,2]}'], [b'{"b":{}}', b'"c"']],
    ],
    ids=["no-batches", "one-empty", "one-batch", "several", "empties-between", "nested"],
)
def test_stream_json_array_output_matches_encoding_every_element_as_one_list(batches):
    elements = [json.loads(element) for batch in batches for element in batch]

    assert b"".join(stream_json_array(batches)) == json.dumps(elements, separators=(",", ":")).encode()


def test_stream_json_array_sends_a_chunk_per_non_empty_batch():
    assert list(stream_json_array([[b"1"], [], [b"2", b"3"]])) == [b"[", b"1", b",2,3", b"]"]


def test_stream_json_array_failure_mid_stream_never_closes_the_array():
    def failing():
        yield [b"1"]
        raise RuntimeError("database went away")

    sent = []
    with pytest.raises(RuntimeError):
        for chunk in stream_json_array(failing()):
            sent.append(chunk)

    assert b"".join(sent) == b"[1"
