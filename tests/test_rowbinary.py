"""RowBinary encoders in sketch_io.

These are the bytes ClickHouse ingests. A silent encoding bug here does not raise -- it
produces a well-formed row with wrong values -- so the expected bytes are spelled out
literally rather than derived by re-running the encoder.
"""

from __future__ import annotations

import base64
import struct

import pytest

from sketch_io import (
    decode_druid_sketch, enc_agg_state, enc_bytes, enc_datetime_iso, enc_float64,
    enc_int64, enc_nullable_string, enc_string, enc_uint64, esc, varint,
)


@pytest.mark.parametrize("value,expected", [
    (0, b"\x00"),
    (1, b"\x01"),
    (127, b"\x7f"),          # last single-byte value
    (128, b"\x80\x01"),      # first two-byte value
    (300, b"\xac\x02"),
    (16383, b"\xff\x7f"),
    (16384, b"\x80\x80\x01"),
    (2649, b"\xd9\x14"),     # a real uniqHLL12 payload length
])
def test_varint_known_values(value, expected):
    assert varint(value) == expected


def test_varint_is_leb128_roundtrip():
    """Decode with an independent implementation rather than the encoder's own logic."""
    def read(b):
        result = shift = 0
        for i, byte in enumerate(b):
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return result, i + 1
            shift += 7
        raise AssertionError("unterminated varint")

    for n in (0, 1, 63, 64, 127, 128, 8016, 65535, 1 << 20):
        decoded, consumed = read(varint(n))
        assert decoded == n
        assert consumed == len(varint(n))


def test_enc_string_length_prefix():
    assert enc_string("") == b"\x00"
    assert enc_string("ab") == b"\x02ab"


def test_enc_string_length_is_bytes_not_characters():
    """A multi-byte character must contribute its UTF-8 length, or every downstream field
    shifts by the difference."""
    s = "café"                       # 4 characters, 5 UTF-8 bytes
    encoded = enc_string(s)
    assert encoded == b"\x05" + s.encode("utf-8")
    assert encoded[0] == 5


def test_enc_bytes_handles_arbitrary_binary():
    """Sketch payloads are not UTF-8; a String column must take them unchanged."""
    payload = bytes(range(256))
    encoded = enc_bytes(payload)
    assert encoded == varint(256) + payload


def test_enc_nullable_string():
    # Null writes the flag and nothing else -- writing a value after it corrupts the row.
    assert enc_nullable_string(None) == b"\x01"
    assert enc_nullable_string("x") == b"\x00\x01x"
    # Empty string is a value, not a null.
    assert enc_nullable_string("") == b"\x00\x00"


def test_enc_agg_state_prefixes_length():
    sketch = b"\xde\xad\xbe\xef"
    assert enc_agg_state(sketch) == b"\x04" + sketch


def test_enc_datetime_iso_is_epoch_seconds_le():
    # 2015-09-12T00:00:00Z -> 1442016000
    assert enc_datetime_iso("2015-09-12T00:00:00.000Z") == struct.pack("<I", 1442016000)
    # The hour bucket used throughout the fixture verification.
    assert enc_datetime_iso("2015-09-12T15:00:00.000Z") == struct.pack("<I", 1442070000)


def test_enc_datetime_iso_truncates_subsecond():
    """DateTime has one-second resolution; milliseconds must floor, not round."""
    assert enc_datetime_iso("2015-09-12T00:00:00.999Z") == struct.pack("<I", 1442016000)


def test_enc_numeric_widths():
    assert enc_uint64(1) == struct.pack("<Q", 1)
    assert enc_int64(-1) == struct.pack("<q", -1)
    assert len(enc_float64(1.5)) == 8
    assert struct.unpack("<d", enc_float64(1.5))[0] == 1.5


def test_enc_int64_accepts_negative_where_uint64_would_not():
    """sum_added is Int64 because Druid deltas can be negative."""
    assert struct.unpack("<q", enc_int64(-4242))[0] == -4242
    with pytest.raises(struct.error):
        enc_uint64(-1)


def test_decode_druid_sketch_strips_the_inner_quotes():
    """Druid returns complex columns as base64 inside a *quoted* JSON string."""
    raw = b"\x02\x01\x07\x0c"
    b64 = base64.b64encode(raw).decode()
    assert decode_druid_sketch(f'"{b64}"') == raw   # as Druid sends it
    assert decode_druid_sketch(b64) == raw          # already unwrapped


def test_esc_escapes_quotes_and_backslashes():
    assert esc("plain") == "'plain'"
    assert esc("it's") == r"'it\'s'"
    assert esc("back\\slash") == r"'back\\slash'"
