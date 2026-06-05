from typing import Optional


class ByteWriter:
    def __init__(self) -> None:
        self._data = bytearray()

    def write_u8(self, value: int) -> None:
        self._data.append(value & 0xFF)

    def write_u16(self, value: int) -> None:
        self._data.extend(int(value).to_bytes(2, "little", signed=False))

    def write_u32(self, value: int) -> None:
        self._data.extend(int(value).to_bytes(4, "little", signed=False))

    def write_u64(self, value: int) -> None:
        self._data.extend(int(value).to_bytes(8, "little", signed=False))

    def write_bytes(self, data: bytes) -> None:
        self._data.extend(data)

    def bytes(self) -> bytes:
        return bytes(self._data)

    def take_bytes(self) -> bytes:
        data = bytes(self._data)
        self._data.clear()
        return data


class ByteReader:
    def __init__(self, data: bytes) -> None:
        self._data = memoryview(data)
        self._offset = 0

    def remaining(self) -> int:
        return len(self._data) - self._offset

    def empty(self) -> bool:
        return self.remaining() == 0

    def offset(self) -> int:
        return self._offset

    def read_u8(self) -> Optional[int]:
        if self.remaining() < 1:
            return None
        value = self._data[self._offset]
        self._offset += 1
        return int(value)

    def read_u16(self) -> Optional[int]:
        return self._read_int(2)

    def read_u32(self) -> Optional[int]:
        return self._read_int(4)

    def read_u64(self) -> Optional[int]:
        return self._read_int(8)

    def read_bytes(self, count: int) -> Optional[bytes]:
        if count < 0 or self.remaining() < count:
            return None
        start = self._offset
        self._offset += count
        return bytes(self._data[start:self._offset])

    def _read_int(self, count: int) -> Optional[int]:
        data = self.read_bytes(count)
        if data is None:
            return None
        return int.from_bytes(data, "little", signed=False)


def crc16_ccitt_false(data: bytes) -> int:
    crc = 0xFFFF
    for value in data:
        crc ^= (value & 0xFF) << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc & 0xFFFF
