"""A minimal Postgres wire protocol client: startup, then simple queries.
Enough to talk to hamilton-server over several connections at once."""
import socket
import struct
from dataclasses import dataclass

PROTOCOL_VERSION = 3 << 16
BOOL_OID = 16


@dataclass
class Rows:
    columns: list[str]
    rows: list[list[str | None]]


@dataclass
class Command:
    tag: str


@dataclass
class Error:
    code: str
    message: str


class Connection:
    def __init__(self, port: int, host: str = "127.0.0.1"):
        self.sock = socket.create_connection((host, port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.file = self.sock.makefile("rb")
        body = struct.pack("!i", PROTOCOL_VERSION) + b"user\0sqltest\0\0"
        self.sock.sendall(struct.pack("!i", len(body) + 4) + body)
        for result in self._until_ready():
            if isinstance(result, Error):
                raise ConnectionError(result.message)

    def query(self, sql: str) -> list[Rows | Command | Error]:
        """Runs `sql` and returns a result for each statement that ran, and
        last the error that stopped the rest, if one did. BOOL values come
        back as `true` and `false`, as the CLI prints them."""
        body = sql.encode() + b"\0"
        self.sock.sendall(b"Q" + struct.pack("!i", len(body) + 4) + body)
        return self._until_ready()

    def close(self):
        self.sock.sendall(b"X" + struct.pack("!i", 4))
        self.file.close()
        self.sock.close()

    def _read(self) -> tuple[bytes, bytes]:
        header = self.file.read(5)
        if len(header) < 5:
            raise ConnectionError("the server closed the connection")
        (length,) = struct.unpack("!i", header[1:])
        return header[:1], self.file.read(length - 4)

    def _until_ready(self) -> list[Rows | Command | Error]:
        results, columns, bools, rows = [], None, [], []
        while True:
            tag, body = self._read()
            if tag == b"T":
                columns, bools, rows = [], [], []
                (count,), at = struct.unpack_from("!h", body), 2
                for _ in range(count):
                    end = body.index(b"\0", at)
                    columns.append(body[at:end].decode())
                    (type_oid,) = struct.unpack_from("!i", body, end + 7)
                    bools.append(type_oid == BOOL_OID)
                    at = end + 19
            elif tag == b"D":
                (count,), at, row = struct.unpack_from("!h", body), 2, []
                for i in range(count):
                    (length,) = struct.unpack_from("!i", body, at)
                    at += 4
                    if length < 0:
                        row.append(None)
                        continue
                    value = body[at : at + length].decode()
                    at += length
                    if bools[i]:
                        value = "true" if value == "t" else "false"
                    row.append(value)
                rows.append(row)
            elif tag == b"C":
                results.append(Rows(columns, rows) if columns is not None else Command(body[:-1].decode()))
                columns = None
            elif tag == b"E":
                fields = dict((f[:1], f[1:].decode()) for f in body.split(b"\0") if f)
                results.append(Error(fields.get(b"C", ""), fields.get(b"M", "")))
            elif tag == b"Z":
                return results
