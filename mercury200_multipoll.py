#!/usr/bin/env python3
# coding: utf-8
"""
mercury200_multipoll.py
Опрос «Меркурий-200» за несколькими RS-485⇆Ethernet шлюзами
(один IP — разные TCP-порты HAProxy).

• Если рядом лежит serials.txt — парсим его.
• Иначе берём словарь POLL_MAP из кода.

serials.txt :  <port> <serial> [serial …]   # разделители пробел/таб.
"""

import socket, struct, time, sys
from pathlib import Path
from typing   import List, Dict, Tuple, Optional

# ---------- IP HAProxy ----------
IP_HAPROXY = ''

# ---------- «план опроса» по умолчанию -------------
POLL_MAP: Dict[int, List[str]] = {
    20003: ['548973', '548974'],
}

# ---------- тайминги ----------
TIMEOUT = 0.8   # c ожидания ответа
RETRY   = 1     # попыток на команду

# ---------- CRC-16 (Modbus RTU) ----------
def crc16_modbus(buf: bytes) -> bytes:
    crc = 0xFFFF
    for ch in buf:
        crc ^= ch
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return struct.pack('<H', crc)  # low-byte, high-byte

# ---------- BCD-утилиты ----------
def bcd_to_int(b: bytes) -> int:
    return int(''.join(f'{byte>>4:X}{byte&0xF:X}' for byte in b).lstrip('0') or '0')

# ---------- кадр ↔ ответ ----------
def send_frame(sock: socket.socket, addr_hex: str, cmd_hex: str) -> Optional[bytes]:
    raw   = bytes.fromhex(addr_hex + cmd_hex)
    frame = raw + crc16_modbus(raw)
    for _ in range(RETRY):
        try:
            sock.sendall(frame)
        except OSError:
            return None
        deadline = time.time() + TIMEOUT
        resp = b''
        while time.time() < deadline:
            try:
                chunk = sock.recv(256)
            except socket.timeout:
                break
            if not chunk:
                break
            resp += chunk
            if len(resp) >= 6:
                return resp
    return None

# ---------- распаковка полезных полей ----------
def parse_63(p: bytes) -> Tuple[float, float, float]:
    return bcd_to_int(p[0:2])/10, bcd_to_int(p[2:4])/100, bcd_to_int(p[4:7])
def parse_27(p: bytes) -> Tuple[float, float]:
    return bcd_to_int(p[0:4])/100, bcd_to_int(p[4:8])/100

# ---------- прочитать serials.txt, если он есть ----------
def load_plan_from_file(fname: str) -> Optional[Dict[int, List[str]]]:
    path = Path(fname)
    if not path.is_file():
        return None
    plan: Dict[int, List[str]] = {}
    for ln in path.read_text().splitlines():
        ln = ln.strip()
        if not ln or ln.startswith('#'):
            continue
        parts = ln.split()
        try:
            port = int(parts[0])
            serials = [s for s in parts[1:] if s.isdigit() and len(s) == 6]
            if serials:
                plan[port] = serials
        except ValueError:
            continue
    return plan or None

file_plan = load_plan_from_file('serials.txt')
if file_plan:
    POLL_MAP = file_plan

# ---------- вывод шапки ----------
row = '{:<5}|{:<8}|{:>6}|{:>6}|{:>7}|{:>10}|{:>10}'
print(row.format('Port', 'Serial', 'U,В', 'I,А', 'P,Вт', 'T1,кВт·ч', 'T2,кВт·ч'))
print('-' * 62)

# ---------- опрос ----------
for port, serials in sorted(POLL_MAP.items()):
    try:
        sock = socket.create_connection((IP_HAPROXY, port), timeout=2)
        sock.settimeout(0.2)
    except OSError as e:
        print(f'{port:<5}| соединение ERR: {e}')
        continue

    for serial in serials:
        addr_hex = f'{int(serial):08X}'  # big-endian
        # --- 63h ---
        r63 = send_frame(sock, addr_hex, '63')
        if not r63 or crc16_modbus(r63[:-2]) != r63[-2:]:
            print(f'{port:<5}|{serial:<8}| нет ответа 63h/CRC')
            continue
        U, I, P = parse_63(r63[5:-2])
        # --- 27h ---
        r27 = send_frame(sock, addr_hex, '27')
        if not r27 or crc16_modbus(r27[:-2]) != r27[-2:]:
            print(row.format(port, serial, f'{U:0.1f}', f'{I:0.2f}', f'{P:.0f}', '-', '-'))
            continue
        T1, T2 = parse_27(r27[5:-2])
        print(row.format(port, serial, f'{U:0.1f}', f'{I:0.2f}', f'{P:.0f}', f'{T1:0.2f}', f'{T2:0.2f}'))
    sock.close()

print('-' * 62)
