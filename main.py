import sys
import asyncio
import httpx
import random
import json
import socket
import struct
import time
import threading
import os
import uuid
import itertools
import contextvars
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass


from google_play_scraper import app as play_scraper
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from protobuf_decoder.protobuf_decoder import Parser
from packet_ids import MESSAGE_ID_TO_NAME
import Pb2_proto


from urllib.parse import urlparse
from cli_ui import app_state as bot_state, start_termux_monitor, stop_termux_monitor, get_loop_lag, interactive_termux_menu, MAX_ACCOUNTS


import udp_engine


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_all_account_files():
    import glob
    pattern = os.path.join(BASE_DIR, "accounts*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        files = [os.path.join(BASE_DIR, "accounts.json")]
    return files
TOKEN_CACHE_FILE = os.path.join(BASE_DIR, "data", "token_cache.json")
DEVICES_FILE = os.path.join(BASE_DIR, "data", "devices.json")
TOKEN_CACHE_TTL = 7200


START_MATCH_INTERVAL = float(os.environ.get("START_MATCH_INTERVAL", "12"))


BR_START_MATCH_INTERVAL = float(os.environ.get("BR_START_MATCH_INTERVAL", "20"))
BR_DOUBLE_SEARCH = os.environ.get("BR_DOUBLE_SEARCH", "0") == "1"
BR_KEEPALIVE_INTERVAL = 12.0
BR_IDLE_LIMIT = 120

NEW_MATCH_DELAY = float(os.environ.get("NEW_MATCH_DELAY", "7"))


MAX_PARALLEL_MATCHES = int(os.environ.get("MAX_PARALLEL_MATCHES", "1"))
BR_MAX_PARALLEL = int(os.environ.get("BR_MAX_PARALLEL", "1"))

MAX_MATCH_DURATION = 700
MATCH_IDLE_TIMEOUT = float(os.environ.get("MATCH_IDLE_TIMEOUT", "18.0"))


try:
    QUICK_FINISH_SECONDS = float(os.environ.get("QUICK_FINISH_SECONDS", "180"))

except Exception:
    QUICK_FINISH_SECONDS = 180.0

try:
    EXP_REFRESH_SECONDS = max(15.0, float(os.environ.get("EXP_REFRESH_SECONDS", "45")))
except Exception:
    EXP_REFRESH_SECONDS = 60.0
PRIORITY_REGIONS = ["IND", "BD", "SG", "TH", "PH", "VN", "MY", "ID", "HK", "TW"]


MAX_CONSECUTIVE_PARSE_FAILURES = 5.0
NON_MATCH_RECONNECT_DELAY = float(os.environ.get("NON_MATCH_RECONNECT_DELAY", "1.0"))

FALLBACK_UID = ""
FALLBACK_PASSWORD = ""


SEND_PRELUDE         = False
SEND_POST_MATCH_2814 = False

PRELUDE_2814_HEX = (
    "080b12dd040ada04b14282da80afada27527bc1a2ab92aa6e8dcc967cee8f531d3e4680538d19808d38c24e928bebbe4cbf26caf3d78d0cb6cc7b8dcb228e586cb4044f0a3202bcef358b69fe7ac41627f6de6c17a6bdcc57c794b453d1eafae4b9483acd22322a9ae95ec4df7e074f4b00ea8485a6853c2dc7ce039f4d4c8efa0e151d28fd219b9ebd7828e217aafd46b42d82ff4b6f393662f6e25a204404e"
    "ba092caef9c667b9cadd07f649c60351b628735ece9ddb6d71317efc998abdb6c823e8b681d6c35f113a4f3663718e8b18e3aaf47891e6a218488fa1dabb70edef94dfd2ce2759ec019474b5f7720403"
    "05953390a562bab411291de0cd093fc0112a46991f32cbaba1f70398bb4caac37f4d137d27d21a9a150b39d91a30615fcd2a67d866272aa3d2731290ee8f3cf8cbf1399362d1f1ebc85f68b36bb76458"
    "c970ecb04e2d7631920e9d4ca2d85063b668fde3422f6377599379de10d93e37b9f2fdfd349b9c01236d1300f910806ab14abbad1bd1edf151e2a62d5814bb6c8821e5673bf2e658ea26bb95dac6bf2b"
    "611a6001dfcbbf33530709e8d8647953df161ad86db8c10012696134be6281d60e4ef36c60ac8fa440bffb0d77710e4439a3187ca0331202aaba120d0bc51732f42d721ae70441d8520063dd8316c9a9"
    "6f859baeb9771819f320815abe1ee4114d41ad4e97deeb1f247acb2dbe058741eab0f1efbc3436f20e2042bd7b53343680f2e34a75cf3934e825bdc5cbfcbe3e55bc527063c1111c33d21a8e09097f6e"
    "6c0af3a66a0fae2c93ef44692c6cf82e01b2ccd64ca6457a355678cadfae7f9bf41048ca8358dddf6b9bb3cdb356d2abd9d3"
)

PRELUDE_1D14_HEX = "080612120803120a3230313931313835323718022001"

PRELUDE_0314_LOBBY_HEX = (
    "080712db031202656e1ad2036db490ebb4616ab30601d5762b2a1595a22f075d5cc3ef6ea97c18740d90ca071deb5ebb71cd079cfe113e87c3c3c428aa840cf36044bc01d39eab33c42cb0fac1a3de5a"
    "7fe10a9771b507c44ac47efe0be6f35540360c475ac709cd0448502b8ccd615613184d46f4ae49fa5a27110d629d641be1250fa29ac4ee01f86c277d2e9ff925c32fdf8cf39f14450099afdca9739a19"
    "2b070bfcf5eb85936e6143e7b161735a066ecc1d6c8c9fd35f6cdf6bc59c68f0dd803ea87ae5d17a1b6d2900f3b06582aa589479cd984d094d890419a9771d1d09f372ec84fea6747535e3db3c6f549d"
    "9e1fc3976d379d7ff9f918cfa038b6101f34c9e2e7f8ddfec0480f17af036ce3b9b2a9f9c3b1b37ed24b7bf1929005ff09823edbf76484bce1976edcb8a6746fb29706d1b0ded463f5c92abeb8f1cff3"
    "485899128cec1c92921363fbedc44ae497aeb246969b9c88df2b3f274222cd0dba8e0d02cb92ecc146b2439f4079223a8aab67f4da3d5832049f33bd0974b2aa61cae6f5c804f7f25b4c95ff0c089331"
    "ebd10e882f9ef72a73e51c290d48c161af057cf50a6db9f61309e27441e438b7dd3051cb082851d1cff3c6dcbb8c23d8cfd2f81f5dcf969560e00db35e7ccb208bd76284851b570539b9e8168fd72801"
)

POST_MATCH_2814_HEX = (
    "080512a1300a9e30c7557f3ca80be7477c09e68fe4ac7b942f7d2b00061800006db490eb5b7e30b13217d5762b2a15959bd8c48dbad74ebafb28e6e45186ef96178a4b1b0da4b44ec7dafc4ee13cf314"
    "39a01250441411f682ce90bf03c8d308a9fc204c77ce2fddf8b44777d513b1ac7548a63c3e0d6a1d054b68b150e1c1d7d76074761fb79bd82485c097cd40613f0bd1a36c9283ae96ec0f4c7f3702b7edd"
    "32322f56099289b6c40f5ae09397283d9a5c80dbe891629079ef4df5fc15dfe3f8d28407b909af4943deea1c3ca4e51b4abb2a6698da85510dbcfe5c91b6be31d8db457c35e5b0f73e098f45d4bd75dc"
    "489de1943a0166986d7c2a5549875bfaf240a1c140fcb16c38d20e91af363376b962af2e7614eb7b72eae838b37ece15dc7505e7cc56759f73d6ce2ba40bc85b09593d19b8ff39a974696062d80cfdae"
    "faee9a74c07475281520c325a930dbc688b495e253edcafb4982baf13f5517af5de395affcffd941fca8ff74b028d4ac5ddeece5a3a9ab5bc4585a5349abf655857a8566d605935761a058c2a62d260fb"
    "2e6a22a1d5b9bd1c789e44001af978907caeb1a9d41188965f58fe85a7112d5f68d718e9e72d437c692556483dbdf8b803e684a21e052c07f433e10dd5aed1534f50b85fa13d19b8f9a7a17aeb1aebe22"
)


TCP_AUTH_OPCODES = (0x8f, 0x78, 0x95, 0x83)


MSG_KEY_HELLO      = 0x63
MSG_KEY_ACK_HELLO  = 0x5B
MSG_KEY_ACK        = 0x68
MSG_KEY_AUTH_447   = 0x5E
MSG_KEY_AUTH_448   = 0x5A
MSG_KEY_PING_POOL  = (0x66, 0x6D, 0x69, 0x6C, 0x6B, 0x6E, 0x6F, 0x70)


UDP_PROFILES = {
    "cap1018": dict(ver="2.133.13", mode=None, fresh=True, dyn_type=True, a17=3,
                    hash="792d831ce9392f40cd041bbeecf7a43f95daa02703f7435f6cb3193a1b7e7276",
                    blk=bytes.fromhex("00" "0000000100")),
    "cap1017": dict(ver="2.133.11", mode=None, fresh=True, dyn_type=True, a17=1,
                    hash="2098bf342547eea78f4d793f19c4b81f93f23f75c54b18c8137321104b88f60b",
                    blk=bytes.fromhex("00" "0000000100")),
    "cap1016": dict(ver="2.133.11", mode=None, fresh=True,
                    hash="b538caf1b3e69370d848faf2bb4ab093cd003591f7230872920db551e99d2850",
                    blk=bytes.fromhex("00" "0000000100")),
    "cap1014": dict(ver="2.133.10", mode=15, fresh=True,
                    hash="e893f944f6a84d4bb12329f5559252c32431876b72c2c2766103fa761157b319",
                    blk=bytes.fromhex("01" "0000030100")),
    "cap1010": dict(ver="2.133.9",  mode=1,  fresh=False,
                    hash="8996cc6627ec7b44200f36c3d439dbd750b489f43023837aa412c1b43ab05e48",
                    blk=bytes.fromhex("01" "0000000200")),
}
UDP_PROFILE_ORDER = ("cap1018", "cap1017", "cap1016", "cap1014", "cap1010")


HANDSHAKE_STEP_DELAY = 10.0
HELLO_TO_447_DELAY   = 2.8
AUTH_447_TO_448      = 0.3
JOIN_WAIT_AFTER_448  = 8.0
TCP_HEARTBEAT_INTERVAL_LW = 1.0


def lw_token_split(token: str) -> int:
    return 1060 if len(token) > 1160 else 660


_DEVICES_MEMORY_CACHE: Dict[str, dict] = {}
_DEVICES_CACHE_LOADED: bool = False
_DEVICES_DIRTY: bool = False


def _load_devices_cache_once():
    global _DEVICES_MEMORY_CACHE, _DEVICES_CACHE_LOADED
    if _DEVICES_CACHE_LOADED:
        return
    if os.path.exists(DEVICES_FILE):
        try:
            with open(DEVICES_FILE, "r", encoding="utf-8") as f:
                _DEVICES_MEMORY_CACHE = json.load(f)
        except Exception:
            _DEVICES_MEMORY_CACHE = {}
    _DEVICES_CACHE_LOADED = True


def _save_devices_cache():
    global _DEVICES_DIRTY
    try:
        os.makedirs(os.path.dirname(DEVICES_FILE), exist_ok=True)
        tmp_file = DEVICES_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(_DEVICES_MEMORY_CACHE, f, indent=4)
        os.replace(tmp_file, DEVICES_FILE)
        _DEVICES_DIRTY = False
    except Exception as e:
        print(f"[WARN] Failed to save devices cache: {e}")


def get_device_for_account(account_identifier: str) -> dict:
    global _DEVICES_DIRTY
    _load_devices_cache_once()

    acc_key = str(account_identifier)


    if acc_key in _DEVICES_MEMORY_CACHE:
        return _DEVICES_MEMORY_CACHE[acc_key]


    device_list = [
        ("Samsung", "SM-G998B", "Adreno (TM) 660", "Android OS 12 / API-31"),
        ("Xiaomi", "2201122G", "Adreno (TM) 730", "Android OS 13 / API-33"),
        ("Realme", "RMX3700", "Mali-G710", "Android OS 14 / API-34"),
        ("OnePlus", "CPH2451", "Adreno (TM) 740", "Android OS 13 / API-33"),
        ("OPPO", "CPH2611", "Adreno (TM) 720", "Android OS 14 / API-34"),
        ("Vivo", "V2203", "Mali-G710", "Android OS 12 / API-31"),
        ("Poco", "M2102J20SG", "Adreno (TM) 660", "Android OS 13 / API-33"),
    ]
    brand, model, gpu, os_ver = random.choice(device_list)

    new_device = {
        "unique_device_id": f"Google|{str(uuid.uuid4())}",
        "brand": brand,
        "model": model,
        "gpu_renderer": gpu,
        "system_software": os_ver,
        "screen_width": random.choice([1080, 1440, 720, 1280]),
        "screen_height": random.choice([2400, 3200, 1600, 2400]),
        "screen_dpi": str(random.randint(300, 420)),
        "memory": random.randint(2800, 6500),
        "processor_details": f"ARM64 FP ASIMD AES VMH | {random.randint(2200, 3200)} | {random.randint(6, 12)}",


        "client_ip": random.choice([
            f"103.{random.randint(4,15)}.{random.randint(0,255)}.{random.randint(1,254)}",
            f"27.{random.randint(123,147)}.{random.randint(0,255)}.{random.randint(1,254)}",
            f"202.{random.randint(134,140)}.{random.randint(0,255)}.{random.randint(1,254)}",
            f"110.{random.randint(232,235)}.{random.randint(0,255)}.{random.randint(1,254)}",
        ])
    }


    _DEVICES_MEMORY_CACHE[acc_key] = new_device
    _DEVICES_DIRTY = True


    try:
        _save_devices_cache()
    except Exception as e:
        pass

    return new_device


CLOUDFLARE_PRIMARY_DNS = "1.1.1.1"
CLOUDFLARE_SECONDARY_DNS = "1.0.0.1"
_DNS_CACHE: Dict[str, Tuple[str, float]] = {}
_DNS_CACHE_TTL = 1800.0

async def resolve_host_cloudflare(hostname: str) -> str:
    if not hostname:
        return hostname

    parts = hostname.split('.')
    if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
        return hostname

    now = time.time()
    if hostname in _DNS_CACHE:
        ip, exp = _DNS_CACHE[hostname]
        if now < exp:
            return ip

    def _query_cloudflare(server_ip: str) -> Optional[str]:
        s = None
        try:
            tx_id = random.randint(1000, 65535)
            header = struct.pack(">HHHHHH", tx_id, 0x0100, 1, 0, 0, 0)
            qname = b"".join(bytes([len(part)]) + part.encode('ascii') for part in hostname.split('.')) + b"\x00"
            query_pkt = header + qname + struct.pack(">HH", 1, 1)

            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(1.2)
            s.sendto(query_pkt, (server_ip, 53))
            resp, _ = s.recvfrom(1024)

            if len(resp) >= 12:
                ancount = struct.unpack(">H", resp[6:8])[0]
                if ancount > 0:
                    offset = 12 + len(qname) + 4
                    for _ in range(ancount):
                        if offset >= len(resp):
                            break
                        if (resp[offset] & 0xC0) == 0xC0:
                            offset += 2
                        else:
                            while offset < len(resp) and resp[offset] != 0:
                                offset += 1 + resp[offset]
                            offset += 1
                        if offset + 10 > len(resp):
                            break
                        rtype, rclass, ttl, rdlen = struct.unpack(">HHIH", resp[offset:offset+10])
                        offset += 10
                        if rtype == 1 and rdlen == 4 and offset + 4 <= len(resp):
                            return socket.inet_ntoa(resp[offset:offset+4])
                        offset += rdlen
        except Exception:
            pass
        finally:
            if s:
                try:
                    s.close()
                except Exception:
                    pass
        return None

    loop = asyncio.get_running_loop()
    ip = await loop.run_in_executor(None, _query_cloudflare, CLOUDFLARE_PRIMARY_DNS)
    if not ip:
        ip = await loop.run_in_executor(None, _query_cloudflare, CLOUDFLARE_SECONDARY_DNS)
    if not ip:
        try:
            ip_info = await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
            if ip_info:
                ip = ip_info[0][4][0]
        except Exception:
            ip = hostname

    if ip:
        _DNS_CACHE[hostname] = (ip, now + _DNS_CACHE_TTL)
    return ip or hostname


def optimize_tcp_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
    except Exception:
        pass


def optimize_udp_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
        if hasattr(socket, 'SIO_UDP_CONNRESET') and os.name == 'nt':
            try:
                sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception:
                pass
    except Exception:
        pass


def _load_proxies(path: str) -> list:
    proxies = []
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                if "socks" in line.lower():
                    continue
                try:
                    port = int(line.split(":")[-1]) if ":" in line else 0
                except Exception:
                    port = 0
                if port == 1080:
                    continue
                if "://" not in line:
                    line = f"http://{line}"
                proxies.append(line)
    except Exception:
        pass
    return proxies

class ProxyPool:
    def __init__(self, proxy_list: list):
        self._proxies   = proxy_list
        self._cycle     = itertools.cycle(proxy_list) if proxy_list else None
        self._direct    = self._make_client(None)
        self._cache     : dict = {}
        self._lock      = asyncio.Lock()
        self._fail_count: dict = {}
        self._MAX_FAILS  = 3

    def _make_client(self, proxy_url):
        kwargs = dict(
            verify   = False,
            timeout  = 12.0,
            limits   = httpx.Limits(
                max_connections            = 200,
                max_keepalive_connections  = 50,
                keepalive_expiry           = 60,
            ),
        )
        if proxy_url:
            kwargs["proxy"] = proxy_url
        return httpx.AsyncClient(**kwargs)

    async def get_client(self) -> tuple:
        if not self._cycle:
            return self._direct, None

        for _ in range(len(self._proxies)):
            proxy_url = next(self._cycle)
            if self._fail_count.get(proxy_url, 0) >= self._MAX_FAILS:
                continue
            async with self._lock:
                if proxy_url not in self._cache:
                    self._cache[proxy_url] = self._make_client(proxy_url)
            return self._cache[proxy_url], proxy_url

        return self._direct, None

    def mark_fail(self, proxy_url):
        if proxy_url:
            self._fail_count[proxy_url] = self._fail_count.get(proxy_url, 0) + 1

    def mark_ok(self, proxy_url):
        if proxy_url and proxy_url in self._fail_count:
            self._fail_count[proxy_url] = 0

    @property
    def count(self):
        return len(self._proxies)

_PROXY_FILE = os.environ.get("PROXY_FILE", "proxies.txt")
_proxy_list = _load_proxies(_PROXY_FILE)
proxy_pool  = ProxyPool(_proxy_list)
pass


client = proxy_pool._direct


_LOGIN_CONCURRENCY = int(os.environ.get("LOGIN_CONCURRENCY", "15"))
_LOGIN_SEMAPHORE = asyncio.Semaphore(_LOGIN_CONCURRENCY)


FORCED_CLIENT_VERSION = os.environ.get('FF_APP_VERSION', '1.134.21')
FORCED_CLIENT_VERSION_CODE = os.environ.get('FF_CLIENT_VERSION_CODE', '2019121269')
FORCED_RELEASE_VERSION = os.environ.get('FF_RELEASE_VERSION', 'OB55')
DEFAULT_REGION = os.environ.get('FF_REGION', 'IND').upper()
LOGINBP_HOST = 'loginbp.ppmainecoonghj.com'
CLIENTBP_IND = 'https://clientbp.freefireind.in'

AES_KEY = b'Yg&tc%DEuh6%Zc^8'
AES_IV = b'6oyZDr22E3ychjM%'

CRC7_TABLE = bytes([
    0, 9, 18, 27, 36, 45, 54, 63, 72, 65, 90, 83, 108, 101, 126, 119,
    25, 16, 11, 2, 61, 52, 47, 38, 81, 88, 67, 74, 117, 124, 103, 110,
    50, 59, 32, 41, 22, 31, 4, 13, 122, 115, 104, 97, 94, 87, 76, 69,
    43, 34, 57, 48, 15, 6, 29, 20, 99, 106, 113, 120, 71, 78, 85, 92,
    100, 109, 118, 127, 64, 73, 82, 91, 44, 37, 62, 55, 8, 1, 26, 19,
    125, 116, 111, 102, 89, 80, 75, 66, 53, 60, 39, 46, 17, 24, 3, 10,
    86, 95, 68, 77, 114, 123, 96, 105, 30, 23, 12, 5, 58, 51, 40, 33,
    79, 70, 93, 84, 107, 98, 121, 112, 7, 14, 21, 28, 35, 42, 49, 56,
    65, 72, 83, 90, 101, 108, 119, 126, 9, 0, 27, 18, 45, 36, 63, 54,
    88, 81, 74, 67, 124, 117, 110, 103, 16, 25, 2, 11, 52, 61, 38, 47,
    115, 122, 97, 104, 87, 94, 69, 76, 59, 50, 41, 32, 31, 22, 13, 4,
    106, 99, 120, 113, 78, 71, 92, 85, 34, 43, 48, 57, 6, 15, 20, 29,
    37, 44, 55, 62, 1, 8, 19, 26, 109, 100, 127, 118, 73, 64, 91, 82,
    60, 53, 46, 39, 24, 17, 10, 3, 116, 125, 102, 111, 80, 89, 66, 75,
    23, 30, 5, 12, 51, 58, 33, 40, 95, 86, 77, 68, 123, 114, 105, 96,
    14, 7, 28, 21, 42, 35, 56, 49, 70, 79, 84, 93, 98, 107, 112, 121,
])

_DELTA = 0x9E3779B9
_ROUNDS = 16
_FIELD_SIZES = {0: 1, 1: 2, 2: 2, 3: 1, 4: 2}
_FIELD_NAMES = {0: "sendOption", 1: "cmd", 2: "orderId", 3: "flags", 4: "length"}

match_tail_bytes = bytes.fromhex(
    "0101030101045452000103000100000410312e3133302e3232"
    "1432303139313231303430ca0163736f7665727365612e737472"
    "6f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
    "3b302e302e302e303b33342e3132362e37362e34353b33342e38"
    "372e3137372e31343b33342e38372e3137302e3233303b33352e"
    "3138352e3138332e353700000000000001000000000000000000"
    "0000000100000000000100000000000100b8eeec91c5d7ffde110200"
)

headers = {
    'User-Agent': 'UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)',
    'Accept': '*/*',
    'Accept-Encoding': 'deflate, gzip',
    'X-GA-SV': '1791861029',
    'X-GA': 'v1 1',
    'ReleaseVersion': 'OB55',
    'Content-Type': 'application/x-www-form-urlencoded',
    'X-Unity-Version': '2018.4.12f1',
}

class Colors:
    HEADER = '\033[95m'
    GREEN = '\033[92m'
    FAIL = '\033[91m'
    WARNING = '\033[93m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    WHITE = '\033[97m'
    ENDC = '\033[0m'

_PRINT_TOKENS = 60.0
_PRINT_LAST = time.monotonic()

def print_colored(text, color=Colors.WHITE):
    if getattr(bot_state, "live_ui_active", False):
        return
    global _PRINT_TOKENS, _PRINT_LAST
    _now = time.monotonic()
    _PRINT_TOKENS = min(60.0, _PRINT_TOKENS + (_now - _PRINT_LAST) * 30.0)
    _PRINT_LAST = _now
    if _PRINT_TOKENS < 1.0:
        return
    _PRINT_TOKENS -= 1.0
    try:
        print(f"{color}{text}{Colors.ENDC}")
    except Exception:
        try:
            print(f"{color}{text.encode('ascii', errors='replace').decode('ascii')}{Colors.ENDC}")
        except Exception:
            pass

def print_success(text):
    print_colored(f"[+] {text}", Colors.GREEN)
    try:
        bot_state.log(text, "success", print_to_console=False)
    except Exception:
        pass

def print_error(text):
    print_colored(f"[-] {text}", Colors.FAIL)
    try:
        bot_state.log(text, "error", print_to_console=False)
    except Exception:
        pass

def print_warning(text):
    print_colored(f"[!] {text}", Colors.WARNING)
    try:
        bot_state.log(text, "warning", print_to_console=False)
    except Exception:
        pass

def print_info(text):
    print_colored(f"[i] {text}", Colors.CYAN)
    try:
        bot_state.log(text, "info", print_to_console=False)
    except Exception:
        pass

def get_proto_field(d, key, default=None):
    if not d or not isinstance(d, dict):
        return default
    if key in d:
        val = d[key].get('data')
        return val if val is not None else default
    if str(key) in d:
        val = d[str(key)].get('data')
        return val if val is not None else default
    return default


_match_counters: Dict[str, int] = {}
_match_counter_lock = asyncio.Lock()

async def _inc_match(uid: str) -> int:
    async with _match_counter_lock:
        _match_counters[uid] = _match_counters.get(uid, 0) + 1
        return _match_counters[uid]

async def _dec_match(uid: str) -> int:
    async with _match_counter_lock:
        if uid in _match_counters and _match_counters[uid] > 0:
            _match_counters[uid] -= 1
        return _match_counters.get(uid, 0)

async def _get_match_count(uid: str) -> int:
    async with _match_counter_lock:
        return _match_counters.get(uid, 0)

async def _get_total_match_count() -> int:
    async with _match_counter_lock:
        return sum(_match_counters.values())


_token_cache_memo: Dict[str, Any] = {}
_token_cache_memo_time: float = 0.0
_TOKEN_CACHE_MEMO_TTL = 30.0

def _json_serializer(obj):
    if isinstance(obj, (bytes, bytearray)):
        return {"__bytes_hex__": bytes(obj).hex()}
    raise TypeError(f"Type {type(obj)} not serializable")

def _json_deserializer(obj):
    if isinstance(obj, dict):
        if "__bytes_hex__" in obj and len(obj) == 1:
            try:
                return bytes.fromhex(obj["__bytes_hex__"])
            except Exception:
                return b""
        return {k: _json_deserializer(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_deserializer(x) for x in obj]
    return obj

def _load_token_cache() -> Dict[str, Any]:
    global _token_cache_memo, _token_cache_memo_time
    now = time.time()
    if _token_cache_memo and (now - _token_cache_memo_time) < _TOKEN_CACHE_MEMO_TTL:
        return _token_cache_memo

    if not os.path.exists(TOKEN_CACHE_FILE):
        return {}
    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return {}
        data = json.loads(content)
        if not isinstance(data, dict):
            raise ValueError("Cache root must be dict")
        parsed = _json_deserializer(data)
        _token_cache_memo = parsed
        _token_cache_memo_time = now
        return parsed
    except Exception as e:
        print_error(f"Token cache corrupt → deleting: {e}")
        try:
            os.remove(TOKEN_CACHE_FILE)
        except Exception:
            pass
        return {}

def _save_token_cache(cache: Dict[str, Any]):
    global _token_cache_memo, _token_cache_memo_time
    try:
        os.makedirs(os.path.dirname(TOKEN_CACHE_FILE), exist_ok=True)
        tmp_file = TOKEN_CACHE_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, default=_json_serializer)
        os.replace(tmp_file, TOKEN_CACHE_FILE)
        _token_cache_memo = cache
        _token_cache_memo_time = time.time()
    except Exception as e:
        print_error(f"Token cache save error: {e}")

def cache_get(uid: str) -> Optional[Dict]:
    cache = _load_token_cache()
    entry = cache.get(str(uid))
    if not entry:
        return None
    if time.time() - entry.get("cached_at", 0) > TOKEN_CACHE_TTL:
        print_info(f"[CACHE] UID {uid} expired. Re-login needed.")
        cache_invalidate(uid)
        return None
    if str(entry.get("account_id", "")).isdigit():
        entry["account_id"] = int(entry["account_id"])
    if not isinstance(entry.get("login_payload_data"), (bytes, bytearray)):
        print_warning(f"[CACHE] UID {uid} missing payload → invalidating")
        cache_invalidate(uid)
        return None

    if "stronghold" not in entry:
        print_info(f"[CACHE] UID {uid} old cache (no stronghold) → fresh login")
        cache_invalidate(uid)
        return None
    return entry

def cache_set(uid: str, account_data: Dict):
    cache = _load_token_cache()
    entry = dict(account_data)


    jitter = random.uniform(-TOKEN_CACHE_TTL * 0.20, TOKEN_CACHE_TTL * 0.20)
    entry["cached_at"] = time.time() + jitter
    cache[str(uid)] = entry
    _save_token_cache(cache)
    print_success(f"[CACHE] Saved credentials for UID {uid}")

def cache_invalidate(uid: str):
    cache = _load_token_cache()
    if str(uid) in cache:
        del cache[str(uid)]
        _save_token_cache(cache)
        print_warning(f"[CACHE] Invalidated: {uid}")


async def aes_encrypt(payload, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return cipher.encrypt(pad(payload, AES.block_size))


_current_login_uid = contextvars.ContextVar("current_login_uid", default=None)

def report_fail(uid, stage: str, detail: str = "", generic: bool = False):
    if uid is None:
        uid = _current_login_uid.get()
    msg = f"[LOGIN FAIL] {stage}" + (f" -> {detail}" if detail else "")
    print_error(msg)
    try:
        bot_state.log(msg, "error", str(uid) if uid is not None else None)
        if uid is not None:
            acc = bot_state.accounts.get(str(uid))

            if not (generic and acc and acc.get("last_error")):
                bot_state.set_error(str(uid), f"{stage}" + (f" -> {detail}" if detail else ""))
    except Exception:
        pass

async def get_playstore_version():

    env_ver = os.environ.get("FF_APP_VERSION", "").strip()
    if env_ver:
        return env_ver
    loop = asyncio.get_running_loop()
    for country in ("id", "bd", "in", "us"):
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda c=country: play_scraper('com.dts.freefireth', lang='hi', country=c)
                ),
                timeout=20
            )
            ver = (result or {}).get("version")
            if ver and any(ch.isdigit() for ch in str(ver)):
                return ver
            report_fail(None, f"Play Store version invalid ({country})", str(ver))
        except Exception as e:
            report_fail(None, f"Play Store scrape failed ({country})", repr(e)[:150])
    return None

async def version_config():
    app_version = await get_playstore_version() or FORCED_CLIENT_VERSION

    if str(app_version).startswith("1.132") or str(app_version).startswith("1.133"):
        app_version = FORCED_CLIENT_VERSION
    api_url = (
        "https://version.ggwhitehawk.com/live/ver.php"
        f"?version={app_version}"
        "&lang=en&device=android&channel=android"
        "&appstore=googleplay&region=IND"
        "&whitelist_version=1.3.0&whitelist_sp_version=1.0.0"
    )
    try:
        response = await client.get(api_url)
        response.raise_for_status()
        data = response.json()
        server_url = data.get("server_url") or f"https://{LOGINBP_HOST}/"
        remote_version = data.get("remote_version") or FORCED_CLIENT_VERSION
        latest_release_version = data.get("latest_release_version") or FORCED_RELEASE_VERSION
        if str(remote_version).startswith("1.132") or str(remote_version).startswith("1.133"):
            remote_version = FORCED_CLIENT_VERSION
            latest_release_version = FORCED_RELEASE_VERSION

        if "ggbluefox" in str(server_url).lower() or "ggpolarbear" in str(server_url).lower():
            server_url = f"https://{LOGINBP_HOST}/"
        return latest_release_version, remote_version, server_url
    except Exception as e:
        report_fail(None, "ver.php request failed — using forced IND live", repr(e)[:200])
        return FORCED_RELEASE_VERSION, FORCED_CLIENT_VERSION, f"https://{LOGINBP_HOST}/"


_version_config_cache: Optional[tuple] = None
_version_config_cache_time: float = 0.0
_VERSION_CONFIG_CACHE_TTL = 7200.0
_version_config_lock: Optional[asyncio.Lock] = None

def _get_version_config_lock():
    global _version_config_lock
    if _version_config_lock is None:
        _version_config_lock = asyncio.Lock()
    return _version_config_lock

async def version_config_cached():
    global _version_config_cache, _version_config_cache_time
    now = time.time()

    if _version_config_cache and (now - _version_config_cache_time) < _VERSION_CONFIG_CACHE_TTL:
        return _version_config_cache

    async with _get_version_config_lock():
        now = time.time()
        if _version_config_cache and (now - _version_config_cache_time) < _VERSION_CONFIG_CACHE_TTL:
            return _version_config_cache
        print_info("[VERSION] Fetching version config (shared for all accounts)...")
        result = await version_config()
        if result:
            _version_config_cache = result
            _version_config_cache_time = time.time()
            print_success(f"[VERSION] Cached: {result[0]} / {result[1]}")
        return result

def version_config_invalidate():
    global _version_config_cache, _version_config_cache_time
    _version_config_cache = None
    _version_config_cache_time = 0.0
    print_warning("[VERSION] Cache invalidated — will re-fetch on next login attempt")

async def get_access_token(uid, password):
    url = "https://100067.connect.garena.com/oauth/guest/token/grant"
    hdrs = {
        "Host": "100067.connect.garena.com",
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; SM-G998B Build/SP1A.210812.016)",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close"
    }
    data = {
        "uid": uid,
        "password": password,
        "response_type": "token",
        "client_type": "2",
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_id": "100067"
    }
    last_err = ""
    for attempt in range(5):
        try:
            response = await client.post(url, headers=hdrs, data=data)
            last_err = f"HTTP {response.status_code} {response.text[:150]}"
            if response.status_code == 200:
                response_data = response.json()
                open_id = response_data.get("open_id")
                access_token = response_data.get("access_token")
                platform = response_data.get("platform", 4)
                if open_id and access_token:
                    return open_id, access_token, platform


                err_code = response_data.get("error_code") or response_data.get("code") or response_data.get("errno")
                err_msg  = str(response_data.get("error_message") or response_data.get("msg") or "").lower()
                _BAN_CODES = {1013, 1014, 1015, 10013, 10014, 10015, 4026531857}
                _BAN_WORDS = ("ban", "suspend", "frozen", "freeze", "terminated", "blocked", "disabled", "restricted")
                is_banned = (err_code in _BAN_CODES) or any(w in err_msg for w in _BAN_WORDS)
                if is_banned:
                    report_fail(uid, f"[BANNED] Account {uid} banned/suspended (code={err_code} msg={err_msg[:80]})", "auto-removing")

                    try:
                        bot_state.deleted_uids.add(str(uid))
                        bot_state._save_deleted_uids()
                        bot_state._remove_from_json(str(uid))
                        bot_state.accounts.pop(str(uid), None)
                        bot_state.recalc_totals()
                    except Exception:
                        pass
                    return "BANNED"
                last_err = f"200 but no token: {str(response_data)[:120]}"
            if response.status_code == 403:

                report_fail(uid, f"[BANNED] Account {uid} HTTP 403 — permanently banned", "auto-removing")
                try:
                    bot_state.deleted_uids.add(str(uid))
                    bot_state._save_deleted_uids()
                    bot_state._remove_from_json(str(uid))
                    bot_state.accounts.pop(str(uid), None)
                    bot_state.recalc_totals()
                except Exception:
                    pass
                return "BANNED"
            if response.status_code == 429:
                wait = 2.0 * (attempt + 1) + random.uniform(0.5, 1.5)
                await asyncio.sleep(wait)
                continue
        except Exception as e:
            last_err = repr(e)[:150]
        await asyncio.sleep(random.uniform(0.3, 0.8))
    report_fail(uid, "Garena guest token grant failed (UID/Password thik ache? IP block?)", last_err)
    return None

async def parse_results(parsed_results):
    result_dict = {}
    for result in parsed_results:
        field_data = {"wire_type": result.wire_type}
        if result.wire_type == "varint":
            field_data["data"] = result.data
        elif result.wire_type == "string":
            field_data["data"] = result.data
        elif result.wire_type == "bytes":
            field_data["data"] = result.data
        elif result.wire_type == "length_delimited":
            field_data["data"] = await parse_results(result.data.results)
        result_dict[result.field] = field_data
    return result_dict

async def decode_protobuf(data):
    parsed_results = Parser().parse(data)
    parsed_results_dict = await parse_results(parsed_results)
    return json.dumps(parsed_results_dict)

async def build_majorlogin_payload_legacy(open_id, access_token, platform, client_version, device_info):
    try:
        proto = Pb2_proto.MajorLoginReq()
        proto.event_time = str(datetime.now())[:-7]
        proto.game_name = "free fire"
        proto.platform_id = 1 if str(platform) in ["1", "4"] else int(platform)
        proto.client_version = client_version
        proto.client_version_code = FORCED_CLIENT_VERSION_CODE


        proto.system_software = device_info.get("system_software", "Android OS 10 / API-29 (QQ3A.200805.001/135998ea76)")
        proto.system_hardware = device_info.get("brand", "Handheld")
        proto.device_type = device_info.get("model", "Handheld")
        proto.screen_width = int(device_info.get("screen_width", 1666))
        proto.screen_height = int(device_info.get("screen_height", 750))
        proto.screen_dpi = str(device_info.get("screen_dpi", "400"))
        proto.processor_details = device_info.get("processor_details", "ARM64 FP ASIMD AES | 2304 | 8")
        proto.memory = int(device_info.get("memory", 5602))
        proto.gpu_renderer = device_info.get("gpu_renderer", "Adreno (TM) 618")
        proto.unique_device_id = device_info.get("unique_device_id", "Google|725030d8-6585-4f55-bcca-a6df7e59935b")
        proto.client_ip = device_info.get("client_ip", "103.145.112.210")


        proto.telecom_operator = "Citycell"
        proto.network_operator_a = "Citycell"
        proto.network_type = "WIFI"
        proto.network_type_a = "WIFI"
        proto.cpu_type = 2
        proto.cpu_architecture = "64"
        proto.gpu_version = "OpenGL ES 3.2 V@415.0 (GIT@29668c6, I386ada412e, 1599643163) (Date:09/09/20)"
        proto.graphics_api = "OpenGLES2"
        proto.language = "en"
        proto.open_id = open_id
        proto.open_id_type = str(platform)
        proto.login_open_id_type = int(platform)
        proto.access_token = access_token
        proto.login_by = 2
        proto.platform_sdk_id = 1
        proto.origin_platform_type = str(platform)
        proto.primary_platform_type = str(platform)
        proto.reg_avatar = 1020000007
        proto.channel_type = 3

        memory_available = proto.memory_available
        memory_available.version = 55
        memory_available.hidden_value = 81


        proto.external_storage_total = 107339
        proto.external_storage_available = 98183
        proto.internal_storage_total = 2477
        proto.internal_storage_available = 673
        proto.game_disk_storage_total = 98311
        proto.game_disk_storage_available = 107339
        proto.external_sdcard_total_storage = 98311
        proto.external_sdcard_avail_storage = 107339

        proto.library_path = "/data/app/com.dts.freefireth-cXks8_q21Wm61sLpm1Mwkw==/lib/arm64"
        proto.library_token = "b8e0cd5e295eee42f5860d3c86e483dd|/data/app/com.dts.freefireth-cXks8_q21Wm61sLpm1Mwkw==/base.apk"
        proto.client_using_version = "7428b253defc164018c604a1ebbfebdf"
        proto.supported_astc_bitset = 4095

        proto.loading_time = random.randint(5000, 9000)
        proto.release_channel = "android"
        proto.extra_info = "KqsHTwUcsotzm7T5wpHoa7zprR/0nb96MPMDflVhet3xaqMVtM9nR8cyff0bn0DL/AqN+M57YFwW+60+kUocBgoAwICMqslgRX9NHghmHqoVcF9O"
        proto.android_engine_init_flag = 111207
        proto.if_push = 1
        proto.is_vpn = 1

        payload = proto.SerializeToString()
        return await aes_encrypt(payload, AES_KEY, AES_IV)
    except Exception:
        return None


def _pb_rd_var(buf, off):
    n = sh = 0
    while True:
        if off >= len(buf):
            raise ValueError("eof")
        b = buf[off]; off += 1
        n |= (b & 0x7F) << sh
        if not (b & 0x80):
            return n, off
        sh += 7
        if sh > 63:
            raise ValueError("long")

def _lw_varint(n):
    if n < 0:
        n &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = n & 0x7F; n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return bytes(out)

def _pb_patch_marker(buf, marker, new_uid):
    if marker not in buf:
        return None
    if buf.startswith(marker):
        end = len(marker)
        while end < len(buf) and 0x30 <= buf[end] <= 0x39:
            end += 1
        return marker + new_uid + buf[end:]
    out, i = bytearray(), 0
    while i < len(buf):
        tag, j = _pb_rd_var(buf, i)
        wt = tag & 7
        if wt == 0:
            _, k = _pb_rd_var(buf, j); out += buf[i:k]; i = k
        elif wt == 1:
            out += buf[i:j + 8]; i = j + 8
        elif wt == 5:
            out += buf[i:j + 4]; i = j + 4
        elif wt == 2:
            ln, k = _pb_rd_var(buf, j)
            payload = buf[k:k + ln]
            if marker in payload:
                inner = _pb_patch_marker(payload, marker, new_uid)
                if inner is None:
                    raise ValueError("marker lost")
                out += buf[i:j] + _lw_varint(len(inner)) + inner
            else:
                out += buf[i:k + ln]
            i = k + ln
        else:
            raise ValueError("unsupported wire type")
    return bytes(out)

def patch_lw_account_id(hex_str: str, new_uid: str) -> bytes:
    raw = bytes.fromhex(hex_str)
    new = str(new_uid).encode("ascii")
    for marker in (b"INDIA_NOTINIT?", b"BD_NOTINIT?", b"US_NOTINIT?"):
        if marker not in raw:
            continue
        try:
            return _pb_patch_marker(raw, marker, new)
        except Exception:
            pass
        raw2 = bytearray(raw)
        idx = raw2.find(marker)
        start = idx + len(marker)
        end = start
        while end < len(raw2) and 0x30 <= raw2[end] <= 0x39:
            end += 1
        delta = len(new) - (end - start)
        raw2 = raw2[:start] + new + raw2[end:]
        if idx >= 2 and raw2[idx - 2] == 0x0a and raw2[idx - 1] < 0x80:
            n = raw2[idx - 1] + delta
            if 0 <= n < 0x80:
                raw2[idx - 1] = n
        return bytes(raw2)
    return raw


LW_HS_MATCH_START_HEX = (
    "080112910b0a010b104a3a0d0a0449444331102b1a03494e443a0d0a0449444332101e1a03494e4440014a0901090a0b1219"
    "202729580162900a0a19494e4449415f4e4f54494e49543f313838393131393938333510101af7037e505554120808440401"
    "05015c5a09015b015b0301520b5b5b00085d000853545609005f065500001b00044f7d54494548110814001c10090b450157"
    "777a0b5e6d4262767363484f7a6e5f7c770e7f01647a0b71750c1602446c095b675e5e686751740d606303794f714d7e0047"
    "7e5268527274051b575a5d4c166a791c7e484803496b557f71665b684e6058655f5058095057510f517e0002700d0f780c1a"
    "090b4d52575f56715360517368071e586f5e4f09056f60737e474073557c0c1201444c06651c536a6b1e6b007c4b6b5f4053"
    "4b705a481276135e4d4079041a034d637c760164616b580c6079606a6f5061404d4b0d700649534b50420e1b004579584962"
    "5f6365434f584167094c0e5d02030b7353717c585d505f0c160200457e4346704e61437176535b0c6375726a7c7f74446b75"
    "691e5f714f0512064f5670774b0116427f567d587e6d7177427b7a08425b7100765c0552041b0c4d58434073405747756075"
    "78535b5b636367425f6a6d7605425a555e0c120b0844526f4b6a0940614b567417616f5169727b6566485d70624c4b096004"
    "12044560567a674179095b087c7d5a137f7b4675436c021a6c6a7c5a5403051a0e4d60526e497d1a0a79745d071a01567c59"
    "7476764a4346695b004e5a08100e4577087f53595c627f63484b0c6941720e00725307797e017d686e000522047f56565d30"
    "0e3a09177d006a77657c1c1c4208322e3133332e3133480250025ad505036262535136396577704c724c417456324b796f56"
    "6c566a57546b63776a64705a6a762b546f696b624e4d4c4e67766a7561334c54705265316364685a616a755772633771626d"
    "624b51754850644d692b65454437695a6d58495a725846474a37667a3832627a504c715a39743767687957547a393769764f"
    "516746674f77566b686755706f4b5652474a50434e414b684c4151574b4c3854636249356a4871566f766659737a43457a54"
    "4838353344656d6269556e4c4e765a4e7272342f56582f516c384f53343264495464755739666444344d376c2b6754443169"
    "2f6c5452374136723344594b5a6178587348704c7571666b7667695052776f573035304c47662b575170436250442f68434f"
    "4f48377964542b464f5836386373525755716d63795242307578746d6f38566d2f6f777559724a6969556e70364a5376382b"
    "646e524f3962456d694a4668557a4b7a4f315452794372545a585a3779324b4833785942455a374553626733796152333135"
    "4b73736d48745068787433704f72654632315967353058554e2f446c67336a6361613135555143626b6b6d76484c2b784a38"
    "37612f2b337256683765356846647748343973696f71764d483637462b30494b7a662b685a785643664a47694965384c4a64"
    "436b544e3967584c45552b304e4e5a634a4e4255384b39733234704d4f6c2f4e487452452b306c58753362343972766a596e"
    "46414b586438373852574a3954392b7378652f34504b554e4a644638435669314e56554167486c6743477636334a526a3759"
    "6175754a5278586a6464356c546f6570344a6c4244416550314a4d4f4a6a4e524d584d696e44426245454b646e435a6d4a6c"
    "35474d554968436835346e7767693661774c6871452b743675513353314d334d637a5a576f704c38526a34747155326c4f52"
    "55554b476c43396e4e493352476c306b723156362b464b764372774f76677252326d326166582b59766b4d5046564a326336"
    "46416e79a20105080310f902a201050804108703a201050805109f01a20105081d10c801a201040816105ca20105080e1090"
    "01a201020815aa0115615f35353037313839393735323830313932373135"
)


def _pb_varint(n: int) -> bytes:
    n = int(n)
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _pb_encode_fields(fields: Dict[int, Any]) -> bytes:
    out = bytearray()
    for num in sorted(fields):
        val = fields[num]
        if val is None:
            continue
        if isinstance(val, bool):
            val = int(val)
        if isinstance(val, int):
            out += _pb_varint((num << 3) | 0) + _pb_varint(val)
        else:
            data = val if isinstance(val, (bytes, bytearray)) else str(val).encode("utf-8")
            out += _pb_varint((num << 3) | 2) + _pb_varint(len(data)) + bytes(data)
    return bytes(out)


_DEVICE_POOL = [

    ("Xiaomi POCO X3",      "10", "29", "QQ3A.200805.001/135998ea76", 1066, 2400, "400", "ARM64 FP ASIMD AES | 2304 | 8", 5602,  "Adreno (TM) 618", "OpenGL ES 3.2 V@415.0 (GIT@29668c6, I386ada412e, 1599643163) (Date:09/09/20)", "Mymensingh"),
    ("Samsung Galaxy A32",  "11", "30", "RP1A.200720.012/A325FXXU5CVJ1",1080, 2400, "420", "ARM64 FP ASIMD AES | 2000 | 8", 5120,  "Mali-G57",        "OpenGL ES 3.2 v1.r32p1-01eac0.37c1571c4ca7e4be7e8f5a5c7e8aba5a", "Dhaka"),
    ("Samsung Galaxy A52",  "11", "30", "RP1A.200720.012/A525FXXU4CVJ2",1080, 2400, "420", "ARM64 FP ASIMD AES | 2000 | 8", 6144,  "Adreno (TM) 618", "OpenGL ES 3.2 V@415.0 (GIT@29668c6, I386ada412e, 1599643163) (Date:09/09/20)", "Chittagong"),
    ("Redmi Note 10",       "11", "30", "RKQ1.200826.002/V12.0.4.0",   1080, 2400, "395", "ARM64 FP ASIMD AES | 2000 | 8", 5120,  "Adreno (TM) 612", "OpenGL ES 3.2 V@0502.0 (GIT@a21ad, I60c1e, 1611770046) (Date:01/27/21)", "Sylhet"),
    ("Redmi 9",             "10", "29", "QKQ1.191117.002/V11.0.5.0",    720, 1600, "269", "ARM64 FP ASIMD AES | 2000 | 8", 4096,  "Mali-G52-MC2",    "OpenGL ES 3.2 v1.r19p0-01rel0.6d1f6b4e72c3c2de76df1f5b0", "Rajshahi"),
    ("Realme C35",          "11", "30", "RP1A.200720.011/RMX3511PU_11", 1080, 2408, "400", "ARM64 FP ASIMD AES | 1800 | 8", 4096,  "Mali-G57 MC2",    "OpenGL ES 3.2 v1.r32p1-01eac0.37c1571c4ca7e4be7e8f5a5c7e8aba5a", "Khulna"),
    ("Vivo Y21",            "11", "30", "RP1A.200720.012/V210_A_6.35.0", 720, 1600, "270", "ARM64 FP ASIMD AES | 1800 | 8", 3072,  "PowerVR GE8320",  "OpenGL ES 3.2 build 1.13@5776728", "Barisal"),
    ("OPPO A57",            "11", "30", "RP1A.200720.011/CPH2387PU_11",  720, 1612, "269", "ARM64 FP ASIMD AES | 1800 | 8", 3072,  "PowerVR GE8320",  "OpenGL ES 3.2 build 1.13@5776728", "Comilla"),
    ("Tecno Spark 8",       "11", "30", "RP1A.200720.011/TECNO-KG6j",    720, 1600, "270", "ARM64 FP ASIMD AES | 1600 | 8", 2048,  "Mali-G52-MC2",    "OpenGL ES 3.2 v1.r19p0-01rel0.6d1f6b4e72c3c2de76df1f5b0", "Rangpur"),
    ("Xiaomi Redmi Note 11","11", "30", "RP1A.200720.011/V13.0.1.0",    1080, 2400, "395", "ARM64 FP ASIMD AES | 2000 | 8", 4096,  "Adreno (TM) 610", "OpenGL ES 3.2 V@0502.0 (GIT@a21ad, I60c1e, 1611770046) (Date:01/27/21)", "Gazipur"),
    ("Samsung Galaxy M32",  "11", "30", "RP1A.200720.012/M325FXXU3CVJ1",1080, 2400, "420", "ARM64 FP ASIMD AES | 2000 | 8", 6144,  "Mali-G57",        "OpenGL ES 3.2 v1.r32p1-01eac0.37c1571c4ca7e4be7e8f5a5c7e8aba5a", "Narayanganj"),
    ("Infinix Hot 11",      "11", "30", "RP1A.200720.011/H6116V2P1",    720, 1600, "270", "ARM64 FP ASIMD AES | 1600 | 8", 2048,  "Mali-G52-MC2",    "OpenGL ES 3.2 v1.r19p0-01rel0.6d1f6b4e72c3c2de76df1f5b0", "Jessore"),
]

_uid_device_cache: dict = {}

def _get_device_for_uid(uid: str) -> dict:
    if uid in _uid_device_cache:
        return _uid_device_cache[uid]
    rng = random.Random(int(uid) if uid.isdigit() else hash(uid))
    d = rng.choice(_DEVICE_POOL)
    model, os_ver, api, build_id, sw, sh, dpi, cpu, ram, gpu_r, gpu_v, city = d

    ext_total  = rng.randint(60000, 120000)
    ext_avail  = rng.randint(int(ext_total * 0.6), int(ext_total * 0.9))
    int_total  = rng.randint(2000, 4000)
    int_avail  = rng.randint(400, 800)
    apk_suffix = ''.join(rng.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-', k=20))
    lib_path   = f"/data/app/com.dts.freefireth-{apk_suffix}==/lib/arm64"
    lib_md5    = ''.join(rng.choices('0123456789abcdef', k=32))
    lib_token  = f"{lib_md5}|/data/app/com.dts.freefireth-{apk_suffix}==/base.apk"
    profile = {
        8:  f"Android OS {os_ver} / API-{api} ({build_id})",
        12: sw,
        13: sh,
        14: str(dpi),
        15: cpu,
        16: ram,
        17: gpu_r,
        18: gpu_v,
        25: model,
        60: ext_total,
        61: ext_avail,
        62: int_total,
        63: int_avail,
        64: ext_total - rng.randint(100, 500),
        65: ext_avail,
        66: ext_total,
        67: ext_avail,
        74: lib_path,
        77: lib_token,
        90: city,
    }
    _uid_device_cache[uid] = profile
    return profile


REAL_CLIENT_PROFILE = {

    8:  "Android OS 10 / API-29 (QQ3A.200805.001/135998ea76)",
    9:  "Handheld",
    12: 1666,
    13: 750,
    14: "400",
    15: "ARM64 FP ASIMD AES | 2304 | 8",
    16: 5602,
    17: "Adreno (TM) 618",
    18: "OpenGL ES 3.2 V@415.0 (GIT@29668c6, I386ada412e, 1599643163) (Date:09/09/20)",
    24: "Handheld",
    25: "Xiaomi POCO X3",
    57: "7428b253defc164018c604a1ebbfebdf",

    60: 107339,
    61: 98183,
    62: 2477,
    63: 673,
    64: 98311,
    65: 107339,
    66: 98311,
    67: 107339,
    74: "/data/app/com.dts.freefireth-cXks8_q21Wm61sLpm1Mwkw==/lib/arm64",
    77: "b8e0cd5e295eee42f5860d3c86e483dd|/data/app/com.dts.freefireth-cXks8_q21Wm61sLpm1Mwkw==/base.apk",
    90: "Mymensingh",
    91: "H",
    94: "KqsHTwUcsotzm7T5wpHoa7zprR/0nb96MPMDflVhet3xaqMVtM9nR8cyff0bn0DL/AqN+M57YFwW+60+kUocBgoAwICMqslgRX9NHghmHqoVcF9O",
    96: '{"cur_rate":[50,30,60,90,120],"support_etc2":false}',

    104: 25589,
    105: 1,
    106: "https://dl-bs.ggpolarbear.com/live/ABHotUpdates/|https://core-bs.ggpolarbear.com/live/ABHotUpdates/|a4332cb1c1a84e51dd77441e4856ed5a",
    107: "1.9393e7b8e53e8aeb",
}


def _build_majorlogin_raw(open_id, access_token, platform, client_version, device_info) -> bytes:
    plat = str(platform)
    f: Dict[int, Any] = dict(REAL_CLIENT_PROFILE)
    f.update({

        3:  str(datetime.now())[:-7],
        4:  "free fire",
        5:  1 if plat in ("1", "4") else int(platform),
        7:  client_version,
        11: "WIFI",
        19: (device_info or {}).get("unique_device_id") or f"Google|{uuid.uuid4()}",
        20: (device_info or {}).get("client_ip") or "27.123.253.203",
        21: "en",
        22: open_id,
        23: plat,
        29: access_token,
        30: 1,
        42: "WIFI",
        73: 2,
        76: 1,
        78: 3,
        79: 2,
        81: "64",
        83: FORCED_CLIENT_VERSION_CODE,
        85: 3,
        86: "OpenGLES2",
        87: 4095,
        88: int(platform),
        92: random.randint(5000, 9000),
        93: "android",
        95: 111207,
        97: 1,
        98: 1,
        99: plat,
        100: plat,

        102: _pb_encode_fields({8: 3602980918476556118}),

    })
    return _pb_encode_fields(f)


async def build_majorlogin_payload(open_id, access_token, platform, client_version, device_info):
    try:
        if os.environ.get("FF_LEGACY_PAYLOAD") == "1":
            return await build_majorlogin_payload_legacy(open_id, access_token, platform, client_version, device_info)
        payload = _build_majorlogin_raw(open_id, access_token, platform, client_version, device_info)
        return await aes_encrypt(payload, AES_KEY, AES_IV)
    except Exception as e:
        report_fail(None, "MajorLogin payload build exception", repr(e)[:200])
        return None


_STRONGHOLD_BY_ACC: Dict[str, str] = {}

async def send_majorlogin(data, release_version, server_url):
    try:
        url = f"{server_url}MajorLogin"
        req_headers = headers.copy()
        req_headers["ReleaseVersion"] = release_version
        req_headers["Host"] = urlparse(url).netloc or LOGINBP_HOST

        response = await client.post(url, headers=req_headers, data=data)
        if response.status_code != 200:
            report_fail(None, f"MajorLogin HTTP {response.status_code}", f"url={url} body={response.text[:150]}")
            return None
        response_content = response.content
        if len(response_content) < 40:
            report_fail(None, "MajorLogin response too short", f"{len(response_content)} bytes: {response_content[:60]!r}")
            return None

        try:
            _acc_s, _sh = udp_engine.extract_majorlogin_extras(response_content)
            if _acc_s:
                _STRONGHOLD_BY_ACC[_acc_s] = _sh or ""
        except Exception:
            pass


        def _check_majorlogin_ban(proto):

            _BAN_STATUS = {2, 3, 8, 9, 10, 14, 15}
            status = getattr(proto, 'status', None) or getattr(proto, 'error_code', None)
            if status in _BAN_STATUS:
                return True

            _BAN_WORDS = ("ban", "suspend", "frozen", "terminat", "block", "disabled")
            msg = str(getattr(proto, 'error_msg', '') or getattr(proto, 'msg', '') or '').lower()
            return any(w in msg for w in _BAN_WORDS)

        res_proto = Pb2_proto.MajorLoginRes()
        try:
            res_proto.ParseFromString(response_content)
            if _check_majorlogin_ban(res_proto):
                report_fail(None, "[BANNED] MajorLoginRes ban status detected", f"status={getattr(res_proto,'status',None)}")
                return "BANNED"
            if res_proto.region and res_proto.token:
                return res_proto
        except Exception:
            pass


        if len(response_content) > 64:
            try:
                res_proto = Pb2_proto.MajorLoginRes()
                res_proto.ParseFromString(response_content[64:])
                if _check_majorlogin_ban(res_proto):
                    return "BANNED"
                if res_proto.region and res_proto.token:
                    return res_proto
            except Exception:
                pass


        for offset in range(min(128, len(response_content))):
            try:
                candidate = Pb2_proto.MajorLoginRes()
                candidate.ParseFromString(response_content[offset:])
                if _check_majorlogin_ban(candidate):
                    return "BANNED"
                if candidate.region and candidate.token:
                    return candidate
            except Exception:
                pass

        res_proto = Pb2_proto.MajorLoginRes()
        res_proto.ParseFromString(response_content)
        if not res_proto.token:
            report_fail(None, "MajorLogin token khali (version/payload mismatch hote pare)", f"{len(response_content)} bytes")

            version_config_invalidate()
            return None
        return res_proto
    except Exception as e:
        _e_str = repr(e)
        report_fail(None, "MajorLogin exception", _e_str[:200])


        if "DecodeError" in _e_str or "Error parsing message" in _e_str:
            version_config_invalidate()
        return None

async def send_getlogin(data, base_url, token, release_version):
    try:
        url = f"{base_url.rstrip('/')}/GetLoginData"
        req_headers = headers.copy()
        req_headers["ReleaseVersion"] = release_version
        req_headers['Authorization'] = f"Bearer {token}"
        req_headers["Host"] = urlparse(url).netloc or LOGINBP_HOST

        response = await client.post(url, headers=req_headers, data=data)
        if response.status_code != 200:
            report_fail(None, f"GetLoginData HTTP {response.status_code}", f"url={url} body={response.text[:150]}")
            return None
        response_content = response.content

        res_proto = Pb2_proto.GetLoginDataRes()
        parsed_successfully = False
        try:
            res_proto.ParseFromString(response_content)
            if res_proto.functional_addrs or res_proto.informational_addrs:
                parsed_successfully = True
        except Exception:
            pass

        if not parsed_successfully:
            for offset in range(min(128, len(response_content))):
                try:
                    candidate = Pb2_proto.GetLoginDataRes()
                    candidate.ParseFromString(response_content[offset:])
                    if candidate.functional_addrs or candidate.informational_addrs:
                        res_proto = candidate
                        break
                except Exception:
                    pass

        dict_res = {}
        try:
            parsed = Parser().parse(response_content.hex())
            dict_res = await parse_results(parsed)
        except Exception:
            pass

        return res_proto, dict_res
    except Exception as e:
        report_fail(None, "GetLoginData exception", repr(e)[:200])
        return None

async def build_tcp_startup_packet(account_id, token, server_time, key, iv, region=DEFAULT_REGION, typ='OnLine'):
    uid_hex = f"{int(account_id):016x}"
    timestamp_hex = f"{int(server_time):08x}"
    encode_token = token.encode()
    encrypted_packet = (await aes_encrypt(encode_token, key, iv)).hex()
    encrypted_packet_length = f"{len(encrypted_packet) // 2:08x}"
    reg = str(region).upper() if region else "BD"
    if typ == 'OnLine':
        prefix = '7119' if reg == 'BD' else ('7114' if reg == 'IND' else '7115')
        return f"{prefix}{uid_hex}{timestamp_hex}00000000{encrypted_packet_length}{encrypted_packet}"
    else:
        prefix = '9219' if reg == 'BD' else ('9214' if reg == 'IND' else '9215')
        return f"{prefix}{uid_hex}{timestamp_hex}{encrypted_packet_length}{encrypted_packet}"

async def send_keep_alive(region=DEFAULT_REGION):
    try:
        reg = str(region).upper() if region else "BD"
        ka_hex = "0219" if reg == "BD" else ("0214" if reg == "IND" else "0215")
        return bytes.fromhex(ka_hex)
    except Exception:
        return bytes.fromhex("0219")


def build_tcp_ping_lw(session_key, session_iv):
    from Crypto.Cipher import AES as _AES
    from Crypto.Util.Padding import pad as _pad
    body = _AES.new(session_key, _AES.MODE_CBC, session_iv).encrypt(
        _pad(b"\x08\x20", _AES.block_size))
    return bytes.fromhex("0e14") + len(body).to_bytes(4, "big") + body


def _match_start_mode(plain: bytes):
    import re as _re
    mm = _re.search(rb"\x0a\x01.\x10", plain[:16], _re.S)
    return plain[mm.end()] if mm and mm.end() < len(plain) else None


def _match_start_type(plain: bytes):
    import re as _re
    mm = _re.search(rb"\x0a\x01.\x10", plain[:16], _re.S)
    return plain[mm.start() + 2] if mm else None


def _send_prelude_tcp(writer, aes_enc_fn, session_key, session_iv):
    if not SEND_PRELUDE:
        return
    try:
        for op_hex, hx in (("2814", PRELUDE_2814_HEX),
                            ("1d14", PRELUDE_1D14_HEX),
                            ("0314", PRELUDE_0314_LOBBY_HEX)):
            plain = bytes.fromhex(hx)
            from Crypto.Cipher import AES as _AES
            from Crypto.Util.Padding import pad as _pad
            c = _AES.new(session_key, _AES.MODE_CBC, session_iv).encrypt(
                _pad(plain, _AES.block_size))
            pkt = bytes.fromhex(op_hex) + len(c).to_bytes(4, "big") + c
            writer.write(pkt)
    except Exception:
        pass


async def send_post_match_2814(writer, session_key, session_iv):
    if not SEND_POST_MATCH_2814 or not POST_MATCH_2814_HEX:
        return
    try:
        from Crypto.Cipher import AES as _AES
        from Crypto.Util.Padding import pad as _pad
        c = _AES.new(session_key, _AES.MODE_CBC, session_iv).encrypt(
            _pad(bytes.fromhex(POST_MATCH_2814_HEX), _AES.block_size))
        writer.write(bytes.fromhex("2814") + len(c).to_bytes(4, "big") + c)
        await writer.drain()
    except Exception:
        pass


async def start_game_lone_wolf(region, client_version, writer, key, iv, account_id=""):
    packet = bytes.fromhex("080112800a0a010b102b3a110a044944433110aa011a064555524f50453a100a044944433210311a064555524f504540014a0801090a0b1219202758016291090a8001303838463832424630324139363736373032303130313030303030303030303030303136303030313030313530303032323246393745454530463030303030303436373632353134303030303030303030303030303030303030303030303030303030303030303030303030303066663030303030303030636163666131366410241afb02735d5e571400024a775d45414d1a041b1c001f11010449715f4243481a001e1d071c1703004b1a4066785c524570735c51486775421b5c5a4c07504042685a63610816054e19025e75196001477c015165406370195f5547404e4550640103020f1304064863754268676c755f65576e40467e5f0a417a4701026d675d6e73670b1108495a4c6a0b78470b740065645e525a057258425f584a447d4e6759440c11044e7c596d7f4b625f7d04055a47505c4e1d6b5b4107447d7201057d7f0f14084e430457674f7e517d72015172415d027473577c4d615f79535256780911030f4d5e027a797f614165067806505d53777750475e75064257076500460817014e741e7e5078487e7a7c465e7669767153497064605a7376677773550d160148037e18675966787f4c42607a645f577e7b441b460776026b18685d0b110205490060020f70676175654674706671797f41067346677c4e06585e780f15074c57047b40517075415f6364027259674b5b0166407f7340600407770a22047a5d5c52300b3a0a167305067162727516134208312e3133302e3232480350015ae90403626253513635686e556f4e36416456324b796f566c636f477776484f624e56526c4d727073504b4f43654177616848494176795556497273743752737149734a7a786b3247525268377a2f637664626d504f6a73552f79626d38547a4c69586d2f474351696d494b53486833447955726f39515152756c34545350626d6d624b7949565937545671577059455372323646572f59624578507338514f706d317372785455736c30796a434144444d4f34616a654b615753366361496c554b4963797a494e396d52516f715277687939797257476d337a644345337a6a61436f492f5a585233656f65365a42647a64677654636b6b665733356e4d4c6a6a565072564b6433523172756174394e50514150724a5546627859696c4c5a3859707336654d5447666b6649793574666a526c314d4648706b51774c6373374439656378566c41636f374e664f6d2b30654756466c4434744478706771385533595973587645384842502f70666c767a737138316a32524f4d7857437556445442492f684735625462773166456e4249725162762b636144775147696f74554e316d4c4b77734379456f4766706746614251457645672b736a764c4c78704743334c304a5344532f74526169504354553344374e6249306547516651622f5a466f4c36455630775a324d6f583932414c572f5049752f56634663584e70596b356f7966326151416a536971486a2f363276354843644f525551303578754e6171795251625653704654303137655237675255636b4966366c6f447476342b514e4a4670766d74757077707774396a5a5974437a4b56743657726d6e36785837706658456251555434684f3758a201050803108703a201050804108103a20105080510c001a20105081d10cc01a2010408161078a20105080e10af01a201020815")
    proto = Pb2_proto.StartMatch()
    try:
        proto.ParseFromString(packet)
        if hasattr(proto.main, 'region_list') and len(proto.main.region_list) > 0:
            proto.main.region_list[0].region = region
            if len(proto.main.region_list) > 1:
                proto.main.region_list[1].region = region
        if hasattr(proto.main, 'client_version'):
            proto.main.client_version.remote_version = client_version
        packet = proto.SerializeToString()
    except Exception:
        pass
    encrypted_packet = (await aes_encrypt(packet, key, iv)).hex()
    packet_length = len(encrypted_packet) // 2
    hex_length = hex(packet_length)[2:]
    hex_length = hex_length if len(hex_length) > 1 else "0" + hex_length
    reg = str(region).upper() if region else "IND"
    reg_prefix = "031900" if reg == "BD" else ("031400" if reg == "IND" else "031500")
    final_packet = reg_prefix + "0" * (6 - len(hex_length)) + hex_length + encrypted_packet
    writer.write(bytes.fromhex(final_packet))
    await writer.drain()
    bot_state.log(f"[🐺] LW Normal Search Sent | Region: {region}", "info")
async def start_game_battle_royale(region, client_version, writer, key, iv):
    packet = bytes.fromhex("080112800a0a010110013a110a044944433110aa011a064555524f50453a100a044944433210311a064555524f504540014a0801090a0b1219202758016291090a8001303838463832424630324139363736373032303130313030303030303030303030303136303030313030313530303032323246393745454530463030303030303436373632353134303030303030303030303030303030303030303030303030303030303030303030303030303066663030303030303030636163666131366410241afb02735d5e571400024a775d45414d1a041b1c001f11010449715f4243481a001e1d071c1703004b1a4066785c524570735c51486775421b5c5a4c07504042685a63610816054e19025e75196001477c015165406370195f5547404e4550640103020f1304064863754268676c755f65576e40467e5f0a417a4701026d675d6e73670b1108495a4c6a0b78470b740065645e525a057258425f584a447d4e6759440c11044e7c596d7f4b625f7d04055a47505c4e1d6b5b4107447d7201057d7f0f14084e430457674f7e517d72015172415d027473577c4d615f79535256780911030f4d5e027a797f614165067806505d53777750475e75064257076500460817014e741e7e5078487e7a7c465e7669767153497064605a7376677773550d160148037e18675966787f4c42607a645f577e7b441b460776026b18685d0b110205490060020f70676175654674706671797f41067346677c4e06585e780f15074c57047b40517075415f6364027259674b5b0166407f7340600407770a22047a5d5c52300b3a0a167305067162727516134208312e3133302e3232480350015ae90403626253513635686e556f4e36416456324b796f566c636f477776484f624e56526c4d727073504b4f43654177616848494176795556497273743752737149734a7a786b3247525268377a2f637664626d504f6a73552f79626d38547a4c69586d2f474351696d494b53486833447955726f39515152756c34545350626d6d624b7949565937545671577059455372323646572f59624578507338514f706d317372785455736c30796a434144444d4f34616a654b615753366361496c554b4963797a494e396d52516f715277687939797257476d337a644345337a6a61436f492f5a585233656f65365a42647a64677654636b6b665733356e4d4c6a6a565072564b6433523172756174394e50514150724a5546627859696c4c5a3859707336654d5447666b6649793574666a526c314d4648706b51774c6373374439656378566c41636f374e664f6d2b30654756466c4434744478706771385533595973587645384842502f70666c767a737138316a32524f4d7857437556445442492f684735625462773166456e4249725162762b636144775147696f74554e316d4c4b77734379456f4766706746614251457645672b736a764c4c78704743334c304a5344532f74526169504354553344374e6249306547516651622f5a466f4c36455630775a324d6f583932414c572f5049752f56634663584e70596b356f7966326151416a536971486a2f363276354843644f525551303578754e6171795251625653704654303137655237675255636b4966366c6f447476342b514e4a4670766d74757077707774396a5a5974437a4b56743657726d6e36785837706658456251555434684f3758a201050803108703a201050804108103a20105080510c001a20105081d10cc01a2010408161078a20105080e10af01a201020815")
    proto = Pb2_proto.StartMatch()
    proto.ParseFromString(packet)
    if hasattr(proto.main, 'region_list') and len(proto.main.region_list) > 0:
        proto.main.region_list[0].region = region
        if len(proto.main.region_list) > 1:
            proto.main.region_list[1].region = region
    if hasattr(proto.main, 'client_version'):
        proto.main.client_version.remote_version = client_version
    packet = proto.SerializeToString()
    encrypted_packet = (await aes_encrypt(packet, key, iv)).hex()
    packet_length = len(encrypted_packet) // 2
    hex_length = hex(packet_length)[2:]
    hex_length = hex_length if len(hex_length) > 1 else "0" + hex_length
    reg = str(region).upper() if region else "BD"
    reg_prefix = "031900" if reg == "BD" else ("031400" if reg == "IND" else "031500")
    final_packet = reg_prefix + "0" * (6 - len(hex_length)) + hex_length + encrypted_packet
    writer.write(bytes.fromhex(final_packet))
    await writer.drain()
    bot_state.log(f"[⚔] Battle Royale Match Search Packet Sent ({packet_length} bytes) | Region: {region}", "info")

async def start_game_lw_headshot(region, client_version, writer, key, iv, account_id=""):


    patched_plain = patch_lw_account_id(LW_HS_MATCH_START_HEX, str(account_id) if account_id else "0")
    packet = patched_plain
    encrypted_packet = (await aes_encrypt(packet, key, iv)).hex()
    packet_length = len(encrypted_packet) // 2
    hex_length = hex(packet_length)[2:]
    hex_length = hex_length if len(hex_length) > 1 else "0" + hex_length
    reg = str(region).upper() if region else "IND"
    reg_prefix = "031900" if reg == "BD" else ("031400" if reg == "IND" else "031500")
    final_packet = reg_prefix + "0" * (6 - len(hex_length)) + hex_length + encrypted_packet
    writer.write(bytes.fromhex(final_packet))
    await writer.drain()
    bot_state.log(f"[🎯] LW Headshot Search Packet Sent | Region: {region} | AccID: {account_id}", "info")


async def zigzag_encode(n):
    z = (n << 1) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while z >= 0x80:
        out.append((z & 0x7F) | 0x80)
        z >>= 7
    out.append(z)
    return bytes(out)

async def uleb_encode(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return bytes(out)

async def tea_enc(v0, v1, k0, k1, k2, k3):
    s = 0
    for _ in range(_ROUNDS):
        s = (s + _DELTA) & 0xFFFFFFFF
        v0 = (v0 + (((((v1 << 4) & 0xFFFFFFFF) + k0) & 0xFFFFFFFF ^
                      ((v1 + s) & 0xFFFFFFFF) ^
                      (((v1 >> 5) + k1) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        v1 = (v1 + (((((v0 << 4) & 0xFFFFFFFF) + k2) & 0xFFFFFFFF ^
                      ((v0 + s) & 0xFFFFFFFF) ^
                      (((v0 >> 5) + k3) & 0xFFFFFFFF)))) & 0xFFFFFFFF
    return v0, v1

async def tea_dec(v0, v1, k0, k1, k2, k3):
    s = (_DELTA * _ROUNDS) & 0xFFFFFFFF
    for _ in range(_ROUNDS):
        v1 = (v1 - (((((v0 << 4) & 0xFFFFFFFF) + k2) & 0xFFFFFFFF ^
                      ((v0 + s) & 0xFFFFFFFF) ^
                      (((v0 >> 5) + k3) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        v0 = (v0 - (((((v1 << 4) & 0xFFFFFFFF) + k0) & 0xFFFFFFFF ^
                      ((v1 + s) & 0xFFFFFFFF) ^
                      (((v1 >> 5) + k1) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        s = (s - _DELTA) & 0xFFFFFFFF
    return v0, v1

async def tea_cbc_encrypt(padded, key_bytes):
    k0, k1, k2, k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0, 4, 8, 12))
    out = bytearray(len(padded))
    prev_cipher = bytearray(8)
    prev_intermediate = bytearray(8)
    for i in range(0, len(padded), 8):
        xored = bytearray(8)
        for j in range(8):
            xored[j] = padded[i + j] ^ prev_cipher[j]
        e0, e1 = await tea_enc(
            struct.unpack_from("<I", xored, 0)[0],
            struct.unpack_from("<I", xored, 4)[0],
            k0, k1, k2, k3,
        )
        enc = bytearray(8)
        struct.pack_into("<I", enc, 0, e0)
        struct.pack_into("<I", enc, 4, e1)
        for j in range(8):
            out[i + j] = enc[j] ^ prev_intermediate[j]
        prev_cipher[:] = out[i:i + 8]
        prev_intermediate[:] = xored
    return bytes(out)

async def build_padded(content):
    pad_len = (8 - (len(content) + 10) % 8) % 8
    return bytes([pad_len, 0, 0]) + b"\x00" * pad_len + content + b"\x00" * 7

async def encode_header(layout, send_option, cmd, order_id, flags, length, k, v80):
    out = bytearray()
    for code in layout:
        value = {0: send_option, 1: cmd, 2: order_id, 3: flags, 4: length}[code]
        if _FIELD_SIZES[code] == 1:
            out.append((value & 0xFF) ^ k)
        else:
            v = ((value & 0xFFFF) ^ v80) & 0xFFFF
            out.append(v & 0xFF)
            out.append((v >> 8) & 0xFF)
    return bytes(out)

async def crc7_buff(crc, buf):
    c = crc & 0x7F
    for b in buf:
        c = CRC7_TABLE[((2 * (c & 0xFF)) ^ (b & 0xFF)) & 0xFF] & 0x7F
    return c & 0x7F

async def sv_frame(msg_key, layout, send_option, cmd, order_id, flags, content, key, encrypted=True):
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key, 0]) + await encode_header(layout, send_option, cmd, order_id, flags, len(body), k, v80)
    packet = bytearray(hdr + body)
    packet[1] = await crc7_buff(0, bytes(packet[2:])) & 0x7F
    return bytes(packet)

async def build_match_startup_packets(token, udp_key, match_code, account_id, block_val,
                                      server_ip="", region=DEFAULT_REGION, client_version=FORCED_CLIENT_VERSION,
                                      client_version_code=FORCED_CLIENT_VERSION_CODE, access_token="",
                                      mode="LONE_WOLF"):
    token = token.strip()
    udp_key = bytes.fromhex(udp_key)
    match_code = [int(ch) for ch in str(match_code).strip()]


    jwt_part1 = token[:660] if len(token) > 660 else token
    jwt_part2 = token[660:] if len(token) > 660 else ""
    encoded_jwt_part1 = jwt_part1.encode() if isinstance(jwt_part1, str) else jwt_part1
    encoded_jwt_part2 = jwt_part2.encode() if isinstance(jwt_part2, str) else jwt_part2

    join_prep_payload = await zigzag_encode(len(encoded_jwt_part1)) + encoded_jwt_part1


    reg = str(region).upper() if region else "BD"

    if str(mode).upper() == "BR":
        csoversea_block = bytes.fromhex(
            "ca0163736f7665727365612e7374726f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
            "3b302e302e302e303b33342e3132362e37362e34353b33342e38372e3137372e31343b33342e38372e"
            "3137302e3233303b33352e3138352e3138332e35370000000000000100000000000000000000000001"
            "00000000000100010000000100b09df8c5fad88bdf110200"
        )
        m_val1, m_val2 = 1, 1
    elif str(mode).upper() == "LW_HS":


        csoversea_block = bytes.fromhex(
            "ca0163736f7665727365612e7374726f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
            "3b302e302e302e303b33342e3132362e37362e34353b33342e38372e3137372e31343b33342e38372e"
            "3137302e3233303b33352e3138352e3138332e35370000000000000100000000000000000000000001"
            "00000800000100000000000100a8a2d7bebd8d8bdf110200"
        )
        m_val1, m_val2 = 74, 11
    else:
        csoversea_block = bytes.fromhex(
            "ca0163736f7665727365612e7374726f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
            "3b302e302e302e303b33342e3132362e37362e34353b33342e38372e3137372e31343b33342e38372e"
            "3137302e3233303b33352e3138352e3138332e35370000000000000100000000000000000000000001"
            "00000800000100000000000100a8a2d7bebd8d8bdf110200"
        )
        m_val1, m_val2 = 43, 11

    mid = bytes.fromhex('0000000001000102030101') + await zigzag_encode(len(reg)) + reg.encode()
    mid += bytes.fromhex('0001030003000004')
    mid += await zigzag_encode(len(client_version)) + client_version.encode()
    mid += await zigzag_encode(len(client_version_code)) + client_version_code.encode()
    mid += csoversea_block

    clean_ip = server_ip.split(':')[0] if server_ip else "0.0.0.0"
    mid += await zigzag_encode(len(clean_ip)) + clean_ip.encode()

    clean_acc_tok = access_token.strip() if access_token else ""
    if clean_acc_tok:
        mid += await zigzag_encode(len(clean_acc_tok)) + clean_acc_tok.encode()

    mid += await zigzag_encode(len(encoded_jwt_part2)) + encoded_jwt_part2

    join_post_payload = (
        await uleb_encode(int(account_id)) +
        await uleb_encode(int(block_val)) +
        await uleb_encode(1) +
        await uleb_encode(m_val1) +
        await uleb_encode(int(block_val)) +
        await uleb_encode(m_val2) +
        mid
    )

    process = await sv_frame(0x5E, match_code, 2, 447, 0, 1, join_prep_payload, udp_key)
    loading = await sv_frame(0x5A, match_code, 2, 448, 1, 1, join_post_payload, udp_key)
    return process.hex(), loading.hex()

async def produce_xor_key(secret_key):
    k = secret_key[0] if secret_key and len(secret_key) > 0 else 10
    return k, ((k << 8) | k) & 0xFFFF

async def parse_layout(layout):
    if isinstance(layout, str):
        return [int(ch) for ch in layout.strip()]
    return list(layout)

async def tea_cbc_decrypt(body, key_bytes):
    k0, k1, k2, k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0, 4, 8, 12))
    out = bytearray(len(body))
    prev_intermediate = bytearray(8)
    prev_cipher = bytearray(8)
    xored = bytearray(8)
    dec = bytearray(8)
    for i in range(0, len(body), 8):
        for j in range(8):
            xored[j] = body[i + j] ^ prev_intermediate[j]
        d0, d1 = await tea_dec(
            struct.unpack_from("<I", xored, 0)[0],
            struct.unpack_from("<I", xored, 4)[0],
            k0, k1, k2, k3
        )
        struct.pack_into("<I", dec, 0, d0)
        struct.pack_into("<I", dec, 4, d1)
        for j in range(8):
            out[i + j] = dec[j] ^ prev_cipher[j]
        prev_cipher[:] = body[i:i + 8]
        prev_intermediate[:] = dec
    return bytes(out)

async def build_hello_packet(text, key, layout):
    data = text.encode("utf-8")
    if len(data) > 25:
        raise ValueError(f"Text is too long ({len(data)} bytes)")
    content = b"\x10\x00\x00\x00" + data + b"\x00" * (29 - 4 - len(data))
    k, v80 = await produce_xor_key(key)
    layout = await parse_layout(layout)
    padded = await build_padded(content)
    enc_body = await tea_cbc_encrypt(padded, key)
    header_bytes = await encode_header(layout, 1, 1, 0, 1, len(enc_body), k, v80)
    packet = bytearray([0x63, 0x00]) + header_bytes + enc_body
    packet[1] = await crc7_buff(0, packet[2:]) & 0x7F
    return bytes(packet).hex()

async def classify(frame):
    cmd = frame["cmd"]
    msg_name = MESSAGE_ID_TO_NAME.get(cmd, f"UNKNOWN_{cmd}")
    if msg_name == "UDP_HELLO":
        return "HELLO"
    if msg_name == "UDP_ACK":
        return "ACK"
    if msg_name == "UDP_PING":
        return "PING"
    if msg_name == "RUDP_JOIN_MATCH":
        return "JOIN_MATCH"
    if msg_name.startswith("RUDP_"):
        return msg_name
    if msg_name.startswith("UDP_"):
        return msg_name
    return "DATA"

async def build_packet(msg_key, layout, send_option, cmd, order_id, flags, content, key, encrypted=True):
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key, 0])
    for code in layout:
        value = {0: send_option, 1: cmd, 2: order_id, 3: flags, 4: len(body)}[code]
        if _FIELD_SIZES[code] == 1:
            hdr.append((value & 0xFF) ^ k)
        else:
            v = ((value & 0xFFFF) ^ v80) & 0xFFFF
            hdr.append(v & 0xFF)
            hdr.append((v >> 8) & 0xFF)
    packet = bytearray(hdr + body)
    packet[1] = await crc7_buff(0, bytes(packet[2:])) & 0x7F
    return bytes(packet)

async def layouts_from_mask(mask):
    ru = [int(c) for c in str(mask).strip()]
    nr = [c for c in ru if c != 2]
    return ru, nr

async def reply_for(frame, key, mask, ack_key=0x68, ping_key=0x6D, hello_key=0x5B, ack_style="short"):
    ru, nr = await layouts_from_mask(mask)
    typ = await classify(frame)
    if typ == "HELLO":
        if ack_style == "echo":
            content = frame["content"] if frame["content"] else b"\x10\x00\x00\x00"
            return typ, await build_packet(hello_key, nr, 1, 1, None, 1, content, key)
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x01\x00", key)
    if typ == "ACK":
        content = frame["content"] if frame["content"] else b"\x01\x00"
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, content, key)
    if typ == "PING":
        c = frame["content"]
        counter = c[:4] if len(c) >= 4 else c
        return typ, await build_packet(ping_key, nr, 0, 3, None, 0, counter + b"\x00\x00\x00", key, encrypted=False)
    if typ == "JOIN_MATCH":
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x02\x00", key)
    return typ, None

async def keepalive_ping(sock, ip, port, key_bytes, mask, stop_event):
    nr = (await layouts_from_mask(mask))[1]
    ping_keys = [0x66, 0x6D, 0x69, 0x6C, 0x6B, 0x6E, 0x6F, 0x70]
    i = 0
    while not stop_event.is_set():
        pk = ping_keys[i % len(ping_keys)]
        counter = int(time.time() * 1000) & 0xFFFFFFFF
        pkt = await build_packet(pk, nr, 0, 3, None, 0,
                                  struct.pack("<I", counter) + b"\x00\x00\x00",
                                  key_bytes, encrypted=False)
        try:
            await udp_sendto(sock, pkt, (ip, port))
        except Exception:
            pass
        i += 1
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            pass

async def try_header(buf, layout, k, v80):
    off = 2
    out = {}
    for code in layout:
        size = _FIELD_SIZES[code]
        if off + size > len(buf):
            return None
        out[_FIELD_NAMES[code]] = (buf[off] ^ k) if size == 1 else ((buf[off] | (buf[off + 1] << 8)) ^ v80) & 0xFFFF
        off += size
    out["headerLen"] = off
    return out

async def oicq_unpad(padded):
    if not padded or len(padded) < 8:
        return None
    if not all(padded[-1 - i] == 0 for i in range(7)):
        return None
    pad_len = padded[0] & 0x07
    s = 3 + pad_len
    e = len(padded) - 7
    return padded[s:e] if s < e else b""

async def decode_packet(packet, key, mask=None):
    data = bytes(packet) if isinstance(packet, bytes) else bytes.fromhex(packet)
    if len(data) < 8:
        return None
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    crc_ok = (data[1] & 0x7F) == await crc7_buff(0, data[2:])
    candidates = []
    if mask:
        ru, nr = await layouts_from_mask(mask)
        layouts = [("RUDP", ru), ("nonRUDP", nr)]
    else:
        layouts = [("RUDP", list(p)) for p in itertools.permutations([0, 1, 2, 3, 4])]
        layouts += [("nonRUDP", list(p)) for p in itertools.permutations([0, 1, 3, 4])]
    for kind, layout in layouts:
        f = await try_header(data, layout, k, v80)
        if not f:
            continue
        if f["flags"] > 7 or f["sendOption"] > 7:
            continue
        if f["length"] != len(data) - f["headerLen"]:
            continue
        body = data[f["headerLen"]:f["headerLen"] + f["length"]]
        content = None
        padded = None
        if f["flags"] & 1:
            if len(body) < 8 or len(body) % 8 != 0:
                continue
            padded = await tea_cbc_decrypt(body, key)
            content = await oicq_unpad(padded)
            if content is None:
                continue
        else:
            content = body
        score = (1 if crc_ok else 0) + (1 if content is not None else 0)
        candidates.append({
            "kind": kind, "layout": layout, "headerLen": f["headerLen"],
            "msgKey": data[0], "cmd": f["cmd"], "flags": f["flags"],
            "sendOption": f["sendOption"], "orderId": f.get("orderId"),
            "length": f["length"], "content": content, "crcOk": crc_ok,
            "padded": padded, "score": score, "total": len(data),
        })
    if not candidates:
        return None
    candidates.sort(key=lambda c: (c["kind"] == "RUDP" or c["kind"] == "nonRUDP", c["score"]), reverse=True)
    return candidates[0]


_UDP_NATIVE = hasattr(asyncio.AbstractEventLoop, "sock_recvfrom") and hasattr(asyncio.AbstractEventLoop, "sock_sendto")

def prepare_udp_socket(sock):
    if _UDP_NATIVE:
        sock.setblocking(False)
    else:
        sock.settimeout(2.0)

async def udp_sendto(sock, data, addr):
    loop = asyncio.get_running_loop()
    if _UDP_NATIVE:
        await loop.sock_sendto(sock, data, addr)
    else:
        await loop.run_in_executor(None, sock.sendto, data, addr)

async def udp_recvfrom(sock, size=65535):
    loop = asyncio.get_running_loop()
    if _UDP_NATIVE:
        return await loop.sock_recvfrom(sock, size)
    return await loop.run_in_executor(None, sock.recvfrom, size)


BR_ANTI_AFK = os.environ.get("BR_ANTI_AFK", "1") != "0"


BR_MATCH_DURATION = float(os.environ.get("BR_MATCH_DURATION", "700"))
BR_IDLE_TIMEOUT = float(os.environ.get("BR_IDLE_TIMEOUT", "25"))
BR_HOLD_MATCH = os.environ.get("BR_HOLD_MATCH", "1") == "1"
LAST_MATCH_NOTE: Dict[str, str] = {}

async def anti_afk_worker(sock, resolved_ip, port, udp_key_bytes, match_code,
                          account_id, stop_event, match_index):


    server_addr = (resolved_ip, port)
    fires = moves = 0


    dir_x  = random.choice([-1, 1]) * random.uniform(30, 80)
    dir_y  = random.uniform(-20, 20)
    dir_z  = random.choice([-1, 1]) * random.uniform(30, 80)
    pos_x  = random.randint(-200, 200)
    pos_y  = random.randint(-30, 30)
    pos_z  = random.randint(-200, 200)
    steps_in_dir   = 0
    max_steps       = random.randint(3, 8)


    primary_wid   = random.choice([101, 102, 201])
    secondary_wid = random.choice([103, 202])
    wid           = primary_wid

    try:
        nr = (await layouts_from_mask(match_code))[1]
        try:
            pid = int(account_id) & 0xFFFFFFFF
        except Exception:
            pid = 0


        await asyncio.sleep(random.uniform(2.0, 5.0))

        while not stop_event.is_set():
            try:
                ts = int(time.time() * 1000) & 0xFFFFFFFF
                action_roll = random.random()

                if action_roll < 0.45:

                    steps_in_dir += 1
                    if steps_in_dir >= max_steps:

                        dir_x       = random.choice([-1, 1]) * random.uniform(20, 90)
                        dir_z       = random.choice([-1, 1]) * random.uniform(20, 90)
                        dir_y       = random.uniform(-15, 15)
                        steps_in_dir = 0
                        max_steps    = random.randint(3, 8)


                    speed = random.uniform(0.3, 0.7)
                    pos_x = max(-500, min(500, int(pos_x + dir_x * speed)))
                    pos_y = max(-80,  min(80,  int(pos_y + dir_y * speed)))
                    pos_z = max(-500, min(500, int(pos_z + dir_z * speed)))

                    body = struct.pack("<IIiiiH", pid, ts, pos_x, pos_y, pos_z, 1)
                    pkt  = await build_packet(0x6B, nr, 0, 2001, None, 0, body, udp_key_bytes, encrypted=False)
                    await udp_sendto(sock, pkt, server_addr)
                    moves += 1


                    if random.random() < 0.2:
                        await asyncio.sleep(random.uniform(0.4, 1.2))

                elif action_roll < 0.80:


                    is_rifle = wid in (101, 201)
                    hold_ms  = random.randint(180, 650) if is_rifle else random.randint(120, 320)

                    pkt = await build_packet(0x68, nr, 0, 104, None, 0,
                                             struct.pack("<IIHB", pid, ts, wid, 1), udp_key_bytes, encrypted=False)
                    await udp_sendto(sock, pkt, server_addr)
                    await asyncio.sleep(hold_ms / 1000.0)
                    pkt = await build_packet(0x68, nr, 0, 105, None, 0,
                                             struct.pack("<IIHB", pid, ts + hold_ms, wid, 0), udp_key_bytes, encrypted=False)
                    await udp_sendto(sock, pkt, server_addr)
                    fires += 1


                    if random.random() < 0.20:
                        await asyncio.sleep(random.uniform(0.08, 0.25))
                        ts2     = int(time.time() * 1000) & 0xFFFFFFFF
                        hold2   = random.randint(100, 280)
                        pkt = await build_packet(0x68, nr, 0, 104, None, 0,
                                                 struct.pack("<IIHB", pid, ts2, wid, 1), udp_key_bytes, encrypted=False)
                        await udp_sendto(sock, pkt, server_addr)
                        await asyncio.sleep(hold2 / 1000.0)
                        pkt = await build_packet(0x68, nr, 0, 105, None, 0,
                                                 struct.pack("<IIHB", pid, ts2 + hold2, wid, 0), udp_key_bytes, encrypted=False)
                        await udp_sendto(sock, pkt, server_addr)
                        fires += 1

                else:

                    if random.random() < 0.3:

                        wid = secondary_wid if wid == primary_wid else primary_wid


                gap = random.uniform(1.8, 5.5)

                if random.random() < 0.15:
                    gap += random.uniform(1.0, 3.0)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=gap)
                    break
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        pass
    finally:
        if fires + moves:
            print_info(f"[🎮] Anti-AFK #{match_index} | Fires: {fires} | Moves: {moves}")


async def play_game(server_ip_port, join_prep_pkt, join_post_pkt, udp_key, match_code,
                    account_id, player_region, client_version, key, iv,
                    match_index: int, dash_uid: str = None, mode: str = "LONE_WOLF"):
    match_start_time = time.time()
    is_br_match = (str(mode).upper() == "BR")
    afk_task = None
    ping_task = None
    join_post_task = None
    sock = None
    ping_stop = asyncio.Event()
    join_post_stop = asyncio.Event()

    uid_str = str(dash_uid or account_id)
    completed_cleanly = False
    dbg = {"pkts": 0, "undecoded": 0, "cmds": {}, "state": "start", "err": "", "reason": "", "last_report": 0.0}

    def _info(msg: str):
        try:
            bot_state.set_info(uid_str, f"Match #{match_index}: {msg}")
        except Exception:
            pass

    def _report(force: bool = False):
        now_t = time.time()
        if not force and now_t - dbg["last_report"] < 4.0:
            return
        dbg["last_report"] = now_t
        cmds = ",".join(f"{k}x{v}" for k, v in list(dbg["cmds"].items())[:8])
        _info(f"state={dbg['state']} pkts={dbg['pkts']} undecoded={dbg['undecoded']} cmds=[{cmds}] t={int(now_t - match_start_time)}s" + (f" err={dbg['err']}" if dbg["err"] else ""))

    try:
        ip, port = server_ip_port.split(":")
        port = int(port)
        resolved_ip = await resolve_host_cloudflare(ip)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        optimize_udp_socket(sock)
        prepare_udp_socket(sock)

        udp_key_bytes = bytes.fromhex(udp_key)
        hello_packet = await build_hello_packet(
            f"{account_id}_2585", udp_key_bytes, match_code
        )
        await udp_sendto(sock, bytes.fromhex(hello_packet), (resolved_ip, port))

        ack_state = "waiting_for_hello_reply"
        join_prep_sent = False
        join_post_sent = False
        join_match_received = False
        local_closed = False
        send_lock = asyncio.Lock()

        ping_task = asyncio.create_task(
            keepalive_ping(sock, resolved_ip, port, udp_key_bytes, match_code, ping_stop)
        )


        async def join_post_worker():
            await asyncio.sleep(5)
            while not join_post_stop.is_set() and not local_closed:
                try:
                    if ack_state == "join_packets_sent":
                        await udp_sendto(sock, bytes.fromhex(join_post_pkt), (resolved_ip, port))
                except Exception:
                    pass
                await asyncio.sleep(1.6)

        join_post_task = asyncio.create_task(join_post_worker())
        if BR_ANTI_AFK:
            afk_task = asyncio.create_task(
                anti_afk_worker(sock, resolved_ip, port, udp_key_bytes, match_code,
                                account_id, ping_stop, match_index)
            )
        last_activity = time.time()
        MAX_IDLE_BEFORE_HELLO_RESEND = 5.0

        print_colored(
            f"[MATCH #{match_index}] UDP started -> {server_ip_port} (DNS: {resolved_ip})",
            Colors.MAGENTA
        )

        async def send_join_packets_inline():
            nonlocal ack_state, join_prep_sent, join_post_sent
            if join_prep_sent:
                return
            async with send_lock:
                if join_prep_sent:
                    return
                try:
                    await udp_sendto(sock, bytes.fromhex(join_prep_pkt), (resolved_ip, port))
                    join_prep_sent = True
                    await asyncio.sleep(0.1)
                    prepare_ack = await build_packet(
                        0x68, (await layouts_from_mask(match_code))[1],
                        0, 2, None, 1, b"\x01\x00", udp_key_bytes
                    )
                    await udp_sendto(sock, prepare_ack, (resolved_ip, port))
                    await asyncio.sleep(0.2)
                    await udp_sendto(sock, bytes.fromhex(join_post_pkt), (resolved_ip, port))
                    join_post_sent = True
                    ack_state = "join_packets_sent"
                    dbg["join_t"] = time.time()
                    bot_state.log(f"[MATCH #{match_index}] Join packets sent!", "success")
                except Exception as e:
                    print_error(f"[MATCH #{match_index}] send error: {e}")

        _info("UDP hello pathano hoyeche, server reply-er opekkha")
        while not local_closed:
            dbg["state"] = ack_state
            _report()
            if time.time() - match_start_time > (BR_MATCH_DURATION if is_br_match else MAX_MATCH_DURATION):
                dbg["reason"] = "max duration"
                if is_br_match and dbg.get("join_t"):
                    completed_cleanly = True
                break


            _qf_jitter = dbg.get("_qf_jitter") or dbg.setdefault("_qf_jitter", random.uniform(-15, 30))
            if (not is_br_match and QUICK_FINISH_SECONDS > 0 and dbg.get("join_t")
                    and time.time() - dbg["join_t"] >= QUICK_FINISH_SECONDS + _qf_jitter):
                dbg["reason"] = f"quick finish ({int(QUICK_FINISH_SECONDS)}s after join)"
                bot_state.log(f"[MATCH #{match_index}] Quick finish after {int(QUICK_FINISH_SECONDS)}s", "success")
                completed_cleanly = True
                break
            try:
                response, server_addr = await asyncio.wait_for(
                    udp_recvfrom(sock, 65535), timeout=2.0
                )
                if response:
                    last_activity = time.time()
                    dbg["pkts"] += 1


                    await asyncio.sleep(0)
                    frame = await decode_packet(response, udp_key_bytes, match_code)
                    if not frame:
                        dbg["undecoded"] += 1
                    if frame:
                        dbg["cmds"][frame['cmd']] = dbg["cmds"].get(frame['cmd'], 0) + 1
                        ptype = await classify(frame)

                        if frame['cmd'] in [103, 107]:
                            bot_state.log(f"[MATCH #{match_index}] Completed (cmd {frame['cmd']})", "success")
                            dbg["reason"] = f"Completed (cmd {frame['cmd']})"
                            completed_cleanly = True
                            local_closed = True
                            continue

                        if frame['cmd'] == 101:
                            try:
                                ack_pkt = await build_packet(
                                    0x68, (await layouts_from_mask(match_code))[1],
                                    0, 2, None, 1, b"\x01\x00", udp_key_bytes
                                )
                                await udp_sendto(sock, ack_pkt, server_addr)
                            except Exception:
                                pass
                            continue

                        if ptype in ["ACK", "PING", "HELLO", "JOIN_MATCH"]:
                            if ptype == "HELLO" and ack_state == "waiting_for_hello_reply":
                                typ, reply = await reply_for(
                                    frame, udp_key_bytes, match_code, ack_style="short"
                                )
                                if reply:
                                    await udp_sendto(sock, reply, server_addr)
                                ack_state = "ack_sent_waiting"
                            elif ptype == "ACK":
                                if ack_state in ("waiting_for_hello_reply", "ack_sent_waiting"):
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply:
                                        await udp_sendto(sock, reply, server_addr)
                                    ack_state = "ready_to_send_join"
                                else:
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply:
                                        await udp_sendto(sock, reply, server_addr)
                            elif ptype == "PING":
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    await udp_sendto(sock, reply, server_addr)
                            elif ptype == "JOIN_MATCH" and not join_match_received:
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    await udp_sendto(sock, reply, server_addr)
                                    join_match_received = True

            except asyncio.TimeoutError:
                if ack_state == "ready_to_send_join" and not join_prep_sent:
                    await send_join_packets_inline()
                elif ack_state == "waiting_for_hello_reply":
                    if (time.time() - last_activity) > MAX_IDLE_BEFORE_HELLO_RESEND:
                        try:
                            pkt = await build_hello_packet(
                                f"{account_id}_2585", udp_key_bytes, match_code
                            )
                            await udp_sendto(sock, bytes.fromhex(pkt), (resolved_ip, port))
                        except Exception:
                            pass
                        last_activity = time.time()
                    if (time.time() - match_start_time) > 15.0:
                        bot_state.log(f"[MATCH #{match_index}] Handshake timeout", "warning")
                        dbg["reason"] = "Handshake timeout (server reply pai ni)"
                        break
                elif ack_state == "join_packets_sent":
                    if (time.time() - last_activity) > (BR_IDLE_TIMEOUT if is_br_match else MATCH_IDLE_TIMEOUT):
                        bot_state.log(f"[MATCH #{match_index}] Finished naturally", "success")
                        dbg["reason"] = "Finished naturally (idle)"
                        completed_cleanly = True
                        break
                continue
            except Exception as _e:
                dbg["err"] = repr(_e)[:80]
                await asyncio.sleep(0.2)
                continue

            if ack_state == "ready_to_send_join" and not join_prep_sent:
                await send_join_packets_inline()

        return f"match #{match_index} finished"

    except Exception as e:
        print_error(f"[MATCH #{match_index}] error: {e}")
        dbg["reason"] = f"exception: {repr(e)[:100]}"
        return f"match #{match_index} error"
    finally:
        _note = (f"{'BR' if is_br_match else 'LW'} #{match_index} {dbg['reason'] or 'cancelled'} "
                 f"| {int(time.time() - match_start_time)}s pkts={dbg['pkts']} undec={dbg['undecoded']} "
                 f"cmds={dict(list(dbg['cmds'].items())[:6])} ok={completed_cleanly}")
        LAST_MATCH_NOTE[uid_str] = _note
        _info(f"SESH - {dbg['reason'] or 'cancelled'} | pkts={dbg['pkts']} undecoded={dbg['undecoded']} cmds={dict(list(dbg['cmds'].items())[:8])} clean={completed_cleanly}")
        bot_state.log(f"[MATCH #{match_index}] SESH → {_note}", "info", uid_str)
        if completed_cleanly:
            try:
                bot_state.increment_match(uid_str)
            except Exception:
                pass

            async def _refresh_exp_later():
                try:
                    if is_br_match:


                        await asyncio.sleep(10)
                        await refresh_account_profile(uid_str)
                        await asyncio.sleep(40)
                        await refresh_account_profile(uid_str)
                        await asyncio.sleep(90)
                        await refresh_account_profile(uid_str)
                        await asyncio.sleep(180)
                        await refresh_account_profile(uid_str)
                    else:


                        await asyncio.sleep(8)
                        await refresh_account_profile(uid_str)
                        await asyncio.sleep(22)
                        await refresh_account_profile(uid_str)
                        await asyncio.sleep(35)
                        await refresh_account_profile(uid_str)
                        await asyncio.sleep(55)
                        await refresh_account_profile(uid_str)
                        await asyncio.sleep(60)
                        await refresh_account_profile(uid_str)
                except Exception:
                    pass
            try:
                asyncio.create_task(_refresh_exp_later())
            except Exception:
                pass
        ping_stop.set()
        join_post_stop.set()
        if join_post_task:
            join_post_task.cancel()
            try:
                await join_post_task
            except (asyncio.CancelledError, Exception):
                pass
        if afk_task:
            afk_task.cancel()
            try:
                await afk_task
            except (asyncio.CancelledError, Exception):
                pass
        if ping_task:
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        if sock:
            try:
                sock.close()
            except Exception:
                pass
        remaining = await _dec_match(uid_str)
        total = await _get_total_match_count()
        bot_state.log(f"[MATCH #{match_index}] Closed. UID active: {remaining} | Total active: {total}", "info")
        try:
            bot_state.update_status(uid_str, "IN_MATCH" if remaining > 0 else "ONLINE", remaining)
        except Exception:
            pass


async def functional_lone_wolf(addrs, starter_packet, account_region, client_version,
                                key, iv, account_id="", account_data=None,
                                max_reconnects=10):
    reconnects = 0
    ip, port = addrs.split(":")
    play_matches: List[asyncio.Task] = []
    no_response_count = 0
    search_attempts = 0
    last_start_time = 0.0
    uid_str = str(account_id)

    consecutive_parse_failures = 0

    current_token = starter_packet
    current_key = key
    current_iv = iv
    current_account_data = account_data
    last_sent_type = None
    last_ka_time = 0.0
    gw = {"pkts": 0, "queue": 0, "big": 0, "last": "-"}

    try:
        while True:
            writer = None


            while BR_HOLD_MATCH and bot_state.get_match_type(uid_str) == "BR":
                play_matches[:] = [m for m in play_matches if not m.done()]
                if not play_matches:
                    break
                try:
                    bot_state.update_status(uid_str, "IN_MATCH", len(play_matches))
                except Exception:
                    pass
                await asyncio.sleep(1.0)

            try:
                if current_account_data:
                    fresh = None
                    if current_account_data.get('auth_type') == 'guest' and current_account_data.get('auth_uid'):
                        fresh = cache_get(str(current_account_data['auth_uid']))
                    elif current_account_data.get('auth_type') == 'token' and current_account_data.get('auth_token'):
                        fresh = cache_get(f"tok_{current_account_data['auth_token'][:20]}")

                    if fresh:
                        current_account_data = fresh
                        current_key = fresh['aes_ak']
                        current_iv = fresh['iv_i']
                        current_token = await build_tcp_startup_packet(
                            fresh['account_id'],
                            fresh['token'],
                            fresh['server_time'],
                            current_key,
                            current_iv,
                            region=fresh.get('region', account_region),
                            typ='OnLine'
                        )
                    else:
                        print_warning(f"[FUNCTIONAL] Cache miss for {uid_str} → re-login needed")
                        try:
                            if current_account_data.get('auth_uid'):
                                cache_invalidate(str(current_account_data['auth_uid']))
                            if current_account_data.get('auth_token'):
                                cache_invalidate(f"tok_{current_account_data['auth_token'][:20]}")
                        except Exception:
                            pass
                        raise ConnectionError("Cache expired, triggering fresh login")

                resolved_ip = await resolve_host_cloudflare(ip)

                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(resolved_ip, int(port)),
                        timeout=6.0
                    )
                except asyncio.TimeoutError:
                    print_warning(f"[FUNCTIONAL] TCP connect timeout for {uid_str} ({resolved_ip}:{port}) — retrying")
                    await asyncio.sleep(0.5)
                    raise ConnectionError("TCP connect timeout")

                raw_sock = writer.get_extra_info('socket')
                if raw_sock:
                    optimize_tcp_socket(raw_sock)

                writer.write(bytes.fromhex(current_token))
                await asyncio.wait_for(writer.drain(), timeout=5.0)


                try:
                    init_ka = await send_keep_alive(account_region)
                    if init_ka and writer and not writer.is_closing():
                        writer.write(init_ka)
                        await asyncio.wait_for(writer.drain(), timeout=3)
                except Exception:
                    pass

                bot_state.log(f"[FUNCTIONAL] TCP Gateway Connected for UID: {uid_str} (DNS: {resolved_ip})", "success")
                reconnects = 0
                no_response_count = 0
                last_start_time = 0.0
                last_sent_type = None
                last_ka_time = 0.0

                async def send_start_match():
                    nonlocal search_attempts, last_start_time, last_sent_type

                    if not bot_state.global_running:
                        last_start_time = asyncio.get_running_loop().time()
                        return
                    search_attempts += 1
                    current_region = account_region or DEFAULT_REGION

                    selected_type = bot_state.get_match_type(uid_str)
                    last_sent_type = selected_type
                    try:


                        human_delay = random.uniform(0.15, 0.4)
                        await asyncio.sleep(human_delay)
                        if selected_type == "BR":
                            bot_state.log(f"[⚔] BR Match Search #{search_attempts} | UID: {uid_str} | Region: {current_region}", "info")
                            await start_game_battle_royale(
                                current_region, client_version, writer,
                                current_key, current_iv
                            )
                            try:
                                bot_state.set_info(uid_str, f"BR Match Search #{search_attempts} — waiting for match... (gw pkts={gw['pkts']} queue={gw['queue']} big={gw['big']} last={gw['last']})" + (f" || LAST: {LAST_MATCH_NOTE[uid_str]}" if uid_str in LAST_MATCH_NOTE else ""))
                            except Exception:
                                pass
                        elif selected_type == "LW_HS":

                            bot_state.log(f"[🎯] LW Headshot Search #{search_attempts} | UID: {uid_str} | Region: {current_region}", "info")
                            await start_game_lw_headshot(
                                current_region, client_version, writer,
                                current_key, current_iv,
                                account_id=uid_str
                            )
                            try:
                                bot_state.set_info(uid_str, f"LW Headshot Search #{search_attempts} — waiting for match...")
                            except Exception:
                                pass
                        else:
                            bot_state.log(f"[🐺] Lone Wolf Search #{search_attempts} | UID: {uid_str} | Region: {current_region}", "info")
                            await start_game_lone_wolf(
                                current_region, client_version, writer,
                                current_key, current_iv
                            )
                            try:
                                bot_state.set_info(uid_str, f"Lone Wolf Search #{search_attempts} — waiting for match...")
                            except Exception:
                                pass
                        active = await _get_match_count(uid_str)
                        try:
                            bot_state.update_status(uid_str, "SEARCHING", active)
                        except Exception:
                            pass
                    except Exception as e:
                        print_error(f"send_start_match error ({selected_type}): {e}")
                    last_start_time = asyncio.get_running_loop().time()


                if not (MAX_PARALLEL_MATCHES > 0 and (await _get_match_count(uid_str)) >= MAX_PARALLEL_MATCHES):
                    await send_start_match()

                while True:
                    play_matches[:] = [m for m in play_matches if not m.done()]

                    active_count = await _get_match_count(uid_str)


                    if not bot_state.global_running:
                        try:
                            bot_state.update_status(uid_str, "PAUSED", active_count)
                        except Exception:
                            pass
                        await asyncio.sleep(1.0)
                        last_start_time = asyncio.get_running_loop().time()
                        last_sent_type = None
                        continue

                    try:
                        bot_state.update_status(
                            uid_str,
                            "ONLINE" if active_count == 0 else "IN_MATCH",
                            active_count
                        )
                    except Exception:
                        pass

                    now = asyncio.get_running_loop().time()
                    cur_type = bot_state.get_match_type(uid_str)
                    is_br = (cur_type == "BR")
                    if is_br and BR_HOLD_MATCH and any(not m.done() for m in play_matches):

                        await asyncio.sleep(0.5)
                        continue
                    _max_p = BR_MAX_PARALLEL if is_br else MAX_PARALLEL_MATCHES
                    cap_reached = _max_p > 0 and active_count >= _max_p
                    if cap_reached:
                        pass
                    elif cur_type != last_sent_type:

                        last_sent_type = cur_type
                        await send_start_match()
                    elif now - last_start_time >= (BR_START_MATCH_INTERVAL if is_br else START_MATCH_INTERVAL):
                        await send_start_match()
                        if is_br and BR_DOUBLE_SEARCH:
                            await asyncio.sleep(0.5)
                            await send_start_match()


                    LW_KEEPALIVE_INTERVAL = 9.0
                    ka_interval = BR_KEEPALIVE_INTERVAL if is_br else LW_KEEPALIVE_INTERVAL
                    if now - last_ka_time >= ka_interval:
                        last_ka_time = now
                        try:
                            if writer and not writer.is_closing():

                                ka_lw = build_tcp_ping_lw(current_key, current_iv)
                                writer.write(ka_lw)
                                await asyncio.wait_for(writer.drain(), timeout=3)
                        except Exception:
                            try:

                                ka = await send_keep_alive(account_region)
                                if ka and writer and not writer.is_closing():
                                    writer.write(ka)
                                    await asyncio.wait_for(writer.drain(), timeout=3)
                            except Exception:
                                pass

                    try:
                        data = await asyncio.wait_for(reader.read(8192), timeout=1.5)
                    except asyncio.TimeoutError:
                        no_response_count += 1


                        LW_IDLE_LIMIT = 60
                        if no_response_count > (BR_IDLE_LIMIT if is_br else LW_IDLE_LIMIT):
                            bot_state.log(f"[FUNCTIONAL] Gateway silent ({uid_str}). Reconnecting...", "warning")
                            raise ConnectionError("Gateway idle timeout")
                        continue

                    if not data:
                        raise ConnectionError("Connection closed by server")

                    hex_data = data.hex()
                    packet_length = len(data)


                    _BAN_HEX_PREFIXES = ("0b00", "0d00", "0500")
                    _is_small = packet_length <= 20
                    _is_ban_prefix = any(hex_data.startswith(p) for p in _BAN_HEX_PREFIXES)
                    if _is_small and _is_ban_prefix and len(hex_data) >= 8:

                        try:
                            inner = bytes.fromhex(hex_data[8:])
                            _BAN_REASON_CODES = {b'\x08\x02', b'\x08\x03', b'\x08\x08', b'\x08\x09', b'\x08\x0e', b'\x08\x0f'}
                            if any(inner.startswith(c) for c in _BAN_REASON_CODES):
                                bot_state.log(f"[BANNED] TCP ban packet received for {uid_str} — auto-removing", "error", uid_str)
                                try:
                                    bot_state.deleted_uids.add(uid_str)
                                    bot_state._save_deleted_uids()
                                    bot_state._remove_from_json(uid_str)
                                    bot_state.accounts.pop(uid_str, None)
                                    bot_state.recalc_totals()
                                except Exception:
                                    pass
                                raise ConnectionError("BAN_DETECTED")
                        except ConnectionError:
                            raise
                        except Exception:
                            pass
                    no_response_count = 0

                    gw["pkts"] += 1
                    gw["last"] = f"{packet_length}B/{hex_data[:8]}"
                    if is_br:
                        if hex_data.startswith("0300") and 10 < packet_length < 30:
                            gw["queue"] += 1
                        elif packet_length >= 300:
                            gw["big"] += 1
                        if os.environ.get("BR_DEBUG", "0") == "1":
                            bot_state.log(f"[BR-DEBUG] gateway pkt len={packet_length} head={hex_data[:24]}", "info", uid_str)
                        try:
                            bot_state.set_info(uid_str, f"BR Search #{search_attempts} | gw pkts={gw['pkts']} queue={gw['queue']} big={gw['big']} last={gw['last']}" + (f" || LAST: {LAST_MATCH_NOTE[uid_str]}" if uid_str in LAST_MATCH_NOTE else ""))
                        except Exception:
                            pass

                    if hex_data.startswith("0300") and 10 < packet_length < 30:
                        bot_state.log("Match starting, please wait...", "info")
                        continue

                    if hex_data.startswith("0300") and packet_length >= 300:
                        bot_state.log("MATCH FOUND! Loading...", "success")
                        try:
                            bot_state.set_info(uid_str, "MATCH FOUND, UDP shuru hocche...")
                        except Exception:
                            pass

                        try:
                            await send_post_match_2814(writer, current_key, current_iv)
                        except Exception:
                            pass

                        try:
                            res = json.loads(await decode_protobuf(hex_data[10:]))
                            token = None
                            udp_key = None
                            match_code = None
                            server_ip_port = None
                            match_account_id = None
                            block_val = None

                            if '42' in res and 'data' in res['42']:
                                match_code = res['42']['data']
                            if '5' in res and 'data' in res['5']:
                                res_field5 = res['5']['data']
                                server_ip_port = res_field5.get('2', {}).get('data')
                                udp_key = res_field5.get('3', {}).get('data')
                                token = res_field5.get('4', {}).get('data')
                                if '42' in res_field5:
                                    match_code = res_field5['42']['data']
                            if '1' in res and 'data' in res['1']:
                                match_account_id = res['1']['data']
                            if '5' in res and 'data' in res['5']:
                                block_val = res['5']['data'].get('1', {}).get('data')

                            effective_acc_id = match_account_id or account_id or "BOT_ACC"

                            if token and udp_key and match_code and server_ip_port:
                                acc_tok = ""
                                if current_account_data:
                                    acc_tok = current_account_data.get('access_token', '') or ""
                                join_prep_pkt, join_post_pkt = await build_match_startup_packets(
                                    token, udp_key, match_code, effective_acc_id, block_val or 0,
                                    server_ip=server_ip_port,
                                    region=account_region,
                                    client_version=client_version,
                                    access_token=acc_tok,
                                    mode=("BR" if bot_state.get_match_type(uid_str) == "BR" else ("LW_HS" if bot_state.get_match_type(uid_str) == "LW_HS" else "LONE_WOLF"))

                                )

                                match_index = await _inc_match(uid_str)
                                total = await _get_total_match_count()
                                _mt = bot_state.get_match_type(uid_str)
                                _mode_label = "BR" if _mt == "BR" else ("HS" if _mt == "LW_HS" else "LW")
                                bot_state.log(f"🚀 [MATCH #{match_index}] [{_mode_label}] UDP starting → {server_ip_port} (background)", "info")
                                bot_state.log(f"[FUNCTIONAL] UDP task started. UID active: {match_index} | Total: {total}", "success")

                                new_match = asyncio.create_task(
                                    play_game(
                                        server_ip_port,
                                        join_prep_pkt,
                                        join_post_pkt,
                                        udp_key,
                                        match_code,
                                        effective_acc_id,
                                        account_region,
                                        client_version,
                                        current_key,
                                        current_iv,
                                        match_index=match_index,
                                        dash_uid=uid_str,
                                        mode=("BR" if _mt == "BR" else ("LW_HS" if _mt == "LW_HS" else "LONE_WOLF"))
                                    )
                                )
                                play_matches.append(new_match)

                                consecutive_parse_failures = 0

                                try:
                                    writer.close()
                                    await writer.wait_closed()
                                except Exception:
                                    pass


                                post_match_wait = max(5.0, NEW_MATCH_DELAY - 3.0) + random.uniform(1.5, 4.0)
                                bot_state.log(f"[SAFE] Post-match cooldown {post_match_wait:.1f}s before next search", "info")
                                await asyncio.sleep(post_match_wait)
                                reconnects = 0
                                break

                            elif bot_state.get_match_type(uid_str) == "BR":
                                bot_state.log(f"[BR] config packet ({packet_length}B) — queue-te-i thakchi", "info", uid_str)
                                continue
                            else:
                                consecutive_parse_failures += 1
                                bot_state.log(f"[FUNCTIONAL] Non-match big packet (#{consecutive_parse_failures}/{MAX_CONSECUTIVE_PARSE_FAILURES}) → reconnecting", "warning")

                                if consecutive_parse_failures >= MAX_CONSECUTIVE_PARSE_FAILURES:
                                    bot_state.log(f"[FUNCTIONAL] {MAX_CONSECUTIVE_PARSE_FAILURES}x parse failures → invalidating cache for fresh login", "error")
                                    if current_account_data:
                                        try:
                                            if current_account_data.get('auth_uid'):
                                                cache_invalidate(str(current_account_data['auth_uid']))
                                            if current_account_data.get('auth_token'):
                                                cache_invalidate(f"tok_{current_account_data['auth_token'][:20]}")
                                        except Exception:
                                            pass
                                    consecutive_parse_failures = 0

                                try:
                                    writer.close()
                                    await writer.wait_closed()
                                except Exception:
                                    pass
                                await asyncio.sleep(NON_MATCH_RECONNECT_DELAY)
                                break

                        except Exception as e:
                            print_error(f"[FUNCTIONAL] Match packet error: {e}")
                            consecutive_parse_failures += 1
                            if consecutive_parse_failures >= MAX_CONSECUTIVE_PARSE_FAILURES:
                                if current_account_data:
                                    try:
                                        if current_account_data.get('auth_uid'):
                                            cache_invalidate(str(current_account_data['auth_uid']))
                                        if current_account_data.get('auth_token'):
                                            cache_invalidate(f"tok_{current_account_data['auth_token'][:20]}")
                                    except Exception:
                                        pass
                                consecutive_parse_failures = 0
                            try:
                                writer.close()
                                await writer.wait_closed()
                            except Exception:
                                pass
                            await asyncio.sleep(NON_MATCH_RECONNECT_DELAY)
                            break

                    if 30 <= packet_length <= 40:
                        continue

            except asyncio.CancelledError:
                bot_state.log(f"[FUNCTIONAL] Cancelled — cancelling {len(play_matches)} UDP matches", "warning")
                for m in play_matches:
                    if not m.done():
                        m.cancel()
                if play_matches:
                    await asyncio.gather(*play_matches, return_exceptions=True)
                play_matches.clear()
                raise
            except Exception as e:
                _e_str = str(e)

                if "BAN_DETECTED" in _e_str:
                    bot_state.log(f"[BANNED] Worker for {uid_str} stopped permanently (ban detected)", "error", uid_str)
                    return

                elif "Connection closed by server" in _e_str or "EOF" in _e_str:
                    bot_state.log(f"[FUNCTIONAL] TCP state ({uid_str}): {_e_str}", "info")
                else:
                    print_error(f"[FUNCTIONAL] TCP state ({uid_str}): {_e_str}")

                play_matches[:] = [m for m in play_matches if not m.done()]

                if writer:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass

                if "Cache expired" in str(e):
                    print_warning(f"[FUNCTIONAL] Triggering re-login for {uid_str}")
                    break

                reconnects += 1
                if reconnects > max_reconnects:
                    print_error("[FUNCTIONAL] Max reconnects reached, retrying...")
                    reconnects = 0
                    await asyncio.sleep(3)
                    continue


                await asyncio.sleep(min(2 ** (reconnects - 1), 8) + random.uniform(0, 1.0))

    except asyncio.CancelledError:
        print_warning(f"[FUNCTIONAL] Outer cancelled. {len(play_matches)} UDP matches still running.")
        for m in play_matches:
            if not m.done():
                m.cancel()
        if play_matches:
            await asyncio.gather(*play_matches, return_exceptions=True)
        play_matches.clear()
        raise


async def informational(addrs, starter_packet, key, iv, region=DEFAULT_REGION, max_reconnects=3):
    reconnects = 0
    ip, port = addrs.split(":")
    while True:
        writer = None
        ping_task = None
        try:
            resolved_ip = await resolve_host_cloudflare(ip)

            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(resolved_ip, int(port)),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                await asyncio.sleep(1.0)
                raise ConnectionError("Informational TCP timeout")

            raw_sock = writer.get_extra_info('socket')
            if raw_sock:
                optimize_tcp_socket(raw_sock)

            writer.write(bytes.fromhex(starter_packet))
            await writer.drain()
            reconnects = 0


            try:
                init_ka = await send_keep_alive(region)
                if init_ka and writer and not writer.is_closing():
                    writer.write(init_ka)
                    await asyncio.wait_for(writer.drain(), timeout=3)
            except Exception:
                pass

            async def info_keepalive():
                ka_bytes = await send_keep_alive(region)
                while True:
                    await asyncio.sleep(5)
                    try:
                        if writer and not writer.is_closing():
                            writer.write(ka_bytes)
                            await writer.drain()
                    except Exception:
                        break

            ping_task = asyncio.create_task(info_keepalive())

            while True:
                data = await reader.read(8192)
                if not data:
                    raise ConnectionError("Connection closed")
        except asyncio.CancelledError:
            if ping_task:
                ping_task.cancel()
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
            raise
        except Exception:
            if ping_task:
                ping_task.cancel()
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
            reconnects += 1
            if reconnects > max_reconnects:
                await asyncio.sleep(3)
                reconnects = 0
            else:
                await asyncio.sleep(1)


def _register_credentials(account_data: Dict):
    try:
        acc_id = str(account_data['account_id'])
        bot_state.account_credentials[acc_id] = account_data
        if account_data.get('auth_uid'):
            bot_state.account_credentials[str(account_data['auth_uid'])] = account_data
        if account_data.get('auth_token'):
            bot_state.account_credentials[f"tok_{account_data['auth_token'][:20]}"] = account_data
    except Exception:
        pass


async def refresh_account_profile(account_data_or_uid: Any):
    try:
        if isinstance(account_data_or_uid, str):
            uid = str(account_data_or_uid)
            account_data = bot_state.account_credentials.get(uid)
        else:
            account_data = account_data_or_uid
            uid = str(account_data.get('account_id'))

        if not account_data:
            return

        url = account_data.get('server_url')
        token = account_data.get('token')
        release_version = account_data.get('release_version')
        payload = account_data.get('login_payload_data')

        if not (url and token and release_version and payload):
            return

        res = await send_getlogin(payload, url, token, release_version)
        if res:
            res_proto, dict_res = res
            level = int(get_proto_field(dict_res, 6, 1))
            exp = int(get_proto_field(dict_res, 7, 0))
            likes = int(get_proto_field(dict_res, 8, 0))
            nickname = res_proto.nickname or get_proto_field(dict_res, 4, "")

            acc_id = str(account_data['account_id'])

            if exp >= 0:
                bot_state.update_exp(acc_id, exp, level)
            if likes > 0 and acc_id in bot_state.accounts:
                bot_state.accounts[acc_id]["likes"] = likes
            if nickname and acc_id in bot_state.accounts:
                bot_state.accounts[acc_id]["nickname"] = nickname
            bot_state.log(f"[EXP-REFRESH] UID {acc_id} -> Level: {level}, EXP: {exp}", "info")
    except Exception as e:
        print_error(f"refresh_account_profile error: {e}")


async def process_account_uid_pass(uid: str, password: str) -> Optional[Dict]:
    _current_login_uid.set(str(uid))
    _acc = bot_state.accounts.get(str(uid))
    if _acc:
        _acc.pop("last_error", None)
    cached = cache_get(uid)
    if cached:
        print_success(f"[CACHE HIT] UID {uid} loaded from token_cache.json (no login)")
        acc_id = str(cached['account_id'])
        bot_state.register_account(
            uid=acc_id,
            nickname=cached.get('nickname', f"Player_{acc_id}"),
            region=cached.get('region', DEFAULT_REGION),
            level=cached.get('level', 1),
            exp=cached.get('exp', 0),
            likes=cached.get('likes', 0)
        )
        _register_credentials(cached)
        return cached

    print_info(f"[LOGIN] Full login for UID {uid}...")

    async with _LOGIN_SEMAPHORE:
      try:

        verconfig_res = await version_config_cached()
        if verconfig_res is None:
            report_fail(uid, "Version config failed", generic=True)
            return None
        release_version, client_version, server_url = verconfig_res

        tokengrant_response = await get_access_token(uid, password)
        if tokengrant_response is None:
            return None

        if tokengrant_response == "BANNED":
            return "BANNED"
        open_id, access_token, platform = tokengrant_response


        device_info = get_device_for_account(uid)

        login_payload_data = await build_majorlogin_payload(open_id, access_token, platform, client_version, device_info)
        if login_payload_data is None:
            report_fail(uid, "MajorLogin payload build failed", generic=True)
            return None
        majorlogin_response = await send_majorlogin(login_payload_data, release_version, server_url)
        if majorlogin_response is None:
            report_fail(uid, "MajorLogin failed", generic=True)
            return None

        if majorlogin_response == "BANNED":
            report_fail(uid, f"[BANNED] MajorLogin ban response for UID {uid}", "auto-removing")
            try:
                bot_state.deleted_uids.add(str(uid))
                bot_state._save_deleted_uids()
                bot_state._remove_from_json(str(uid))
                bot_state.accounts.pop(str(uid), None)
                bot_state.recalc_totals()
            except Exception:
                pass
            return "BANNED"
        getlogin_result = await send_getlogin(login_payload_data, majorlogin_response.url, majorlogin_response.token, release_version)
        if getlogin_result is None:
            report_fail(uid, "GetLoginData failed", generic=True)
            return None
        res_proto, dict_res = getlogin_result

        acc_id = str(majorlogin_response.account_id)
        level = int(get_proto_field(dict_res, 6, 1))
        exp = int(get_proto_field(dict_res, 7, 0))
        likes = int(get_proto_field(dict_res, 8, 0))
        nickname = res_proto.nickname or get_proto_field(dict_res, 4, f"Player_{acc_id}")
        region = majorlogin_response.region or get_proto_field(dict_res, 3, "BD")

        bot_state.register_account(uid=acc_id, nickname=nickname, region=region, level=level, exp=exp, likes=likes)

        account_data = {
            'account_id': majorlogin_response.account_id,
            'nickname': nickname,
            'region': region,
            'level': level,
            'exp': exp,
            'likes': likes,
            'open_id': open_id,
            'access_token': access_token,
            'platform': str(platform),
            'token': majorlogin_response.token,
            'server_time': majorlogin_response.server_time,
            'aes_ak': majorlogin_response.aes_ak,
            'iv_i': majorlogin_response.iv_i,
            'functional_addrs': res_proto.functional_addrs or get_proto_field(dict_res, 14),
            'informational_addrs': res_proto.informational_addrs or get_proto_field(dict_res, 32),
            'release_version': release_version,
            'client_version': client_version,
            'server_url': majorlogin_response.url,
            'stronghold': _STRONGHOLD_BY_ACC.get(str(majorlogin_response.account_id), ''),
            'login_payload_data': login_payload_data,
            'auth_type': 'guest',
            'auth_uid': uid,
            'auth_password': password
        }
        _register_credentials(account_data)
        cache_set(uid, account_data)
        return account_data
      except Exception as e:
        report_fail(uid, "process_account_uid_pass exception", repr(e)[:250])
        return None


async def process_account_token(access_token: str) -> Optional[Dict]:
    cache_key = f"tok_{access_token[:20]}"
    cached = cache_get(cache_key)
    if cached:
        print_success(f"[CACHE HIT] Token {access_token[:10]}... loaded from cache")
        acc_id = str(cached['account_id'])
        bot_state.register_account(
            uid=acc_id,
            nickname=cached.get('nickname', f"Player_{acc_id}"),
            region=cached.get('region', DEFAULT_REGION),
            level=cached.get('level', 1),
            exp=cached.get('exp', 0),
            likes=cached.get('likes', 0)
        )
        _register_credentials(cached)
        return cached

    print_info("[LOGIN] Full login with Access Token...")

    async with _LOGIN_SEMAPHORE:
      try:

        verconfig_res = await version_config_cached()
        if verconfig_res is None:
            return None
        release_version, client_version, server_url = verconfig_res


        url = f"https://100067.connect.garena.com/oauth/token/inspect?token={access_token}"
        hdrs = {
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "close",
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "100067.connect.garena.com",
            "User-Agent": "GarenaMSDK/4.0.19P4(G011A ;Android 9;en;US;)"
        }
        resp = await client.get(url, headers=hdrs)
        data = resp.json()

        if 'error' in data:
            return None

        open_id = data.get('open_id')
        platform = data.get('platform', 4)

        if not open_id:
            return None


        device_info = get_device_for_account(open_id)

        login_payload_data = await build_majorlogin_payload(open_id, access_token, str(platform), client_version, device_info)
        if not login_payload_data:
            return None

        majorlogin_response = await send_majorlogin(login_payload_data, release_version, server_url)
        if majorlogin_response is None:
            return None

        getlogin_result = await send_getlogin(
            login_payload_data,
            majorlogin_response.url,
            majorlogin_response.token,
            release_version
        )
        if getlogin_result is None:
            return None

        res_proto, dict_res = getlogin_result
        acc_id = str(majorlogin_response.account_id)
        level = int(get_proto_field(dict_res, 6, 1))
        exp = int(get_proto_field(dict_res, 7, 0))
        likes = int(get_proto_field(dict_res, 8, 0))
        nickname = res_proto.nickname or get_proto_field(dict_res, 4, f"Player_{acc_id}")
        region = majorlogin_response.region or get_proto_field(dict_res, 3, "BD")

        bot_state.register_account(uid=acc_id, nickname=nickname, region=region, level=level, exp=exp, likes=likes)

        account_data = {
            'account_id': majorlogin_response.account_id,
            'nickname': nickname,
            'region': region,
            'level': level,
            'exp': exp,
            'likes': likes,
            'open_id': open_id,
            'access_token': access_token,
            'platform': str(platform),
            'token': majorlogin_response.token,
            'server_time': majorlogin_response.server_time,
            'aes_ak': majorlogin_response.aes_ak,
            'iv_i': majorlogin_response.iv_i,
            'functional_addrs': res_proto.functional_addrs or get_proto_field(dict_res, 14),
            'informational_addrs': res_proto.informational_addrs or get_proto_field(dict_res, 32),
            'release_version': release_version,
            'client_version': client_version,
            'server_url': majorlogin_response.url,
            'stronghold': _STRONGHOLD_BY_ACC.get(str(majorlogin_response.account_id), ''),
            'login_payload_data': login_payload_data,
            'auth_type': 'token',
            'auth_token': access_token
        }
        _register_credentials(account_data)
        cache_set(cache_key, account_data)
        return account_data
      except Exception as e:
        print_error(f"process_account_token error: {e}")
        return None


async def run_account_worker(account_data: Dict, label: str):
    acc_id = str(account_data['account_id'])
    informational_task = None
    exp_task = None
    try:

        bot_state.update_status(acc_id, "ONLINE")
        reg = account_data.get('region', DEFAULT_REGION)
        tcp_packet_online = await build_tcp_startup_packet(
            account_data['account_id'],
            account_data['token'],
            account_data['server_time'],
            account_data['aes_ak'],
            account_data['iv_i'],
            region=reg,
            typ='OnLine'
        )

        tcp_packet_chat = await build_tcp_startup_packet(
            account_data['account_id'],
            account_data['token'],
            account_data['server_time'],
            account_data['aes_ak'],
            account_data['iv_i'],
            region=reg,
            typ='ChaT'
        )

        if account_data.get('informational_addrs') and ":" in str(account_data.get('informational_addrs')):
            informational_task = asyncio.create_task(
                informational(
                    account_data['informational_addrs'],
                    tcp_packet_chat,
                    account_data['aes_ak'],
                    account_data['iv_i'],
                    region=reg
                )
            )

        async def exp_refresher():
            while True:
                await asyncio.sleep(EXP_REFRESH_SECONDS)
                fresh = bot_state.account_credentials.get(acc_id)
                if fresh:
                    await refresh_account_profile(fresh)

        exp_task = asyncio.create_task(exp_refresher())


        if not account_data.get('functional_addrs') or ":" not in str(account_data.get('functional_addrs')):
            print_error(f"Missing functional_addrs for {label}, retrying login...")
            return
        selected_mode = bot_state.get_match_type(acc_id)
        if selected_mode == "BR_EVO":
            bre_auth = bre_build_auth_packet(
                account_data['account_id'],
                account_data['token'],
                account_data['server_time'],
                account_data['aes_ak'],
                account_data['iv_i'],
            )
            functional_task = asyncio.create_task(
                functional_br_evo(
                    account_data['functional_addrs'],
                    bre_auth,
                    account_data['aes_ak'],
                    account_data['iv_i'],
                    account_id=acc_id,
                    region=reg,
                    game_version=account_data.get('client_version', FORCED_CLIENT_VERSION),
                    stronghold=account_data.get('stronghold', ''),
                )
            )
        else:
            functional_task = asyncio.create_task(
                functional_lone_wolf(
                    account_data['functional_addrs'],
                    tcp_packet_online,
                    reg,
                    account_data.get('client_version', FORCED_CLIENT_VERSION),
                    account_data['aes_ak'],
                    account_data['iv_i'],
                    account_id=acc_id,
                    account_data=account_data,
                )
            )

        await functional_task

    except asyncio.CancelledError:
        raise
    except Exception as e:
        print_error(f"run_account_worker error for {label}: {e}")
    finally:
        for t in (informational_task, exp_task):
            if t and not t.done():
                t.cancel()
        for t in (informational_task, exp_task):
            if t:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass


async def account_loop_guest(uid: str, password: str):
    uid_str = str(uid)


    display_uid = uid_str

    if uid_str not in bot_state.accounts:
        bot_state.register_account(
            uid=uid_str,
            nickname=f"Player_{uid_str[:6]}",
            region=DEFAULT_REGION,
            level=1,
            exp=0
        )
    bot_state.update_status(uid_str, "CONNECTING")

    while True:


        if uid_str in bot_state.deleted_uids or display_uid in bot_state.deleted_uids:
            print_warning(f"[BLACKLIST] Worker for {uid} self-terminated.")


            bot_state.accounts.pop(display_uid, None)
            bot_state.recalc_totals()
            break

        try:
            print_info(f"[LOGIN] Starting login for Guest UID: {uid}...")
            bot_state.update_status(display_uid, "CONNECTING")

            account_data = await process_account_uid_pass(uid, password)

            if account_data == "BANNED":
                bot_state.update_status(display_uid, "ERROR")
                bot_state.set_error(display_uid, "Account permanently banned — auto-removed")
                break
            if not account_data:
                bot_state.update_status(display_uid, "ERROR")

                bot_state.record_login_fail(display_uid)
                if display_uid in bot_state.deleted_uids:
                    break


                fail_count = bot_state.login_fail_counts.get(display_uid, 1)
                if fail_count <= 3:
                    retry_delay = random.uniform(8.0, 15.0)
                    print_error(f"Login failed for UID: {uid} (attempt {fail_count}). Retrying in {retry_delay:.0f}s...")
                else:
                    retry_delay = random.uniform(20.0, 45.0)
                    print_error(f"Login failed for UID: {uid} (attempt {fail_count}). Retrying in {retry_delay:.0f}s...")
                await asyncio.sleep(retry_delay)
                continue


            bot_state.reset_login_fail(display_uid)
            real_acc_id = str(account_data.get('account_id', uid_str))

            if real_acc_id != display_uid:

                if real_acc_id in bot_state.deleted_uids:
                    print_warning(f"[BLACKLIST] Real ID {real_acc_id} blacklisted. Worker stopping.")


                    bot_state.accounts.pop(display_uid, None)
                    bot_state.recalc_totals()
                    bot_state._remove_from_json(display_uid)
                    bot_state.deleted_uids.add(display_uid)
                    bot_state._save_deleted_uids()
                    break


                if display_uid in bot_state.account_workers:
                    running_task = bot_state.account_workers.pop(display_uid)
                    bot_state.account_workers[real_acc_id] = running_task

                bot_state.guest_alias[uid_str] = real_acc_id

                if display_uid in bot_state.accounts:
                    del bot_state.accounts[display_uid]

                bot_state.register_account(
                    uid=real_acc_id,
                    nickname=account_data.get('nickname', f"Player_{real_acc_id[:6]}"),
                    region=account_data.get('region', DEFAULT_REGION),
                    level=account_data.get('level', 1),
                    exp=account_data.get('exp', 0),
                    likes=account_data.get('likes', 0)
                )

                display_uid = real_acc_id

                bot_state.update_status(display_uid, "ONLINE")

            elif display_uid in bot_state.accounts:
                entry = bot_state.accounts[display_uid]
                entry['nickname']    = account_data.get('nickname', entry['nickname'])
                entry['region']      = account_data.get('region',   entry['region'])
                entry['level']       = account_data.get('level',    entry['level'])
                entry['current_exp'] = account_data.get('exp',      entry['current_exp'])
                entry['likes']       = account_data.get('likes',    entry['likes'])
                entry['status']      = 'ONLINE'
                entry['last_updated'] = time.strftime("%H:%M:%S")
                bot_state.recalc_totals()
                bot_state.check_level_limit(display_uid)

            await run_account_worker(account_data, uid)
            print_warning(f"Session finished for {uid}. Reconnecting in 10-25s...")
            bot_state.update_status(display_uid, "CONNECTING")

            await asyncio.sleep(random.uniform(10.0, 25.0))
        except asyncio.CancelledError:
            print_warning(f"Worker for {uid} stopped.")
            bot_state.update_status(display_uid, "OFFLINE")
            break
        except Exception as e:
            print_error(f"Error for UID {uid}: {e}. Retrying in 10s...")
            bot_state.update_status(display_uid, "ERROR")
            bot_state.set_error(display_uid, f"Worker exception: {repr(e)[:200]}")
            await asyncio.sleep(10)


async def account_loop_token(token: str):
    token_label = token[:10]
    placeholder_uid = f"tok_{token_label}"
    if placeholder_uid not in bot_state.accounts:
        bot_state.register_account(
            uid=placeholder_uid,
            nickname=f"Token_{token_label}",
            region=DEFAULT_REGION,
            level=1,
            exp=0
        )
    bot_state.update_status(placeholder_uid, "CONNECTING")

    while True:

        if placeholder_uid in bot_state.deleted_uids or token_label in bot_state.deleted_uids:
            print_warning(f"[BLACKLIST] Token worker {token_label} self-terminated.")
            bot_state.update_status(placeholder_uid, "OFFLINE")
            break

        try:
            print_info("[LOGIN] Starting login with Access Token...")
            bot_state.update_status(placeholder_uid, "CONNECTING")

            account_data = await process_account_token(token)
            if not account_data:
                print_error("Login failed for Token. Retrying in 20-45 seconds...")
                bot_state.update_status(placeholder_uid, "ERROR")

                bot_state.record_login_fail(placeholder_uid)
                if placeholder_uid in bot_state.deleted_uids:
                    break

                await asyncio.sleep(random.uniform(20.0, 45.0))
                continue


            bot_state.reset_login_fail(placeholder_uid)
            acc_id = str(account_data['account_id'])


            if placeholder_uid in bot_state.accounts:
                entry = bot_state.accounts[placeholder_uid]
                entry['nickname']    = account_data.get('nickname', entry['nickname'])
                entry['region']      = account_data.get('region',   entry['region'])
                entry['level']       = account_data.get('level',    entry['level'])
                entry['current_exp'] = account_data.get('exp',      entry['current_exp'])
                entry['likes']       = account_data.get('likes',    entry['likes'])
                entry['last_updated'] = time.strftime("%H:%M:%S")
                bot_state.recalc_totals()
                bot_state.check_level_limit(placeholder_uid)

            bot_state.update_status(placeholder_uid, "ONLINE")

            await run_account_worker(account_data, acc_id)
            print_warning("Token session finished. Reconnecting in 2s...")
            bot_state.update_status(placeholder_uid, "CONNECTING")
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            print_warning(f"Worker for token {token_label} stopped.")
            bot_state.update_status(placeholder_uid, "OFFLINE")
            break
        except Exception as e:
            print_error(f"Token error: {e}. Retrying in 10s...")
            bot_state.update_status(placeholder_uid, "ERROR")
            await asyncio.sleep(10)


def load_accounts():
    accounts = []
    seen_uids = set()
    seen_tokens = set()

    files = get_all_account_files()
    for filepath in files:
        if not os.path.exists(filepath):
            continue
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                continue
            fname = os.path.basename(filepath)
            loaded = 0
            for acc in data:
                if "token" in acc and acc["token"]:
                    tok = str(acc["token"]).strip()
                    if tok not in seen_tokens:
                        seen_tokens.add(tok)
                        accounts.append(acc)
                        loaded += 1
                elif "uid" in acc and "password" in acc and acc["uid"]:
                    uid = str(acc["uid"]).strip()
                    if uid not in seen_uids:
                        seen_uids.add(uid)
                        accounts.append(acc)
                        loaded += 1
            print_success(f"[LOADER] {fname} → {loaded} account(s) loaded")
        except Exception as e:
            print_error(f"Could not load {filepath}: {e}")

    if not accounts and FALLBACK_UID and FALLBACK_PASSWORD:
        accounts.append({"uid": FALLBACK_UID, "password": FALLBACK_PASSWORD})

    if len(accounts) > MAX_ACCOUNTS:
        accounts = accounts[:MAX_ACCOUNTS]
    return accounts


async def _periodic_device_saver():
    while True:
        await asyncio.sleep(60)
        if _DEVICES_DIRTY:
            try:
                _save_devices_cache()
            except Exception:
                pass


_bg_tasks: list = []


async def _launch_gate(max_wait: float = 20.0):
    waited = 0.0
    while get_loop_lag() > 0.6 and waited < max_wait:
        await asyncio.sleep(1.0)
        waited += 1.0


async def main():
    import gc
    gc.set_threshold(20000, 20, 20)
    gc.collect()


    _load_devices_cache_once()

    _bg_tasks.append(asyncio.create_task(_periodic_device_saver()))


    saved_accounts = load_accounts()
    selected_accounts = interactive_termux_menu(saved_accounts)
    if selected_accounts is None:
        print_warning("Exiting CLI.")
        return

    global DEFAULT_REGION
    DEFAULT_REGION = bot_state.default_region

    try:
        await start_termux_monitor()
    except Exception as e:
        print_error(f"Could not start Termux monitor: {e}")

    async def on_account_added_handler(data):
        active_workers = [t for t in bot_state.account_workers.values() if not t.done()]
        if len(active_workers) >= MAX_ACCOUNTS:
            print_warning(f"Account limit reached (max {MAX_ACCOUNTS}).")
            return
        if "token" in data and data["token"]:
            t = str(data["token"]).strip()
            placeholder = f"tok_{t[:10]}"

            bot_state.deleted_uids.discard(t[:10])
            bot_state.deleted_uids.discard(placeholder)
            if placeholder not in bot_state.accounts:
                bot_state.register_account(uid=placeholder, nickname=f"Token_{t[:10]}", region=DEFAULT_REGION, level=1, exp=0)
            bot_state.update_status(placeholder, "CONNECTING")
            await asyncio.sleep(random.uniform(0.2, 0.6))
            task = asyncio.create_task(account_loop_token(t))
            bot_state.account_workers[t[:10]] = task
        elif "uid" in data and "password" in data:
            u = str(data["uid"]).strip()
            p = str(data["password"]).strip()

            bot_state.deleted_uids.discard(u)
            if u not in bot_state.accounts:
                bot_state.register_account(uid=u, nickname=f"Player_{u[:6]}", region=DEFAULT_REGION, level=1, exp=0)
            bot_state.update_status(u, "CONNECTING")
            await asyncio.sleep(random.uniform(0.2, 0.6))
            task = asyncio.create_task(account_loop_guest(u, p))
            bot_state.account_workers[u] = task

    async def on_refresh_account_handler(uid):
        await refresh_account_profile(uid)

    async def on_reload_accounts_handler():
        fresh_accounts = load_accounts()
        started = 0
        _RB = max(1, int(os.environ.get("LAUNCH_BATCH_SIZE", "20")))
        for acc in fresh_accounts:
            if started and started % _RB == 0:
                await _launch_gate()
                await asyncio.sleep(1.0)
            if "token" in acc and acc["token"]:
                t = str(acc["token"]).strip()
                key = t[:10]
                placeholder = f"tok_{key}"

                if key in bot_state.account_workers and not bot_state.account_workers[key].done():
                    continue
                bot_state.deleted_uids.discard(key)
                bot_state.deleted_uids.discard(placeholder)
                if placeholder not in bot_state.accounts:
                    bot_state.register_account(uid=placeholder, nickname=f"Token_{key}", region=DEFAULT_REGION, level=1, exp=0)
                bot_state.update_status(placeholder, "CONNECTING")
                await asyncio.sleep(random.uniform(0.05, 0.25))
                task = asyncio.create_task(account_loop_token(t))
                bot_state.account_workers[key] = task
                started += 1
            elif "uid" in acc and "password" in acc and acc["uid"]:
                u = str(acc["uid"]).strip()
                p = str(acc["password"]).strip()

                real_key = bot_state.guest_alias.get(u, u)
                if any(k in bot_state.account_workers and not bot_state.account_workers[k].done() for k in (u, real_key)):
                    continue
                bot_state.deleted_uids.discard(u)
                bot_state.deleted_uids.discard(real_key)
                if u not in bot_state.accounts:
                    bot_state.register_account(uid=u, nickname=f"Player_{u[:6]}", region=DEFAULT_REGION, level=1, exp=0)
                bot_state.update_status(u, "CONNECTING")
                await asyncio.sleep(random.uniform(0.05, 0.25))
                task = asyncio.create_task(account_loop_guest(u, p))
                bot_state.account_workers[u] = task
                started += 1
        return started

    bot_state.refresh_callbacks["on_account_added"] = on_account_added_handler
    bot_state.refresh_callbacks["on_refresh_account"] = on_refresh_account_handler
    bot_state.refresh_callbacks["on_reload_accounts"] = on_reload_accounts_handler

    accounts = selected_accounts[:MAX_ACCOUNTS]

    if not accounts:
        print_warning("No accounts selected.")
        return


    for acc in accounts:
        if "token" in acc and acc["token"]:
            placeholder = f"tok_{acc['token'][:10]}"
            if placeholder in bot_state.deleted_uids:
                continue
            bot_state.register_account(uid=placeholder, nickname=f"Token_{acc['token'][:10]}", region=DEFAULT_REGION, level=1, exp=0)
            bot_state.update_status(placeholder, "CONNECTING")
        elif "uid" in acc and "password" in acc and acc["uid"]:
            u = str(acc["uid"])
            if u in bot_state.deleted_uids:
                continue
            bot_state.register_account(uid=u, nickname=f"Player_{u[:6]}", region=DEFAULT_REGION, level=1, exp=0)
            bot_state.update_status(u, "CONNECTING")


    BATCH_SIZE = int(os.environ.get("LAUNCH_BATCH_SIZE", "20"))
    BATCH_DELAY = float(os.environ.get("LAUNCH_BATCH_DELAY", "1.2"))

    total = len(accounts)
    print_info(f"Starting {total} account(s)...")

    for batch_start in range(0, total, BATCH_SIZE):
        batch = accounts[batch_start: batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        for acc in batch:
            if "token" in acc and acc["token"]:
                t_key = acc["token"][:10]
                placeholder = f"tok_{t_key}"

                if placeholder in bot_state.deleted_uids or t_key in bot_state.deleted_uids:
                    continue
                t = asyncio.create_task(account_loop_token(acc["token"]))
                bot_state.account_workers[t_key] = t
            elif "uid" in acc and "password" in acc and acc["uid"]:
                u = str(acc["uid"])

                if u in bot_state.deleted_uids:
                    continue
                t = asyncio.create_task(account_loop_guest(u, acc["password"]))
                bot_state.account_workers[u] = t

            await asyncio.sleep(random.uniform(0.05, 0.25))

        pass


        if batch_start + BATCH_SIZE < total:
            await asyncio.sleep(BATCH_DELAY)
            await _launch_gate()

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print_warning("\n[STOP] Shutting down all sessions...")
        for t in list(bot_state.account_workers.values()):
            t.cancel()
        if bot_state.account_workers:
            await asyncio.gather(*bot_state.account_workers.values(), return_exceptions=True)
        await stop_termux_monitor()
        try:
            await client.aclose()
        except Exception:
            pass
        print_success("All sessions cleanly closed.")


import ast as _ast

def bre_encode_protobuf(data):
    if not isinstance(data, dict):
        return b""
    out = bytearray()
    try:
        for k, v in data.items():
            f = int(k)
            items = v if isinstance(v, list) else [v]
            for item in items:
                if isinstance(item, str) and len(item) >= 3 and item[0] == 'b' and item[1] in ("'", '"') and item[-1] == item[1]:
                    try:
                        _tv = _ast.literal_eval(item)
                        if isinstance(_tv, (bytes, bytearray)):
                            item = bytes(_tv)
                    except Exception:
                        pass
                if isinstance(item, bool):
                    item = int(item)
                if isinstance(item, int):
                    tv = (f << 3) | 0
                    if tv < 0:
                        tv &= 0xFFFFFFFFFFFFFFFF
                    while True:
                        tb = tv & 0x7F; tv >>= 7
                        out.append(tb | (0x80 if tv else 0))
                        if not tv: break
                    uv = item
                    if uv < 0:
                        uv &= 0xFFFFFFFFFFFFFFFF
                    while True:
                        ub = uv & 0x7F; uv >>= 7
                        out.append(ub | (0x80 if uv else 0))
                        if not uv: break
                elif isinstance(item, str):
                    bb = item.encode('utf-8')
                    tv = (f << 3) | 2
                    while True:
                        tb = tv & 0x7F; tv >>= 7
                        out.append(tb | (0x80 if tv else 0))
                        if not tv: break
                    lv = len(bb)
                    while True:
                        lb = lv & 0x7F; lv >>= 7
                        out.append(lb | (0x80 if lv else 0))
                        if not lv: break
                    out.extend(bb)
                elif isinstance(item, (bytes, bytearray)):
                    tv = (f << 3) | 2
                    while True:
                        tb = tv & 0x7F; tv >>= 7
                        out.append(tb | (0x80 if tv else 0))
                        if not tv: break
                    lv = len(item)
                    while True:
                        lb = lv & 0x7F; lv >>= 7
                        out.append(lb | (0x80 if lv else 0))
                        if not lv: break
                    out.extend(item)
                elif isinstance(item, dict):
                    sub = bytearray()
                    for sk, sv in item.items():
                        sf = int(sk)
                        svs = sv if isinstance(sv, list) else [sv]
                        for si in svs:
                            if isinstance(si, str) and len(si) >= 3 and si[0] == 'b' and si[1] in ("'", '"') and si[-1] == si[1]:
                                try:
                                    _tv2 = _ast.literal_eval(si)
                                    if isinstance(_tv2, (bytes, bytearray)):
                                        si = bytes(_tv2)
                                except Exception:
                                    pass
                            if isinstance(si, bool): si = int(si)
                            if isinstance(si, int):
                                tv = (sf << 3) | 0
                                if tv < 0: tv &= 0xFFFFFFFFFFFFFFFF
                                while True:
                                    tb = tv & 0x7F; tv >>= 7
                                    sub.append(tb | (0x80 if tv else 0))
                                    if not tv: break
                                uv = si
                                if uv < 0: uv &= 0xFFFFFFFFFFFFFFFF
                                while True:
                                    ub = uv & 0x7F; uv >>= 7
                                    sub.append(ub | (0x80 if uv else 0))
                                    if not uv: break
                            elif isinstance(si, str):
                                bb = si.encode('utf-8')
                                tv = (sf << 3) | 2
                                while True:
                                    tb = tv & 0x7F; tv >>= 7
                                    sub.append(tb | (0x80 if tv else 0))
                                    if not tv: break
                                lv = len(bb)
                                while True:
                                    lb = lv & 0x7F; lv >>= 7
                                    sub.append(lb | (0x80 if lv else 0))
                                    if not lv: break
                                sub.extend(bb)
                            elif isinstance(si, (bytes, bytearray)):
                                tv = (sf << 3) | 2
                                while True:
                                    tb = tv & 0x7F; tv >>= 7
                                    sub.append(tb | (0x80 if tv else 0))
                                    if not tv: break
                                lv = len(si)
                                while True:
                                    lb = lv & 0x7F; lv >>= 7
                                    sub.append(lb | (0x80 if lv else 0))
                                    if not lv: break
                                sub.extend(si)
                            elif isinstance(si, dict):
                                subsub = bre_encode_protobuf(si)
                                tv = (sf << 3) | 2
                                while True:
                                    tb = tv & 0x7F; tv >>= 7
                                    sub.append(tb | (0x80 if tv else 0))
                                    if not tv: break
                                lv = len(subsub)
                                while True:
                                    lb = lv & 0x7F; lv >>= 7
                                    sub.append(lb | (0x80 if lv else 0))
                                    if not lv: break
                                sub.extend(subsub)
                    tv = (f << 3) | 2
                    while True:
                        tb = tv & 0x7F; tv >>= 7
                        out.append(tb | (0x80 if tv else 0))
                        if not tv: break
                    lv = len(sub)
                    while True:
                        lb = lv & 0x7F; lv >>= 7
                        out.append(lb | (0x80 if lv else 0))
                        if not lv: break
                    out.extend(sub)
    except Exception:
        return bytes(out)
    return bytes(out)


def bre_create_packet(Pk, N, K, V):
    from Crypto.Cipher import AES as _AES
    from Crypto.Util.Padding import pad as _pad
    Pk = bytes.fromhex(Pk) if isinstance(Pk, str) else bytes(Pk)
    N  = bytes.fromhex(N)  if isinstance(N, str)  else bytes(N)
    K  = bytes.fromhex(K)  if isinstance(K, str) else K
    V  = bytes.fromhex(V)  if isinstance(V, str) else V
    ct = _AES.new(K, _AES.MODE_CBC, V).encrypt(_pad(Pk, 16))
    return N + len(ct).to_bytes(4, 'big') + ct


def bre_exit_team(account_id, key, iv):
    fields = {1: 7, 2: {1: int(account_id)}}
    return bre_create_packet(bre_encode_protobuf(fields), '0524', key, iv)


def bre_create_br_evolution_team(region, game_version, key, iv):
    fields = {
        "1": 1,
        "2": {
            "2": "b'\\x01'",
            "3": 85,
            "4": 3,
            "5": "en",
            "8": [
                {
                    "1": "IDC2",
                    "2": 18446744073709551615,
                    "3": str(region)
                },
                {
                    "1": "IDC3",
                    "2": 18446744073709551615,
                    "3": str(region)
                },
                {
                    "1": "IDC1",
                    "2": 18446744073709551615,
                    "3": str(region)
                }
            ],
            "9": 1,
            "10": "b'\\x01\\x07\\t\\n\\x0b\\x12\\x19\\x1a \"\\')'",
            "11": 1,
            "13": 1,
            "14": {
                "1": "088B823F75E72D5402010100000000010013000200100002E0D167540F000000467625141100000138e848dc0e748c3f6aa04d81000000ff00000100cacfa16d",
                "2": 43,
                "3": "b'\\x7fX[V\\x17\\t\\x07E\\x05TS\\x0c\\x01\\x05\\x00W\\x0fQZ\\x0e\\x0c\\x08\\x0c\\x04\\x01\\x0b\\x03\\t\\x00\\x00\\x02\\x00\\x00\\r\\x04T\\x07P\\x0bR\\x1a\\x06\\x04M|\\\\GGM\\x10\\x07\\x15\\x06\\x1c\\x12\\x08\\x03K\\x03Rvu\\nXm@c~}aMNuoY|u\\x0fw\\x0ff\\x7f\\n~t\\n\\x16\\x00E{ZaU~TtMze~vQRp~NRyT^[[VRgO\\n\\x1aQZ_M\\x1ed{\\x19\\x7fGI\\x06IfkfG\\x03GLRC[@f\\x12K\\x7fKC\\x01nv\\x7f^Dk\\x1eA\\x0b\\t\\x1b\\x06\\nK_pM`bCP{MUmP]\\tzcG\\x1f\\x0ebrCoCwvn\\x0e\\x17\\x00KM\\x00e\\x1eRbe\\x1cn\\x01sJm_BRC~XM\\x13y\\x12XMBx\\x0c\\x14\\x01H|v[s_IAS|_\\x1b\\x01Ex`\\x06|N@Ot^_P\\x0ebx\\x0c\\x1a\\x08K\\x04^mB_DLe{B}uS`\\x01HFyboCzkxY}l\\n\\x16\\x00\\x01M\\x7fI~rDn\\x07P\\t\\x01Y\\x05{N^qTCah`BQbfT\\x07\\x04\\x14\\x06Mq\\x1a\\x06Wgk\\x06}A\\x1e\\x03_\\x05_e\\x01YB\\x12X\\x06v\\x12w\\x1cd\\x04\\x05\\x14\\rKSH~P@ymy\\x06hn\\x00Chhq\\x06gLT\\x16DO\\x06~tr\\x0e\\x17\\n\\x07EFr\\x07\\x12P^Az_{K\\\\`eKCB\\x02\\x03@y\\nB\\\\~Np\\n\\x10\\x01DXOD\\\\y__\\\\rFZBT\\x1cavQATyeO\\x18\\x00coF\\x04'",
                "4": "yZ_\\",
                "6": 12,
                "7": "b'\\x17\\x00\\x0c}Xe`\\x15\\x1d'",
                "8": str(game_version),
                "9": 3,
                "10": 1,
                "11": "b'\\x03bbSQ69FffPcRAdV2KyoVlQ9A7D7vFHmHKb+QNR0P/OO+dIWBLA2zgQ/uYw1wb+KaMHwTzDL3iPt6H4TxAcHoCElgUNTMsp3rZtE9gezzw1PNViHXclpnXQlCPDZ3GFSpeIRpgS8mvmxG43RdPmibe0G2rrBloBr5YcPCr0GS0+3ULe0NW+EuhsEQagECjD7xiI7aCEGPto2/PkH3GuN45xRK9MZqvKFc24TucPL29R3m3RxCKAlJbBrRvo5uB760+9WQluDjY+m9ceHe/BSnT/su4VL7WXu1ULm42mKklTnn7qW/Gn7s3thxaRXgIxBU8ISjyW/spYFw6qhmAT2hfyC/2VMW4gjy6bL9xlXfjPwU7dHg8eK8Cof7hvFX5mG69FTyuzR0EpNcryjFeVioRFC7eZVrcNfMCIlIfVZTAUGHgGzeqHQ5ERSbd2Ju/iC6Xg25txeleLLgEeaFerrfX4dGsocndVish4TV+CjLzJEbFnU27CdcsxEYpaXhe063SP//LbUb/ensKPwkMbhVW7oaBWi7df0zgDk69xNsB2NsRmLDHzS9duo1RAdwUugs4VB0YrNOJRnMzdxonQWprBwWgun/eKC8OaiPBXzhRDbooFxHWzwAKloePHUIrwd89xizOks='"
            },
            "19": 329,
            "21": "b'7OR\\x19'",
            "24": [
                {
                    "1": 3,
                    "2": 394
                },
                {
                    "1": 4,
                    "2": 391
                },
                {
                    "1": 5,
                    "2": 192
                },
                {
                    "1": 29,
                    "2": 204
                },
                {
                    "1": 22,
                    "2": 120
                },
                {
                    "1": 14,
                    "2": 175
                },
                "b'\\x08\\x15'"
            ],
            "27": "a_6534489873065906362",
            "30": "https://dl-sg-production.freefiremobile.com/D5A73B5E05EC88BB7181_7104104913_107_1767710870_0"
        }
    }

    return bre_create_packet(bre_encode_protobuf(fields), '0524', key, iv)


def bre_start_br_evolution(account_id, region, key, iv):
    fields = {
        1: 9,
        2: {
            1: int(account_id),
            7: [
                {"1": "IDC2", "2": 18446744073709551615, "3": str(region)},
                {"1": "IDC3", "2": 18446744073709551615, "3": str(region)},
                {"1": "IDC1", "2": 18446744073709551615, "3": str(region)},
            ]
        }
    }
    return bre_create_packet(bre_encode_protobuf(fields), '0524', key, iv)


def bre_build_auth_packet(account_id, jwt_token, timestamp, key, iv):
    from Crypto.Cipher import AES as _AES
    from Crypto.Util.Padding import pad as _pad
    K = bytes.fromhex(key) if isinstance(key, str) else key
    V = bytes.fromhex(iv)  if isinstance(iv, str)  else iv
    ct = _AES.new(K, _AES.MODE_CBC, V).encrypt(_pad(jwt_token.encode(), 16))
    header = bytes.fromhex('7724') + int(account_id).to_bytes(8, 'big') + int(timestamp).to_bytes(4, 'big')
    header += b'\x00\x00\x00\x00'
    header += len(ct).to_bytes(4, 'big')
    return (header + ct).hex()


async def functional_br_evo(addrs, auth_packet, key, iv, account_id,
                             region, game_version, max_reconnects=15, stronghold=""):
    reconnects = 0
    ip, port = addrs.split(":")
    uid_str = str(account_id)
    loop_count = 0
    write_lock = asyncio.Lock()
    debug = os.environ.get("BR_EVO_DEBUG", "0") == "1"

    def _blog(msg, level="info"):
        bot_state.log(f"[EVO] {msg}", level)

    while True:
        writer = None
        hb_task = None
        evo_task = None
        udp_task = None
        current_match = None
        seen_ops = set()

        async def _run_udp(decoded):
            st = {}
            try:
                await udp_engine.play_br_evo_match(
                    decoded, region, game_version, stronghold, log=_blog, state=st
                )
            finally:
                if st.get("joined"):
                    try:
                        bot_state.increment_match(uid_str)
                    except Exception:
                        pass
                    try:
                        asyncio.create_task(refresh_account_profile(uid_str))
                    except Exception:
                        pass

        try:
            resolved_ip = await resolve_host_cloudflare(ip)
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(resolved_ip, int(port)),
                    timeout=8.0
                )
            except asyncio.TimeoutError:
                raise ConnectionError("BR-EVO TCP timeout")

            raw_sock = writer.get_extra_info('socket')
            if raw_sock:
                optimize_tcp_socket(raw_sock)


            writer.write(bytes.fromhex(auth_packet))
            await asyncio.wait_for(writer.drain(), timeout=5.0)


            try:
                writer.write(await send_keep_alive(region))
                await asyncio.wait_for(writer.drain(), timeout=3)
            except Exception:
                pass

            bot_state.log(f"[EVO] Connected | UID: {uid_str} | {resolved_ip}:{port}", "success")
            bot_state.update_status(uid_str, "ONLINE")
            reconnects = 0


            async def heartbeat():
                while True:
                    await asyncio.sleep(10)
                    try:
                        if writer and not writer.is_closing():
                            async with write_lock:
                                writer.write(await send_keep_alive(region))
                                await writer.drain()
                    except Exception:
                        break


            async def br_evo_loop():
                nonlocal loop_count
                await asyncio.sleep(3)
                while True:
                    try:
                        if writer is None or writer.is_closing():
                            break
                        while not bot_state.global_running:
                            bot_state.update_status(uid_str, 'PAUSED', 0)
                            await asyncio.sleep(1.0)
                        loop_count += 1


                        bot_state.update_status(uid_str, "SEARCHING", 0)


                        pkt = bre_exit_team(account_id, key, iv)
                        async with write_lock:
                            writer.write(pkt)
                            await writer.drain()
                        await asyncio.sleep(2)

                        if writer is None or writer.is_closing():
                            break


                        pkt = bre_create_br_evolution_team(region, game_version, key, iv)
                        async with write_lock:
                            writer.write(pkt)
                            await writer.drain()
                        await asyncio.sleep(2)

                        if writer is None or writer.is_closing():
                            break


                        pkt = bre_start_br_evolution(account_id, region, key, iv)
                        async with write_lock:
                            writer.write(pkt)
                            await writer.drain()
                        bot_state.log(f"[EVO #{loop_count}] Match START sent | UID: {uid_str}", "info")
                        bot_state.set_info(uid_str, f"EVO #{loop_count} — ম্যাচ খুঁজছে...")


                        task_before = udp_task


                        search_deadline = asyncio.get_running_loop().time() + 120
                        matched_task = None
                        while asyncio.get_running_loop().time() < search_deadline:
                            cur = udp_task
                            if cur is not None and cur is not task_before and not cur.done():
                                matched_task = cur
                                break
                            await asyncio.sleep(0.5)

                        if matched_task is None:

                            bot_state.log(
                                f"[EVO #{loop_count}] MATCH FOUND নেই (120s) — retry | UID: {uid_str}",
                                "info"
                            )
                            await asyncio.sleep(3)
                            continue


                        bot_state.update_status(uid_str, "IN_MATCH", 1)
                        bot_state.set_info(uid_str, f"EVO Match #{loop_count} চলছে...")


                        try:
                            await asyncio.wait({matched_task}, timeout=700)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            pass


                        bot_state.update_status(uid_str, "ONLINE", 0)
                        try:
                            asyncio.create_task(refresh_account_profile(uid_str))
                        except Exception:
                            pass


                        await asyncio.sleep(random.uniform(5, 10))

                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        bot_state.log(f"[EVO] Loop error: {e}", "error")
                        await asyncio.sleep(5)

            hb_task  = asyncio.create_task(heartbeat())
            evo_task = asyncio.create_task(br_evo_loop())


            idle_count = 0
            buf = b""
            while True:
                try:
                    data = await asyncio.wait_for(reader.read(65536), timeout=30.0)
                    if not data:
                        raise ConnectionError("Server closed connection")
                    idle_count = 0
                except asyncio.TimeoutError:
                    idle_count += 1
                    if idle_count > 10:
                        raise ConnectionError("BR-EVO gateway idle timeout")
                    continue

                buf += data
                frames, buf, desync = udp_engine.split_tcp_frames(buf)
                if desync:
                    bot_state.log(f"[EVO] TCP stream desync — reconnecting | UID: {uid_str}", "warning")
                    raise ConnectionError("TCP stream desync")

                for frame in frames:
                    if debug:
                        op = frame[:2].hex()
                        if op not in seen_ops:
                            seen_ops.add(op)
                            bot_state.log(f"[EVO][DEBUG] new opcode {op} len={len(frame)} | UID: {uid_str}", "info")

                    if frame[:2] != b"\x03\x00":
                        continue
                    decoded = udp_engine.decode_tcp_packet(frame)
                    if not udp_engine.is_match_found(decoded):
                        continue

                    match_id = decoded[5].get(1)
                    if udp_task and not udp_task.done():
                        if match_id == current_match:
                            continue
                        udp_task.cancel()
                    current_match = match_id
                    bot_state.log(
                        f"[EVO #{loop_count}] MATCH FOUND → UDP join | UID: {uid_str} | match {match_id} | {decoded[5].get(2)}",
                        "success"
                    )
                    bot_state.set_info(uid_str, f"EVO ম্যাচ পাওয়া গেছে, জয়েন হচ্ছে... (#{loop_count})")
                    udp_task = asyncio.create_task(_run_udp(decoded))

        except asyncio.CancelledError:
            bot_state.log(f"[EVO] Worker cancelled | UID: {uid_str}", "warning")
            for t in (udp_task, evo_task, hb_task):
                if t and not t.done():
                    t.cancel()
            if writer:
                try: writer.close(); await writer.wait_closed()
                except Exception: pass
            raise

        except Exception as e:
            _es = str(e)
            if "BAN_DETECTED" in _es:
                bot_state.log(f"[EVO] BAN detected — stopping UID: {uid_str}", "error")
                return
            bot_state.log(f"[EVO] Error ({uid_str}): {_es[:120]}", "warning")
        finally:
            for t in (udp_task, evo_task, hb_task):
                if t and not t.done():
                    t.cancel()
                if t:
                    try: await t
                    except (asyncio.CancelledError, Exception): pass
            if writer:
                try: writer.close(); await writer.wait_closed()
                except Exception: pass
            try:
                bot_state.update_status(uid_str, "CONNECTING")
            except Exception:
                pass

        reconnects += 1
        if reconnects > max_reconnects:
            reconnects = 0
        delay = min(2 ** (reconnects - 1), 15) + random.uniform(0, 2.0)
        bot_state.log(f"[EVO] Reconnecting in {delay:.1f}s... | UID: {uid_str}", "info")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        print_warning("\nProgram stopped by user.")
