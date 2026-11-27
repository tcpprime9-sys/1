import asyncio
import glob
import json
import os
import re
import shutil
import sys
import time
from collections import deque
from typing import Dict, Any, Optional, List

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_FILE_PATH = os.path.join(BASE_DIR, "accounts.json")
DELETED_ACCOUNTS_FILE = os.path.join(BASE_DIR, "delete_accounts.json")
SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")
UI_REFRESH_SECONDS = float(os.environ.get("UI_REFRESH_SECONDS", "0.5"))
MAX_ACCOUNTS = 2
VALID_MODES = ("LONE_WOLF", "BR", "LW_HS", "BR_EVO")

EXP_LIST = [
    0, 48, 202, 544, 1012, 1844, 2792, 3800, 4870, 6004,
    7192, 8448, 9760, 11140, 12566, 14060, 15610, 17224, 18902, 20632,
    22424, 24278, 26192, 28166, 30200, 32294, 34448, 37804, 41274, 44870,
    48582, 53394, 58566, 64096, 69994, 76260, 83506, 91128, 99322, 108092,
    120144, 133266, 147472, 162760, 179126, 196572, 215368, 235316, 257010, 279860,
    304056, 348318, 394982, 444044, 495508, 549364, 633756, 721744, 813336, 908522,
    1041438, 1180352, 1325266, 1476184, 1634300, 1840946, 2056594, 2281242, 2514880, 2757530,
    3059506, 3372284, 3699456, 4041030, 4397002, 4829104, 5282204, 5756304, 6251404, 6767502,
    7381324, 8043154, 8752982, 9510808, 10316638, 11277190, 12291748, 13360304, 14482858, 15659418,
    17026708, 18453990, 19941280, 21488570, 23095858, 24763138, 26490428, 28277708, 30124996, 32032284,
]

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


class C:
    R = "\033[0m"
    B = "\033[1m"
    D = "\033[90m"
    RED = "\033[91m"
    GRN = "\033[92m"
    YEL = "\033[93m"
    BLU = "\033[94m"
    MAG = "\033[95m"
    CYN = "\033[96m"
    WHT = "\033[97m"


def _vlen(text: str) -> int:
    return len(_ANSI_RE.sub("", text))


def _trim_ansi(text: str, max_w: int) -> str:
    if _vlen(text) <= max_w:
        return text
    plain = _ANSI_RE.sub("", text)
    if max_w <= 1:
        return plain[:max_w]
    return plain[: max_w - 1] + "…"


def _box_width() -> int:
    try:
        cols = shutil.get_terminal_size((64, 24)).columns
    except Exception:
        cols = 64
    return max(58, min(78, cols - 2))


def _box_top(w: int, title: str = "") -> str:
    if title:
        t = f" {title} "
        rem = max(0, w - 2 - len(t) - 2)
        return f"{C.D}╭──{C.R}{C.CYN}{C.B}{t}{C.R}{C.D}{'─' * rem}╮{C.R}"
    return f"{C.D}╭{'─' * (w - 2)}╮{C.R}"


def _box_mid(w: int, title: str = "") -> str:
    if title:
        t = f" {title} "
        rem = max(0, w - 2 - len(t) - 2)
        return f"{C.D}├──{C.R}{C.WHT}{t}{C.R}{C.D}{'─' * rem}┤{C.R}"
    return f"{C.D}├{'─' * (w - 2)}┤{C.R}"


def _box_bot(w: int) -> str:
    return f"{C.D}╰{'─' * (w - 2)}╯{C.R}"


def _box_row(w: int, content: str) -> str:
    inner = w - 4
    vis = _vlen(content)
    if vis > inner:
        content = _trim_ansi(content, inner)
        vis = _vlen(content)
    pad = max(0, inner - vis)
    return f"{C.D}│{C.R} {content}{' ' * pad} {C.D}│{C.R}"


def clear_screen():
    try:
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()
    except Exception:
        pass


def get_all_account_files_cli() -> List[str]:
    files = sorted(glob.glob(os.path.join(BASE_DIR, "accounts*.json")))
    if not files:
        files = [ACCOUNTS_FILE_PATH]
    return files


def level_info(level: int, exp: int):
    lv = max(1, int(level or 1))
    cur_exp = max(0, int(exp or 0))
    if lv >= len(EXP_LIST):
        return 100, 0, cur_exp, cur_exp
    cur_floor = EXP_LIST[lv - 1]
    nxt_target = EXP_LIST[lv]
    if nxt_target <= cur_floor:
        return 100, 0, cur_floor, nxt_target
    pct = max(0, min(100, int((cur_exp - cur_floor) * 100 / (nxt_target - cur_floor))))
    need = max(0, nxt_target - cur_exp)
    return pct, need, cur_floor, nxt_target


def progress_bar(pct: int, width: int = 22) -> str:
    filled = int(round(width * pct / 100))
    return f"{C.GRN}{'━' * filled}{C.D}{'─' * (width - filled)}{C.R}"


def mode_label(mode: str) -> str:
    m = str(mode or "LONE_WOLF").upper()
    if m == "LONE_WOLF":
        return "Lone Wolf"
    if m == "BR":
        return "Battle Royale"
    if m == "LW_HS":
        return "LW Headshot"
    if m == "BR_EVO":
        return "BR-EVO"
    return m


_LOOP_LAG = 0.0
_BG_TASKS: List[asyncio.Task] = []


def get_loop_lag() -> float:
    return _LOOP_LAG


async def _loop_lag_monitor():
    global _LOOP_LAG
    while True:
        t = time.monotonic()
        await asyncio.sleep(0.5)
        lag = max(0.0, time.monotonic() - t - 0.5)
        _LOOP_LAG = lag if lag > _LOOP_LAG else (_LOOP_LAG * 0.6 + lag * 0.4)


class AppState:
    def __init__(self):
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: deque = deque(maxlen=150)
        self.exp_history: deque = deque(maxlen=30)
        self.total_matches: int = 0
        self.total_gained_exp: int = 0
        self.last_exp_gain: int = 0
        self.last_exp_time: str = ""
        self.start_time: float = time.time()
        self.account_workers: Dict[str, asyncio.Task] = {}
        self.refresh_callbacks: Dict[str, Any] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}
        self.guest_alias: Dict[str, str] = {}
        self.match_types: Dict[str, str] = {}
        self.global_mode: str = os.environ.get("FF_MODE", "LONE_WOLF").upper()
        if self.global_mode not in VALID_MODES:
            self.global_mode = "LONE_WOLF"
        self.global_running: bool = True
        self.live_ui_active: bool = False

        self.exp_limit: int = int(os.environ.get("EXP_LIMIT", "0"))
        self.level_limit: int = int(os.environ.get("LEVEL_LIMIT", "0"))
        self.default_region: str = os.environ.get("FF_REGION", "BD").upper()
        self.auto_delete_queue: set = set()
        self.level_remove_queue: set = set()

        self.deleted_uids_file = os.path.join(BASE_DIR, "deleted_uids.json")
        self.deleted_uids: set = set()
        self._load_deleted_uids()
        self._load_settings()

        self.login_fail_counts: Dict[str, int] = {}
        self.MAX_LOGIN_FAILS: int = int(os.environ.get("MAX_LOGIN_FAILS", "5"))

    def _load_settings(self):
        try:
            if os.path.exists(SETTINGS_FILE):
                with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                    d = json.load(f)
                if isinstance(d, dict):
                    if os.environ.get("EXP_LIMIT") is None:
                        self.exp_limit = max(0, int(d.get("exp_limit", 0) or 0))
                    if os.environ.get("LEVEL_LIMIT") is None:
                        self.level_limit = max(0, int(d.get("level_limit", 0) or 0))
                    if os.environ.get("FF_REGION") is None and d.get("default_region"):
                        self.default_region = str(d["default_region"]).upper()
                    if os.environ.get("FF_MODE") is None and d.get("global_mode") in VALID_MODES:
                        self.global_mode = str(d["global_mode"]).upper()
        except Exception:
            pass

    def save_settings(self):
        try:
            tmp = SETTINGS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "exp_limit": self.exp_limit,
                        "level_limit": self.level_limit,
                        "default_region": self.default_region,
                        "global_mode": self.global_mode,
                    },
                    f,
                    indent=2,
                )
            os.replace(tmp, SETTINGS_FILE)
        except Exception:
            pass

    def _load_deleted_uids(self):
        try:
            if os.path.exists(self.deleted_uids_file):
                with open(self.deleted_uids_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    self.deleted_uids = set(str(u) for u in data)
        except Exception:
            self.deleted_uids = set()

    def _save_deleted_uids(self):
        try:
            tmp = self.deleted_uids_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(list(self.deleted_uids), f, ensure_ascii=False)
            os.replace(tmp, self.deleted_uids_file)
        except Exception:
            pass

    def _json_ids(self, uid_str: str) -> set:
        ids = {uid_str}
        for guest, real in self.guest_alias.items():
            if real == uid_str:
                ids.add(guest)
        return ids

    def _remove_from_json(self, uid_str: str, reason: Optional[str] = None):
        short_key = uid_str.replace("tok_", "")[:10]
        ids = self._json_ids(uid_str)
        archived = []
        for accounts_file in get_all_account_files_cli():
            if not os.path.exists(accounts_file):
                continue
            try:
                with open(accounts_file, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                if not isinstance(existing, list):
                    continue
                new_list, removed = [], []
                for acc in existing:
                    hit = isinstance(acc, dict) and (
                        str(acc.get("uid", "")).strip() in ids
                        or str(acc.get("token", ""))[:10] == short_key
                    )
                    (removed if hit else new_list).append(acc)
                if removed:
                    tmp = accounts_file + ".tmp"
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(new_list, f, indent=2, ensure_ascii=False)
                    os.replace(tmp, accounts_file)
                    archived.extend(removed)
            except Exception:
                pass
        if reason and archived:
            self._archive_deleted(archived, uid_str, reason)

    def _archive_deleted(self, archived: list, acc_key: str, reason: str):
        try:
            items = []
            if os.path.exists(DELETED_ACCOUNTS_FILE):
                with open(DELETED_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                    d = json.load(f)
                if isinstance(d, list):
                    items = d
            for acc in archived:
                rec = dict(acc)
                rec["_account_id"] = acc_key
                rec["_reason"] = reason
                rec["_removed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                items.append(rec)
            tmp = DELETED_ACCOUNTS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(items, f, indent=2, ensure_ascii=False)
            os.replace(tmp, DELETED_ACCOUNTS_FILE)
        except Exception:
            pass

    def save_account_to_json(self, entry: Dict[str, str]):
        existing = []
        if os.path.exists(ACCOUNTS_FILE_PATH):
            try:
                with open(ACCOUNTS_FILE_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    existing = data
            except Exception:
                existing = []

        if "uid" in entry:
            u = str(entry["uid"]).strip()
            existing = [a for a in existing if str(a.get("uid", "")).strip() != u]
            existing.append({"uid": u, "password": str(entry["password"]).strip()})
            self.deleted_uids.discard(u)
            self._save_deleted_uids()
        elif "token" in entry:
            t = str(entry["token"]).strip()
            existing = [a for a in existing if str(a.get("token", "")).strip() != t]
            existing.append({"token": t})
            self.deleted_uids.discard(t[:10])
            self.deleted_uids.discard(f"tok_{t[:10]}")
            self._save_deleted_uids()

        existing = existing[-MAX_ACCOUNTS:]
        tmp = ACCOUNTS_FILE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        os.replace(tmp, ACCOUNTS_FILE_PATH)

    def get_match_type(self, uid: str) -> str:
        return self.match_types.get(str(uid), self.global_mode)

    def log(self, message: str, level: str = "info", uid: Optional[str] = None, print_to_console: bool = True):
        ts = time.strftime("%H:%M:%S")
        clean_msg = str(message).replace("\n", " ").strip()
        self.logs.append({"time": ts, "level": level, "message": clean_msg, "uid": uid})
        if self.live_ui_active or not print_to_console:
            return
        colors = {
            "success": C.GRN,
            "error": C.RED,
            "warning": C.YEL,
            "info": C.CYN,
        }
        c = colors.get(level, C.WHT)
        tag = f"{C.D}[{uid}]{C.R} " if uid else ""
        try:
            print(f"{C.D}{ts}{C.R} {tag}{c}{clean_msg}{C.R}", flush=True)
        except Exception:
            safe_msg = clean_msg.encode("ascii", errors="replace").decode("ascii")
            print(f"{C.D}{ts}{C.R} {tag}{c}{safe_msg}{C.R}", flush=True)

    def register_account(self, uid: str, nickname: str, region: str, level: int, exp: int, likes: int = 0):
        uid_str = str(uid)
        if uid_str in self.deleted_uids:
            return
        if uid_str not in self.accounts:
            if len(self.accounts) >= MAX_ACCOUNTS:
                return
            self.accounts[uid_str] = {
                "uid": uid_str,
                "nickname": nickname or f"Player_{uid_str[:6]}",
                "region": region or self.default_region,
                "initial_level": level or 1,
                "level": level or 1,
                "initial_exp": exp,
                "current_exp": exp,
                "gained_exp": 0,
                "last_gain": 0,
                "last_gain_time": "",
                "likes": likes or 0,
                "status": "ONLINE",
                "matches_played": 0,
                "active_matches": 0,
                "last_updated": time.strftime("%H:%M:%S"),
                "match_type": self.global_mode,
            }
        else:
            acc = self.accounts[uid_str]
            if nickname:
                acc["nickname"] = nickname
            if region:
                acc["region"] = region
            if level:
                acc["level"] = level
            acc["current_exp"] = exp
            acc["gained_exp"] = max(0, exp - acc["initial_exp"])
            acc["likes"] = likes
            acc["status"] = "ONLINE"
            acc["last_updated"] = time.strftime("%H:%M:%S")
        self.recalc_totals()
        self.check_level_limit(uid_str)

    def update_exp(self, uid: str, current_exp: int, level: Optional[int] = None):
        uid_str = str(uid)
        if uid_str in self.deleted_uids or uid_str not in self.accounts:
            return
        acc = self.accounts[uid_str]
        old_exp = int(acc.get("current_exp", 0) or 0)
        old_lv = int(acc.get("level", 1) or 1)
        acc["current_exp"] = current_exp
        if level is not None and level > 0:
            acc["level"] = level
        new_lv = int(acc.get("level", 1) or 1)
        acc["gained_exp"] = max(0, current_exp - acc["initial_exp"])
        ts = time.strftime("%H:%M:%S")
        acc["last_updated"] = ts

        diff = current_exp - old_exp
        if diff > 0:
            acc["last_gain"] = diff
            acc["last_gain_time"] = ts
            self.last_exp_gain = diff
            self.last_exp_time = ts
            self.exp_history.append({
                "time": ts,
                "nick": acc["nickname"],
                "diff": diff,
                "total": acc["gained_exp"],
                "level": new_lv,
                "leveled_up": new_lv > old_lv,
            })

        if new_lv > old_lv:
            self.log(
                f"LEVEL UP {acc['nickname']} Lv.{old_lv} -> Lv.{new_lv} (+{max(0, diff):,} EXP)",
                "success",
                uid_str,
            )
        elif diff > 0:
            self.log(
                f"+{diff:,} EXP · {acc['nickname']} (Total +{acc['gained_exp']:,} · Lv.{new_lv})",
                "success",
                uid_str,
            )

        self.recalc_totals()
        self.check_level_limit(uid_str)

        if self.exp_limit > 0 and acc["gained_exp"] >= self.exp_limit and uid_str not in self.auto_delete_queue:
            self.auto_delete_queue.add(uid_str)
            self._remove_account_now(uid_str, f"EXP target reached ({self.exp_limit:,})", "complete")

    def check_level_limit(self, uid_str: str):
        if self.level_limit <= 0 or uid_str in self.level_remove_queue or uid_str in self.deleted_uids:
            return
        acc = self.accounts.get(uid_str)
        if not acc:
            return
        if int(acc.get("level", 1) or 1) >= self.level_limit:
            self.level_remove_queue.add(uid_str)
            self._remove_account_now(uid_str, f"Level target reached (Lv.{self.level_limit})", "complete")

    def _remove_account_now(self, uid_str: str, msg: str, reason: str):
        self.deleted_uids.add(uid_str)
        self._save_deleted_uids()
        self.accounts.pop(uid_str, None)
        self.recalc_totals()
        self._remove_from_json(uid_str, reason)
        self.log(f"{msg} - stopped", "warning", uid_str)
        short_key = uid_str.replace("tok_", "")[:10]
        for key in (uid_str, short_key):
            task = self.account_workers.pop(key, None)
            if task and not task.done():
                task.cancel()

    def update_status(self, uid: str, status: str, active_matches: Optional[int] = None):
        uid_str = str(uid)
        if uid_str in self.deleted_uids or uid_str not in self.accounts:
            return
        self.accounts[uid_str]["status"] = status
        if status in ("ONLINE", "IN_MATCH", "SEARCHING"):
            self.accounts[uid_str].pop("last_error", None)
        if active_matches is not None:
            self.accounts[uid_str]["active_matches"] = active_matches
        self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")

    def set_error(self, uid: str, message: str):
        uid_str = str(uid)
        if uid_str in self.accounts and uid_str not in self.deleted_uids:
            self.accounts[uid_str]["last_error"] = message[:200]

    def set_info(self, uid: str, message: str):
        uid_str = str(uid)
        if uid_str in self.accounts and uid_str not in self.deleted_uids:
            self.accounts[uid_str]["last_info"] = message[:200]

    def record_login_fail(self, uid: str):
        uid_str = str(uid)
        if uid_str in self.deleted_uids:
            return
        self.login_fail_counts[uid_str] = self.login_fail_counts.get(uid_str, 0) + 1
        cnt = self.login_fail_counts[uid_str]
        self.log(f"Login failed ({cnt}/{self.MAX_LOGIN_FAILS})", "warning", uid_str)
        if cnt >= self.MAX_LOGIN_FAILS:
            self.login_fail_counts.pop(uid_str, None)
            self._remove_account_now(uid_str, f"{cnt}x login failed", "error")

    def reset_login_fail(self, uid: str):
        self.login_fail_counts.pop(str(uid), None)

    def increment_match(self, uid: str):
        uid_str = str(uid)
        self.total_matches += 1
        if uid_str in self.accounts:
            self.accounts[uid_str]["matches_played"] += 1
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")
            self.log(
                f"{self.accounts[uid_str]['nickname']} completed match #{self.accounts[uid_str]['matches_played']}",
                "success",
                uid_str,
            )

    def recalc_totals(self):
        self.total_gained_exp = sum(
            int(acc.get("gained_exp", 0) or 0) for acc in self.accounts.values()
        )


app_state = AppState()


def interactive_termux_menu(saved_accounts: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    saved_accounts = saved_accounts[:MAX_ACCOUNTS]
    if not sys.stdin.isatty():
        return saved_accounts

    while True:
        clear_screen()
        w = _box_width()
        exp_txt = f"{app_state.exp_limit:,}" if app_state.exp_limit else "OFF"
        lv_txt = f"Lv.{app_state.level_limit}" if app_state.level_limit else "OFF"

        rows = [
            _box_top(w, "Session"),
            _box_row(
                w,
                f"{C.CYN}Mode{C.R} {C.WHT}{C.B}{mode_label(app_state.global_mode)}{C.R}   "
                f"{C.D}│{C.R}   {C.CYN}Region{C.R} {C.WHT}{C.B}{app_state.default_region}{C.R}   "
                f"{C.D}│{C.R}   {C.CYN}Limit{C.R} {C.WHT}1-{MAX_ACCOUNTS} ID{C.R}",
            ),
            _box_row(
                w,
                f"{C.CYN}Saved{C.R} {C.WHT}{len(saved_accounts)}/{MAX_ACCOUNTS}{C.R}   "
                f"{C.D}│{C.R}   {C.CYN}Level Stop{C.R} {C.GRN}{lv_txt}{C.R}   "
                f"{C.D}│{C.R}   {C.CYN}EXP Stop{C.R} {C.GRN}{exp_txt}{C.R}",
            ),
            _box_mid(w, "Menu"),
            _box_row(w, f"  {C.GRN}{C.B}1{C.R}   Login with UID & Password {C.D}(1-{MAX_ACCOUNTS} IDs){C.R}"),
            _box_row(w, f"  {C.CYN}{C.B}2{C.R}   Login with Access Token {C.D}(1-{MAX_ACCOUNTS} IDs){C.R}"),
            _box_row(w, f"  {C.YEL}{C.B}3{C.R}   Start Saved Accounts {C.D}({len(saved_accounts)}/{MAX_ACCOUNTS}){C.R}"),
            _box_row(w, f"  {C.MAG}{C.B}4{C.R}   Import .txt File {C.D}(max {MAX_ACCOUNTS} IDs){C.R}"),
            _box_row(w, f"  {C.BLU}{C.B}5{C.R}   Mode & Target Settings"),
            _box_row(w, f"  {C.RED}{C.B}0{C.R}   Exit"),
            _box_bot(w),
        ]
        print("\n".join(rows) + "\n")

        try:
            choice = input(f"  {C.CYN}❯{C.R} Select [1]: ").strip() or "1"
        except (EOFError, KeyboardInterrupt):
            return None

        if choice == "0":
            return None

        if choice == "1":
            selected: List[Dict[str, Any]] = []
            while len(selected) < MAX_ACCOUNTS:
                idx = len(selected) + 1
                print(f"\n  {C.D}── Account {idx}/{MAX_ACCOUNTS} ──{C.R}")
                uid_raw = input("  UID (or UID:Pass): ").strip()
                if not uid_raw:
                    if selected:
                        break
                    print(f"  {C.RED}UID required.{C.R}")
                    continue
                if ":" in uid_raw:
                    u, p = uid_raw.split(":", 1)
                    u, p = u.strip(), p.strip()
                else:
                    u = uid_raw
                    p = input("  Password: ").strip()
                if not u or not p:
                    print(f"  {C.RED}Both UID and Password required.{C.R}")
                    continue
                if any(a.get("uid") == u for a in selected):
                    print(f"  {C.YEL}Already added.{C.R}")
                    continue
                acc = {"uid": u, "password": p}
                app_state.save_account_to_json(acc)
                selected.append(acc)
                print(f"  {C.GRN}✓ Added {u} ({len(selected)}/{MAX_ACCOUNTS}){C.R}")
                if len(selected) >= MAX_ACCOUNTS:
                    break
                more = input("  Add 2nd account? (y/N): ").strip().lower()
                if more != "y":
                    break
            if selected:
                return selected[:MAX_ACCOUNTS]

        elif choice == "2":
            selected = []
            while len(selected) < MAX_ACCOUNTS:
                idx = len(selected) + 1
                print(f"\n  {C.D}── Token {idx}/{MAX_ACCOUNTS} ──{C.R}")
                tok = input("  Access Token: ").strip()
                if not tok:
                    if selected:
                        break
                    print(f"  {C.RED}Token required.{C.R}")
                    continue
                acc = {"token": tok}
                app_state.save_account_to_json(acc)
                selected.append(acc)
                print(f"  {C.GRN}✓ Added token ({len(selected)}/{MAX_ACCOUNTS}){C.R}")
                if len(selected) >= MAX_ACCOUNTS:
                    break
                more = input("  Add 2nd token? (y/N): ").strip().lower()
                if more != "y":
                    break
            if selected:
                return selected[:MAX_ACCOUNTS]

        elif choice == "3":
            if not saved_accounts:
                print(f"\n  {C.RED}No saved accounts found.{C.R}")
                time.sleep(1.0)
                continue
            app_state.deleted_uids.clear()
            app_state._save_deleted_uids()
            return saved_accounts[:MAX_ACCOUNTS]

        elif choice == "4":
            path = input("\n  .txt file path: ").strip()
            if not path or not os.path.exists(path):
                print(f"  {C.RED}File not found.{C.R}")
                time.sleep(1.0)
                continue
            imported = []
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if len(imported) >= MAX_ACCOUNTS:
                        break
                    line = line.strip()
                    if not line or line.startswith("#") or ":" not in line:
                        continue
                    u, p = line.split(":", 1)
                    u, p = u.strip(), p.strip()
                    if u and p:
                        acc = {"uid": u, "password": p}
                        app_state.save_account_to_json(acc)
                        imported.append(acc)
            if not imported:
                print(f"  {C.RED}No valid UID:Password found.{C.R}")
                time.sleep(1.0)
                continue
            return imported[:MAX_ACCOUNTS]

        elif choice == "5":
            print(f"\n  {C.D}── Mode & Settings ──{C.R}")
            print(f"  {C.GRN}1{C.R} Lone Wolf   {C.CYN}2{C.R} Battle Royale   {C.YEL}3{C.R} LW Headshot   {C.MAG}4{C.R} BR-EVO")
            m_in = input(f"  Select Mode [{mode_label(app_state.global_mode)}]: ").strip()
            mode_map = {"1": "LONE_WOLF", "2": "BR", "3": "LW_HS", "4": "BR_EVO"}
            if m_in in mode_map:
                app_state.global_mode = mode_map[m_in]
            reg = input(f"  Region (BD/IND/SG) [{app_state.default_region}]: ").strip().upper()
            if reg in ("IND", "BD", "SG", "ID", "TH", "VN", "BR", "US"):
                app_state.default_region = reg
            lv_in = input(f"  Target Level (0=OFF) [{app_state.level_limit}]: ").strip()
            if lv_in.isdigit():
                app_state.level_limit = int(lv_in)
            exp_in = input(f"  Target EXP (0=OFF) [{app_state.exp_limit}]: ").strip()
            if exp_in.isdigit():
                app_state.exp_limit = int(exp_in)
            app_state.save_settings()
            print(f"  {C.GRN}✓ Saved.{C.R}")
            time.sleep(0.7)


_SPIN_IDX = 0


def _render_live_frame() -> str:
    global _SPIN_IDX
    _SPIN_IDX = (_SPIN_IDX + 1) % 10
    w = _box_width()
    accs = list(app_state.accounts.values())[:MAX_ACCOUNTS]
    elapsed = max(1.0, time.time() - app_state.start_time)
    uptime_s = int(elapsed)
    uh, um, us = uptime_s // 3600, (uptime_s % 3600) // 60, uptime_s % 60
    up_str = f"{uh}h {um:02d}m {us:02d}s" if uh else f"{um:02d}m {us:02d}s"
    sp = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")[_SPIN_IDX]

    exp_per_hr = int(app_state.total_gained_exp * 3600.0 / max(60.0, elapsed)) if app_state.total_gained_exp > 0 else 0
    last_drop_txt = (
        f"{C.GRN}{C.B}+{app_state.last_exp_gain:,}{C.R}"
        if app_state.last_exp_gain > 0
        else f"{C.D}Waiting...{C.R}"
    )

    lines = [
        _box_top(w, f"{sp} {mode_label(app_state.global_mode)}"),
        _box_row(
            w,
            f"{C.WHT}IDs:{C.R} {C.CYN}{C.B}{len(accs)}/{MAX_ACCOUNTS}{C.R}   "
            f"{C.D}│{C.R}   {C.WHT}Matches:{C.R} {C.CYN}{C.B}{app_state.total_matches}{C.R}   "
            f"{C.D}│{C.R}   {C.WHT}Uptime:{C.R} {C.WHT}{up_str}{C.R}",
        ),
        _box_row(
            w,
            f"{C.WHT}Total EXP:{C.R} {C.GRN}{C.B}+{app_state.total_gained_exp:,}{C.R}  "
            f"{C.D}│{C.R}  {C.WHT}Rate:{C.R} {C.YEL}{exp_per_hr:,}/hr{C.R}  "
            f"{C.D}│{C.R}  {C.WHT}Last:{C.R} {last_drop_txt}",
        ),
    ]

    lines.append(_box_mid(w, "Accounts"))
    lines.append(_box_row(w, ""))

    if not accs:
        lines.append(_box_row(w, f"{C.D}Connecting account...{C.R}"))
    else:
        bar_w = max(16, min(26, w - 34))
        for idx, a in enumerate(accs):
            st = str(a.get("status", "ONLINE"))
            st_up = st.upper()
            if "MATCH" in st_up:
                dot = f"{C.CYN}▶{C.R}"
                st_c = C.CYN
            elif st_up == "ONLINE":
                dot = f"{C.GRN}●{C.R}"
                st_c = C.GRN
            elif "SEARCH" in st_up or "CONNECT" in st_up:
                dot = f"{C.YEL}◌{C.R}"
                st_c = C.YEL
            else:
                dot = f"{C.RED}●{C.R}"
                st_c = C.RED

            lv = int(a.get("level", 1) or 1)
            init_lv = int(a.get("initial_level", lv) or lv)
            lv_up = max(0, lv - init_lv)
            lv_badge = f" {C.GRN}{C.B}↑+{lv_up}{C.R}" if lv_up > 0 else ""

            cur_exp = int(a.get("current_exp", 0) or 0)
            gained = int(a.get("gained_exp", 0) or 0)
            last_g = int(a.get("last_gain", 0) or 0)
            last_t = str(a.get("last_gain_time", ""))
            reg = str(a.get("region", app_state.default_region)).upper()
            pct, need, _, nxt_target = level_info(lv, cur_exp)
            bar = progress_bar(pct, bar_w)
            nick = str(a.get("nickname", ""))[:16]
            matches = int(a.get("matches_played", 0) or 0)

            lines.append(
                _box_row(
                    w,
                    f"{dot} {C.WHT}{C.B}{nick}{C.R} {C.D}({a.get('uid')}) [{reg}]{C.R}   {st_c}{C.B}{st}{C.R}",
                )
            )
            lines.append(
                _box_row(
                    w,
                    f"  Lv.{C.YEL}{C.B}{lv:<2}{C.R}{lv_badge}  [{bar}]  {C.WHT}{C.B}{pct:>3}%{C.R}",
                )
            )
            lines.append(
                _box_row(
                    w,
                    f"  {C.D}EXP:{C.R} {C.WHT}{cur_exp:,}{C.R} {C.D}/{C.R} {C.D}{nxt_target:,}{C.R}   "
                    f"{C.D}│{C.R}   {C.D}Need:{C.R} {C.YEL}{need:,}{C.R}",
                )
            )
            last_str = f"{C.GRN}{C.B}+{last_g:,}{C.R}" if last_g > 0 else f"{C.D}+0{C.R}"
            lines.append(
                _box_row(
                    w,
                    f"  {C.D}Gained:{C.R} {C.GRN}{C.B}+{gained:,} EXP{C.R}  "
                    f"{C.D}│{C.R}  {C.D}Last:{C.R} {last_str}  "
                    f"{C.D}│{C.R}  {C.D}Matches:{C.R} {C.CYN}{C.B}{matches}{C.R}",
                )
            )
            note = a.get("last_error") or a.get("last_info") or ""
            if note:
                note_c = C.RED if a.get("last_error") else C.D
                lines.append(_box_row(w, f"  {note_c}{str(note)}{C.R}"))
            if idx < len(accs) - 1:
                lines.append(_box_row(w, f"{C.D}{'·' * (w - 6)}{C.R}"))

    lines.append(_box_row(w, ""))
    lines.append(_box_mid(w, "EXP Drops & Activity"))
    recent_exp = list(app_state.exp_history)[-3:]
    if recent_exp:
        for e in recent_exp:
            tag = f"{C.YEL}{C.B}LV UP!{C.R} " if e.get("leveled_up") else ""
            lines.append(
                _box_row(
                    w,
                    f"{C.D}{e['time']}{C.R}  {tag}{C.GRN}{C.B}+{e['diff']:,} EXP{C.R}  "
                    f"{C.WHT}{str(e['nick'])[:12]}{C.R} {C.D}(Lv.{e['level']} · Total +{e['total']:,}){C.R}",
                )
            )
        lines.append(_box_row(w, f"{C.D}{'─' * (w - 6)}{C.R}"))

    recent = list(app_state.logs)[-5:]
    if not recent:
        lines.append(_box_row(w, f"{C.D}Starting engine...{C.R}"))
    else:
        lvl_map = {"success": C.GRN, "error": C.RED, "warning": C.YEL, "info": C.CYN}
        for entry in recent:
            lc = lvl_map.get(entry.get("level", "info"), C.WHT)
            msg = str(entry.get("message", ""))
            lines.append(_box_row(w, f"{C.D}{entry.get('time', '')}{C.R}  {lc}{msg}{C.R}"))

    lines.append(_box_bot(w))
    lines.append(f"  {C.D}Smooth Live View · Ctrl+C to stop{C.R}")

    return "\033[H" + "\n".join(ln + "\033[K" for ln in lines) + "\033[J"


async def _terminal_status_printer():
    try:
        sys.stdout.write("\033[?1049h\033[?25l\033[2J\033[H")
        sys.stdout.flush()
    except Exception:
        pass
    try:
        while True:
            try:
                sys.stdout.write(_render_live_frame())
                sys.stdout.flush()
            except Exception:
                pass
            await asyncio.sleep(max(0.3, UI_REFRESH_SECONDS))
    finally:
        try:
            sys.stdout.write("\033[?25h\033[?1049l")
            sys.stdout.flush()
        except Exception:
            pass


async def start_termux_monitor():
    app_state.live_ui_active = True
    _BG_TASKS.append(asyncio.ensure_future(_loop_lag_monitor()))
    _BG_TASKS.append(asyncio.ensure_future(_terminal_status_printer()))


async def stop_termux_monitor():
    app_state.live_ui_active = False
    for t in _BG_TASKS:
        if not t.done():
            t.cancel()
    if _BG_TASKS:
        await asyncio.gather(*_BG_TASKS, return_exceptions=True)
    _BG_TASKS.clear()
    try:
        sys.stdout.write("\033[?25h\033[?1049l")
        sys.stdout.flush()
    except Exception:
        pass
