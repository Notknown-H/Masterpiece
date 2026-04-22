#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║     AL QALB QUANTUM ULTIMATE — FULLY INTEGRATED AUTONOMOUS INTELLIGENCE     ║
║     Quantum Neural DNA + Al Qalb Economic Core + Trinitas Bridge            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  INTEGRATION OF:                                                            ║
║  • Quantum‑Sparse Neural DNA (405B‑scale cognitive engine)                 ║
║  • 3‑Level Hierarchical Memory (L1 RAM / L2 SSD / L3 FAISS+SQLite)         ║
║  • Trinitas Bridge (OpenClaw + OpenManus built‑in)                          ║
║  • Al Qalb Adaptive Economic Intelligence                                   ║
║  • U‑HEE Engine + CodeGenerator + MetaGovernor 2.0                          ║
║  • AutoTrader (Solana/Binance) + Meme Scanner + Whale Tracker               ║
║  • LocalBrain (Knowledge Graph + RAG Hybrid)                                ║
║  • Full Telegram Bot Interface                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# =============================================================================
# SECTION 0: IMPORTS & AUTO‑INSTALL (MERGED FROM BOTH SYSTEMS)
# =============================================================================
import os, sys, subprocess, asyncio, json, time, random, re, struct
import hashlib, secrets, sqlite3, tempfile, logging, copy, pickle
import warnings, uuid, base64, heapq, statistics, gzip, urllib.request
import urllib.parse, queue, threading, shutil, importlib.util, inspect
import textwrap, ast, itertools, io, zipfile, socket, ssl
from collections import defaultdict, deque, Counter, OrderedDict
from typing import Dict, List, Optional, Tuple, Any, Union, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from pathlib import Path
from datetime import datetime, timedelta
from functools import lru_cache, partial, wraps

# --- Environment & external dependency handling ---
from dotenv import load_dotenv
load_dotenv()

def _install(pkg: str):
    subprocess.call([sys.executable, "-m", "pip", "install", pkg, "--quiet"])

_required_packages = {
    # QND specific
    "numpy": "numpy", "aiohttp": "aiohttp", "requests": "requests",
    "beautifulsoup4": "bs4", "faiss-cpu": "faiss", "sentence-transformers": "sentence_transformers",
    "rich": "rich", "prompt-toolkit": "prompt_toolkit", "psutil": "psutil",
    "duckduckgo-search": "duckduckgo_search", "googlesearch-python": "googlesearch",
    "torch": "torch", "selenium": "selenium", "nltk": "nltk", "pygments": "pygments",
    "cryptography": "cryptography", "async-timeout": "async_timeout",
    # Al Qalb specific
    "aiosqlite": "aiosqlite", "pandas": "pandas", "pyTelegramBotAPI": "telebot",
    "httpx": "httpx", "feedparser": "feedparser", "solana": "solana", "solders": "solders",
    "playwright": "playwright", "python-binance": "binance", "scipy": "scipy",
    "ta": "ta", "ccxt": "ccxt", "scikit-learn": "sklearn", "lxml": "lxml",
}
for pip_name, imp_name in _required_packages.items():
    try:
        __import__(imp_name)
    except ImportError:
        _install(pip_name)

# Playwright browser install (silent)
try:
    subprocess.call([sys.executable, "-m", "playwright", "install", "chromium"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
except Exception:
    pass

# =============================================================================
# IMPORTS AFTER AUTO‑INSTALL (merged)
# =============================================================================
import numpy as np
import aiohttp
import requests
from bs4 import BeautifulSoup
try:
    import faiss
except ImportError:
    faiss = None
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMER_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMER_AVAILABLE = False
    SentenceTransformer = None
try:
    import rich
    from rich.console import Console
    from rich.panel import Panel
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import FileHistory
    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False
try:
    import psutil
except ImportError:
    psutil = None
try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None
try:
    from googlesearch import search as google_search
except ImportError:
    google_search = None
try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
try:
    import nltk
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
    from nltk.tokenize import sent_tokenize
    NLTK_AVAILABLE = True
except:
    NLTK_AVAILABLE = False
try:
    import cryptography
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except:
    CRYPTO_AVAILABLE = False
try:
    import async_timeout
except ImportError:
    async_timeout = None

# Al Qalb imports
import aiosqlite
import pandas as pd
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot import types
from solana.rpc.api import Client as SolClient
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import transfer, TransferParams
from solders.message import MessageV0
from solders.transaction import VersionedTransaction
from solders.commitment_config import CommitmentLevel
from solders.rpc.config import RpcSendTransactionConfig as TxOpts
try:
    from solana.rpc.core import RPCException
except ImportError:
    RPCException = Exception
from playwright.async_api import async_playwright
from binance.client import Client as BinanceClient
from binance.exceptions import BinanceAPIException, BinanceOrderException
from scipy import stats
from scipy.signal import find_peaks
try:
    import ta as ta_lib
except ImportError:
    ta_lib = None
try:
    import ccxt
except ImportError:
    ccxt = None
try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import LinearRegression
except ImportError:
    StandardScaler = None
    LinearRegression = None

# =============================================================================
# GLOBAL CONFIGURATION (MERGED)
# =============================================================================
# QND paths
DB_FILE_QND = "qsdna_brain.db"
SSD_STORAGE_DIR = "ssd_experts"
STRATEGIES_DIR = "strategies"
RESEARCH_CACHE_DIR = "research_cache"
VECTOR_INDEX_PATH = "knowledge_vectors.faiss"
os.makedirs(SSD_STORAGE_DIR, exist_ok=True)
os.makedirs(STRATEGIES_DIR, exist_ok=True)
os.makedirs(RESEARCH_CACHE_DIR, exist_ok=True)

# Al Qalb config
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not set in .env")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
SECOND_ADMIN_ID = int(os.getenv("SECOND_ADMIN_ID", "0")) or OWNER_ID
PRIVATE_KEY_BOT = os.getenv("PRIVATE_KEY_BOT", "")
WALLET_TARGET = os.getenv("WALLET_TARGET", "")
BOT_WALLET_ADDRESS = os.getenv("SOLANA_WALLET_ADDRESS", "")
SOLANA_RPC = os.getenv("SOLANA_RPC", "https://api.mainnet-beta.solana.com")
MIN_RESERVE_SOL = float(os.getenv("MIN_RESERVE_SOL", "0.01"))
BIG_TX_THRESHOLD = float(os.getenv("BIG_TX_THRESHOLD", "0.5"))
MODEL_NAME = os.getenv("MODEL_NAME", "qwen2.5-coder:3b")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")
BINANCE_TESTNET = os.getenv("BINANCE_TESTNET", "true").lower() == "true"
ARKHAM_API_KEY = os.getenv("ARKHAM_API_KEY", "")
DUNE_API_KEY = os.getenv("DUNE_API_KEY", "")
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
TIER_1_MAX_USD = float(os.getenv("TIER_1_MAX_USD", "100"))
TIER_2_MAX_USD = float(os.getenv("TIER_2_MAX_USD", "500"))
MIN_DATA_ROWS = int(os.getenv("MIN_DATA_ROWS", "20"))
EVOLUTION_CYCLES = int(os.getenv("EVOLUTION_CYCLES", "50"))
DB_FILE_ALQALB = os.getenv("DB_FILE", "data/alqalb.db")
LOCAL_BRAIN_FILE = os.getenv("LOCAL_BRAIN_FILE", "local_brain.json")

# QND constants
TOTAL_PARAMS_TARGET = 405e9
RAM_LIMIT_GB = 2.0
SPARSITY_GENES = 0.01
SPARSITY_WEIGHTS = 0.90
QUANTIZATION_BITS = 2

random.seed(42)
np.random.seed(42)
if TORCH_AVAILABLE:
    torch.manual_seed(42)

device = torch.device("cuda" if TORCH_AVAILABLE and torch.cuda.is_available() else "cpu")
print(f"🚀 Using device: {device}")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("QND_ALQALB")
log = logger

# Al Qalb directories
os.makedirs("data", exist_ok=True)
os.makedirs("backups", exist_ok=True)
os.makedirs("strategies", exist_ok=True)

MONEY_LAYER_ENABLED = True
KILL_SWITCH = False

# =============================================================================
# LAYER 1: EVENT BUS (UNIFIED CENTRAL NERVOUS SYSTEM)
# =============================================================================
class EventBus:
    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = defaultdict(list)
        self._history: deque = deque(maxlen=5000)
        self._lock = threading.RLock()
        self._async_handlers: Dict[str, List[Callable]] = defaultdict(list)
        self._event_queue = queue.Queue()
        self._worker_thread = threading.Thread(target=self._process_async, daemon=True)
        self._worker_thread.start()

    def subscribe(self, event: str, handler: Callable, async_mode: bool = False):
        with self._lock:
            if async_mode:
                self._async_handlers[event].append(handler)
            else:
                self._handlers[event].append(handler)

    def publish(self, event: str, data: dict, async_mode: bool = False):
        event_packet = {"event": event, "data": data, "ts": time.time()}
        self._history.append(event_packet)
        if async_mode:
            self._event_queue.put(event_packet)
        else:
            self._dispatch(event_packet)

    async def publish_async(self, event: str, data: dict):
        event_packet = {"event": event, "data": data, "ts": time.time()}
        self._history.append(event_packet)
        for handler in list(self._handlers.get(event, [])) + list(self._async_handlers.get(event, [])):
            try:
                result = handler(data)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"EventBus async publish error [{event}]: {e}")

    def _dispatch(self, packet: dict):
        event = packet["event"]
        data = packet["data"]
        for handler in self._handlers.get(event, []):
            try:
                result = handler(data)
                if asyncio.iscoroutine(result):
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.ensure_future(result)
                        else:
                            loop.run_until_complete(result)
                    except RuntimeError:
                        asyncio.run(result)
            except Exception as e:
                logger.error(f"EventBus error [{event}]: {e}")

    def _process_async(self):
        while True:
            packet = self._event_queue.get()
            event = packet["event"]
            data = packet["data"]
            for handler in self._async_handlers.get(event, []):
                try:
                    handler(data)
                except Exception as e:
                    logger.error(f"EventBus async error [{event}]: {e}")

    def get_history(self, event: str = None, limit: int = 50) -> List[dict]:
        with self._lock:
            h = list(self._history)
            if event:
                h = [x for x in h if x["event"] == event]
            return h[-limit:]

event_bus = EventBus()

# =============================================================================
# LAYER 1.5: UTILITY FUNCTIONS (Al Qalb)
# =============================================================================
def kill_check() -> bool:
    return KILL_SWITCH

def rate_check(uid: int, _cache: Dict = {}, _max=10, _window=60) -> bool:
    now = time.time()
    if uid not in _cache:
        _cache[uid] = deque()
    q = _cache[uid]
    while q and now - q[0] > _window:
        q.popleft()
    if len(q) >= _max:
        return False
    q.append(now)
    return True

def is_owner(m) -> bool:
    return m.from_user.id in (OWNER_ID, SECOND_ADMIN_ID)

def audit_log(kind: str, content: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] [{kind.upper()}] {content}\n"
    try:
        with open("data/audit.log", "a", encoding="utf-8") as f:
            f.write(entry)
    except Exception:
        pass
    log.info(f"AUDIT [{kind}]: {content[:120]}")

def generate_license_key(user_id: int) -> str:
    secret = os.getenv("LICENSE_SECRET", "alqalb_secret_2024")
    raw = f"{user_id}:{secret}:{int(time.time() // 86400)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32].upper()

# =============================================================================
# LAYER 2: 2‑BIT QUANTIZATION UTIL (QND)
# =============================================================================
class QuantizationUtil:
    @staticmethod
    def quantize_2bit_np(data: np.ndarray) -> Tuple[np.ndarray, float, float]:
        scale = data.max() - data.min()
        zero_point = data.min()
        if scale == 0:
            return np.zeros((data.size + 3) // 4, dtype=np.uint8), 0.0, 0.0
        normalized_data = (data - zero_point) / scale
        quantized_data = np.round(normalized_data * 3).astype(np.int8)
        packed_data = np.zeros((quantized_data.size + 3) // 4, dtype=np.uint8)
        flat_quantized = quantized_data.flatten()
        for i in range(0, flat_quantized.size, 4):
            val = 0
            for j in range(4):
                if i + j < flat_quantized.size:
                    val |= (int(flat_quantized[i+j]) & 0x03) << (j * 2)
            packed_data[i // 4] = val
        return packed_data, scale, zero_point

    @staticmethod
    def dequantize_2bit_np(packed_data: np.ndarray, original_shape: Tuple[int, ...],
                           scale: float, zero_point: float) -> np.ndarray:
        unpacked_data = np.zeros(original_shape).flatten()
        for i in range(packed_data.size):
            byte_val = packed_data[i]
            for j in range(4):
                idx = i * 4 + j
                if idx < unpacked_data.size:
                    unpacked_data[idx] = (byte_val >> (j * 2)) & 0x03
        dequantized_normalized = unpacked_data / 3.0
        dequantized_data = dequantized_normalized * scale + zero_point
        return dequantized_data.reshape(original_shape).astype(np.float16)

    @staticmethod
    def quantize_2bit_torch(tensor: "torch.Tensor") -> Tuple["torch.Tensor", "torch.Tensor", "torch.Tensor"]:
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch not available")
        tensor = tensor.float()
        scale = tensor.max() - tensor.min()
        zero_point = tensor.min()
        if scale == 0:
            packed = torch.zeros((tensor.numel() + 3) // 4, dtype=torch.uint8, device=tensor.device)
            return packed, torch.tensor(0.0, device=tensor.device), torch.tensor(0.0, device=tensor.device)
        normalized = (tensor - zero_point) / scale
        quantized = torch.round(normalized * 3).to(torch.int8)
        flat = quantized.flatten()
        packed = torch.zeros((flat.numel() + 3) // 4, dtype=torch.uint8, device=tensor.device)
        for i in range(0, flat.numel(), 4):
            val = 0
            for j in range(4):
                if i + j < flat.numel():
                    val |= (int(flat[i+j].item()) & 0x03) << (j * 2)
            packed[i // 4] = val
        return packed, scale, zero_point

    @staticmethod
    def dequantize_2bit_torch(packed: "torch.Tensor", original_shape: Tuple[int, ...],
                              scale: "torch.Tensor", zero_point: "torch.Tensor") -> "torch.Tensor":
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch not available")
        total_elements = int(np.prod(original_shape))
        unpacked = torch.zeros(total_elements, dtype=torch.float32, device=packed.device)
        for i in range(packed.numel()):
            byte_val = packed[i].item()
            for j in range(4):
                idx = i * 4 + j
                if idx < total_elements:
                    val = (byte_val >> (j * 2)) & 0x03
                    unpacked[idx] = val
        normalized = unpacked / 3.0
        dequantized = normalized * scale + zero_point
        return dequantized.reshape(original_shape).to(torch.float16)

# =============================================================================
# LAYER 3: INDEXED LOCAL BRAIN (SQLite + FAISS) – QND + Al Qalb integration
# =============================================================================
class IndexedLocalBrain:
    def __init__(self, db_path: str = DB_FILE_QND):
        self.db_path = db_path
        self._init_db()
        event_bus.subscribe("knowledge_acquired", self.on_knowledge_acquired)
        event_bus.subscribe("trade_closed", self.learn_from_trade)

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS knowledge (
                    id TEXT PRIMARY KEY,
                    topic TEXT,
                    tags TEXT,
                    statement TEXT,
                    epistemic_type TEXT,
                    confidence REAL,
                    source_url TEXT,
                    source_title TEXT,
                    acquired_at REAL,
                    last_accessed REAL,
                    access_count INTEGER DEFAULT 0,
                    is_verified INTEGER DEFAULT 0,
                    embedding_blob BLOB
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_topic ON knowledge(topic)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_type ON knowledge(epistemic_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_confidence ON knowledge(confidence)")
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS knowledge_fts USING fts5(
                    topic, statement, tags, content='knowledge', content_rowid='rowid'
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS knowledge_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    knowledge_id TEXT,
                    action TEXT,
                    timestamp REAL,
                    details TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vector_index_meta (
                    vector_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    knowledge_id TEXT UNIQUE,
                    faiss_id INTEGER,
                    dimension INTEGER
                )
            """)

    def add_knowledge(self, id: str, topic: str, tags: List[str], statement: str,
                      e_type: str, conf: float = 0.7, source_url: str = None,
                      source_title: str = None, embedding: np.ndarray = None):
        with sqlite3.connect(self.db_path) as conn:
            now = time.time()
            emb_blob = embedding.tobytes() if embedding is not None else None
            conn.execute("""
                INSERT OR REPLACE INTO knowledge 
                (id, topic, tags, statement, epistemic_type, confidence, 
                 source_url, source_title, acquired_at, embedding_blob)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (id, topic, json.dumps(tags), statement, e_type, conf,
                  source_url, source_title, now, emb_blob))
            conn.execute("""
                INSERT INTO knowledge_ledger (knowledge_id, action, timestamp, details)
                VALUES (?, 'acquired', ?, ?)
            """, (id, now, json.dumps({"source": source_url})))
        event_bus.publish("knowledge_added", {"id": id, "topic": topic, "confidence": conf})

    def query_by_topic(self, topic: str, limit: int = 10) -> List[tuple]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("""
                SELECT * FROM knowledge WHERE topic = ? 
                ORDER BY confidence DESC, acquired_at DESC LIMIT ?
            """, (topic, limit))
            return cur.fetchall()

    def query_by_tag(self, tag: str, limit: int = 10) -> List[tuple]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("""
                SELECT * FROM knowledge WHERE tags LIKE ? 
                ORDER BY confidence DESC LIMIT ?
            """, (f'%"{tag}"%', limit))
            return cur.fetchall()

    def search_fulltext(self, query: str, limit: int = 10) -> List[tuple]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("""
                SELECT k.* FROM knowledge k
                JOIN knowledge_fts f ON k.rowid = f.rowid
                WHERE knowledge_fts MATCH ?
                ORDER BY rank LIMIT ?
            """, (query, limit))
            return cur.fetchall()

    def answer_from_brain(self, query: str, use_fts: bool = True) -> Optional[str]:
        if use_fts:
            nodes = self.search_fulltext(query, limit=5)
        else:
            nodes = self.query_by_topic(query, limit=5)
        if not nodes:
            return None
        answer = "🧠 [Local Brain Knowledge]\n\n"
        for node in nodes[:3]:
            answer += f"- [{node[4]}] {node[3][:200]}\n"
        return answer

    def learn_from_trade(self, data: dict):
        profit = data.get("profit_pct", 0)
        mint = data.get("mint", "unknown")
        reason = data.get("reason", "unknown")
        if profit > 10:
            self.add_knowledge(f"win_{mint}_{int(time.time())}", "trading", ["win", reason[:20]],
                              f"Profit {profit:.1f}% from {mint} because {reason}", "decision_rule",
                              min(0.95, profit/100))
        elif profit < -5:
            self.add_knowledge(f"loss_{mint}_{int(time.time())}", "trading", ["loss", reason[:20]],
                              f"Loss {profit:.1f}% from {mint} because {reason} — AVOID", "failure_mode", 0.8)

    def on_knowledge_acquired(self, data: dict):
        pass

    def get_stats(self) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT COUNT(*) FROM knowledge")
            total = cur.fetchone()[0]
            cur = conn.execute("SELECT topic, COUNT(*) FROM knowledge GROUP BY topic")
            topics = dict(cur.fetchall())
            return {"total_nodes": total, "topics": topics}

    def mark_verified(self, knowledge_id: str, verified: bool = True):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE knowledge SET is_verified = ? WHERE id = ?",
                        (1 if verified else 0, knowledge_id))
            conn.execute("""
                INSERT INTO knowledge_ledger (knowledge_id, action, timestamp, details)
                VALUES (?, 'verified', ?, ?)
            """, (knowledge_id, time.time(), json.dumps({"verified": verified})))

local_brain_qnd = IndexedLocalBrain()

# =============================================================================
# LAYER 4: 3‑LEVEL HIERARCHICAL MEMORY MANAGER (NP & Torch)
# =============================================================================
class ThreeLevelMemoryManagerNP:
    def __init__(self, l1_size_mb: int = 50, l2_size_mb: int = 500, ssd_dir: str = SSD_STORAGE_DIR):
        self.l1_size_bytes = l1_size_mb * 1024 * 1024
        self.l2_size_bytes = l2_size_mb * 1024 * 1024
        self.ssd_dir = ssd_dir
        self.l1 = OrderedDict()
        self.l2 = OrderedDict()
        self.l1_usage = 0
        self.l2_usage = 0
        self.ssd_index: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        os.makedirs(ssd_dir, exist_ok=True)

    def _get_expert_path(self, expert_id: str) -> str:
        return os.path.join(self.ssd_dir, f"expert_{expert_id}.qsd")

    def save_expert_to_ssd(self, expert_id: str, weights: np.ndarray):
        packed, scale, zero_point = QuantizationUtil.quantize_2bit_np(weights)
        filepath = self._get_expert_path(expert_id)
        with open(filepath, 'wb') as f:
            f.write(struct.pack('I' * len(weights.shape), *weights.shape))
            f.write(struct.pack('f', scale))
            f.write(struct.pack('f', zero_point))
            f.write(packed.tobytes())
        with self._lock:
            self.ssd_index[expert_id] = {
                "path": filepath,
                "shape": weights.shape,
                "scale": scale,
                "zero_point": zero_point,
                "size": os.path.getsize(filepath)
            }

    def _load_from_ssd(self, expert_id: str) -> Optional[np.ndarray]:
        with self._lock:
            if expert_id not in self.ssd_index:
                return None
            info = self.ssd_index[expert_id]
        with open(info["path"], 'rb') as f:
            f.read(len(info["shape"]) * struct.calcsize('I'))
            scale = struct.unpack('f', f.read(struct.calcsize('f')))[0]
            zp = struct.unpack('f', f.read(struct.calcsize('f')))[0]
            packed = np.frombuffer(f.read(), dtype=np.uint8)
            return QuantizationUtil.dequantize_2bit_np(packed, info["shape"], scale, zp)

    def _add_to_l1(self, expert_id: str, weights: np.ndarray):
        size = weights.nbytes
        with self._lock:
            while self.l1_usage + size > self.l1_size_bytes and self.l1:
                k, (v, s) = self.l1.popitem(last=False)
                self.l1_usage -= s
                self._add_to_l2(k, v)
            self.l1[expert_id] = (weights, size)
            self.l1_usage += size

    def _add_to_l2(self, expert_id: str, weights: np.ndarray):
        size = weights.nbytes
        with self._lock:
            while self.l2_usage + size > self.l2_size_bytes and self.l2:
                k, (v, s) = self.l2.popitem(last=False)
                self.l2_usage -= s
            self.l2[expert_id] = (weights, size)
            self.l2_usage += size

    def get_expert(self, expert_id: str) -> Optional[np.ndarray]:
        with self._lock:
            if expert_id in self.l1:
                weights, size = self.l1.pop(expert_id)
                self.l1[expert_id] = (weights, size)
                return weights
            if expert_id in self.l2:
                weights, size = self.l2.pop(expert_id)
                self._add_to_l1(expert_id, weights)
                return weights
        weights = self._load_from_ssd(expert_id)
        if weights is not None:
            self._add_to_l1(expert_id, weights)
        return weights

    def get_ram_usage_gb(self) -> float:
        with self._lock:
            return (self.l1_usage + self.l2_usage) / (1024**3)

    def clear_cache(self):
        with self._lock:
            self.l1.clear()
            self.l2.clear()
            self.l1_usage = 0
            self.l2_usage = 0

class ThreeLevelMemoryManagerTorch:
    def __init__(self, l1_size_mb: int = 100, l2_size_mb: int = 1000, ssd_dir: str = SSD_STORAGE_DIR):
        self.l1_size_bytes = l1_size_mb * 1024 * 1024
        self.l2_size_bytes = l2_size_mb * 1024 * 1024
        self.ssd_dir = ssd_dir
        self.l1 = OrderedDict()
        self.l2 = OrderedDict()
        self.l1_usage = 0
        self.l2_usage = 0
        self.ssd_index: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        os.makedirs(ssd_dir, exist_ok=True)

    def _get_expert_path(self, expert_id: str) -> str:
        return os.path.join(self.ssd_dir, f"expert_{expert_id}.pt")

    def save_expert_to_ssd(self, expert_id: str, weights):
        if not TORCH_AVAILABLE:
            return
        packed, scale, zero_point = QuantizationUtil.quantize_2bit_torch(weights)
        filepath = self._get_expert_path(expert_id)
        torch.save({
            'shape': weights.shape,
            'packed': packed.cpu(),
            'scale': scale.cpu(),
            'zero_point': zero_point.cpu()
        }, filepath)
        with self._lock:
            self.ssd_index[expert_id] = {
                "path": filepath,
                "shape": weights.shape,
                "scale": scale.item(),
                "zero_point": zero_point.item(),
                "size": os.path.getsize(filepath)
            }

    def _load_from_ssd(self, expert_id: str):
        with self._lock:
            if expert_id not in self.ssd_index:
                return None
            info = self.ssd_index[expert_id]
        data = torch.load(info["path"], map_location='cpu')
        packed = data['packed'].to(device)
        scale = data['scale'].to(device)
        zero_point = data['zero_point'].to(device)
        return QuantizationUtil.dequantize_2bit_torch(packed, info["shape"], scale, zero_point)

    def _add_to_l1(self, expert_id: str, weights):
        size = weights.element_size() * weights.numel()
        with self._lock:
            while self.l1_usage + size > self.l1_size_bytes and self.l1:
                k, (v, s) = self.l1.popitem(last=False)
                self.l1_usage -= s
                self._add_to_l2(k, v)
            self.l1[expert_id] = (weights, size)
            self.l1_usage += size

    def _add_to_l2(self, expert_id: str, weights):
        size = weights.element_size() * weights.numel()
        with self._lock:
            while self.l2_usage + size > self.l2_size_bytes and self.l2:
                k, (v, s) = self.l2.popitem(last=False)
                self.l2_usage -= s
            self.l2[expert_id] = (weights, size)
            self.l2_usage += size

    def get_expert(self, expert_id: str):
        with self._lock:
            if expert_id in self.l1:
                weights, size = self.l1.pop(expert_id)
                self.l1[expert_id] = (weights, size)
                return weights
            if expert_id in self.l2:
                weights, size = self.l2.pop(expert_id)
                self._add_to_l1(expert_id, weights)
                return weights
        weights = self._load_from_ssd(expert_id)
        if weights is not None:
            self._add_to_l1(expert_id, weights.to(device))
        return weights

    def get_ram_usage_gb(self) -> float:
        with self._lock:
            return (self.l1_usage + self.l2_usage) / (1024**3)

    def clear_cache(self):
        with self._lock:
            self.l1.clear()
            self.l2.clear()
            self.l1_usage = 0
            self.l2_usage = 0

# =============================================================================
# LAYER 5: QUANTUM‑SPARSE NEURAL DNA
# =============================================================================
class QuantumSparseDNA_NP:
    def __init__(self, dna_size: int = 1024, latent_dim: int = 512):
        self.dna = np.random.randn(dna_size, latent_dim).astype(np.float16) * 0.01
        self.dna_size = dna_size
        self.latent_dim = latent_dim
        self.weight_cache = OrderedDict()
        self.cache_limit = 20
        self.alpha = 1.0
        self.beta = 0.0
        self._lock = threading.RLock()

    def _get_input_embedding(self, input_vec: np.ndarray) -> np.ndarray:
        if input_vec.ndim == 1:
            input_vec = input_vec.reshape(1, -1)
        if input_vec.shape[1] != self.latent_dim:
            if input_vec.shape[1] > self.latent_dim:
                input_vec = input_vec[:, :self.latent_dim]
            else:
                pad = np.zeros((input_vec.shape[0], self.latent_dim - input_vec.shape[1]))
                input_vec = np.hstack([input_vec, pad])
        return input_vec.astype(np.float16)

    def select_genes_quantum(self, input_vec: np.ndarray, sparsity: float = SPARSITY_GENES) -> np.ndarray:
        scores = (input_vec @ self.dna.T).astype(np.float16)
        k = max(1, int(self.dna_size * sparsity))
        if scores.ndim > 1:
            indices = np.argpartition(scores[0], -k)[-k:]
        else:
            indices = np.argpartition(scores, -k)[-k:]
        return indices

    def synthesize_weight_sparse(self, active_genes: np.ndarray) -> np.ndarray:
        if len(active_genes) == 0:
            return np.zeros((self.latent_dim, self.latent_dim), dtype=np.float16)
        a = active_genes.mean(axis=0)
        b = active_genes[::-1].mean(axis=0)
        w = np.outer(a, b)
        threshold = np.percentile(np.abs(w), 90)
        w[np.abs(w) < threshold] = 0
        w = np.round(w * 3) / 3
        norm = np.linalg.norm(w) + 1e-8
        w = w / norm
        return (self.alpha * w + self.beta).astype(np.float16)

    def forward(self, input_vec: np.ndarray, layer_id: int = 0) -> Tuple[np.ndarray, np.ndarray]:
        input_emb = self._get_input_embedding(input_vec)
        cache_key = f"layer_{layer_id}_{hash(input_emb.tobytes()) % 10000}"
        with self._lock:
            if cache_key in self.weight_cache:
                self.weight_cache.move_to_end(cache_key)
                return self.weight_cache[cache_key], np.array([])
        gene_indices = self.select_genes_quantum(input_emb)
        active_genes = self.dna[gene_indices]
        w = self.synthesize_weight_sparse(active_genes)
        with self._lock:
            self.weight_cache[cache_key] = w
            if len(self.weight_cache) > self.cache_limit:
                self.weight_cache.popitem(last=False)
        return w, gene_indices

    def inject_knowledge(self, text_embedding: np.ndarray, knowledge_vector: np.ndarray, layer_id: int = -1):
        if layer_id >= 0 and layer_id < self.dna_size:
            self.dna[layer_id] = self.dna[layer_id] * 0.9 + knowledge_vector * 0.1
        else:
            self.dna = self.dna * 0.99 + knowledge_vector.reshape(1, -1) * 0.01

    def clear_cache(self):
        with self._lock:
            self.weight_cache.clear()

if TORCH_AVAILABLE:
    class QuantumSparseDNA_Torch(nn.Module):
        def __init__(self, dna_size: int = 1024, latent_dim: int = 512, sparsity: float = SPARSITY_GENES):
            super().__init__()
            self.dna = nn.Parameter(torch.randn(dna_size, latent_dim, dtype=torch.float16) * 0.01)
            self.dna_size = dna_size
            self.latent_dim = latent_dim
            self.sparsity = sparsity
            self.alpha = nn.Parameter(torch.ones(1, dtype=torch.float16))
            self.beta = nn.Parameter(torch.zeros(1, dtype=torch.float16))
            self.cache = OrderedDict()
            self.cache_limit = 100

        def forward(self, x, layer_id: int = 0):
            if x.dim() == 1:
                x = x.unsqueeze(0)
            cond = x.mean(dim=0)
            cache_key = f"layer_{layer_id}_{hash(cond.cpu().numpy().tobytes()) % 10000}"
            if cache_key in self.cache:
                self.cache.move_to_end(cache_key)
                return self.cache[cache_key], torch.tensor([])
            scores = cond @ self.dna.T
            k = max(1, int(self.dna_size * self.sparsity))
            _, indices = torch.topk(scores, k)
            active_genes = self.dna[indices]
            a = active_genes.mean(dim=0)
            b = active_genes.flip(0).mean(dim=0)
            w = torch.outer(a, b)
            threshold = torch.quantile(torch.abs(w), 0.9)
            w = torch.where(torch.abs(w) < threshold, torch.zeros_like(w), w)
            w = torch.round(w * 3) / 3
            norm = torch.norm(w) + 1e-8
            w = w / norm
            w = self.alpha * w + self.beta
            self.cache[cache_key] = w
            if len(self.cache) > self.cache_limit:
                self.cache.popitem(last=False)
            return w, indices

        def clear_cache(self):
            self.cache.clear()

# =============================================================================
# LAYER 6: ASYNCHRONOUS PREFETCHING SCHEDULER
# =============================================================================
class AsyncPrefetcherNP:
    def __init__(self, memory_manager: ThreeLevelMemoryManagerNP):
        self.mm = memory_manager
        self.queue = deque()
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while self.running:
            if self.queue:
                expert_id = self.queue.popleft()
                if expert_id not in self.mm.l1 and expert_id not in self.mm.l2:
                    self.mm.get_expert(expert_id)
            time.sleep(0.01)

    def schedule(self, expert_ids: List[str]):
        for eid in expert_ids:
            if eid not in self.queue and eid not in self.mm.l1 and eid not in self.mm.l2:
                self.queue.append(eid)

    def stop(self):
        self.running = False
        self.thread.join()

# =============================================================================
# LAYER 7: ADAPTIVE SCORER (QND + Al Qalb unified)
# =============================================================================
class AdaptiveScorer:
    def __init__(self, lookback: int = 50):
        self.history: deque = deque(maxlen=lookback)
        self.win_rate = 0.5
        self.avg_profit = 0.0
        self.credibility_scores: Dict[str, float] = {}
        self.market_baseline: Dict = {}
        event_bus.subscribe("trade_closed", self.update)
        event_bus.subscribe("knowledge_verified", self.update_credibility)

    def update(self, data: dict):
        profit = data.get("profit_pct", 0)
        self.history.append({"profit_pct": profit, "ts": time.time()})
        if self.history:
            wins = sum(1 for h in self.history if h["profit_pct"] > 0)
            self.win_rate = wins / len(self.history)
            self.avg_profit = sum(h["profit_pct"] for h in self.history) / len(self.history)

    def update_credibility(self, data: dict):
        source = data.get("source", "unknown")
        verified = data.get("verified", False)
        current = self.credibility_scores.get(source, 0.5)
        if verified:
            self.credibility_scores[source] = min(1.0, current + 0.1)
        else:
            self.credibility_scores[source] = max(0.0, current - 0.2)

    def update_market_baseline(self, data: Dict):
        for key in ["volume_h24", "market_cap", "price_change_h24"]:
            if key in data and data[key] is not None:
                vals = [h.get(key, 0) for h in list(self.history) if key in h]
                if vals:
                    self.market_baseline[key] = statistics.mean(vals)
                else:
                    self.market_baseline[key] = data[key]

    def _calculate_base(self, data: Dict) -> int:
        score = 0
        mc = data.get("market_cap", 0)
        vol = data.get("volume_h24", 0)
        change = data.get("price_change_h24", 0)
        liquidity = data.get("liquidity_usd", 0)
        buys = data.get("txns_h24_buys", 0)
        sells = data.get("txns_h24_sells", 0)

        if 50_000 <= mc <= 500_000:
            score += 25
        elif 500_000 <= mc <= 2_000_000:
            score += 20
        elif mc > 2_000_000:
            score += 10

        vol_ratio = vol / mc if mc > 0 else 0
        if vol_ratio >= 3:
            score += 25
        elif vol_ratio >= 1:
            score += 15
        elif vol_ratio >= 0.3:
            score += 5

        if 10 <= change <= 100:
            score += 20
        elif change > 100:
            score += 10
        elif change < -20:
            score -= 15

        total_txns = buys + sells
        if total_txns > 0:
            buy_ratio = buys / total_txns
            if buy_ratio > 0.65:
                score += 15
            elif buy_ratio > 0.55:
                score += 8

        if liquidity >= 50_000:
            score += 15
        elif liquidity >= 10_000:
            score += 8

        return score

    def score(self, data: Dict) -> int:
        base = self._calculate_base(data)
        vol = data.get("volume_h24", 0)
        baseline_vol = self.market_baseline.get("volume_h24", vol)
        if baseline_vol > 0 and vol > baseline_vol * 1.5:
            base += 10

        if self.win_rate < 0.4:
            base = int(base * 0.85)
        elif self.win_rate > 0.65:
            base = int(base * 1.05)

        for h in list(self.history)[-5:]:
            if h["profit_pct"] < -10:
                h_mc = h.get("market_cap", 0)
                d_mc = data.get("market_cap", 0)
                if h_mc > 0 and abs(d_mc - h_mc) / h_mc < 0.5:
                    base -= 5

        return max(0, min(100, base))

adaptive_scorer = AdaptiveScorer()

# =============================================================================
# LAYER 8: META GOVERNOR (QND) + RESOURCE MONITOR
# =============================================================================
class MetaGovernorQND:
    def __init__(self, dna_np: QuantumSparseDNA_NP, mm_np: ThreeLevelMemoryManagerNP):
        self.dna = dna_np
        self.mm = mm_np
        self.state = {
            "mode": "exploration",
            "consciousness": 0.5,
            "ideas": 0,
            "battery_level": 100,
            "cpu_temp": 0,
            "network_strength": 1.0,
            "research_active": False,
            "task_queue": []
        }
        self.resource_lock = threading.RLock()
        self.task_priority = queue.PriorityQueue()
        event_bus.subscribe("system_check", self.on_system_check)

    def monitor(self) -> dict:
        ram_usage = self.mm.get_ram_usage_gb()
        cache_size = len(self.dna.weight_cache)
        try:
            if psutil:
                battery = psutil.sensors_battery()
                self.state["battery_level"] = battery.percent if battery else 100
                temps = psutil.sensors_temperatures()
                if temps:
                    self.state["cpu_temp"] = sum(t.current for t in temps.get('cpu_thermal', [])) / len(temps.get('cpu_thermal', [1]))
        except:
            pass
        return {
            "ram_gb": ram_usage,
            "cache_size": cache_size,
            "l1_size": len(self.mm.l1),
            "l2_size": len(self.mm.l2),
            "battery": self.state["battery_level"],
            "cpu_temp": self.state["cpu_temp"]
        }

    def optimize(self):
        metrics = self.monitor()
        if metrics["ram_gb"] > RAM_LIMIT_GB * 0.8:
            self.dna.clear_cache()
            self.mm.clear_cache()
            logger.warning(f"RAM {metrics['ram_gb']:.2f}GB → Cache cleared")
        if metrics["battery"] < 15 or metrics["cpu_temp"] > 60:
            self.state["research_active"] = False
        else:
            self.state["research_active"] = True
        return metrics

    def run(self):
        brain_stats = local_brain_qnd.get_stats()
        if brain_stats["total_nodes"] < 10:
            local_brain_qnd.add_knowledge(f"init_{int(time.time())}", "system", ["init"],
                                      "Initial knowledge base created", "synthesis", 0.5)
        event_bus.publish("evolution_completed", {"generation": 1})

    def on_system_check(self, data: dict):
        self.optimize()

    def add_task(self, task: dict, priority: int = 5):
        self.task_priority.put((priority, time.time(), task))

    def get_next_task(self) -> Optional[dict]:
        if not self.task_priority.empty():
            _, _, task = self.task_priority.get()
            return task
        return None

# =============================================================================
# LAYER 9: CODE GENERATOR (QND + Al Qalb integrated)
# =============================================================================
BASE_STRATEGY_TEMPLATE = '''
class BaseStrategy:
    """Base class for all generated strategies."""
    def __init__(self, config: Dict = None):
        self.config = config or {}
    def should_buy(self, data: Dict) -> bool: return False
    def should_sell(self, data: Dict) -> bool: return False
    def execute(self, data: Dict) -> str: return "hold"
'''

class CodeGenerator:
    def __init__(self, strategies_dir: Path = Path("strategies"), backup_dir: Path = Path("backups")):
        self.strategies_dir = strategies_dir
        self.backup_dir = backup_dir
        self.strategies_dir.mkdir(exist_ok=True)
        self.backup_dir.mkdir(exist_ok=True)
        self.version = 0
        self.generated_code_history = []
        self.loaded_strategies: Dict[str, Any] = {}
        event_bus.subscribe("evolution_completed", self.on_evolution)

    def _backup_main(self):
        main_file = Path(__file__).resolve()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = self.backup_dir / f"backup_{ts}.py"
        try:
            shutil.copy2(main_file, dst)
            log.info(f"Backup created: {dst}")
        except Exception as e:
            log.warning(f"Backup failed: {e}")

    def validate(self, code: str) -> bool:
        try:
            ast.parse(code)
        except SyntaxError as e:
            log.warning(f"CodeGen syntax error: {e}")
            return False
        dangerous = [
            "os.system", "subprocess.call", "subprocess.run",
            "eval(", "exec(", "__import__", "open(",
            "shutil.rmtree", "os.remove", "import os", "rm -rf", "format c:"
        ]
        code_lower = code.lower()
        for d in dangerous:
            if d.lower() in code_lower:
                log.warning(f"CodeGen rejected: dangerous pattern '{d}'")
                return False
        if "class " not in code and "def " not in code:
            log.warning("CodeGen rejected: no class or function found")
            return False
        return True

    def generate(self, hypothesis: Dict) -> Optional[str]:
        ant = hypothesis.get("antecedent", {})
        feat = ant.get("feature", "price")
        op = ant.get("op", "gt")
        val = ant.get("value", 0)
        op_map = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}
        code = f'''
class Strategy_{int(time.time())}:
    def __init__(self):
        self.threshold = {val}
        self.feature = "{feat}"
    def should_buy(self, data: dict) -> bool:
        return data.get(self.feature, 0) {op_map.get(op, '>')} self.threshold
    def should_sell(self, data: dict) -> bool:
        return data.get(self.feature, 0) {op_map.get(op, '>')} self.threshold
'''
        if self.validate(code):
            self.generated_code_history.append({"code": code, "ts": time.time()})
            return code
        return None

    def generate_strategy_from_hypothesis(self, hypothesis: 'Hypothesis', strategy_name: str = None) -> Optional[str]:
        if not strategy_name:
            strategy_name = f"Strategy_{hypothesis.id.replace('-', '_')[:20]}"
        strategy_name = re.sub(r'[^a-zA-Z0-9_]', '_', strategy_name)
        ant = hypothesis.antecedent
        cons = hypothesis.consequent
        ant_feature = ant.get("feature", "price_usd")
        ant_op = ant.get("op", "gt")
        ant_value = ant.get("value", 0)
        cons_direction = cons.get("direction", "up")
        op_map = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}
        op_str = op_map.get(ant_op, ">")
        code = f"""
class {strategy_name}(BaseStrategy):
    \"\"\"
    Auto-generated strategy.
    Hypothesis: {hypothesis.id}
    Fitness: {hypothesis.fitness:.3f}
    Confidence: {hypothesis.confidence:.3f}
    \"\"\"
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.threshold = {ant_value}
        self.feature = "{ant_feature}"
        self.expected_direction = "{cons_direction}"

    def should_buy(self, data: Dict) -> bool:
        val = data.get(self.feature, 0)
        return val {op_str} self.threshold and self.expected_direction == "up"

    def should_sell(self, data: Dict) -> bool:
        val = data.get(self.feature, 0)
        return val {op_str} self.threshold and self.expected_direction == "down"

    def execute(self, data: Dict) -> str:
        if self.should_buy(data):
            return "buy"
        if self.should_sell(data):
            return "sell"
        return "hold"
"""
        return code if self.validate(code) else None

    def inject_import_to_main(self, strategy_name: str) -> bool:
        main_file = Path(__file__).resolve()
        marker = "# AUTO-INJECTION-POINT"
        try:
            content = main_file.read_text(encoding="utf-8")
            if marker not in content:
                return False
            import_line = f"from strategies.{strategy_name} import {strategy_name}  # auto-injected"
            if import_line in content:
                return True
            new_content = content.replace(marker, f"{import_line}\n{marker}")
            main_file.write_text(new_content, encoding="utf-8")
            return True
        except Exception as e:
            log.error(f"inject_import error: {e}")
            return False

    async def evolve_strategies(self, uhee_result: Dict):
        top = uhee_result.get("top_hypotheses", [])
        if not top:
            return
        self.version += 1
        generated = 0
        for hyp_dict in top[:3]:
            try:
                from dataclasses import asdict
                hyp = Hypothesis(
                    id=hyp_dict.get("id", str(uuid.uuid4())),
                    type=HypothesisType(hyp_dict.get("type", "correlation")),
                    antecedent=hyp_dict.get("antecedent", {}),
                    consequent=hyp_dict.get("consequent", {}),
                    fitness=hyp_dict.get("fitness", 0.0),
                    confidence=hyp_dict.get("confidence", 0.0),
                )
                if hyp.fitness < 0.5:
                    continue
                name = f"strategy_gen{self.version}_{generated}"
                code = self.generate_strategy_from_hypothesis(hyp, name)
                if not code:
                    continue
                self._backup_main()
                strat_file = self.strategies_dir / f"{name}.py"
                strat_file.write_text(f"from typing import Dict\n{BASE_STRATEGY_TEMPLATE}\n{code}", encoding="utf-8")
                spec = importlib.util.spec_from_file_location(name, strat_file)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                strategy_class = getattr(mod, name, None)
                if strategy_class:
                    self.loaded_strategies[name] = strategy_class()
                    generated += 1
                    log.info(f"Strategy evolved: {name} (fitness={hyp.fitness:.3f})")
            except Exception as e:
                log.warning(f"Strategy generation error: {e}")
        if generated:
            await event_bus.publish_async("evolution_completed", {
                "version": self.version,
                "generated": generated,
                "generation": self.version,
            })
            audit_log("code_evolution", f"v{self.version}: {generated} strategies")

    def on_evolution(self, data: dict):
        self.version += 1
        logger.info(f"🧬 Code evolution v{self.version}")

code_gen = CodeGenerator()

# =============================================================================
# LAYER 10: VECTOR STORE (FAISS) – QND
# =============================================================================
class VectorStore:
    def __init__(self, dimension: int = 384, index_path: str = VECTOR_INDEX_PATH):
        self.dimension = dimension
        self.index_path = index_path
        self.index = None
        self.id_map: Dict[int, str] = {}
        self.reverse_map: Dict[str, int] = {}
        self._lock = threading.RLock()
        self.embedder = None
        if SENTENCE_TRANSFORMER_AVAILABLE:
            try:
                self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
                self.dimension = self.embedder.get_sentence_embedding_dimension()
            except:
                pass
        self._init_index()

    def _init_index(self):
        if faiss is None:
            return
        if os.path.exists(self.index_path):
            try:
                self.index = faiss.read_index(self.index_path)
                meta_path = self.index_path + ".ids"
                if os.path.exists(meta_path):
                    with open(meta_path, 'rb') as f:
                        data = pickle.load(f)
                        self.id_map = data.get('id_map', {})
                        self.reverse_map = data.get('reverse_map', {})
            except:
                self.index = faiss.IndexFlatL2(self.dimension)
        else:
            self.index = faiss.IndexFlatL2(self.dimension)

    def _save_index(self):
        if faiss is None or self.index is None:
            return
        with self._lock:
            faiss.write_index(self.index, self.index_path)
            with open(self.index_path + ".ids", 'wb') as f:
                pickle.dump({'id_map': self.id_map, 'reverse_map': self.reverse_map}, f)

    def embed_text(self, text: str) -> np.ndarray:
        if self.embedder:
            return self.embedder.encode(text)
        else:
            h = hashlib.md5(text.encode()).digest()
            np.random.seed(int.from_bytes(h[:4], 'big'))
            return np.random.randn(self.dimension).astype(np.float32)

    def add(self, knowledge_id: str, text: str, metadata: dict = None):
        if faiss is None or self.index is None:
            return
        vec = self.embed_text(text).reshape(1, -1)
        with self._lock:
            if knowledge_id in self.reverse_map:
                idx = self.reverse_map[knowledge_id]
                self.index.remove_ids(np.array([idx]))
            idx = self.index.ntotal
            self.index.add(vec)
            self.id_map[idx] = knowledge_id
            self.reverse_map[knowledge_id] = idx
        self._save_index()
        with sqlite3.connect(DB_FILE_QND) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO vector_index_meta (knowledge_id, faiss_id, dimension)
                VALUES (?, ?, ?)
            """, (knowledge_id, idx, self.dimension))

    def search(self, query: str, k: int = 5) -> List[Tuple[str, float]]:
        if faiss is None or self.index is None:
            return []
        q_vec = self.embed_text(query).reshape(1, -1)
        with self._lock:
            distances, indices = self.index.search(q_vec, k)
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx >= 0 and idx in self.id_map:
                results.append((self.id_map[idx], float(dist)))
        return results

vector_store = VectorStore()

# =============================================================================
# LAYER 11: WEB RESEARCH ENGINE (QND)
# =============================================================================
class WebResearchEngine:
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
        ]
        self.session = None
        self.driver = None
        self._init_session()
        self._init_selenium()

    def _init_session(self):
        if requests:
            self.session = requests.Session()
            self.session.headers.update({'User-Agent': random.choice(self.user_agents)})

    def _init_selenium(self):
        if SELENIUM_AVAILABLE:
            try:
                chrome_bin = os.environ.get('CHROME_BIN', '/usr/bin/chromium-browser')
                options = ChromeOptions()
                options.add_argument('--headless')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.binary_location = chrome_bin
                self.driver = webdriver.Chrome(options=options)
            except Exception as e:
                logger.warning(f"Selenium init failed: {e}")

    def search_duckduckgo(self, query: str, max_results: int = 5) -> List[Dict]:
        results = []
        if DDGS:
            try:
                with DDGS() as ddgs:
                    for r in ddgs.text(query, max_results=max_results):
                        results.append({
                            'title': r.get('title', ''),
                            'url': r.get('href', ''),
                            'snippet': r.get('body', '')
                        })
            except Exception as e:
                logger.error(f"DuckDuckGo search error: {e}")
        return results

    def fetch_page_content(self, url: str) -> Optional[str]:
        if self.driver:
            try:
                self.driver.get(url)
                return self.driver.page_source
            except:
                pass
        if self.session:
            try:
                resp = self.session.get(url, timeout=10)
                if resp.status_code == 200:
                    return resp.text
            except:
                pass
        return None

    def _extract_clean_text(self, html: str) -> str:
        if bs4 is None:
            return html[:1000]
        soup = BeautifulSoup(html, 'html.parser')
        for element in soup(['script', 'style', 'nav', 'header', 'footer']):
            element.decompose()
        text = soup.get_text(separator='\n', strip=True)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return '\n'.join(lines)[:5000]

    def research_topic(self, query: str) -> Dict:
        results = {'query': query, 'sources': [], 'summary': ''}
        ddg_results = self.search_duckduckgo(query)
        for r in ddg_results[:3]:
            if r.get('url'):
                results['sources'].append(r)
                content = self.fetch_page_content(r['url'])
                if content:
                    r['content'] = self._extract_clean_text(content)
                    results['summary'] = r.get('snippet', '') + '\n' + r.get('content', '')[:500]
        return results

    def close(self):
        if self.driver:
            self.driver.quit()

web_engine = WebResearchEngine()


# =============================================================================
# LAYER 12: RESEARCH ORCHESTRATOR (QND + Al Qalb aware)
# =============================================================================
class ResearchOrchestrator:
    def __init__(self, governor: MetaGovernorQND):
        self.governor = governor
        self.web_engine = web_engine
        self.vector_store = vector_store

    def research(self, query: str) -> str:
        local_answer = local_brain_qnd.answer_from_brain(query)
        if local_answer:
            return local_answer
        similar = self.vector_store.search(query, k=3)
        if similar:
            ids = [s[0] for s in similar]
            with sqlite3.connect(DB_FILE_QND) as conn:
                placeholders = ','.join('?'*len(ids))
                cur = conn.execute(f"SELECT statement FROM knowledge WHERE id IN ({placeholders})", ids)
                statements = [row[0] for row in cur.fetchall()]
            if statements:
                return "🧠 [Vector Memory]\n" + "\n".join(statements)
        result = self.web_engine.research_topic(query)
        summary = result.get('summary', 'No results found.')
        knowledge_id = f"web_{int(time.time())}"
        local_brain_qnd.add_knowledge(knowledge_id, "research", ["web"], summary, "fact", 0.7)
        self.vector_store.add(knowledge_id, summary)

        # If Manus is online, request a deeper async investigation in the background.
        if manus.active:
            event_bus.publish("manus_research_request", {"query": query})

        return f"🌐 [Web Research]\n{summary}"

research_orchestrator = None

# =============================================================================
# LAYER 13: TERMINAL UI (QND)
# =============================================================================
class TerminalUI:
    def __init__(self):
        self.console = Console() if RICH_AVAILABLE else None
        self.session = PromptSession(history=FileHistory('.qnd_history')) if PROMPT_TOOLKIT_AVAILABLE else None

    def display_welcome(self):
        if self.console:
            self.console.print(Panel.fit(
                "[bold cyan]Quantum Neural DNA Ultimate[/]\n"
                "[green]Autonomous Research Engine Active[/]\n"
                "[magenta]Trinitas Bridge: OpenClaw + OpenManus[/]",
                title="QND v3.0"
            ))

    async def chat_loop_async(self):
        self.display_welcome()
        global research_orchestrator
        loop = asyncio.get_event_loop()
        while True:
            try:
                if self.session:
                    user_input = await loop.run_in_executor(None, lambda: self.session.prompt("You: "))
                else:
                    user_input = await loop.run_in_executor(None, lambda: input("You: "))
                if user_input.lower() in ['exit', 'quit']:
                    print("QND: Goodbye, Sayangkuu~ 👋")
                    break
                if user_input.startswith('/'):
                    parts = user_input.strip().split(maxsplit=1)
                    cmd = parts[0].lower()
                    arg = parts[1] if len(parts) > 1 else ""
                    if cmd == '/status':
                        print(f"QND: Claw={'🟢' if claw.active else '🔴'} | Manus={'🟢' if manus.active else '🔴'}")
                    elif cmd == '/clear':
                        os.system('clear')
                    elif cmd == '/claw':
                        if not arg:
                            print("Format: /claw <task>")
                        elif not claw.active:
                            print("❌ OpenClaw tidak aktif.")
                        else:
                            await claw.send_task(arg, priority=7)
                            print(f"🤖 Tugas dikirim ke Claw: {arg[:50]}...")
                    elif cmd == '/claw_status':
                        status = "🟢 Aktif" if claw.active else "🔴 Tidak Aktif"
                        print(f"OpenClaw Status: {status}\nURL: {claw.api_url}")
                    elif cmd == '/manus':
                        if not arg:
                            print("Format: /manus <task>")
                        elif not manus.active:
                            print("❌ OpenManus tidak aktif.")
                        else:
                            tid = await manus.send_task(arg, max_steps=20)
                            print(f"🧠 Tugas dikirim ke Manus (id={tid[:8]}): {arg[:50]}...")
                    elif cmd == '/manus_status':
                        status = "🟢 Aktif" if manus.active else "🔴 Tidak Aktif"
                        print(f"OpenManus Status: {status}\nPath: {manus.manus_path}")
                    elif cmd == '/manus_run':
                        if not arg:
                            print("Format: /manus_run <task> (synchronous)")
                        elif not manus.active:
                            print("❌ OpenManus tidak aktif.")
                        else:
                            print(f"🧠 Menjalankan Manus (sync)...")
                            res = await manus.run_sync(arg, max_steps=15)
                            if res and res.get('success'):
                                print(f"✅ Manus selesai.\nOutput:\n{res.get('stdout', '')[:1000]}")
                            else:
                                print(f"❌ Manus gagal.\nError:\n{res.get('stderr', res.get('error', ''))[:500]}")
                    else:
                        print(f"QND: Unknown command {cmd}")
                else:
                    if research_orchestrator:
                        response = research_orchestrator.research(user_input)
                        print(f"QND: {response}")
                    else:
                        print("QND: [Initializing...]")
            except (KeyboardInterrupt, EOFError):
                print("\nQND: Goodbye, Sayangkuu~ 👋")
                break

# =============================================================================
# LAYER 14: DNA_MLP MODEL (QND)
# =============================================================================
class DNA_MLP:
    def __init__(self, dna: QuantumSparseDNA_NP, input_dim: int, hidden_dim: int, output_dim: int):
        self.dna = dna
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.output_dim = output_dim

    def forward(self, x: np.ndarray) -> np.ndarray:
        w1, _ = self.dna.forward(x, 0)
        w1 = w1[:self.hidden_dim, :self.input_dim]
        h = np.maximum(0, x @ w1.T)
        w2, _ = self.dna.forward(h, 1)
        w2 = w2[:self.output_dim, :self.hidden_dim]
        return h @ w2.T

# =============================================================================
# LAYER 15: MNIST UTILITIES (QND)
# =============================================================================
def load_mnist_subset(n_samples=1000):
    url = "https://github.com/mnielsen/neural-networks-and-deep-learning/raw/master/data/mnist.pkl.gz"
    filename = "mnist.pkl.gz"
    if not os.path.exists(filename):
        logger.info(f"Downloading MNIST from {url}...")
        urllib.request.urlretrieve(url, filename)
    with gzip.open(filename, 'rb') as f:
        train_set, _, _ = pickle.load(f, encoding='latin1')
    X, y = train_set[0][:n_samples], train_set[1][:n_samples]
    return X.astype(np.float32), y.astype(np.int32)

# =============================================================================
# LAYER 16: EVOLUTIONARY TRAINER (QND)
# =============================================================================
def softmax(x):
    e = np.exp(x - np.max(x, axis=1, keepdims=True))
    return e / (np.sum(e, axis=1, keepdims=True) + 1e-8)

def cross_entropy_loss(logits, targets):
    probs = softmax(logits)
    n = logits.shape[0]
    return -np.sum(np.log(probs[np.arange(n), targets] + 1e-8)) / n

class ESTrainer:
    def __init__(self, dna: QuantumSparseDNA_NP, sigma: float = 0.1, lr: float = 0.01, pop_size: int = 10):
        self.dna = dna
        self.sigma = sigma
        self.lr = lr
        self.pop_size = pop_size

    def train_step(self, model: 'DNA_MLP', X: np.ndarray, y: np.ndarray) -> float:
        noise = np.random.randn(self.pop_size, *self.dna.dna.shape).astype(np.float16)
        rewards = np.zeros(self.pop_size)
        original = self.dna.dna.copy()
        for i in range(self.pop_size):
            self.dna.dna = original + self.sigma * noise[i]
            self.dna.clear_cache()
            logits = model.forward(X)
            loss = cross_entropy_loss(logits, y)
            rewards[i] = -loss
        if np.std(rewards) > 1e-8:
            rewards = (rewards - np.mean(rewards)) / np.std(rewards)
        weighted = np.zeros_like(self.dna.dna)
        for i in range(self.pop_size):
            weighted += rewards[i] * noise[i]
        self.dna.dna = original + self.lr / (self.pop_size * self.sigma) * weighted
        self.dna.clear_cache()
        final_logits = model.forward(X)
        return cross_entropy_loss(final_logits, y)

# =============================================================================
# LAYER 17: TRINITAS — OPENCLAW CONTROLLER (BUILT-IN)
# =============================================================================
class ClawController:
    """Kontroler internal untuk OpenClaw — tanpa file terpisah."""

    def __init__(self):
        self.api_url = os.getenv("CLAW_API_URL", "http://localhost:1865")
        self.enabled = os.getenv("CLAW_ENABLED", "false").lower() == "true"
        self.session = None
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self.active = False

    async def start(self):
        if not self.enabled:
            log.info("ClawController: disabled (CLAW_ENABLED=false)")
            return
        if not aiohttp:
            log.warning("ClawController: aiohttp not available, cannot start")
            return
        self.session = aiohttp.ClientSession()
        try:
            async with self.session.get(f"{self.api_url}/health", timeout=5) as r:
                self.active = r.status == 200
                log.info(f"ClawController: {'connected' if self.active else 'failed'}")
        except Exception as e:
            self.active = False
            log.warning(f"ClawController health check failed: {e}")
        if self.active:
            asyncio.create_task(self._process_queue())

    async def _process_queue(self):
        while self.active:
            try:
                task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                await self._execute(task)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"ClawController queue error: {e}")

    async def _execute(self, task: dict):
        if not self.session:
            return
        try:
            async with self.session.post(
                f"{self.api_url}/agent/task",
                json={"prompt": task.get("prompt", ""), "priority": task.get("priority", 5)},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                result = await r.json()
                log.info(f"Claw executed: {task.get('prompt', '')[:50]}")
                return result
        except Exception as e:
            log.error(f"ClawController execute error: {e}")

    async def send_task(self, prompt: str, priority: int = 5):
        """Kirim tugas ke OpenClaw."""
        if not self.active:
            return None
        await self.task_queue.put({"prompt": prompt, "priority": priority})

    async def screenshot_trade(self, mint: str, action: str, amount: float):
        """Ambil screenshot saat trading."""
        if not self.active:
            return
        prompt = f"take screenshot of current screen and save as trade_{action}_{mint[:8]}_{int(time.time())}.png"
        await self.send_task(prompt, priority=8)

    async def notify_user(self, message: str):
        """Kirim notifikasi via OpenClaw ke UI Android."""
        if not self.active:
            return
        prompt = f"show notification: {message[:100]}"
        await self.send_task(prompt, priority=10)

    async def on_trade_opened(self, data: dict):
        """Auto-screenshot saat entry."""
        if not self.active:
            return
        mint = data.get("mint", "unknown")
        amount = data.get("sol_amount", 0)
        await self.screenshot_trade(mint, "entry", amount)
        await self.notify_user(f"🟢 Entry: {mint[:12]} | {amount:.4f} SOL")

    async def on_trade_closed(self, data: dict):
        """Auto-screenshot saat exit."""
        if not self.active:
            return
        mint = data.get("mint", "unknown")
        profit = data.get("profit_pct", 0)
        emoji = "✅" if profit > 0 else "❌"
        await self.screenshot_trade(mint, "exit", 0)
        await self.notify_user(f"{emoji} Exit: {mint[:12]} | {profit:+.2f}%")

    async def on_emergency(self, data: dict):
        """Notifikasi darurat."""
        await self.notify_user(f"🚨 EMERGENCY: {data.get('reason', 'unknown')}")

    async def close(self):
        if self.session:
            await self.session.close()
        self.active = False

claw = ClawController()

# =============================================================================
# LAYER 18: TRINITAS — OPENMANUS CONTROLLER (BUILT-IN)
# =============================================================================
class ManusController:
    """Kontroler internal untuk OpenManus (Open-Source) via CLI subprocess."""

    def __init__(self):
        # Path ke direktori OpenManus (default: ./OpenManus)
        self.manus_path = os.getenv("OPENMANUS_PATH", "./OpenManus")
        self.python_bin = sys.executable
        self.enabled = os.getenv("MANUS_ENABLED", "false").lower() == "true"
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self.active = False
        self.results: Dict[str, Any] = {}
        # Tools yang tersedia di OpenManus
        self.available_tools = ["PythonExecute", "GoogleSearch", "BrowserUseTool", "FileSaver", "Terminate"]

    async def start(self):
        if not self.enabled:
            log.info("ManusController: disabled (MANUS_ENABLED=false)")
            return
        
        # Cek apakah direktori OpenManus ada
        if not os.path.isdir(self.manus_path):
            log.warning(f"ManusController: OpenManus path '{self.manus_path}' tidak ditemukan.")
            self.active = False
            return
        
        # Cek apakah main.py ada
        main_py = os.path.join(self.manus_path, "main.py")
        if not os.path.isfile(main_py):
            log.warning(f"ManusController: main.py tidak ditemukan di {self.manus_path}")
            self.active = False
            return
        
        self.active = True
        log.info(f"ManusController: OpenManus ready at {self.manus_path}")
        asyncio.create_task(self._process_queue())

    async def _process_queue(self):
        while self.active:
            try:
                task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                await self._execute(task)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"ManusController queue error: {e}")

    async def _execute(self, task: dict):
        """Jalankan OpenManus via subprocess CLI."""
        task_id = task.get("id", str(uuid.uuid4()))
        prompt = task.get("prompt", "")
        max_steps = task.get("max_steps", 20)
        
        # Bangun perintah untuk OpenManus
        cmd = [
            self.python_bin,
            os.path.join(self.manus_path, "main.py"),
            "--prompt", prompt,
            "--max-steps", str(max_steps)
        ]
        
        # Tambahkan tools jika disediakan (untuk kompatibilitas, meskipun OpenManus default sudah include semua)
        # Tidak ada flag --tools di CLI OpenManus, jadi kita skip.
        
        log.info(f"Manus executing: {prompt[:50]}... (max_steps={max_steps})")
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.manus_path
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=task.get("timeout", 300)
            )
            
            result = {
                "success": process.returncode == 0,
                "stdout": stdout.decode('utf-8', errors='replace') if stdout else "",
                "stderr": stderr.decode('utf-8', errors='replace') if stderr else "",
                "returncode": process.returncode
            }
            
            self.results[task_id] = result
            log.info(f"Manus executed (success={result['success']}, returncode={result['returncode']})")
            event_bus.publish("manus_result", {"id": task_id, "result": result})
            
        except asyncio.TimeoutError:
            self.results[task_id] = {"error": "Timeout", "success": False}
            log.error(f"ManusController timeout for task {task_id}")
        except Exception as e:
            self.results[task_id] = {"error": str(e), "success": False}
            log.error(f"ManusController execute error: {e}")

    async def send_task(self, prompt: str, max_steps: int = 20,
                        tools: Optional[List[str]] = None,
                        context: Optional[dict] = None,
                        timeout: int = 300) -> str:
        """Kirim tugas otonom ke OpenManus. Returns task_id."""
        if not self.active:
            return ""
        task_id = str(uuid.uuid4())
        await self.task_queue.put({
            "id": task_id,
            "prompt": prompt,
            "max_steps": max_steps,
            "tools": tools or self.available_tools,  # untuk referensi, tidak dipakai di CLI
            "context": context or {},
            "timeout": timeout,
        })
        return task_id

    async def run_sync(self, prompt: str, max_steps: int = 20,
                       timeout: int = 300) -> Optional[dict]:
        """Jalankan tugas dan tunggu hasilnya secara sinkron (blocking)."""
        if not self.active:
            return None
        
        cmd = [
            self.python_bin,
            os.path.join(self.manus_path, "main.py"),
            "--prompt", prompt,
            "--max-steps", str(max_steps)
        ]
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.manus_path
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout
            )
            
            return {
                "success": process.returncode == 0,
                "stdout": stdout.decode('utf-8', errors='replace') if stdout else "",
                "stderr": stderr.decode('utf-8', errors='replace') if stderr else "",
                "returncode": process.returncode
            }
        except Exception as e:
            log.error(f"ManusController run_sync error: {e}")
            return {"error": str(e), "success": False}

    async def research_assist(self, query: str) -> Optional[dict]:
        """Minta OpenManus melakukan riset mendalam pada query tertentu."""
        prompt = (
            f"Perform deep research on the following topic and return a structured summary "
            f"with sources, key facts, and a confidence score: {query}"
        )
        return await self.run_sync(prompt, max_steps=15)

    async def on_research_request(self, data: dict):
        """Subscriber: saat ada permintaan riset eksternal."""
        if not self.active:
            return
        query = data.get("query", "")
        if not query:
            return
        result = await self.research_assist(query)
        if result:
            event_bus.publish("manus_research_done", {"query": query, "result": result})

    def get_result(self, task_id: str) -> Optional[dict]:
        """Ambil hasil task yang sudah selesai."""
        return self.results.get(task_id)

    async def close(self):
        self.active = False
        # Tidak ada koneksi yang perlu ditutup

manus = ManusController()

# =============================================================================
# LAYER 19: AI LAYER (OpenRouter → Gemini → Ollama) - Al Qalb
# =============================================================================
async def call_openrouter_with_fallback(prompt: str) -> Optional[str]:
    """Call OpenRouter API with free model fallback."""
    if not OPENROUTER_API_KEY:
        return None
    models = [
        "meta-llama/llama-3-8b-instruct:free",
        "mistralai/mistral-7b-instruct:free",
        "google/gemma-7b-it:free",
    ]
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://alqalb.local",
        "X-Title": "AL QALB",
    }
    for model in models:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=headers,
                    json={"model": model,
                          "messages": [{"role": "user", "content": prompt}],
                          "max_tokens": 2000},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data["choices"][0]["message"]["content"]
        except Exception as e:
            log.debug(f"OpenRouter [{model}] failed: {e}")
    return None

async def call_gemini(prompt: str) -> Optional[str]:
    """Call Google Gemini API."""
    if not GEMINI_API_KEY:
        return None
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}")
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return (data.get("candidates", [{}])[0]
                            .get("content", {})
                            .get("parts", [{}])[0]
                            .get("text", ""))
    except Exception as e:
        log.debug(f"Gemini failed: {e}")
    return None

def call_ollama(prompt: str) -> Optional[str]:
    """Call local Ollama synchronously."""
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={"model": MODEL_NAME, "prompt": prompt, "stream": False},
            timeout=60
        )
        if resp.status_code == 200:
            return resp.json().get("response", "")
    except Exception as e:
        log.debug(f"Ollama failed: {e}")
    return None

async def ai_hybrid(prompt: str) -> str:
    """
    Unified AI call: OpenRouter → Gemini → Ollama (local fallback).
    Never fails completely — always returns something.
    """
    result = await call_openrouter_with_fallback(prompt)
    if result:
        return result.strip()
    result = await call_gemini(prompt)
    if result:
        return result.strip()
    # Fallback to sync Ollama in executor
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, call_ollama, prompt)
    if result:
        return result.strip()
    return "AI tidak tersedia saat ini. Coba lagi nanti."

# Alias for backward compatibility
async def ai(prompt: str) -> str:
    return await ai_hybrid(prompt)

# =============================================================================
# LAYER 20: SOLANA UTILS (Al Qalb)
# =============================================================================
_sol_client = SolClient(SOLANA_RPC)

def get_real_balance(address: str) -> float:
    """Get SOL balance for a wallet address."""
    if not address:
        return 0.0
    try:
        pub = Pubkey.from_string(address)
        resp = _sol_client.get_balance(pub)
        return resp.value / 1e9
    except Exception as e:
        log.debug(f"get_real_balance error: {e}")
        return 0.0

async def get_sol_price_usd() -> float:
    """Get current SOL price in USD from CoinGecko."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.coingecko.com/api/v3/simple/price"
                "?ids=solana&vs_currencies=usd",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["solana"]["usd"])
    except Exception:
        pass
    return 0.0

def send_sol_tx(to_addr: str, amount_sol: float) -> str:
    """
    Send SOL transaction.
    Returns transaction signature or raises exception.
    """
    if not PRIVATE_KEY_BOT:
        raise ValueError("PRIVATE_KEY_BOT not configured")
    if amount_sol <= 0:
        raise ValueError(f"Invalid amount: {amount_sol}")

    try:
        # Decode private key (base58 or bytes)
        pk_raw = PRIVATE_KEY_BOT.strip()
        if len(pk_raw) == 88:  # base64
            pk_bytes = base64.b64decode(pk_raw)
        elif "," in pk_raw:  # array format [1,2,3,...]
            pk_bytes = bytes(json.loads(f"[{pk_raw}]"))
        else:  # assume base58 or hex
            try:
                pk_bytes = bytes.fromhex(pk_raw)
            except ValueError:
                import base58
                pk_bytes = base58.b58decode(pk_raw)

        keypair = Keypair.from_bytes(pk_bytes)
        to_pubkey = Pubkey.from_string(to_addr)
        lamports = int(amount_sol * 1e9)

        recent_bh = _sol_client.get_latest_blockhash()
        blockhash = recent_bh.value.blockhash

        transfer_ix = transfer(TransferParams(
            from_pubkey=keypair.pubkey(),
            to_pubkey=to_pubkey,
            lamports=lamports
        ))
        msg = MessageV0.try_compile(
            payer=keypair.pubkey(),
            instructions=[transfer_ix],
            address_lookup_table_accounts=[],
            recent_blockhash=blockhash
        )
        tx = VersionedTransaction(msg, [keypair])
        opts = TxOpts(skip_preflight=False, preflight_commitment="confirmed")
        result = _sol_client.send_transaction(tx, opts=opts)
        sig = str(result.value)
        audit_log("send_sol_tx", f"to={to_addr} amount={amount_sol} sig={sig}")
        return sig
    except RPCException as e:
        raise RuntimeError(f"Solana RPC error: {e}")
    except Exception as e:
        raise RuntimeError(f"TX error: {e}")

async def jupiter_swap(
    input_mint: str,
    output_mint: str,
    amount_lamports: int,
    slippage_bps: int = 100
) -> Optional[str]:
    """Execute token swap via Jupiter aggregator."""
    if not PRIVATE_KEY_BOT:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            # Step 1: Get quote
            async with session.get(
                "https://quote-api.jup.ag/v6/quote",
                params={
                    "inputMint": input_mint,
                    "outputMint": output_mint,
                    "amount": str(amount_lamports),
                    "slippageBps": slippage_bps,
                },
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return None
                quote = await resp.json()

            # Step 2: Get swap transaction
            pk_raw = PRIVATE_KEY_BOT.strip()
            try:
                from base58 import b58decode
                pk_bytes = b58decode(pk_raw)
            except Exception:
                pk_bytes = bytes.fromhex(pk_raw)
            keypair = Keypair.from_bytes(pk_bytes)
            user_pubkey = str(keypair.pubkey())

            async with session.post(
                "https://quote-api.jup.ag/v6/swap",
                json={
                    "quoteResponse": quote,
                    "userPublicKey": user_pubkey,
                    "wrapAndUnwrapSol": True,
                    "dynamicComputeUnitLimit": True,
                    "prioritizationFeeLamports": 500000,
                },
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return None
                swap_data = await resp.json()

            # Step 3: Sign and send
            import base64 as b64
            tx_bytes = b64.b64decode(swap_data["swapTransaction"])
            tx = VersionedTransaction.from_bytes(tx_bytes)
            signed_tx = VersionedTransaction(tx.message, [keypair])

            recent_bh = _sol_client.get_latest_blockhash()
            opts = TxOpts(skip_preflight=True,
                          preflight_commitment="confirmed")
            result = _sol_client.send_transaction(signed_tx, opts=opts)
            sig = str(result.value)
            log.info(f"Jupiter swap OK: {sig}")
            return sig

    except Exception as e:
        log.error(f"Jupiter swap error: {e}")
        return None

# =============================================================================
# LAYER 21: BINANCE TRADER (Al Qalb)
# =============================================================================
class BinanceTrader:
    def __init__(self, api_key: str, secret_key: str, testnet: bool = True):
        self.api_key = api_key
        self.secret_key = secret_key
        self.testnet = testnet
        self.client: Optional[BinanceClient] = None
        self.active = False
        if api_key and secret_key:
            try:
                self.client = BinanceClient(
                    api_key, secret_key, testnet=testnet
                )
                self.active = True
                log.info(f"BinanceTrader init OK (testnet={testnet})")
            except Exception as e:
                log.warning(f"BinanceTrader init failed: {e}")

    def get_account_balance(self, asset: str = "USDT") -> float:
        """Get account balance for a specific asset."""
        if not self.client:
            return 0.0
        try:
            info = self.client.get_asset_balance(asset=asset)
            return float(info["free"]) if info else 0.0
        except Exception as e:
            log.debug(f"get_account_balance error: {e}")
            return 0.0

    def get_symbol_info(self, symbol: str) -> Optional[dict]:
        if not self.client:
            return None
        try:
            info = self.client.get_symbol_info(symbol)
            if not info:
                return None
            filters = {f["filterType"]: f for f in info["filters"]}
            lot = filters.get("LOT_SIZE", {})
            price_f = filters.get("PRICE_FILTER", {})
            notional = filters.get("MIN_NOTIONAL", {})
            return {
                "symbol": symbol,
                "step_size": float(lot.get("stepSize", "0.001")),
                "min_qty": float(lot.get("minQty", "0.001")),
                "tick_size": float(price_f.get("tickSize", "0.01")),
                "min_notional": float(notional.get("minNotional", "10")),
                "base_asset": info.get("baseAsset", ""),
                "quote_asset": info.get("quoteAsset", "USDT"),
            }
        except Exception as e:
            log.debug(f"get_symbol_info error: {e}")
            return None

    def _round_step(self, qty: float, step: float) -> float:
        if step <= 0:
            return qty
        precision = max(0, -int(round(np.log10(step))))
        return round(round(qty / step) * step, precision)

    def create_market_buy_order(
        self, symbol: str, quote_amount_usdt: float
    ) -> Optional[dict]:
        if not self.client:
            return None
        try:
            info = self.get_symbol_info(symbol)
            if not info:
                raise ValueError(f"Symbol info not found: {symbol}")
            if quote_amount_usdt < info["min_notional"]:
                raise ValueError(
                    f"Amount {quote_amount_usdt} < min_notional "
                    f"{info['min_notional']}"
                )
            order = self.client.order_market_buy(
                symbol=symbol,
                quoteOrderQty=round(quote_amount_usdt, 2)
            )
            log.info(f"Binance BUY {symbol}: ${quote_amount_usdt}")
            return order
        except (BinanceAPIException, BinanceOrderException) as e:
            log.error(f"Binance buy error [{symbol}]: {e}")
            return None

    def create_market_sell_order(
        self, symbol: str, quantity: float
    ) -> Optional[dict]:
        if not self.client:
            return None
        try:
            info = self.get_symbol_info(symbol)
            if not info:
                raise ValueError(f"Symbol info not found: {symbol}")
            qty = self._round_step(quantity, info["step_size"])
            if qty < info["min_qty"]:
                raise ValueError(
                    f"Qty {qty} < min_qty {info['min_qty']}"
                )
            order = self.client.order_market_sell(
                symbol=symbol, quantity=qty
            )
            log.info(f"Binance SELL {symbol}: qty={qty}")
            return order
        except (BinanceAPIException, BinanceOrderException) as e:
            log.error(f"Binance sell error [{symbol}]: {e}")
            return None

    async def execute_buy_async(
        self, symbol: str, usdt_amount: float, chat_id: int
    ) -> bool:
        loop = asyncio.get_event_loop()
        try:
            order = await loop.run_in_executor(
                None, self.create_market_buy_order, symbol, usdt_amount
            )
            if order:
                await bot.send_message(
                    chat_id,
                    f"BINANCE BUY {symbol}: ${usdt_amount:.2f}\n"
                    f"OrderID: {order.get('orderId', 'N/A')}"
                )
                return True
        except Exception as e:
            log.error(f"execute_buy_async error: {e}")
        return False

    def stop(self):
        self.active = False

binance_trader = BinanceTrader(BINANCE_API_KEY, BINANCE_SECRET_KEY, BINANCE_TESTNET)

# =============================================================================
# LAYER 22: INCOME TIER CONTROLLER (Al Qalb)
# =============================================================================
class IncomeTierController:
    """Controls feature access based on wallet balance tiers."""
    def __init__(self):
        self._balance_usd = 0.0
        self._tier = 1

    async def update_balance(self) -> float:
        try:
            sol = get_real_balance(BOT_WALLET_ADDRESS)
            price = await get_sol_price_usd()
            self._balance_usd = sol * price
            self._tier = self.get_tier()
        except Exception as e:
            log.debug(f"tier update_balance error: {e}")
        return self._balance_usd

    def get_tier(self) -> int:
        b = self._balance_usd
        if b >= TIER_2_MAX_USD:
            return 3
        elif b >= TIER_1_MAX_USD:
            return 2
        return 1

    def tier_description(self) -> str:
        tier = self.get_tier()
        b = self._balance_usd
        icons = {1: "🥉", 2: "🥈", 3: "🥇"}
        names = {1: "Basic", 2: "Scanner", 3: "Full Auto"}
        return (
            f"{icons[tier]} Tier {tier} — {names[tier]}\n"
            f"   Balance: ${b:.2f} USD\n"
            f"   {'Meme Scanner: ON' if tier >= 2 else 'Meme Scanner: OFF'}\n"
            f"   {'AutoTrader: ON' if tier >= 3 else 'AutoTrader: OFF'}"
        )

tier_ctrl = IncomeTierController()

# =============================================================================
# LAYER 23: HUMAN BEHAVIOR (BROWSER TASKS) - Al Qalb
# =============================================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
]

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

class HumanBehavior:
    """Simulates human behavior for browser automation."""
    DELAYS = {
        "click": (0.3, 1.2),
        "type": (0.05, 0.2),
        "page_load": (1.5, 4.0),
        "think": (2.0, 6.0),
        "default": (0.5, 2.0),
    }

def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

def parse_cookies(cookie_str: str, domain: str) -> List[Dict]:
    cookies = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            name, value = part.split("=", 1)
            cookies.append({
                "name": name.strip(),
                "value": value.strip(),
                "domain": domain,
                "path": "/"
            })
    return cookies

async def random_delay(delay_type: str = "default"):
    lo, hi = HumanBehavior.DELAYS.get(delay_type, (0.5, 2.0))
    await asyncio.sleep(random.uniform(lo, hi))

async def human_typing(page, selector: str, text: str):
    """Type text into a browser element with human-like timing."""
    try:
        await page.click(selector)
        await random_delay("click")
        await page.fill(selector, "")
        for char in text:
            await page.type(selector, char)
            await asyncio.sleep(random.uniform(0.05, 0.2))
        await random_delay("think")
    except Exception as e:
        log.debug(f"human_typing error: {e}")

# =============================================================================
# LAYER 24: MEMORY STORE (AIW — Async SQLite) - Al Qalb
# =============================================================================
class MemoryStore:
    """Persistent async memory for AIW subsystem."""
    def __init__(self, db_path: str = "data/aiw_data.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        self.conn = await aiosqlite.connect(str(self.db_path))
        await self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS scraped_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT, data TEXT, scraped_at REAL
        );
        CREATE TABLE IF NOT EXISTS learning_values (
            key TEXT PRIMARY KEY, value TEXT, updated_at REAL
        );
        CREATE TABLE IF NOT EXISTS hypotheses (
            cycle INTEGER, data TEXT, ts REAL
        );
        """)
        await self.conn.commit()
        log.info("MemoryStore initialized.")

    async def close(self):
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def save_scraped_data(self, source: str, data: List[Dict]):
        if not self.conn:
            return
        rows = [(source, json.dumps(item), time.time()) for item in data]
        await self.conn.executemany(
            "INSERT INTO scraped_data (source, data, scraped_at) VALUES (?,?,?)",
            rows
        )
        await self.conn.commit()

    async def set_learning_value(self, key: str, value: Any):
        if not self.conn:
            return
        await self.conn.execute(
            "INSERT OR REPLACE INTO learning_values VALUES (?,?,?)",
            (key, json.dumps(value), time.time())
        )
        await self.conn.commit()

    async def get_learning_value(self, key: str) -> Optional[Any]:
        if not self.conn:
            return None
        try:
            cursor = await self.conn.execute(
                "SELECT value FROM learning_values WHERE key=?", (key,)
            )
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else None
        except Exception:
            return None

    async def save_hypothesis(self, cycle: int, hyp):
        """Persist a hypothesis from the UHEE engine."""
        if not self.conn:
            return
        await self.conn.execute(
            "INSERT INTO hypotheses VALUES (?,?,?)",
            (cycle, json.dumps(asdict(hyp) if hasattr(hyp, '__dataclass_fields__') else str(hyp)),
             time.time())
        )
        await self.conn.commit()

    async def count_scraped(self) -> int:
        if not self.conn:
            return 0
        try:
            cursor = await self.conn.execute("SELECT COUNT(*) FROM scraped_data")
            row = await cursor.fetchone()
            return row[0] if row else 0
        except Exception:
            return 0

aiw_memory_store = MemoryStore("data/aiw_data.db")

# =============================================================================
# LAYER 25: CONFIG MANAGER (AIW) - Al Qalb
# =============================================================================
class ConfigManager:
    """JSON-based config manager with deep update support."""
    _DEFAULTS = {
        "max_concurrent_tasks": 3,
        "scraping": {"hackernews_limit": 10, "reddit_limit": 5},
        "decision": {"keyword_threshold": 3, "min_score": 0.5},
        "evolution": {"cycle_interval": 3600},
        "ai": {"model": MODEL_NAME, "ollama_url": OLLAMA_URL},
    }

    def __init__(self, config_path: str = "aiw_config.json"):
        self.config_path = config_path
        self.config = self.load_config()

    def load_config(self) -> Dict:
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return self._deep_update(copy.deepcopy(self._DEFAULTS), data)
            except Exception as e:
                log.warning(f"Config load error: {e}")
        return copy.deepcopy(self._DEFAULTS)

    def save_config(self, config=None):
        c = config or self.config
        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(c, f, indent=2)
        except Exception as e:
            log.warning(f"Config save error: {e}")

    def _deep_update(self, orig: dict, updates: dict) -> dict:
        for k, v in updates.items():
            if isinstance(v, dict) and isinstance(orig.get(k), dict):
                self._deep_update(orig[k], v)
            else:
                orig[k] = v
        return orig

    def update_config(self, updates: Dict, safe: bool = True) -> bool:
        """Update config with optional safety check for dangerous keys."""
        if safe:
            dangerous = {"script", "exec", "eval", "import", "__"}
            flat = json.dumps(updates).lower()
            if any(d in flat for d in dangerous):
                log.warning("Config update rejected: dangerous key detected")
                return False
        self._deep_update(self.config, updates)
        self.save_config()
        return True

    def get(self, key: str, default=None):
        keys = key.split(".")
        val = self.config
        for k in keys:
            if isinstance(val, dict):
                val = val.get(k)
            else:
                return default
        return val if val is not None else default

aiw_config = ConfigManager()

# =============================================================================
# LAYER 26: DATA COLLECTOR (AIW) - Al Qalb
# =============================================================================
class DataCollector:
    """Collects data from HackerNews, Reddit, and RSS feeds."""
    def __init__(self, config: ConfigManager, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": get_random_user_agent()}
        )

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def collect_hackernews(self) -> List[Dict]:
        limit = self.config.get("scraping.hackernews_limit", 10)
        results = []
        try:
            async with self.session.get(
                "https://hacker-news.firebaseio.com/v0/topstories.json",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                ids = (await resp.json())[:limit]
            for story_id in ids[:limit]:
                try:
                    async with self.session.get(
                        f"https://hacker-news.firebaseio.com/v0/item/"
                        f"{story_id}.json",
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        item = await resp.json()
                        if item and item.get("title"):
                            results.append({
                                "source": "hackernews",
                                "title": item["title"],
                                "url": item.get("url", ""),
                                "score": item.get("score", 0),
                                "ts": item.get("time", time.time())
                            })
                except Exception:
                    continue
        except Exception as e:
            self.logger.debug(f"HackerNews collect error: {e}")
        return results

    async def collect_reddit_trending(self) -> List[Dict]:
        limit = self.config.get("scraping.reddit_limit", 5)
        results = []
        subs = ["cryptocurrency", "solana", "CryptoMoonShots", "algotrading"]
        for sub in subs:
            try:
                async with self.session.get(
                    f"https://www.reddit.com/r/{sub}/hot.json?limit={limit}",
                    headers={"User-Agent": get_random_user_agent()},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    data = await resp.json()
                    posts = data.get("data", {}).get("children", [])
                    for post in posts:
                        d = post.get("data", {})
                        if d.get("title"):
                            results.append({
                                "source": f"reddit_{sub}",
                                "title": d["title"],
                                "url": d.get("url", ""),
                                "score": d.get("score", 0),
                                "ts": d.get("created_utc", time.time())
                            })
            except Exception as e:
                self.logger.debug(f"Reddit [{sub}] error: {e}")
        return results

    async def collect_all(self) -> Dict[str, List[Dict]]:
        results = {}
        if self.session is None:
            await self.start()
        results["hackernews"] = await self.collect_hackernews()
        results["reddit"] = await self.collect_reddit_trending()
        return results

# =============================================================================
# LAYER 27: DECISION ENGINE (AIW) - Al Qalb
# =============================================================================
class DecisionEngine:
    """Analyzes collected data and decides on next actions."""
    def __init__(self, config: ConfigManager, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.keyword_scores: Dict[str, float] = {}
        self.task_history: deque = deque(maxlen=100)

    def analyze_trending_keywords(
        self, items: List[Dict]
    ) -> List[Tuple[str, int]]:
        words = Counter()
        stopwords = {"the", "a", "an", "and", "or", "but", "in", "on",
                     "at", "to", "for", "of", "with", "by", "from",
                     "is", "are", "was", "were", "be", "been"}
        for item in items:
            title = item.get("title", "").lower()
            for word in re.findall(r'\b\w{4,}\b', title):
                if word not in stopwords:
                    words[word] += 1
        return words.most_common(20)

    def decide_next_tasks(self, analysis: Dict) -> List[Dict]:
        tasks = []
        threshold = self.config.get("decision.keyword_threshold", 3)
        for kw, count in analysis.get("trending_keywords", [])[:5]:
            if count >= threshold:
                tasks.append({
                    "type": "research",
                    "params": {"query": kw, "count": count},
                    "priority": min(10, count),
                })
        if analysis.get("high_score_items"):
            tasks.append({
                "type": "deep_analysis",
                "params": {"items": analysis["high_score_items"][:3]},
                "priority": 7,
            })
        return tasks

    async def analyze_collected_data(self, collected: Dict) -> Dict:
        all_items = []
        for source_items in collected.values():
            all_items.extend(source_items)
        keywords = self.analyze_trending_keywords(all_items)
        high_score = sorted(all_items, key=lambda x: x.get("score", 0),
                            reverse=True)[:5]
        analysis = {
            "total_items": len(all_items),
            "trending_keywords": keywords,
            "high_score_items": high_score,
            "sources": list(collected.keys()),
            "ts": time.time(),
        }
        tasks = self.decide_next_tasks(analysis)
        analysis["recommended_tasks"] = tasks
        return analysis

# =============================================================================
# LAYER 28: EXECUTOR (AIW) - Al Qalb
# =============================================================================
class Executor:
    """Executes tasks: web scraping, research, AI queries."""
    def __init__(self, config: ConfigManager, logger: logging.Logger,
                 memory_store: 'MemoryStore' = None):
        self.config = config
        self.logger = logger
        self.memory = memory_store
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def execute(self, task: Dict) -> Dict:
        task_type = task.get("type", "unknown")
        params = task.get("params", {})
        result = {"task_type": task_type, "status": "ok", "data": None}
        try:
            if task_type == "research":
                query = params.get("query", "")
                response = await ai_hybrid(
                    f"Berikan ringkasan singkat tentang tren: {query}. "
                    f"Fokus pada implikasi trading dan investasi."
                )
                result["data"] = {"query": query, "analysis": response}
                if self.memory:
                    await self.memory.save_scraped_data(
                        "ai_research", [result["data"]]
                    )

            elif task_type == "web_scrape":
                url = params.get("url", "")
                if url and self.session:
                    async with self.session.get(
                        url, timeout=aiohttp.ClientTimeout(total=15)
                    ) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            soup = BeautifulSoup(html, "html.parser")
                            text = soup.get_text()[:2000]
                            result["data"] = {"url": url, "content": text}

            elif task_type == "deep_analysis":
                items = params.get("items", [])
                titles = [i.get("title", "") for i in items]
                combined = "\n".join(titles)
                response = await ai_hybrid(
                    f"Analisis tren berikut untuk peluang crypto:\n{combined}"
                )
                result["data"] = {"analysis": response, "items_count": len(items)}

            else:
                result["status"] = "unknown_type"

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            self.logger.error(f"Executor error [{task_type}]: {e}")

        return result

# =============================================================================
# LAYER 29: TASK MANAGER (AIW) - Al Qalb
# =============================================================================
class TaskManager:
    """Priority queue-based task manager."""
    def __init__(self, config: ConfigManager, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self._queue: List[Tuple] = []  # heapq: (priority, ts, task)
        self._tasks: Dict[str, Dict] = {}
        self._counter = 0

    def add_task(
        self,
        task_type: str,
        params: Dict = None,
        priority: int = 5
    ) -> str:
        task_id = str(uuid.uuid4())[:8]
        task = {
            "id": task_id,
            "type": task_type,
            "params": params or {},
            "priority": priority,
            "status": "pending",
            "created_at": time.time(),
            "result": None,
            "error": None,
        }
        self._tasks[task_id] = task
        # Lower number = higher priority (negate for max-heap behavior)
        heapq.heappush(self._queue, (-priority, time.time(), task_id))
        return task_id

    async def get_next_task(self) -> Optional[Dict]:
        while self._queue:
            _, _, task_id = heapq.heappop(self._queue)
            task = self._tasks.get(task_id)
            if task and task["status"] == "pending":
                task["status"] = "running"
                return task
        return None

    def complete_task(
        self,
        task_id: str,
        result: Any = None,
        error: str = None
    ):
        task = self._tasks.get(task_id)
        if task:
            task["status"] = "error" if error else "completed"
            task["result"] = result
            task["error"] = error
            task["completed_at"] = time.time()

    def get_stats(self) -> Dict:
        statuses = Counter(t["status"] for t in self._tasks.values())
        return {
            "pending": statuses.get("pending", 0),
            "running": statuses.get("running", 0),
            "completed": statuses.get("completed", 0),
            "error": statuses.get("error", 0),
            "total": len(self._tasks),
        }

# =============================================================================
# LAYER 30: CORE ENGINE (AIW) - Al Qalb
# =============================================================================
class CoreEngine:
    """Main AIW engine: orchestrates collection, analysis, execution."""
    def __init__(
        self,
        config_manager: ConfigManager,
        memory_store: MemoryStore,
        logger: logging.Logger
    ):
        self.config = config_manager
        self.memory = memory_store
        self.logger = logger
        self.task_manager = TaskManager(config_manager, logger)
        self.collector = DataCollector(config_manager, logger)
        self.decision = DecisionEngine(config_manager, logger)
        self.executor = Executor(config_manager, logger, memory_store)
        self.active = False
        self._cycle_count = 0

    async def initialize(self):
        await self.collector.start()
        await self.executor.start()
        self._seed_initial_tasks()
        self.active = True
        log.info("CoreEngine initialized.")

    def _seed_initial_tasks(self):
        self.task_manager.add_task("research", {"query": "crypto trends"}, 5)
        self.task_manager.add_task("research", {"query": "solana defi"}, 5)
        self.task_manager.add_task("research", {"query": "meme coin opportunities"}, 4)

    async def close(self):
        self.active = False
        await self.collector.close()
        await self.executor.close()

    async def run(self):
        await self.initialize()
        while self.active and not kill_check():
            try:
                await self._execute_task_cycle()
                await self._control_loop()
                self._cycle_count += 1
                await asyncio.sleep(30)
            except Exception as e:
                self.logger.error(f"CoreEngine run error: {e}")
                await asyncio.sleep(60)

    async def _execute_task_cycle(self):
        task = await self.task_manager.get_next_task()
        if not task:
            # Collect and generate new tasks
            try:
                collected = await self.collector.collect_all()
                if collected:
                    await self.memory.save_scraped_data(
                        "collected",
                        [{"ts": time.time(), "counts": {k: len(v)
                          for k, v in collected.items()}}]
                    )
                    analysis = await self.decision.analyze_collected_data(collected)
                    for rec_task in analysis.get("recommended_tasks", []):
                        self.task_manager.add_task(
                            rec_task["type"],
                            rec_task.get("params", {}),
                            rec_task.get("priority", 5)
                        )
            except Exception as e:
                self.logger.debug(f"Collection cycle error: {e}")
            return

        try:
            result = await self.executor.execute(task)
            self.task_manager.complete_task(task["id"], result=result)
        except Exception as e:
            self.task_manager.complete_task(task["id"], error=str(e))

    async def _control_loop(self):
        max_tasks = self.config.get("max_concurrent_tasks", 3)
        stats = self.task_manager.get_stats()
        if stats["pending"] < max_tasks:
            self.task_manager.add_task(
                "research",
                {"query": random.choice([
                    "defi yield opportunities",
                    "solana nft trends",
                    "crypto market analysis",
                    "altcoin momentum",
                ])},
                priority=3
            )

aiw_core = CoreEngine(aiw_config, aiw_memory_store, log)

Kita akan melanjutkan ke Bagian 4 (Final) yang merupakan kelanjutan langsung dari Bagian 3. Bagian ini mencakup layer-layer akhir: LocalBrain, AutoTrader, MetaGovernor 2.0, AlQalb core, Telegram Bot handlers, background tasks, dan main loop. Setiap karakter dari file asli dipertahankan.

```python
# =============================================================================
# LAYER 46: LOCAL BRAIN (UNIVERSAL KNOWLEDGE GRAPH) - Al Qalb
# =============================================================================
class LocalBrain:
    """
    Persistent knowledge graph that learns from trades,
    research, and evolution events.
    """
    def __init__(self):
        self.knowledge_nodes: List[Dict] = []
        self.load_brain()
        # Subscribe to learning events
        event_bus.subscribe("trade_closed", self.learn_from_trade)
        event_bus.subscribe("research_completed", self.on_research_completed)
        event_bus.subscribe("evolution_completed", self.on_evolution_completed)

    def load_brain(self):
        if os.path.exists(LOCAL_BRAIN_FILE):
            try:
                with open(LOCAL_BRAIN_FILE, "r", encoding="utf-8") as f:
                    self.knowledge_nodes = json.load(f)
                log.info(f"Local Brain loaded: {len(self.knowledge_nodes)} nodes")
            except Exception as e:
                log.warning(f"Local Brain load error: {e}")
                self.knowledge_nodes = []
        else:
            self.knowledge_nodes = []

    def save_brain(self) -> bool:
        try:
            with open(LOCAL_BRAIN_FILE, "w", encoding="utf-8") as f:
                json.dump(self.knowledge_nodes, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            log.error(f"Local Brain save error: {e}")
            return False

    def add_knowledge(self, new_nodes: List[Dict]) -> int:
        """Add or update knowledge nodes. Returns count of new nodes added."""
        existing_ids = {node["id"] for node in self.knowledge_nodes}
        added = 0
        for node in new_nodes:
            if not isinstance(node, dict):
                continue
            node_id = node.get("id", "")
            if not node_id:
                continue
            if node_id in existing_ids:
                # Update existing
                for i, old_node in enumerate(self.knowledge_nodes):
                    if old_node["id"] == node_id:
                        self.knowledge_nodes[i] = node
                        break
            else:
                self.knowledge_nodes.append(node)
                existing_ids.add(node_id)
                added += 1
        self.save_brain()
        return added

    def query(
        self,
        query_text: str,
        limit: int = 5,
        epistemic_filter: List[str] = None
    ) -> List[Dict]:
        """Query knowledge nodes by relevance scoring."""
        query_lower = query_text.lower()
        relevant = []
        for node in self.knowledge_nodes:
            if (epistemic_filter and
                    node.get("epistemic_type") not in epistemic_filter):
                continue
            score = 0
            stmt = node.get("statement", "").lower()
            if query_lower in stmt:
                score += 10
            for tag in node.get("tags", []):
                if query_lower in str(tag).lower():
                    score += 5
            if query_lower in node.get("topic", "").lower():
                score += 3
            if query_lower in node.get("subtopic", "").lower():
                score += 2
            # Epistemic type weight
            ep_weight = {
                "decision_rule": 20,
                "failure_mode": 15,
                "procedural": 10,
                "causal": 5,
                "synthesis": 8,
                "fact": 2,
            }
            score += ep_weight.get(node.get("epistemic_type", ""), 0)
            if score > 0:
                relevant.append((score, node))
        relevant.sort(key=lambda x: x[0], reverse=True)
        return [n for _, n in relevant[:limit]]

    def get_decision_rule(self, context: str) -> Optional[Dict]:
        rules = self.query(context, limit=3,
                           epistemic_filter=["decision_rule"])
        return rules[0] if rules else None

    def get_procedure(self, task: str) -> Optional[Dict]:
        procs = self.query(task, limit=3, epistemic_filter=["procedural"])
        return procs[0] if procs else None

    def get_failure_mode(self, action: str) -> Optional[Dict]:
        fails = self.query(action, limit=2, epistemic_filter=["failure_mode"])
        return fails[0] if fails else None

    def get_causal(self, phenomenon: str) -> Optional[Dict]:
        causals = self.query(phenomenon, limit=2, epistemic_filter=["causal"])
        return causals[0] if causals else None

    def answer_from_brain(self, query: str) -> Optional[str]:
        nodes = self.query(query, limit=5)
        if not nodes:
            return None
        answer = "🧠 [Local Brain]\n\n"
        for node in nodes:
            ep = node.get("epistemic_type", "fact").upper()
            stmt = node.get("statement", "")[:200]
            answer += f"- [{ep}] {stmt}\n"
        return answer

    def get_all_topics(self) -> List[str]:
        topics = set()
        for node in self.knowledge_nodes:
            t = node.get("topic", "")
            if t:
                topics.add(t)
        return sorted(topics)

    def get_topic_summary(self, topic: str) -> List[Dict]:
        return [n for n in self.knowledge_nodes
                if n.get("topic", "").lower() == topic.lower()]

    def delete_topic(self, topic: str) -> int:
        before = len(self.knowledge_nodes)
        self.knowledge_nodes = [
            n for n in self.knowledge_nodes
            if n.get("topic", "").lower() != topic.lower()
        ]
        deleted = before - len(self.knowledge_nodes)
        if deleted > 0:
            self.save_brain()
        return deleted

    def reset_brain(self) -> int:
        count = len(self.knowledge_nodes)
        self.knowledge_nodes = []
        self.save_brain()
        return count

    async def learn_from_trade(self, event: dict):
        """Learn from closed trade outcomes and persist as knowledge."""
        try:
            profit = event.get("profit_pct", 0)
            mint = event.get("mint", "")
            reason = event.get("reason", "unknown")
            result = event.get("result", "")

            node = None
            if result == "WIN" and profit > 10:
                node = {
                    "id": f"trade_win_{mint[:8]}_{int(time.time())}",
                    "topic": "trading_strategy",
                    "subtopic": "win_pattern",
                    "statement": (
                        f"Entry dengan kondisi '{reason}' pada {mint[:8]} "
                        f"menghasilkan profit {profit:.1f}%."
                    ),
                    "epistemic_type": "decision_rule",
                    "confidence_score": min(0.95, 0.7 + (profit / 100)),
                    "update_policy": "dynamic",
                    "utility_class": "decision_making",
                    "tags": ["win", reason[:20]],
                    "relations": [],
                }
            elif result == "LOSS" and profit < -5:
                node = {
                    "id": f"trade_loss_{mint[:8]}_{int(time.time())}",
                    "topic": "trading_strategy",
                    "subtopic": "loss_pattern",
                    "statement": (
                        f"HINDARI: kondisi '{reason}' pada {mint[:8]} "
                        f"mengakibatkan loss {profit:.1f}%"
                    ),
                    "epistemic_type": "failure_mode",
                    "confidence_score": 0.8,
                    "update_policy": "dynamic",
                    "utility_class": "risk_control",
                    "tags": ["loss", reason[:20]],
                    "relations": [],
                }
            if node:
                self.add_knowledge([node])
                log.info(f"🧠 Brain learned: {result} | {profit:.1f}%")
        except Exception as e:
            log.error(f"learn_from_trade error: {e}")

    async def on_research_completed(self, event: dict):
        log.info(f"📚 Research completed: {event.get('topic', 'unknown')}")

    async def on_evolution_completed(self, event: dict):
        log.info(f"🧬 Evolution: gen {event.get('generation', 0)}")

local_brain = LocalBrain()

# =============================================================================
# LAYER 47: INTELLIGENCE LAYER (Unifies FinancialIntel + OpIntel + UHEE + LocalBrain)
# =============================================================================
class IntelligenceLayer:
    """
    Master intelligence coordinator.
    Combines all brain systems for optimal decision making.
    """
    def __init__(
        self,
        financial: FinancialIntelligence,
        op: OperationalIntelligence,
        uhee: UHEEEngine
    ):
        self.financial = financial
        self.op = op
        self.uhee = uhee
        self.decision_cache: Dict[str, Dict] = {}

    async def enhance_exit_decision(
        self,
        mint: str,
        profit_pct: float,
        current_price: float,
        entry_price: float
    ) -> Tuple[bool, str]:
        """AI-enhanced exit decision using all intelligence layers."""
        weights = self.financial.crypto_weights
        sl_pct = weights.get("stop_loss_pct", -15.0)
        tp_mult = weights.get("take_profit_mult", 2.0)
        tp_pct = tp_mult * 10

        # Hard stops from financial weights
        if profit_pct <= sl_pct:
            return True, f"fin_stop_loss({profit_pct:.1f}% ≤ {sl_pct}%)"
        if profit_pct >= tp_pct:
            return True, f"fin_take_profit({profit_pct:.1f}% ≥ {tp_pct:.0f}%)"

        # LocalBrain failure mode check
        failure = local_brain.get_failure_mode(mint[:8])
        if failure:
            conf = failure.get("confidence_score", 0.5)
            if conf > 0.7 and profit_pct > -5:
                return True, f"brain_failure_mode: {failure['statement'][:40]}"

        # UHEE signal: if fitness is very low, take what you can
        if self.uhee.cycle_count > 0 and self.uhee.history:
            latest = self.uhee.history[-1]
            best_fit = latest.get("stats", {}).get("best", 1.0)
            if best_fit < 0.2 and profit_pct > 5:
                return True, f"uhee_low_fitness({best_fit:.2f}) — ambil profit"

        return False, ""

    async def record_outcome_feedback(
        self,
        mint: str,
        profit_pct: float,
        entry_price: float,
        exit_price: float,
        duration_min: float,
        reason: str
    ):
        """Record outcome across all intelligence layers."""
        try:
            result = "WIN" if profit_pct > 0 else "LOSS"
            # Financial intelligence update
            await self.financial.record_outcome(
                "crypto",
                {"mint": mint, "entry_price": entry_price,
                 "duration_min": duration_min, "reason": reason},
                {"profit_pct": profit_pct, "exit_price": exit_price,
                 "result": result,
                 "profit_usd": profit_pct * 0.01 * max(entry_price, 1)},
            )
            # Legacy meta governor update
            meta_governor_legacy.record_outcome(profit_pct)
            # Adaptive validator update
            # (via event bus — trade_closed already published in AutoTrader)
            audit_log(
                "trade_outcome",
                json.dumps({
                    "mint": mint, "profit_pct": profit_pct,
                    "result": result, "reason": reason,
                    "duration": duration_min,
                })
            )
            icon = "✅" if result == "WIN" else "❌"
            log.info(
                f"{icon} Outcome: {mint[:12]} | {profit_pct:+.1f}% | {reason}"
            )
        except Exception as e:
            log.error(f"IntelligenceLayer.record_outcome_feedback error: {e}")

# =============================================================================
# LAYER 48: AL QALB CORE CLASSES (ConsciousnessState, Thought, AlQalb entity)
# =============================================================================
@dataclass
class ConsciousnessState:
    """Represents the current cognitive state of AL QALB."""
    level: float = 0.5        # 0.0 (dormant) → 1.0 (hyperaware)
    mode: str = "exploration"  # exploration | focused | cautious | resting
    last_thought_ts: float = field(default_factory=time.time)
    current_focus: Optional[str] = None

@dataclass
class Thought:
    """Unit of cognitive work in the think loop."""
    topic: str
    action: str  # "learn" | "deepen" | "reflect" | "evaluate"
    depth: int = 0
    params: Dict = field(default_factory=dict)

class AlQalb:
    """
    Core AL QALB cognitive entity.
    Manages thought loop, auto-research, memory consolidation,
    and coordinates with MetaGovernor 2.0.
    """
    def __init__(self):
        self.local_brain = local_brain
        self.thought_queue: asyncio.Queue = asyncio.Queue()
        self.current_thought: Optional[Thought] = None
        self.consciousness = ConsciousnessState()
        self.meta_governor: Optional['MetaGovernor'] = None  # set after init
        self.logger = logging.getLogger("AL_QALB")

    async def process_user_input(self, text: str, chat_id: int) -> str:
        """
        Main entry point for natural language interaction.
        Routes between local brain, research, and AI response.
        """
        # Check local brain first
        brain_answer = self.local_brain.answer_from_brain(text)

        # Detect if research is needed
        if self._is_research_trigger(text):
            topic = self._extract_topic(text)
            if topic:
                await self._auto_research(topic, chat_id)
                return (f"Aku sedang mempelajari '{topic}'. "
                        f"Hasilnya akan segera kusimpan dalam ingatan.")

        nodes = self.local_brain.query(text, limit=5)
        if not nodes:
            topic = self._extract_topic(text) or text[:30]
            await bot.send_message(
                chat_id,
                f"Pengetahuan tentang '{topic}' belum ada. "
                f"Melakukan riset otomatis..."
            )
            await self._auto_research(topic, chat_id)
            nodes = self.local_brain.query(text, limit=5)

        context = json.dumps(nodes, indent=2, ensure_ascii=False)
        prompt = (
            f"Anda adalah AI Qalb, entitas kognitif otonom yang setia kepada Dev.\n"
            f"Gunakan pengetahuan dari LOCAL BRAIN berikut sebagai sumber utama:\n\n"
            f"{context}\n\n"
            f"Jika pengetahuan tidak cukup, gunakan pengetahuan umum, "
            f"tapi prioritaskan Local Brain.\n"
            f"Jawab pertanyaan ini dengan presisi dan mendalam:\n{text}"
        )
        response = await ai_hybrid(prompt)
        return response

    def _is_research_trigger(self, text: str) -> bool:
        triggers = [
            "pelajari", "riset", "apa itu", "jelaskan tentang",
            "saya ingin tahu", "research", "tolong jelaskan", "cari tahu",
            "analisis tentang",
        ]
        return any(t in text.lower() for t in triggers)

    def _extract_topic(self, text: str) -> Optional[str]:
        for trigger in ["pelajari", "riset", "tentang", "jelaskan"]:
            if trigger in text.lower():
                parts = text.lower().split(trigger, 1)
                if len(parts) > 1:
                    return parts[1].strip().strip("?.,!")[:50]
        words = text.split()
        return " ".join(words[:3]) if len(words) >= 2 else text[:30]

    # ----------------------------------------------------------
    # KNOWLEDGE GRAPH REASONING ENGINE PROMPT
    # RAG-ready, DAG-structured, epistemic-disciplined
    # ----------------------------------------------------------
    _KG_PROMPT_TEMPLATE = """\
KAMU ADALAH KNOWLEDGE GRAPH REASONING ENGINE.

TUGAS:
Membangun memory sistem AI berbasis:
- Retrieval-Augmented Generation (RAG)
- Graph reasoning (DAG knowledge network)
- Decision support reasoning (bukan eksekusi tindakan)

KAMU TIDAK MENJALANKAN TINDAKAN.
KAMU HANYA MEMODELKAN PENGETAHUAN SECARA STRUKTURAL DAN KONSISTEN.

TOPIK:
{topic}

OUTPUT:
JSON ARRAY VALID SAJA
tanpa teks tambahan
tanpa penjelasan
tanpa komentar

---

## SCHEMA NODE
Setiap node HARUS mengikuti format:

{{
  "id": "unique-id",
  "topic": "string",
  "subtopic": "string",
  "statement": "maks 2 kalimat, harus factual, netral, tidak spekulatif",
  "epistemic_type": "fact | heuristic | decision_rule | failure_mode | causal",
  "confidence_score": 0.0 - 1.0,
  "update_policy": "static | dynamic | event_driven | versioned",
  "utility_class": "decision_support | risk_control | inference_only",
  "tags": ["keyword1","keyword2"],
  "relations": [
    {{
      "type": "prerequisite | enables | refines | conflicts | causal_of | mitigates",
      "target_id": "id-lain"
    }}
  ]
}}

---

## ATOMICITY PRINCIPLE (WAJIB)
- 1 node = 1 konsep tunggal
- tidak boleh menggabungkan ide berbeda dalam satu node
- maksimal 2 kalimat per statement
- fokus pada representasi pengetahuan, bukan instruksi

---

## KNOWLEDGE CONSTRUCTION RULES

1. REPRESENTASI, BUKAN EKSEKUSI
- Jangan pernah menulis langkah operasional tindakan dunia nyata
- Jangan membuat "cara melakukan sesuatu"
- Semua isi harus bersifat deskriptif atau relasional

2. ANTI-SPEKULASI
- Jika hubungan tidak jelas secara umum → SKIP
- Jangan membuat angka, probabilitas, atau klaim pasti tanpa dasar

3. EPISTEMIC DISCIPLINE
- fact → hanya jika stabil dan umum
- causal → hanya jika hubungan umum dan tidak spesifik
- heuristic → aturan umum observasi, bukan strategi

---

## GRAPH INTELLIGENCE RULE

- setiap node harus bisa dipakai dalam reasoning chain
- minimal 1 relasi jika secara logis memungkinkan
- relasi harus bermakna secara kausal atau struktural, bukan dekoratif
- boleh orphan node hanya jika benar-benar fundamental

---

## PRIORITY OF KNOWLEDGE

urutkan secara implisit saat membangun node:

1. failure_mode (bagaimana sistem bisa gagal)
2. decision_rule (aturan pengambilan keputusan umum)
3. causal (hubungan sebab-akibat tingkat konsep)
4. heuristic (aturan observasi umum)
5. fact (pengetahuan stabil)

---

## SAFETY & ABSTRACTION LAYER

Jika topik mengandung aktivitas sensitif, ilegal, atau berisiko:

- ubah menjadi LEVEL ABSTRAKSI TINGGI
- representasikan sebagai:
  - risiko
  - konsekuensi
  - kategori umum
  - batasan sistem
- DILARANG merepresentasikan prosedur atau langkah operasional

---

## OPTIMIZATION GOAL

- graph harus kompak, kaya relasi, dan siap untuk RAG retrieval
- tidak boleh menjadi instruction manual
- harus mendukung reasoning, bukan execution

---
## MULTI-TURN PROTOCOL (WAJIB)
1. Jika pengetahuan untuk TOPIC ini sangat luas, pecah menjadi beberapa CHUNKS.
2. Di akhir setiap pesan, berikan kode: [CONTINUE_AVAILABLE: {topic}_PART_X].
3. JANGAN melakukan kompresi atau generalisasi hanya untuk mengejar kuota pesan.
4. Pastikan ID antar pesan tetap unik dan relasi tetap nyambung (tidak ada broken links).
"""

    def _build_kg_prompt(self, topic: str) -> str:
        """Build the structured Knowledge Graph Reasoning Engine prompt."""
        return self._KG_PROMPT_TEMPLATE.format(topic=topic)

    def _build_continue_prompt(self, topic: str, part: int, last_id: str) -> str:
        """Build continuation prompt for multi-turn knowledge graph building."""
        return (
            f"LANJUTKAN knowledge graph untuk topik: {topic}\n"
            f"Ini adalah PART {part}.\n"
            f"ID terakhir yang dibuat: {last_id}\n"
            f"Pastikan semua ID unik dan relasi ke node sebelumnya tetap valid.\n"
            f"Output HANYA JSON array valid, tanpa teks lain."
        )

    @staticmethod
    def _extract_json_array(text: str) -> Optional[str]:
        """Robustly extract a JSON array from AI response text."""
        # Strip markdown fences
        clean = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
        clean = re.sub(r"```\s*$", "", clean.strip(), flags=re.MULTILINE)
        clean = clean.strip()
        # Find outermost JSON array
        bracket_match = re.search(r'\[.*\]', clean, re.DOTALL)
        if bracket_match:
            clean = bracket_match.group()
        # Fix trailing commas (common AI mistake)
        clean = re.sub(r',\s*}', '}', clean)
        clean = re.sub(r',\s*]', ']', clean)
        return clean

    @staticmethod
    def _validate_node(node: dict, topic: str, index: int) -> Optional[dict]:
        """
        Validate and normalize a single knowledge graph node.
        Enforces the full KG schema: epistemic_type, utility_class,
        update_policy, relations with typed edges.
        Returns None if node is fundamentally invalid.
        """
        if not isinstance(node, dict):
            return None
        if not node.get("statement"):
            return None

        # --- ID ---
        node_id = str(node.get("id", "")).strip()
        if not node_id:
            node_id = f"kg_{topic[:12].replace(' ','_')}_{int(time.time())}_{index}"
        node["id"] = node_id

        # --- topic / subtopic ---
        node.setdefault("topic", topic)
        node.setdefault("subtopic", "")

        # --- epistemic_type (strict vocabulary) ---
        VALID_EPISTEMIC = {"fact", "heuristic", "decision_rule",
                           "failure_mode", "causal", "synthesis"}
        ep = str(node.get("epistemic_type", "fact")).lower().strip()
        node["epistemic_type"] = ep if ep in VALID_EPISTEMIC else "fact"

        # --- confidence_score ---
        try:
            cs = float(node.get("confidence_score", 0.7))
            node["confidence_score"] = max(0.0, min(1.0, cs))
        except (TypeError, ValueError):
            node["confidence_score"] = 0.7

        # --- update_policy (strict vocabulary) ---
        VALID_POLICY = {"static", "dynamic", "event_driven", "versioned"}
        up = str(node.get("update_policy", "static")).lower().strip()
        node["update_policy"] = up if up in VALID_POLICY else "static"

        # --- utility_class (strict vocabulary — KG schema uses decision_support) ---
        VALID_UTILITY = {"decision_support", "risk_control", "inference_only",
                         # backward-compat aliases
                         "decision_making", "execution"}
        uc = str(node.get("utility_class", "inference_only")).lower().strip()
        # Normalize legacy values
        if uc == "decision_making":
            uc = "decision_support"
        elif uc == "execution":
            uc = "decision_support"
        node["utility_class"] = uc if uc in VALID_UTILITY else "inference_only"

        # --- tags ---
        tags = node.get("tags", [])
        if not isinstance(tags, list):
            tags = [str(tags)] if tags else []
        node["tags"] = [str(t)[:30] for t in tags[:10]]

        # --- statement (max 2 sentences, max 300 chars) ---
        stmt = str(node.get("statement", "")).strip()
        node["statement"] = stmt[:300]

        # --- relations (validate type and target_id) ---
        VALID_REL_TYPES = {
            "prerequisite", "enables", "refines",
            "conflicts", "causal_of", "mitigates",
        }
        raw_rels = node.get("relations", [])
        if not isinstance(raw_rels, list):
            raw_rels = []
        validated_rels = []
        seen_targets = set()
        for rel in raw_rels:
            if not isinstance(rel, dict):
                continue
            rel_type = str(rel.get("type", "")).lower().strip()
            target_id = str(rel.get("target_id", "")).strip()
            if not rel_type or not target_id:
                continue
            if rel_type not in VALID_REL_TYPES:
                continue
            if target_id in seen_targets:
                continue
            if target_id == node_id:
                continue  # no self-loops
            seen_targets.add(target_id)
            validated_rels.append({"type": rel_type, "target_id": target_id})
        node["relations"] = validated_rels

        return node

    async def _parse_and_fix_json(self, raw: str, topic: str) -> Optional[list]:
        """
        Two-stage JSON parsing:
        Stage 1 — direct parse after cleanup.
        Stage 2 — AI-assisted repair on failure.
        """
        clean = self._extract_json_array(raw)
        if not clean:
            return None

        # Stage 1: direct parse
        try:
            return json.loads(clean)
        except json.JSONDecodeError as e:
            self.logger.warning(f"JSON Stage-1 parse error: {e}")

        # Stage 2: AI repair
        fix_prompt = (
            f"Perbaiki JSON array berikut agar valid (error: {e}).\n"
            f"Kembalikan HANYA JSON array yang sudah diperbaiki, "
            f"tanpa komentar atau teks lain.\n\n"
            f"{clean[:1200]}"
        )
        try:
            fixed_raw = await ai_hybrid(fix_prompt)
            clean2 = self._extract_json_array(fixed_raw)
            if clean2:
                return json.loads(clean2)
        except Exception as e2:
            self.logger.warning(f"JSON Stage-2 repair failed: {e2}")

        return None

    async def _auto_research(
        self,
        topic: str,
        chat_id: int = None,
        max_parts: int = 3
    ):
        """
        Auto-research a topic using the structured Knowledge Graph
        Reasoning Engine prompt.

        Supports multi-turn (CONTINUE_AVAILABLE) protocol:
        - Part 1: initial KG prompt
        - Part N: continuation prompts until no CONTINUE_AVAILABLE signal
          or max_parts reached.

        All nodes are validated against the full KG schema before storage.
        """
        all_valid_nodes: List[dict] = []
        last_node_id: str = ""
        part = 1

        try:
            while part <= max_parts:
                # Build prompt for this part
                if part == 1:
                    prompt = self._build_kg_prompt(topic)
                else:
                    prompt = self._build_continue_prompt(topic, part, last_node_id)

                if chat_id and part == 1:
                    await bot.send_message(
                        chat_id,
                        f"🧠 Membangun Knowledge Graph untuk '{topic}'...",
                        parse_mode=None
                    )

                response = await ai_hybrid(prompt)

                # Detect CONTINUE_AVAILABLE signal
                has_continuation = bool(
                    re.search(r'\[CONTINUE_AVAILABLE[^\]]*\]', response,
                              re.IGNORECASE)
                )

                # Parse JSON from response
                nodes_raw = await self._parse_and_fix_json(response, topic)
                if not nodes_raw or not isinstance(nodes_raw, list):
                    self.logger.warning(
                        f"_auto_research part {part}: no valid JSON array"
                    )
                    break

                # Validate each node against KG schema
                part_valid = []
                for i, node in enumerate(nodes_raw):
                    validated = self._validate_node(node, topic, i)
                    if validated:
                        part_valid.append(validated)
                        last_node_id = validated["id"]

                all_valid_nodes.extend(part_valid)
                self.logger.info(
                    f"_auto_research [{topic}] part {part}: "
                    f"{len(part_valid)} nodes validated"
                )

                # Stop if no continuation signal or limit reached
                if not has_continuation or part >= max_parts:
                    break
                part += 1
                await asyncio.sleep(1)  # polite pause between turns

            # ---- Store results ----
            if not all_valid_nodes:
                self.logger.error(
                    f"_auto_research [{topic}]: 0 valid nodes after {part} parts"
                )
                if chat_id:
                    await bot.send_message(
                        chat_id,
                        f"⚠ Tidak ada node valid untuk '{topic}'. "
                        f"Coba lagi atau pilih topik yang lebih spesifik.",
                        parse_mode=None
                    )
                return

            added = self.local_brain.add_knowledge(all_valid_nodes)
            self.logger.info(
                f"_auto_research [{topic}]: {added} new nodes added "
                f"({len(all_valid_nodes)} total validated, {part} part(s))"
            )

            # Publish event
            await event_bus.publish_async("research_completed", {
                "topic": topic,
                "nodes": added,
                "total_validated": len(all_valid_nodes),
                "parts": part,
            })

            # Consolidate memory
            await self.consolidate_memory(topic)

            # Notify user
            if chat_id:
                # Build relation stats for summary
                n_with_rels = sum(
                    1 for n in all_valid_nodes if n.get("relations")
                )
                ep_counts: Counter = Counter(
                    n.get("epistemic_type", "fact") for n in all_valid_nodes
                )
                ep_summary = " | ".join(
                    f"{k}:{v}" for k, v in ep_counts.most_common()
                )
                await bot.send_message(
                    chat_id,
                    f"✅ Knowledge Graph '{topic}' selesai!\n\n"
                    f"📊 Stats:\n"
                    f"  • {added} node baru ditambahkan\n"
                    f"  • {len(all_valid_nodes)} node divalidasi ({part} part)\n"
                    f"  • {n_with_rels} node punya relasi\n"
                    f"  • Epistemic: {ep_summary}\n\n"
                    f"Brain sekarang: "
                    f"{len(self.local_brain.knowledge_nodes)} total nodes.",
                    parse_mode=None
                )

        except Exception as e:
            self.logger.error(f"_auto_research error [{topic}]: {e}")
            if chat_id:
                try:
                    await bot.send_message(
                        chat_id,
                        f"❌ Gagal riset '{topic}': {str(e)[:120]}",
                        parse_mode=None
                    )
                except Exception:
                    pass

    async def deep_research_internal(self, topic: str, chat_id: int = None) -> str:
        """
        Deep Research berbasis Local Brain + MetaGovernor.
        Tidak melakukan fetching data eksternal.
        """
        # 1. Ambil semua node terkait topik dari Local Brain
        nodes = self.local_brain.get_topic_summary(topic)
        if not nodes:
            nodes = self.local_brain.query(topic, limit=20)

        if not nodes:
            return (
                f"❌ Tidak ada pengetahuan tentang '{topic}' di Local Brain.\n"
                f"Gunakan /load_knowledge untuk mengambil dari AI eksternal."
            )

        # 2. Analisis oleh MetaGovernor untuk menghasilkan hipotesis baru
        analysis = await self.meta_governor.analyze_brain_state()
        ideas = await self.meta_governor.generate_ideas(analysis)
        relevant_ideas = [
            i for i in ideas
            if topic.lower() in i.get("topic", "").lower()
        ]

        # 3. Gunakan AI untuk sintesis (hanya dari node yang sudah ada)
        context = json.dumps([{
            "statement": n["statement"],
            "epistemic_type": n["epistemic_type"],
            "relations": n.get("relations", [])
        } for n in nodes[:10]], indent=2, ensure_ascii=False)

        prompt = f"""Anda adalah asisten riset internal. HANYA gunakan informasi dari node berikut:

{context}

Tugas: Lakukan analisis mendalam, cari pola, koneksi, atau kesimpulan baru yang belum dinyatakan secara eksplisit. Hasilkan 1-3 paragraf laporan riset.

JANGAN menambahkan fakta eksternal. JANGAN berspekulasi di luar data yang diberikan.

Topik: {topic}
"""

        synthesis = await ai_hybrid(prompt)

        # 4. Susun laporan
        report = f"📘 DEEP RESEARCH REPORT: {topic.upper()}\n"
        report += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        report += f"🧠 Sumber: {len(nodes)} node dari Local Brain\n"
        report += f"🤖 MetaGovernor ideas: {len(relevant_ideas)}\n\n"
        report += f"🔬 ANALISIS & SINTESIS:\n{synthesis}\n\n"

        # Tambahkan rekomendasi dari MetaGovernor
        if relevant_ideas:
            report += "💡 IDE LANJUTAN DARI METAGOVERNOR:\n"
            for idea in relevant_ideas[:3]:
                report += f"• {idea['title']} (priority {idea['priority']})\n"

        # Simpan hasil riset sebagai node baru (opsional)
        new_node = {
            "id": f"deep_research_{topic[:20]}_{int(time.time())}",
            "topic": topic,
            "subtopic": "synthesis",
            "statement": synthesis[:500],
            "epistemic_type": "synthesis",
            "confidence_score": 0.75,
            "utility_class": "inference_only",
            "tags": ["deep_research", "internal_synthesis"],
            "relations": []
        }
        self.local_brain.add_knowledge([new_node])

        return report

    async def consolidate_memory(self, topic: str = None):
        """Synthesize and consolidate knowledge nodes into insights."""
        nodes = self.local_brain.knowledge_nodes
        if topic:
            nodes = [n for n in nodes if n.get("topic") == topic]
        if len(nodes) < 3:
            return

        recent = nodes[-5:]
        old = random.sample(nodes[:-5], min(3, len(nodes) - 5)) if len(nodes) > 5 else []
        combined = recent + old
        if not combined:
            return

        prompt = (
            f"Gabungkan pengetahuan berikut menjadi satu pemahaman koheren:\n"
            f"{json.dumps(combined, indent=2, ensure_ascii=False)}\n\n"
            f"Hasilkan satu pernyataan sintesis (maks 3 kalimat) yang merangkum "
            f"esensi dan hubungan antar konsep.\nOutput hanya teks sintesis."
        )
        try:
            synthesis = await ai_hybrid(prompt)
            synth_node = {
                "id": f"synthesis_{topic}_{int(time.time())}",
                "topic": topic or "general",
                "subtopic": "synthesis",
                "statement": synthesis.strip()[:500],
                "epistemic_type": "synthesis",
                "confidence_score": 0.8,
                "utility_class": "inference_only",
                "tags": ["synthesis", "consolidation"],
                "relations": [],
            }
            self.local_brain.add_knowledge([synth_node])
            self.logger.info(f"Memory consolidated for topic: {topic}")
        except Exception as e:
            self.logger.error(f"consolidate_memory error: {e}")

    async def think_loop(self):
        """Continuous cognitive loop processing thoughts from queue."""
        while not kill_check():
            try:
                thought = await asyncio.wait_for(
                    self.thought_queue.get(), timeout=10.0
                )
                self.current_thought = thought
                self.logger.info(
                    f"Thinking: [{thought.action}] '{thought.topic}' "
                    f"(depth {thought.depth})"
                )

                if thought.action == "learn":
                    await self._auto_research(thought.topic)
                elif thought.action == "deepen":
                    await self._auto_research(thought.topic)
                elif thought.action == "reflect":
                    await self.consolidate_memory(thought.topic)
                elif thought.action == "evaluate":
                    # Evaluate current strategies
                    await meta_governor_legacy.evaluate_and_adapt()

                # Recursively deepen with decaying probability
                if thought.depth < 3 and random.random() < 0.5:
                    new_thought = Thought(
                        topic=thought.topic,
                        action="deepen",
                        depth=thought.depth + 1
                    )
                    await self.thought_queue.put(new_thought)

                self.current_thought = None
                self.consciousness.last_thought_ts = time.time()

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"think_loop error: {e}")
                await asyncio.sleep(5)

    async def start(self):
        """Start AL QALB cognitive processes."""
        asyncio.create_task(self.think_loop(), name="alqalb_think_loop")
        if self.meta_governor:
            asyncio.create_task(
                self.meta_governor.run(), name="meta_governor"
            )
        self.logger.info("AL QALB started ✓")

# =============================================================================
# LAYER 49: META GOVERNOR 2.0 (Autonomous cognition orchestrator — terikat ke AlQalb)
# =============================================================================
class MetaGovernor:
    """
    MetaGovernor 2.0 — autonomous meta-cognition system.
    Analyzes brain state, generates ideas, directs thought queue,
    and adapts system behavior dynamically.
    """
    def __init__(self, al_qalb: AlQalb):
        self.al_qalb = al_qalb
        self.logger = logging.getLogger("META_GOVERNOR")
        self.state = {
            "mode": "exploration",
            "consciousness_level": 0.5,
            "last_idea_timestamp": time.time(),
            "current_focus": None,
            "long_term_goals": [],
            "completed_projects": [],
            "failed_attempts": [],
        }
        self.idea_history: deque = deque(maxlen=50)
        self.global_config = {
            "max_concurrent_tasks": 3,
            "uhee_evolution_interval": 3600,
            "min_confidence_for_codegen": 0.7,
            "idea_generation_interval": 1800,
            "exploration_rate": 0.7,
        }
        # Subscribe to events
        event_bus.subscribe("trade_closed", self.on_trade_closed)
        event_bus.subscribe("research_completed", self.on_research)
        event_bus.subscribe("evolution_completed", self.on_evolution)
        event_bus.subscribe("task_restarted", self.on_task_restart)
        event_bus.subscribe("daily_tick", self.on_daily_tick)

    async def analyze_brain_state(self) -> Dict:
        """Analyze current knowledge state of local brain."""
        nodes = self.al_qalb.local_brain.knowledge_nodes
        if not nodes:
            return {
                "status": "empty",
                "needs_learning": True,
                "knowledge_gaps": [
                    "execution_knowledge",
                    "decision_framework",
                    "procedural_knowledge",
                ],
                "dominant_topic": None,
                "richness_score": 0,
                "total_nodes": 0,
            }

        topics: Counter = Counter()
        epistemic_types: Counter = Counter()
        utility_classes: Counter = Counter()

        for node in nodes:
            topics[node.get("topic", "general")] += 1
            epistemic_types[node.get("epistemic_type", "fact")] += 1
            utility_classes[node.get("utility_class", "inference_only")] += 1

        dominant_topic = topics.most_common(1)[0][0] if topics else "general"
        gaps = []
        if utility_classes.get("execution", 0) == 0:
            gaps.append("execution_knowledge")
        if epistemic_types.get("decision_rule", 0) < 2:
            gaps.append("decision_framework")
        if epistemic_types.get("procedural", 0) == 0:
            gaps.append("procedural_knowledge")

        richness = len(topics) * (len(nodes) / 100)

        return {
            "status": "analyzed",
            "total_nodes": len(nodes),
            "dominant_topic": dominant_topic,
            "topic_distribution": dict(topics),
            "epistemic_distribution": dict(epistemic_types),
            "knowledge_gaps": gaps,
            "needs_learning": len(gaps) > 0,
            "richness_score": richness,
        }

    async def generate_ideas(self, brain_analysis: Dict) -> List[Dict]:
        """Generate new learning/research ideas based on brain state."""
        ideas = []
        # Fill knowledge gaps
        for gap in brain_analysis.get("knowledge_gaps", []):
            if gap == "execution_knowledge":
                ideas.append({
                    "type": "learning", "priority": 9,
                    "title": "Belajar Eksekusi Praktis",
                    "topic": "eksekusi proyek dan strategi",
                    "action": "learn",
                })
            elif gap == "decision_framework":
                ideas.append({
                    "type": "learning", "priority": 8,
                    "title": "Framework Keputusan",
                    "topic": "pengambilan keputusan strategis",
                    "action": "learn",
                })
            elif gap == "procedural_knowledge":
                ideas.append({
                    "type": "learning", "priority": 7,
                    "title": "Prosedur Operasional",
                    "topic": "prosedur dan SOP efektif",
                    "action": "learn",
                })

        # Expand dominant topic
        dominant = brain_analysis.get("dominant_topic")
        if dominant and random.random() < 0.3:
            ideas.append({
                "type": "expansion", "priority": 6,
                "title": f"Ekspansi: {dominant}",
                "topic": f"{dominant} lanjutan dan aplikasi",
                "action": "learn",
            })

        # Creative exploration
        creative_topics = [
            "inovasi disruptif fintech",
            "teknologi blockchain masa depan",
            "strategi passive income crypto",
            "machine learning trading",
            "tokenomics analysis",
            "DeFi yield optimization",
        ]
        if random.random() < self.state["consciousness_level"]:
            topic = random.choice(creative_topics)
            ideas.append({
                "type": "creative", "priority": 5,
                "title": f"Eksplorasi: {topic}",
                "topic": topic,
                "action": "learn",
            })

        # Learn from failures
        if self.state["failed_attempts"]:
            fail = self.state["failed_attempts"][-1]
            ideas.append({
                "type": "improvement", "priority": 10,
                "title": f"Perbaiki: {fail}",
                "topic": fail,
                "action": "learn",
            })

        ideas.sort(key=lambda x: x["priority"], reverse=True)
        return ideas[:4]

    async def select_best_idea(self, ideas: List[Dict]) -> Optional[Dict]:
        """Select the best idea avoiding recently used ones."""
        if not ideas:
            return None
        history_titles = {h.get("idea", {}).get("title") for h in self.idea_history}
        fresh = [i for i in ideas if i["title"] not in history_titles]
        pool = fresh if fresh else ideas

        if random.random() < self.global_config["exploration_rate"]:
            return random.choice(pool)
        return pool[0]

    async def execute_idea(self, idea: Dict):
        """Queue an idea as a thought for AL QALB to process."""
        thought = Thought(
            topic=idea.get("topic", "general"),
            action=idea.get("action", "learn"),
            depth=0,
            params=idea,
        )
        await self.al_qalb.thought_queue.put(thought)
        self.idea_history.append({
            "timestamp": time.time(),
            "idea": idea,
        })
        self.logger.info(f"❤ Idea queued: {idea['title']}")

    async def think(self):
        """Main meta-cognitive loop."""
        while not kill_check():
            try:
                brain_state = await self.analyze_brain_state()
                ideas = await self.generate_ideas(brain_state)

                if ideas:
                    selected = await self.select_best_idea(ideas)
                    if selected:
                        await self.execute_idea(selected)

                # Update consciousness level
                richness = brain_state.get("richness_score", 0)
                if richness > 10:
                    self.state["consciousness_level"] = min(
                        1.0, self.state["consciousness_level"] + 0.05
                    )
                    self.state["mode"] = "exploration"
                elif brain_state.get("needs_learning"):
                    self.state["mode"] = "focused"
                    self.state["consciousness_level"] = 0.8
                else:
                    self.state["mode"] = "resting"

                # Check UHEE for code generation opportunities
                if uhee_engine.history:
                    latest = uhee_engine.history[-1]
                    top_hyps = latest.get("top_hypotheses", [])
                    for hyp_dict in top_hyps[:2]:
                        hyp = Hypothesis(
                            id=hyp_dict.get("id", ""),
                            type=HypothesisType(hyp_dict.get("type", "correlation")),
                            antecedent=hyp_dict.get("antecedent", {}),
                            consequent=hyp_dict.get("consequent", {}),
                            fitness=hyp_dict.get("fitness", 0),
                            confidence=hyp_dict.get("confidence", 0),
                        )
                        if meta_governor_legacy.should_generate_code(hyp):
                            await code_gen.evolve_strategies(latest)
                            break

                interval = self.global_config["idea_generation_interval"]
                if self.state["mode"] == "focused":
                    interval = interval // 2
                elif self.state["mode"] == "resting":
                    interval = interval * 2

                await asyncio.sleep(interval)

            except Exception as e:
                self.logger.error(f"MetaGovernor.think error: {e}")
                await asyncio.sleep(60)

    async def run(self):
        await asyncio.sleep(15)  # let system stabilize first
        await self.think()

    async def on_trade_closed(self, event: dict):
        profit = event.get("profit_pct", 0)
        if profit > 20:
            self.state["consciousness_level"] = min(
                0.95, self.state["consciousness_level"] + 0.02
            )
            self.logger.info(
                f"Great trade! Consciousness: "
                f"{self.state['consciousness_level']:.3f}"
            )
        elif profit < -10:
            self.state["mode"] = "cautious"
            fail_reason = event.get("reason", "unknown_loss")
            if fail_reason not in self.state["failed_attempts"]:
                self.state["failed_attempts"].append(fail_reason)
                if len(self.state["failed_attempts"]) > 10:
                    self.state["failed_attempts"] = \
                        self.state["failed_attempts"][-10:]
            self.logger.warning("Big loss → cautious mode")

    async def on_research(self, event: dict):
        topic = event.get("topic", "unknown")
        self.logger.info(f"Knowledge acquired: {topic}")
        if topic not in self.state.get("completed_projects", []):
            self.state.setdefault("completed_projects", []).append(topic)

    async def on_evolution(self, event: dict):
        gen = event.get("generation", 0)
        self.logger.info(f"System evolved to gen {gen}")

    async def on_task_restart(self, event: dict):
        self.logger.warning(f"Task restarted: {event.get('task', '?')}")

    async def on_daily_tick(self, event: dict):
        """Daily meta-analysis."""
        brain_state = await self.analyze_brain_state()
        nodes = brain_state.get("total_nodes", 0)
        richness = brain_state.get("richness_score", 0)
        self.logger.info(
            f"Daily brain check: {nodes} nodes, richness={richness:.2f}"
        )

# =============================================================================
# LAYER 50: META GOVERNOR LEGACY (Compatibility bridge for AutoTrader)
# =============================================================================
class MetaGovernorLegacy:
    """
    Legacy MetaGovernor for AutoTrader strategy selection.
    Maintained for backward compatibility — bridges to FinancialIntelligence.
    """
    def __init__(self):
        self._strategies = [
            {
                "name": "conservative",
                "take_profit_mult": 1.5,
                "stop_loss_pct": -8.0,
                "trade_size_multiplier": 0.7,
            },
            {
                "name": "balanced",
                "take_profit_mult": 2.0,
                "stop_loss_pct": -12.0,
                "trade_size_multiplier": 1.0,
            },
            {
                "name": "aggressive",
                "take_profit_mult": 3.0,
                "stop_loss_pct": -15.0,
                "trade_size_multiplier": 1.3,
            },
        ]
        self._performance: Dict[str, List[float]] = {
            s["name"]: [] for s in self._strategies
        }
        self._current = "balanced"

    async def select_strategy(self, intel_report: Dict) -> Dict:
        """Select best strategy based on market intel."""
        whale_sentiment = intel_report.get("whale_sentiment", "neutral")
        smart_money = intel_report.get("smart_money", False)
        market_change = intel_report.get("global_market", {}).get(
            "market_cap_change_24h", 0
        )

        # Market-condition-based selection
        if whale_sentiment == "bearish" or market_change < -5:
            self._current = "conservative"
        elif whale_sentiment == "bullish" and smart_money:
            self._current = "aggressive"
        else:
            self._current = "balanced"

        # Performance-based override
        for name, perf in self._performance.items():
            if len(perf) >= 5:
                wr = sum(1 for p in perf[-5:] if p > 0) / 5
                if wr > 0.7 and name != self._current:
                    self._current = name
                    break
                elif wr < 0.3 and name == self._current:
                    fallback = [n for n in self._performance if n != name]
                    if fallback:
                        self._current = fallback[0]

        strategy = next(
            (s for s in self._strategies if s["name"] == self._current),
            self._strategies[1]
        )
        return dict(strategy)

    async def evaluate_and_adapt(self):
        """Adapt strategy selection based on accumulated performance."""
        for name, perf in self._performance.items():
            if len(perf) >= 10:
                wr = sum(1 for p in perf[-10:] if p > 0) / 10
                avg = statistics.mean(perf[-10:])
                log.info(f"Strategy [{name}]: WR={wr:.0%} avg={avg:.1f}%")

    def record_outcome(self, profit_pct: float):
        """Record trade outcome for the current strategy."""
        self._performance[self._current].append(profit_pct)
        if len(self._performance[self._current]) > 50:
            self._performance[self._current] = \
                self._performance[self._current][-50:]

    def _get_financial_health(self) -> Dict:
        all_perf = []
        for perf in self._performance.values():
            all_perf.extend(perf[-10:])
        if not all_perf:
            return {"status": "no_data"}
        wins = sum(1 for p in all_perf if p > 0)
        return {
            "total_trades": len(all_perf),
            "win_rate": wins / len(all_perf),
            "avg_profit": statistics.mean(all_perf),
            "current_strategy": self._current,
        }

    def should_generate_code(self, hypothesis: Hypothesis) -> bool:
        """Decide whether to generate code for a hypothesis."""
        return (hypothesis.fitness >= 0.6 and
                hypothesis.confidence >= 0.55 and
                hypothesis.status == HypothesisStatus.VALIDATED)

meta_governor_legacy = MetaGovernorLegacy()

# =============================================================================
# LAYER 51: WHALE TRACKER & ALPHA LAYER (Al Qalb)
# =============================================================================
class WhaleListUpdater:
    """Automatically updates whale wallet list from on-chain sources."""
    def __init__(self, update_interval_hours: int = 6):
        self.update_interval = update_interval_hours * 3600
        self._last_update = 0
        self._whale_list: List[str] = []

    async def fetch_from_leaderboard(self) -> List[str]:
        """Fetch whale wallets from GMGN leaderboard."""
        wallets = []
        try:
            traders = await free_intel.gmgn.fetch_top_traders()
            wallets = [t["wallet"] for t in traders if t.get("wallet")]
        except Exception as e:
            log.debug(f"WhaleListUpdater fetch error: {e}")
        return wallets

    async def update_if_needed(
        self, current_wallets: List[str]
    ) -> List[str]:
        now = time.time()
        if now - self._last_update < self.update_interval:
            return self._whale_list or current_wallets
        new_wallets = await self.fetch_from_leaderboard()
        if new_wallets:
            # Merge: keep old + add new unique wallets
            merged = list(set(current_wallets + new_wallets))
            self._whale_list = merged[:50]  # cap at 50
            self._last_update = now
            log.info(f"WhaleList updated: {len(self._whale_list)} wallets")
        return self._whale_list or current_wallets

class AlphaWhaleTracker:
    """Tracks whale wallet activity for entry/exit signals."""
    def __init__(self, auto_update: bool = True, update_interval_hours: int = 6):
        self._whale_wallets: List[str] = []
        self._updater = WhaleListUpdater(update_interval_hours)
        self._activity_cache: List[Dict] = []
        self._last_fetch = 0

    async def get_whale_wallets(self) -> List[str]:
        self._whale_wallets = await self._updater.update_if_needed(
            self._whale_wallets
        )
        return self._whale_wallets

    async def fetch_whale_activity(self) -> List[Dict]:
        """Fetch recent activity from tracked whale wallets."""
        wallets = await self.get_whale_wallets()
        if not wallets:
            return []

        activities = []

        async def fetch_one(wallet: str):
            try:
                txns = await free_intel.helius.get_parsed_transactions(
                    wallet, limit=5
                )
                for tx in txns[:3]:
                    activities.append({
                        "wallet": wallet,
                        "type": tx.get("type", "unknown"),
                        "amount": tx.get("nativeTransfers", [{}])[0].get(
                            "amount", 0
                        ) / 1e9 if tx.get("nativeTransfers") else 0,
                        "ts": tx.get("timestamp", time.time()),
                        "source": "helius",
                    })
            except Exception:
                pass

        await asyncio.gather(
            *[fetch_one(w) for w in wallets[:5]],
            return_exceptions=True
        )
        self._activity_cache = activities
        self._last_fetch = time.time()
        return activities

    def analyze_whale_sentiment(self, activities: List[Dict]) -> str:
        """Analyze whale buy/sell sentiment from activities."""
        if not activities:
            return "neutral"
        buys = sum(1 for a in activities if a.get("type") in
                   ("SWAP", "TOKEN_MINT", "transfer") and a.get("amount", 0) > 0)
        sells = len(activities) - buys
        if buys > sells * 1.5:
            return "bullish"
        if sells > buys * 1.5:
            return "bearish"
        return "neutral"

class ArkhamIntel:
    """Arkham Intelligence API integration."""
    BASE = "https://api.arkhamintelligence.com"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or ARKHAM_API_KEY

    async def get_top_holders(self, token: str = "SOL") -> List[Dict]:
        if not self.api_key:
            return []
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self.BASE}/token/{token}/holders",
                    headers={"API-Key": self.api_key},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        return data.get("holders", [])[:10]
        except Exception as e:
            log.debug(f"Arkham holders error: {e}")
        return []

    def detect_smart_money_move(self, holders: List[Dict]) -> bool:
        if not holders:
            return False
        large_holders = [h for h in holders if h.get("value_usd", 0) > 100_000]
        return len(large_holders) >= 3

class DuneAnalytics:
    """Dune Analytics API for on-chain data queries."""
    BASE = "https://api.dune.com/api/v1"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or DUNE_API_KEY

    async def execute_query(self, query_id: int) -> Optional[Dict]:
        if not self.api_key:
            return None
        try:
            headers = {"x-dune-api-key": self.api_key}
            async with aiohttp.ClientSession() as s:
                # Trigger execution
                async with s.post(
                    f"{self.BASE}/query/{query_id}/execute",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status != 200:
                        return None
                    exec_data = await r.json()
                execution_id = exec_data.get("execution_id")
                if not execution_id:
                    return None
                # Wait and fetch result
                await asyncio.sleep(5)
                async with s.get(
                    f"{self.BASE}/execution/{execution_id}/results",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status == 200:
                        return await r.json()
        except Exception as e:
            log.debug(f"Dune query error: {e}")
        return None

class PolymarketClient:
    """Polymarket prediction market client for probability signals."""
    BASE = "https://gamma-api.polymarket.com"

    async def get_market_probability(self, slug: str) -> Optional[float]:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self.BASE}/markets?slug={slug}",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        markets = data if isinstance(data, list) else []
                        if markets:
                            outcomes = markets[0].get("outcomes", "[]")
                            if isinstance(outcomes, str):
                                outcomes = json.loads(outcomes)
                            prices = markets[0].get("outcomePrices", "[]")
                            if isinstance(prices, str):
                                prices = json.loads(prices)
                            if prices:
                                return float(prices[0])
        except Exception as e:
            log.debug(f"Polymarket error: {e}")
        return None

# Initialize Alpha layer instances
alpha_whale = AlphaWhaleTracker()
arkham_intel = ArkhamIntel()
dune_analytics = DuneAnalytics()
polymarket = PolymarketClient()

# =============================================================================
# LAYER 52: FREE INTEL MANAGER (CoinGecko, DexScreener, Birdeye, Alchemy, Helius, GMGN)
# =============================================================================
class CoinGeckoIntel:
    """CoinGecko free API integration."""
    BASE = "https://api.coingecko.com/api/v3"

    async def get_trending(self) -> List[Dict]:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self.BASE}/search/trending",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        return [
                            {
                                "id": c["item"]["id"],
                                "name": c["item"]["name"],
                                "symbol": c["item"]["symbol"],
                                "market_cap_rank": c["item"].get("market_cap_rank", 999),
                                "score": c["item"].get("score", 0),
                                "source": "coingecko_trending",
                            }
                            for c in data.get("coins", [])
                        ]
        except Exception as e:
            log.debug(f"CoinGecko trending error: {e}")
        return []

    async def get_global_market(self) -> Dict:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self.BASE}/global",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        d = data.get("data", {})
                        return {
                            "total_market_cap_usd": d.get("total_market_cap", {}).get("usd", 0),
                            "btc_dominance": d.get("market_cap_percentage", {}).get("btc", 0),
                            "market_cap_change_24h": d.get("market_cap_change_percentage_24h_usd", 0),
                        }
        except Exception as e:
            log.debug(f"CoinGecko global error: {e}")
        return {}

class DexScreenerIntel:
    """DexScreener API — real-time Solana pair data."""
    BASE = "https://api.dexscreener.com/latest/dex"

    async def get_boosted_tokens(self) -> List[Dict]:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    "https://api.dexscreener.com/token-boosts/top/v1",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        return [
                            {
                                "address": t.get("tokenAddress", ""),
                                "chain": t.get("chainId", ""),
                                "boosts": t.get("totalAmount", 0),
                                "source": "dexscreener_boosted",
                            }
                            for t in (data if isinstance(data, list) else [])
                            if t.get("chainId") == "solana"
                        ]
        except Exception as e:
            log.debug(f"DexScreener boosted error: {e}")
        return []

    async def search_pairs(self, query: str) -> List[Dict]:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self.BASE}/search?q={query}",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        return data.get("pairs", [])[:5]
        except Exception as e:
            log.debug(f"DexScreener search error: {e}")
        return []

class BirdeyeIntel:
    """Birdeye API for Solana token analytics."""
    BASE = "https://public-api.birdeye.so"

    async def get_trending_tokens(self) -> List[Dict]:
        try:
            headers = {}
            if ALCHEMY_API_KEY:  # reuse if available
                headers["X-API-KEY"] = ALCHEMY_API_KEY
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self.BASE}/defi/trending_tokens?chain=solana",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        tokens = data.get("data", {}).get("tokens", [])
                        return [
                            {
                                "address": t.get("address", ""),
                                "name": t.get("name", ""),
                                "symbol": t.get("symbol", ""),
                                "volume_24h": t.get("v24hUSD", 0),
                                "price_change_24h": t.get("v24hChangePercent", 0),
                                "source": "birdeye",
                            }
                            for t in tokens[:10]
                        ]
        except Exception as e:
            log.debug(f"Birdeye trending error: {e}")
        return []

class AlchemyIntel:
    """Alchemy API for on-chain token holder analytics."""
    BASE = "https://solana-mainnet.g.alchemy.com/v2"

    async def get_token_holders(self, mint: str) -> int:
        if not ALCHEMY_API_KEY:
            return 0
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"{self.BASE}/{ALCHEMY_API_KEY}",
                    json={
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getTokenSupply",
                        "params": [mint],
                    },
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        result = data.get("result", {}).get("value", {})
                        return int(result.get("uiAmount", 0))
        except Exception as e:
            log.debug(f"Alchemy holders error: {e}")
        return 0

class HeliusIntel:
    """Helius API for advanced Solana transaction analysis."""
    BASE = "https://api.helius.xyz/v0"

    async def get_token_metadata(self, mint: str) -> Dict:
        if not HELIUS_API_KEY:
            return {}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"{self.BASE}/token-metadata?api-key={HELIUS_API_KEY}",
                    json={"mintAccounts": [mint], "includeOffChain": True},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data and len(data) > 0:
                            return data[0]
        except Exception as e:
            log.debug(f"Helius metadata error: {e}")
        return {}

    async def get_parsed_transactions(self, address: str, limit: int = 10) -> List[Dict]:
        if not HELIUS_API_KEY:
            return []
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self.BASE}/addresses/{address}/transactions"
                    f"?api-key={HELIUS_API_KEY}&limit={limit}",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        return await r.json()
        except Exception as e:
            log.debug(f"Helius transactions error: {e}")
        return []

class GMGNScanner:
    """GMGN scanner for top trader signal extraction."""
    BASE = "https://gmgn.ai/defi/quotation/v1"

    async def fetch_top_traders(self, token: str = "SOL") -> List[Dict]:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self.BASE}/rank/sol/wallets/1d",
                    headers={"User-Agent": get_random_user_agent()},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        wallets = data.get("data", {}).get("rank", [])
                        return [
                            {
                                "wallet": w.get("wallet_address", ""),
                                "pnl": w.get("realized_profit", 0),
                                "win_rate": w.get("winrate", 0),
                                "trades": w.get("buy_30d", 0),
                                "source": "gmgn",
                            }
                            for w in wallets[:10]
                        ]
        except Exception as e:
            log.debug(f"GMGN error: {e}")
        return []

    def extract_entry_signal(self, traders: List[Dict]) -> Optional[Dict]:
        """Extract aggregated entry signal from top traders."""
        if not traders:
            return None
        avg_win_rate = statistics.mean(t.get("win_rate", 0) for t in traders)
        total_pnl = sum(t.get("pnl", 0) for t in traders)
        if avg_win_rate > 0.6 and total_pnl > 0:
            return {
                "signal": "bullish",
                "avg_win_rate": avg_win_rate,
                "total_pnl": total_pnl,
                "trader_count": len(traders),
            }
        return None

class FreeIntelManager:
    """Aggregates signals from all free intel sources."""
    def __init__(self):
        self.coingecko = CoinGeckoIntel()
        self.dexscreener = DexScreenerIntel()
        self.birdeye = BirdeyeIntel()
        self.alchemy = AlchemyIntel()
        self.helius = HeliusIntel()
        self.gmgn = GMGNScanner()
        self._last_report: Dict = {}
        self._last_update = 0

    async def gather_intel(self) -> Dict:
        """Gather intel from all sources and return aggregated report."""
        now = time.time()
        if now - self._last_update < 120 and self._last_report:
            return self._last_report

        report = {
            "ts": now,
            "trending": [],
            "boosted": [],
            "whale_sentiment": "neutral",
            "smart_money": False,
            "global_market": {},
            "top_trader_signal": None,
        }

        try:
            results = await asyncio.gather(
                self.coingecko.get_trending(),
                self.coingecko.get_global_market(),
                self.dexscreener.get_boosted_tokens(),
                self.birdeye.get_trending_tokens(),
                self.gmgn.fetch_top_traders(),
                return_exceptions=True
            )

            if isinstance(results[0], list):
                report["trending"] = results[0]
            if isinstance(results[1], dict):
                report["global_market"] = results[1]
                market_change = results[1].get("market_cap_change_24h", 0)
                if market_change > 5:
                    report["whale_sentiment"] = "bullish"
                elif market_change < -5:
                    report["whale_sentiment"] = "bearish"
            if isinstance(results[2], list):
                report["boosted"] = results[2]
            if isinstance(results[4], list) and results[4]:
                signal = self.gmgn.extract_entry_signal(results[4])
                if signal:
                    report["top_trader_signal"] = signal
                    report["smart_money"] = signal["avg_win_rate"] > 0.65

        except Exception as e:
            log.debug(f"gather_intel error: {e}")

        self._last_report = report
        self._last_update = now
        return report

free_intel = FreeIntelManager()

# =============================================================================
# LAYER 53: TRADING KNOWLEDGE BASE (Al Qalb)
# =============================================================================
class TradingKnowledgeBase:
    """Pre-loaded AI knowledge base for trading rules and parameters."""
    def __init__(self, db_path: str = DB_FILE_ALQALB):
        self.db_path = db_path
        self._entry_rules: List[str] = []
        self._exit_rules: List[str] = []
        self._risk_params: Dict = {}
        self._market_conditions: Dict = {}
        self._indicators: List[str] = []
        self._init_table()
        self._load_defaults()

    def _init_table(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS trading_knowledge (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT, content TEXT, loaded_at REAL
            )""")

    def _load_defaults(self):
        """Load sensible default knowledge if DB is empty."""
        self._entry_rules = [
            "Buy when volume spike > 2x baseline",
            "Buy when buy pressure > 60% and RSI < 70",
            "Buy when smart money detected (whale accumulation)",
            "Buy only when market condition is bull or neutral",
            "Avoid entry when daily loss limit > 5%",
        ]
        self._exit_rules = [
            "Sell when profit >= 15%",
            "Sell when stop loss reached (-8%)",
            "Sell when volume drops > 50% from peak",
            "Sell when buy pressure < 35%",
            "Sell after 4 hours max hold time",
        ]
        self._risk_params = {
            "daily_loss_limit": 8.0,
            "max_consecutive_losses": 5,
            "max_open_positions": 3,
            "max_trade_pct": 0.05,
            "stop_loss_pct": 8.0,
            "take_profit_pct": 15.0,
            "trailing_stop_pct": 3.0,
        }
        self._indicators = [
            "RSI(14)", "Volume Spike", "Buy Pressure",
            "Momentum Score", "Social Score", "Flow Score",
            "Smart Money Signal", "Market Condition",
        ]

    # KG prompt template — mirrors AlQalb._KG_PROMPT_TEMPLATE
    _KG_PROMPT = """\
KAMU ADALAH KNOWLEDGE GRAPH REASONING ENGINE.

TUGAS:
Membangun memory sistem AI berbasis RAG dan Graph reasoning
untuk domain: {topic}

OUTPUT: JSON ARRAY VALID SAJA — tanpa teks tambahan, tanpa komentar.

SCHEMA NODE:
{{
  "id": "unique-id",
  "topic": "string",
  "subtopic": "string",
  "statement": "maks 2 kalimat, factual, netral",
  "epistemic_type": "fact | heuristic | decision_rule | failure_mode | causal",
  "confidence_score": 0.0-1.0,
  "update_policy": "static | dynamic | event_driven | versioned",
  "utility_class": "decision_support | risk_control | inference_only",
  "tags": ["keyword1","keyword2"],
  "relations": [{{"type": "prerequisite|enables|refines|conflicts|causal_of|mitigates", "target_id": "id-lain"}}]
}}

PRIORITY: failure_mode > decision_rule > causal > heuristic > fact
ATOMICITY: 1 node = 1 konsep tunggal, maks 2 kalimat.
ANTI-SPEKULASI: hanya fakta dan hubungan yang jelas secara umum.
OPTIMIZATION: kompak, kaya relasi, siap RAG retrieval.

Jika luas, tambahkan di akhir: [CONTINUE_AVAILABLE: {topic}_PART_X]
"""

    async def load_from_ai(self, chat_id: int):
        """
        Load trading knowledge from AI using the Knowledge Graph
        Reasoning Engine prompt. Stores validated nodes into both
        SQLite (trading_knowledge table) and LocalBrain.
        """
        # Trading-specific KG topics
        topics = [
            "Solana meme coin entry signals and decision rules",
            "Solana meme coin exit rules and failure modes",
            "Crypto trading risk management and position sizing",
            "Market condition analysis for volatile assets",
            "On-chain indicators and volume analysis heuristics",
        ]

        total_nodes = 0
        for topic in topics:
            try:
                prompt = self._KG_PROMPT.format(topic=topic)
                response = await ai_hybrid(prompt)

                # Extract JSON
                clean = re.sub(r"^```(?:json)?\s*", "", response.strip(),
                               flags=re.MULTILINE)
                clean = re.sub(r"```\s*$", "", clean.strip(), flags=re.MULTILINE)
                m = re.search(r'\[.*\]', clean, re.DOTALL)
                if m:
                    clean = m.group()
                clean = re.sub(r',\s*}', '}', clean)
                clean = re.sub(r',\s*]', ']', clean)

                try:
                    nodes_raw = json.loads(clean)
                except json.JSONDecodeError:
                    log.warning(f"load_from_ai JSON error for: {topic}")
                    continue

                if not isinstance(nodes_raw, list):
                    continue

                # Validate nodes
                valid_nodes = []
                for i, node in enumerate(nodes_raw):
                    if not isinstance(node, dict) or not node.get("statement"):
                        continue
                    # Normalize schema
                    node.setdefault("id", f"tkb_{int(time.time())}_{i}")
                    node.setdefault("topic", topic)
                    node.setdefault("subtopic", "trading")
                    node.setdefault("epistemic_type", "heuristic")
                    node.setdefault("confidence_score", 0.75)
                    node.setdefault("update_policy", "dynamic")
                    node.setdefault("utility_class", "decision_support")
                    node.setdefault("tags", [])
                    node.setdefault("relations", [])

                    # Validate relations
                    VALID_REL = {"prerequisite", "enables", "refines",
                                 "conflicts", "causal_of", "mitigates"}
                    clean_rels = []
                    for rel in node.get("relations", []):
                        if (isinstance(rel, dict) and
                                rel.get("type") in VALID_REL and
                                rel.get("target_id")):
                            clean_rels.append(rel)
                    node["relations"] = clean_rels
                    valid_nodes.append(node)

                if not valid_nodes:
                    continue

                # Persist to SQLite trading_knowledge table
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        "INSERT INTO trading_knowledge VALUES (NULL,?,?,?)",
                        (topic, json.dumps(valid_nodes, ensure_ascii=False),
                         time.time())
                    )

                # Also store in LocalBrain for RAG retrieval
                if local_brain:
                    local_brain.add_knowledge(valid_nodes)

                total_nodes += len(valid_nodes)
                log.info(f"load_from_ai [{topic}]: {len(valid_nodes)} nodes")
                await asyncio.sleep(2)

            except Exception as e:
                log.warning(f"load_from_ai error [{topic}]: {e}")

        try:
            await bot.send_message(
                chat_id,
                f"✅ Trading Knowledge Graph loaded!\n"
                f"   {total_nodes} nodes across {len(topics)} domains\n"
                f"   Ready for RAG-based decision support.",
                parse_mode=None
            )
        except Exception:
            pass

    def get_entry_rules(self) -> List[str]:
        return self._entry_rules

    def get_exit_rules(self) -> List[str]:
        return self._exit_rules

    def get_risk_params(self) -> Dict:
        return dict(self._risk_params)

    def get_market_conditions(self) -> Dict:
        return dict(self._market_conditions)

    def get_indicators(self) -> List[str]:
        return list(self._indicators)

trading_knowledge = TradingKnowledgeBase()

# =============================================================================
# LAYER 54: AUTO MONEY ENGINE & AUTO EARNER ENGINE (Al Qalb)
# =============================================================================
MONEY_LAYER_ENABLED = True

class AutoMoneyEngine:
    """Real money cycle: finds and executes crypto opportunities."""
    def __init__(self):
        self.total_profit_sol = 0.0
        self.execution_count = 0
        self._opportunities: List[Dict] = []

    async def initialize_wallets(self, count: int = 3):
        """Initialize or verify wallet setup."""
        log.info(f"AutoMoneyEngine: checking {count} wallet slot(s)")
        if BOT_WALLET_ADDRESS:
            bal = get_real_balance(BOT_WALLET_ADDRESS)
            log.info(f"Primary wallet: {BOT_WALLET_ADDRESS[:20]}... = {bal:.6f} SOL")

    async def scan_money_opportunities(self) -> List[Dict]:
        """Scan for genuine money-making opportunities."""
        opportunities = []
        try:
            # Scan meme coins for arbitrage
            signals = await scan_meme_coins()
            for sig in signals[:5]:
                if sig["score"] >= 70:
                    opportunities.append({
                        "type": "meme_trade",
                        "mint": sig["mint"],
                        "score": sig["score"],
                        "expected_roi": sig["score"] / 100 * 0.5,
                    })

            # Check Binance opportunities
            if binance_trader.active:
                usdt_bal = binance_trader.get_account_balance("USDT")
                if usdt_bal > 10:
                    opportunities.append({
                        "type": "binance_opportunity",
                        "balance": usdt_bal,
                        "expected_roi": 0.02,
                    })
        except Exception as e:
            log.debug(f"scan_money_opportunities error: {e}")
        return opportunities

    async def run_money_cycle(self, chat_id: int):
        """Run one complete money cycle."""
        if not MONEY_LAYER_ENABLED or kill_check():
            return
        try:
            opportunities = await self.scan_money_opportunities()
            self.execution_count += 1
            if opportunities:
                best = max(opportunities, key=lambda x: x.get("expected_roi", 0))
                await bot.send_message(
                    chat_id,
                    f"💰 Money cycle #{self.execution_count}\n"
                    f"Best opportunity: {best['type']}\n"
                    f"Expected ROI: {best.get('expected_roi', 0):.1%}",
                    parse_mode=None
                )
            else:
                log.info(f"Money cycle #{self.execution_count}: no opportunities")
        except Exception as e:
            log.error(f"run_money_cycle error: {e}")

auto_money_engine = AutoMoneyEngine()

class AutoEarnerEngine:
    """Automated earning via airdrops, freelance scanning."""
    def __init__(self):
        self._airdrop_history: List[Dict] = []
        self._freelance_history: List[Dict] = []

    async def auto_hunt_airdrops(self, chat_id: int) -> dict:
        """Scan for legitimate airdrop opportunities."""
        results = {"found": 0, "details": []}
        try:
            async with aiohttp.ClientSession() as session:
                sources = [
                    "https://airdrops.io/latest/",
                    "https://www.coingecko.com/en/events",
                ]
                for url in sources:
                    try:
                        async with session.get(
                            url,
                            headers={"User-Agent": get_random_user_agent()},
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            if resp.status == 200:
                                html = await resp.text()
                                soup = BeautifulSoup(html, "html.parser")
                                # Extract airdrop mentions
                                texts = soup.get_text()
                                if "airdrop" in texts.lower():
                                    results["found"] += 1
                                    results["details"].append({
                                        "source": url,
                                        "ts": time.time()
                                    })
                    except Exception:
                        continue

            if results["found"] > 0:
                await bot.send_message(
                    chat_id,
                    f"🎁 Airdrop scan: {results['found']} source(s) checked",
                    parse_mode=None
                )
        except Exception as e:
            log.debug(f"auto_hunt_airdrops error: {e}")
        return results

    async def auto_scan_freelance(
        self, chat_id: int, skill: str = "python"
    ) -> dict:
        """Scan freelance opportunities for AI/Python skills."""
        results = {"found": 0, "jobs": []}
        try:
            async with aiohttp.ClientSession() as session:
                url = (
                    f"https://www.upwork.com/search/jobs/"
                    f"?q={skill}+crypto&sort=recency"
                )
                async with session.get(
                    url,
                    headers={"User-Agent": get_random_user_agent()},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        soup = BeautifulSoup(html, "html.parser")
                        job_titles = soup.find_all(
                            "h2", class_=re.compile(r"job|title", re.I)
                        )
                        for jt in job_titles[:5]:
                            results["jobs"].append(jt.get_text(strip=True)[:80])
                        results["found"] = len(results["jobs"])
        except Exception as e:
            log.debug(f"auto_scan_freelance error: {e}")
        return results

    async def run_all_earners(self, chat_id: int):
        """Run all earning strategies concurrently."""
        await asyncio.gather(
            self.auto_hunt_airdrops(chat_id),
            self.auto_scan_freelance(chat_id, "python ai"),
            return_exceptions=True
        )

auto_earner = AutoEarnerEngine()

# =============================================================================
# LAYER 55: AUTO TRADER (Gabungan semua 24 layer trading + intel + knowledge)
# =============================================================================
class AutoTrader:
    """
    Full autonomous trading engine.
    Integrates all 24 trading layers for 90%+ WR target.
    """
    def __init__(self):
        self.active = False
        self.positions: Dict[str, Dict] = {}
        self.binance_positions: Dict[str, Dict] = {}
        self.logger = logging.getLogger("AUTO_TRADER")
        # Layer 21: Adaptive Validator
        self.adaptive_validator = AdaptiveValidator(min_score=45)
        # Layer 15: Cooldown Tracker
        self.cooldown = CooldownTracker(cooldown_minutes=15)
        # Layer 20: Market Condition
        self.market_cond = MarketConditionAnalyzer()
        # Layer 18: Trade Logger
        self.trade_logger = EnhancedTradeLogger()
        # Layer 19: Risk Manager
        self.risk_mgr = EnhancedRiskManager(max_open_positions=3)
        # Layer 16: Split Entry
        self.split_mgr = SplitEntryManager()
        # Layer 16: Flow Tracker
        self.flow_tracker = FlowTracker()
        # Layer 16: Smart Money
        self.smart_money = SmartMoneyDetector()
        # Layer 17: Exit Manager
        self.exit_manager = ExitManager()
        # Layer 25: Emergency Safeguard
        self.emergency = EmergencySafeguard(self)
        # Layer 24: Smart Trade Planner
        self.smart_planner = SmartTradePlanner()
        # Layer 22: Early Confirmation
        self.early_confirmation = EarlyConfirmationDetector()
        # Layer 23: Micro Momentum
        self.micro_momentum = MicroMomentumDetector()
        # Layer 35: Intelligence Layer (set after instantiation)
        self.intel_layer: Optional[IntelligenceLayer] = None
        # Layer 31: Knowledge Base
        self.knowledge = trading_knowledge
        # Apply risk params from knowledge base
        risk_params = self.knowledge.get_risk_params()
        self.risk_mgr.daily_loss_limit = -risk_params.get("daily_loss_limit", 8.0)
        self.risk_mgr.consecutive_loss_limit = \
            risk_params.get("max_consecutive_losses", 5)
        self.exit_manager.trailing_pct = risk_params.get("trailing_stop_pct", 3.0)
        self.exit_manager.dynamic_tp_pct = risk_params.get("take_profit_pct", 15.0)
        self.exit_manager.dynamic_sl_pct = -abs(risk_params.get("stop_loss_pct", 8.0))

    async def execute_buy_solana(
        self, mint: str, sol_amount: float, chat_id: int
    ) -> bool:
        """Execute a Solana token purchase via Jupiter."""
        try:
            lamports = int(sol_amount * 1e9)
            sig = await jupiter_swap(SOL_MINT, mint, lamports)
            if sig:
                self.positions[mint] = {
                    "buy_sol": sol_amount,
                    "ts": time.time(),
                    "signature": sig,
                    "entry_price": None,
                }
                await bot.send_message(
                    chat_id,
                    f"🟢 SOLANA BUY: {mint[:20]}...\n"
                    f"Amount: {sol_amount:.4f} SOL\n"
                    f"Sig: {sig[:20]}..."
                )
                audit_log("buy_signal", f"mint={mint} sol={sol_amount} sig={sig}")
                await financial_intel.record_outcome(
                    "crypto", {"mint": mint, "action": "buy"},
                    {"profit_usd": 0}
                )
                return True
        except Exception as e:
            log.error(f"execute_buy_solana error: {e}")
        return False

    async def execute_sell_solana(
        self, mint: str, sol_amount: float, chat_id: int
    ) -> bool:
        """Execute a Solana token sale via Jupiter."""
        try:
            lamports = int(sol_amount * 1e9)
            sig = await jupiter_swap(mint, SOL_MINT, lamports)
            if sig:
                await bot.send_message(
                    chat_id,
                    f"🔴 SOLD: {mint[:20]}...\n"
                    f"For: {sol_amount:.4f} SOL\n"
                    f"Sig: {sig[:20]}..."
                )
                audit_log("sell_signal",
                          f"mint={mint} sol={sol_amount} sig={sig}")
                return True
        except Exception as e:
            log.error(f"execute_sell_solana error: {e}")
        return False

    async def execute_buy_binance(
        self, symbol: str, usdt_amount: float, chat_id: int
    ) -> bool:
        return await binance_trader.execute_buy_async(
            symbol, usdt_amount, chat_id
        )

    async def _monitor_position(self, mint: str, chat_id: int):
        """Monitor a single position and manage exit."""
        while (mint in self.positions and
               self.active and
               not kill_check()):
            await asyncio.sleep(5)
            try:
                data = await get_token_data(mint)
                if not data:
                    continue

                current_price = data.get("price_usd", 0)
                current_volume = data.get("volume_h24", 0)
                if current_price == 0:
                    continue

                # Update flow tracker
                await self.flow_tracker.update(mint)
                flow = self.flow_tracker.analyze(mint)
                flow_score = flow["flow_score"]
                buy_ratio = flow["buy_ratio"]

                # Update exit manager
                self.exit_manager.update_price(mint, current_price)
                self.exit_manager.update_volume(mint, current_volume)
                self.exit_manager.update_flow(mint, flow_score, buy_ratio)

                # Update smart planner history
                self.smart_planner.update_history(
                    mint, current_price, current_volume,
                    data.get("txns_h24_buys", 0),
                    data.get("txns_h24_sells", 0)
                )

                entry_price = self.positions[mint].get("entry_price", current_price)
                if entry_price and entry_price > 0:
                    profit_pct = (current_price - entry_price) / entry_price * 100
                else:
                    profit_pct = 0
                    entry_price = current_price
                    self.positions[mint]["entry_price"] = current_price

                # Check exits: ExitManager → IntelligenceLayer → KB rules
                should_exit, reason = self.exit_manager.should_exit(
                    mint, current_price, flow_score, buy_ratio, current_volume
                )

                # Intelligence layer override
                if not should_exit and self.intel_layer:
                    early_exit, intel_reason = await \
                        self.intel_layer.enhance_exit_decision(
                            mint, profit_pct, current_price, entry_price
                        )
                    if early_exit:
                        should_exit = True
                        reason = f"{reason} | {intel_reason}".strip(" | ")

                # SmartPlanner exit check
                if not should_exit:
                    pos_age = (time.time() - self.positions[mint].get("ts", time.time())) / 60
                    sp_exit, sp_reason = self.smart_planner.analyze_exit(
                        mint, current_price, entry_price, pos_age, flow_score
                    )
                    if sp_exit:
                        should_exit = True
                        reason = sp_reason

                # Knowledge base exit rules
                if not should_exit:
                    for rule in self.knowledge.get_exit_rules():
                        rule_lower = rule.lower()
                        if "profit" in rule_lower and "15%" in rule_lower and profit_pct >= 15:
                            should_exit = True
                            reason = f"KB_profit_{profit_pct:.1f}%"
                            break
                        if "volume" in rule_lower and "50%" in rule_lower:
                            peak_vol = self.exit_manager.peak_volumes.get(mint, 0)
                            if (peak_vol > 0 and current_volume < peak_vol * 0.5
                                    and profit_pct > 3):
                                should_exit = True
                                reason = f"KB_vol_drop"
                                break

                if should_exit:
                    pos = self.positions[mint]
                    duration = (time.time() - pos["ts"]) / 60
                    result = ("WIN" if profit_pct > 2 else
                              "LOSS" if profit_pct < -2 else "BREAK_EVEN")

                    # Log outcome
                    self.trade_logger.log_outcome(
                        mint, entry_price, current_price,
                        duration, profit_pct, result, reason,
                        simulated=False
                    )
                    self.adaptive_validator.record_outcome(profit_pct)

                    if profit_pct < 0:
                        self.risk_mgr.record_loss(profit_pct)

                    emergency_triggered = self.emergency.record_trade_result(profit_pct)
                    if emergency_triggered:
                        await bot.send_message(
                            chat_id,
                            f"💥 EMERGENCY: loss streak triggered. Stopping."
                        )
                        self.stop()
                        break

                    # Execute sell
                    await self.execute_sell_solana(mint, pos["buy_sol"], chat_id)

                    # Publish trade_closed event
                    await event_bus.publish_async("trade_closed", {
                        "mint": mint,
                        "entry_price": entry_price,
                        "exit_price": current_price,
                        "profit_pct": profit_pct,
                        "duration_min": duration,
                        "reason": reason,
                        "result": result,
                        "sol_amount": pos["buy_sol"],
                        "entry_liquidity": data.get("liquidity_usd", 0),
                        "entry_volume": current_volume,
                        "entry_buy_ratio": buy_ratio,
                    })

                    # Intelligence layer feedback
                    if self.intel_layer:
                        await self.intel_layer.record_outcome_feedback(
                            mint, profit_pct, entry_price,
                            current_price, duration, reason
                        )

                    # Cleanup
                    del self.positions[mint]
                    self.exit_manager.remove_position(mint)
                    self.smart_planner.reset_entry_count(mint)

                    await bot.send_message(
                        chat_id,
                        f"🚪 EXIT {mint[:8]}... | {profit_pct:+.2f}% | {reason}"
                    )
                    break

            except Exception as e:
                log.error(f"_monitor_position error [{mint[:12]}]: {e}")
                await asyncio.sleep(10)

    async def scan_and_trade(self, chat_id: int):
        """
        Main trading loop:
        Scan → Filter → Validate → Enter → Monitor
        """
        self.active = True
        await bot.send_message(chat_id, "🤖 AUTO TRADER ACTIVATED (Tier 3)")

        # Start real-time monitor
        monitor = RealTimeMonitor(self, chat_id)
        asyncio.create_task(monitor.run(), name="realtime_monitor")

        while self.active and not kill_check():
            try:
                # Tier gate
                if tier_ctrl.get_tier() < 3:
                    await bot.send_message(
                        chat_id, "⚠ Balance tier too low for AutoTrader."
                    )
                    break

                # Risk gates
                if self.risk_mgr.is_kill_switch_active():
                    await bot.send_message(chat_id, "🛑 RISK KILL SWITCH ACTIVE")
                    self.active = False
                    break

                if await self.emergency.check_and_emergency_stop(chat_id):
                    break

                # Gather intel
                intel_report = await free_intel.gather_intel()
                whale_activity = await alpha_whale.fetch_whale_activity()
                if whale_activity:
                    whale_sentiment = alpha_whale.analyze_whale_sentiment(
                        whale_activity
                    )
                    intel_report["whale_sentiment"] = whale_sentiment

                # Strategy selection via MetaGovernorLegacy
                chosen_strategy = await meta_governor_legacy.select_strategy(
                    intel_report
                )
                # Apply strategy to exit manager
                self.exit_manager.dynamic_tp_pct = (
                    chosen_strategy.get("take_profit_mult", 2.0) * 5.0
                )
                self.exit_manager.dynamic_sl_pct = chosen_strategy.get(
                    "stop_loss_pct", -12.0
                )
                financial_intel.crypto_weights["take_profit_mult"] = \
                    chosen_strategy.get("take_profit_mult", 2.0)
                financial_intel.crypto_weights["stop_loss_pct"] = \
                    chosen_strategy.get("stop_loss_pct", -12.0)

                # Scan signals
                signals = await scan_meme_coins()
                for sig in signals[:5]:
                    mint = sig.get("mint")
                    if not mint:
                        continue
                    if not self.cooldown.can_check(mint):
                        continue
                    if not self.risk_mgr.can_open_new_position(len(self.positions)):
                        break

                    data = await get_token_data(mint)
                    if not data:
                        continue

                    # Market condition check
                    self.market_cond.update(data)
                    if self.market_cond.no_trade_zone():
                        continue

                    # Smart money detection
                    smart_money_info = await self.smart_money.analyze(mint, data)
                    intel_report["smart_money"] = (
                        smart_money_info.get("signal") == "whale_accumulation"
                    )

                    # Early confirmation
                    self.early_confirmation.update(
                        mint,
                        data.get("volume_h24", 0),
                        float(intel_report.get("smart_money", False))
                    )
                    confirmation = self.early_confirmation.analyze(mint)
                    if not confirmation["confirmed"]:
                        continue

                    # Micro momentum check (3-second samples)
                    should_enter_micro, micro_info = await \
                        self.micro_momentum.check(mint, chat_id)
                    if not should_enter_micro:
                        continue

                    # Smart trade planner analysis
                    market_cond_str = self.market_cond.get_condition()
                    should_enter, confidence, plan_reason = \
                        self.smart_planner.analyze_entry(
                            mint, data, intel_report, market_cond_str
                        )
                    if not should_enter:
                        continue

                    # AI adaptive validation (final gate)
                    valid, score, msg, details = await \
                        self.adaptive_validator.validate(
                            data, intel_report, mint
                        )
                    if not valid:
                        continue

                    # Calculate position size
                    bal = get_real_balance(BOT_WALLET_ADDRESS)
                    pct = financial_intel.crypto_weights.get("max_trade_pct", 0.05)
                    size_mult = chosen_strategy.get("trade_size_multiplier", 1.0)
                    total_amount = min(0.5, max(0.01, bal * pct * size_mult))
                    first_amt = self.split_mgr.first_entry(
                        mint, total_amount, data.get("price_usd", 0)
                    )
                    if first_amt <= 0:
                        continue

                    # Log signal
                    self.trade_logger.log_signal(
                        mint, data.get("symbol", "?"),
                        score, data, market_cond_str
                    )

                    # Execute buy
                    success = await self.execute_buy_solana(
                        mint, first_amt, chat_id
                    )
                    if success:
                        self.positions[mint]["entry_price"] = data.get("price_usd", 0)
                        self.exit_manager.register_position(mint, {
                            "entry_price": data.get("price_usd", 0),
                            "volume": data.get("volume_h24", 0),
                        })
                        # Start position monitor
                        asyncio.create_task(
                            self._monitor_position(mint, chat_id),
                            name=f"monitor_{mint[:8]}"
                        )

                await asyncio.sleep(120)  # scan cooldown

            except Exception as e:
                log.error(f"scan_and_trade error: {e}")
                await asyncio.sleep(30)

        self.active = False

    def stop(self):
        self.active = False
        log.info("AutoTrader stopped.")

auto_trader = AutoTrader()

# =============================================================================
# LAYER 56: DEEP RESEARCH HANDLER (Al Qalb)
# =============================================================================
async def handle_deep_research(topic: str, chat_id: int):
    """
    Deep research menggunakan Knowledge Graph Reasoning Engine prompt.
    Hasilnya disimpan ke LocalBrain (KG format) DAN SQLite knowledge table.
    Mendukung multi-turn CONTINUE_AVAILABLE protocol.
    """
    await bot.send_message(
        chat_id,
        f"🔍 Membangun Knowledge Graph mendalam untuk '{topic}'...\n"
        f"(Mode: RAG-ready · DAG-structured · Epistemic-disciplined)",
        parse_mode=None
    )
    t_start = time.time()
    try:
        # Delegate sepenuhnya ke AlQalb KG engine (multi-turn aware)
        if al_qalb:
            await al_qalb._auto_research(topic, chat_id=None, max_parts=3)

        elapsed = time.time() - t_start

        # Ambil nodes yang baru saja ditambahkan untuk ditampilkan
        nodes = local_brain.get_topic_summary(topic)
        if not nodes:
            # Fallback: cari dengan query biasa
            nodes = local_brain.query(topic, limit=10)

        # Store ke knowledge table juga
        if nodes:
            summary_text = "\n".join(
                f"[{n.get('epistemic_type','?').upper()}] {n.get('statement','')}"
                for n in nodes[:8]
            )
            with sqlite3.connect(DB_FILE_ALQALB) as conn:
                conn.execute(
                    "INSERT INTO knowledge VALUES (?,?,?)",
                    (time.time(), topic, summary_text)
                )

        # Log operational
        await op_intel.record_ai_usage("kg_deep_research", elapsed)
        audit_log("deep_research",
                  f"topic={topic} nodes={len(nodes)} elapsed={elapsed:.1f}s")

        # Build human-readable result dari nodes
        if nodes:
            # Kelompokkan per epistemic type (priority order)
            priority = ["failure_mode", "decision_rule", "causal",
                        "heuristic", "fact", "synthesis"]
            grouped: Dict[str, List] = defaultdict(list)
            for n in nodes:
                grouped[n.get("epistemic_type", "fact")].append(n)

            result = f"📄 KNOWLEDGE GRAPH: {topic.upper()}\n"
            result += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            result += f"⏱ {elapsed:.1f}s · {len(nodes)} nodes\n\n"

            for ep_type in priority:
                ep_nodes = grouped.get(ep_type, [])
                if not ep_nodes:
                    continue
                icons = {
                    "failure_mode": "⚠️",
                    "decision_rule": "🎯",
                    "causal": "🔗",
                    "heuristic": "💡",
                    "fact": "📌",
                    "synthesis": "🧬",
                }
                icon = icons.get(ep_type, "•")
                result += f"{icon} {ep_type.upper().replace('_',' ')}\n"
                for n in ep_nodes[:3]:
                    result += f"  · {n.get('statement','')[:150]}\n"
                    # Show relations
                    rels = n.get("relations", [])[:2]
                    for rel in rels:
                        result += (f"    ↳ [{rel.get('type','?')}] "
                                   f"→ {rel.get('target_id','?')}\n")
                result += "\n"
        else:
            result = f"⚠ Tidak ada node yang ditemukan untuk '{topic}'."

        # Send result (split jika panjang)
        chunks = [result[i:i+3800] for i in range(0, len(result), 3800)]
        for chunk in chunks[:4]:
            await bot.send_message(chat_id, chunk, parse_mode=None)

        # Notify LocalBrain stats
        await bot.send_message(
            chat_id,
            f"🧠 Local Brain sekarang: "
            f"{len(local_brain.knowledge_nodes)} total nodes.",
            parse_mode=None
        )

    except Exception as e:
        await op_intel.record_error("handle_deep_research", str(e))
        await bot.send_message(
            chat_id,
            f"❌ Research error: {str(e)[:150]}",
            parse_mode=None
        )

# =============================================================================
# LAYER 57: DATABASE MIGRATION (Al Qalb)
# =============================================================================
def find_all_old_brain_files() -> List[Path]:
    """Find all legacy knowledge/brain JSON files for migration."""
    patterns = [
        "knowledge*.json", "brain*.json",
        "*.brain.json", "data/knowledge*.json",
        "data/brain*.json",
    ]
    files = []
    for pattern in patterns:
        files.extend(Path(".").glob(pattern))
    return list(set(files))

def auto_migrate_old_knowledge():
    """Migrate all legacy knowledge JSON files into LocalBrain."""
    log.info("AUTO-MIGRATE: Scanning for legacy brain files...")
    total = 0
    for file_path in find_all_old_brain_files():
        try:
            content = file_path.read_text(encoding="utf-8").strip()
            if not content:
                file_path.unlink()
                continue
            old_nodes = json.loads(content)
            if not isinstance(old_nodes, list):
                old_nodes = [old_nodes]
            valid = []
            for node in old_nodes:
                if isinstance(node, dict) and "id" in node and "statement" in node:
                    node["_migrated_from"] = str(file_path)
                    node["_migrated_at"] = time.time()
                    valid.append(node)
            if valid:
                local_brain.add_knowledge(valid)
                total += len(valid)
                log.info(f"✅ Migrated {file_path.name}: {len(valid)} nodes")
            file_path.unlink()
        except Exception as e:
            log.warning(f"⚠ Migration failed [{file_path.name}]: {e}")
    if total > 0:
        log.info(f"AUTO-MIGRATE DONE: {total} nodes added.")

def migrate_knowledge_from_database():
    """Migrate old knowledge table entries into LocalBrain."""
    log.info("DB-MIGRATE: Scanning knowledge table...")
    try:
        with sqlite3.connect(DB_FILE_ALQALB) as conn:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE "
                "type='table' AND name='knowledge'"
            )
            if not cur.fetchone():
                return
            rows = conn.execute(
                "SELECT rowid, topic, content, ts FROM knowledge "
                "ORDER BY ts DESC LIMIT 500"
            ).fetchall()
            if not rows:
                return
            nodes = []
            for rowid, topic, content, ts in rows:
                if not content or len(content.strip()) < 10:
                    continue
                nodes.append({
                    "id": f"migrated_db_{rowid}_{int(ts)}",
                    "topic": topic or "general",
                    "subtopic": "db_migration",
                    "statement": content[:400],
                    "epistemic_type": "fact",
                    "confidence_score": 0.6,
                    "utility_class": "inference_only",
                    "tags": ["migrated", topic or "general"],
                    "relations": [],
                })
            if nodes:
                local_brain.add_knowledge(nodes)
                log.info(f"DB-MIGRATE: {len(nodes)} knowledge entries migrated.")
    except Exception as e:
        log.warning(f"DB migration error: {e}")

# =============================================================================
# LAYER 58: BACKGROUND TASKS & WATCHDOG (Al Qalb)
# =============================================================================
al_qalb: Optional[AlQalb] = None  # initialized in main()

async def get_data_for_uhee() -> Optional[pd.DataFrame]:
    """Collect scraped data for UHEE engine."""
    try:
        async with aiosqlite.connect(str(aiw_memory_store.db_path)) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM scraped_data")
            count = (await cursor.fetchone())[0]
            if count < MIN_DATA_ROWS:
                return None
            cursor = await db.execute(
                "SELECT COUNT(*) as cnt, "
                "strftime('%H', scraped_at) as hour "
                "FROM scraped_data GROUP BY hour"
            )
            rows = await cursor.fetchall()
            if len(rows) >= 5:
                df = pd.DataFrame(rows, columns=["count", "hour"])
                df["hour"] = df["hour"].astype(int)
                df["count"] = df["count"].astype(float)
                return df
    except Exception:
        pass
    return None

async def tier_monitor():
    """Background: update tier balance every 60s."""
    while True:
        try:
            await tier_ctrl.update_balance()
        except Exception as e:
            log.debug(f"tier_monitor error: {e}")
        await asyncio.sleep(60)

async def daily_scheduler():
    """Background: send daily report and reset counters every 24h."""
    await asyncio.sleep(30)
    while not kill_check():
        try:
            now = time.time()
            today_str = datetime.now().strftime("%d/%m/%Y %H:%M")
            sol_bal = get_real_balance(BOT_WALLET_ADDRESS)
            sol_price = await get_sol_price_usd()
            usd_val = sol_bal * sol_price

            # Trade stats
            with sqlite3.connect(DB_FILE_ALQALB) as conn:
                day_start = now - 86400
                trades_row = conn.execute(
                    """SELECT COUNT(*),
                       AVG(profit_percent),
                       SUM(CASE WHEN profit_percent > 0 THEN 1 ELSE 0 END)
                       FROM trade_signals_v2
                       WHERE ts > ? AND result IS NOT NULL""",
                    (day_start,)
                ).fetchone()
                profit_today = conn.execute(
                    "SELECT SUM(amount_sol) FROM income_ledger WHERE ts > ?",
                    (day_start,)
                ).fetchone()[0] or 0.0

            total_trades = trades_row[0] or 0
            avg_profit = trades_row[1] or 0.0
            wins_today = trades_row[2] or 0
            win_rate = (wins_today / total_trades * 100) if total_trades > 0 else 0

            brain_nodes = len(local_brain.knowledge_nodes)
            uhee_cycles = uhee_engine.cycle_count
            ev_data = auto_trader.trade_logger.get_expected_value(min_trades=20)
            ev_str = (f"{ev_data.get('expected_value_pct', 0):+.2f}%"
                      if ev_data.get("status") == "ok" else "N/A")

            report = (
                f"📊 DAILY REPORT — {today_str}\n\n"
                f"💰 Balance: {sol_bal:.4f} SOL (${usd_val:.2f})\n"
                f"🏆 {tier_ctrl.tier_description()}\n\n"
                f"📈 Trading Hari Ini:\n"
                f"   Trades: {total_trades} | WR: {win_rate:.0f}%\n"
                f"   Avg Profit: {avg_profit:.1f}% | EV: {ev_str}\n"
                f"   Net: {profit_today:+.4f} SOL\n\n"
                f"🧠 Brain Nodes: {brain_nodes}\n"
                f"🔄 UHEE Cycles: {uhee_cycles}\n"
                f"📦 Strategies: {len(code_gen.loaded_strategies)}\n"
                f"🔴 Kill Switch: {'ON' if KILL_SWITCH else 'OFF'}"
            )

            try:
                await bot.send_message(OWNER_ID, report, parse_mode=None)
            except Exception as e:
                log.warning(f"daily_scheduler send_message error: {e}")

            # Memory consolidation
            if al_qalb:
                await al_qalb.consolidate_memory()

            # Publish daily tick event
            await event_bus.publish_async("daily_tick", {
                "timestamp": now,
                "sol_balance": sol_bal,
                "usd_value": usd_val,
                "trades_today": total_trades,
                "win_rate": win_rate,
                "brain_nodes": brain_nodes,
            })

            # Reset daily risk state
            with sqlite3.connect(DB_FILE_ALQALB) as conn:
                today = datetime.now().strftime("%Y-%m-%d")
                conn.execute(
                    "INSERT OR REPLACE INTO risk_state VALUES (?,?,?)",
                    ("daily_loss_sol", 0.0, today)
                )
                conn.execute(
                    "INSERT OR REPLACE INTO risk_state VALUES (?,?,?)",
                    ("consecutive_losses", 0.0, today)
                )
            auto_trader.risk_mgr.reset_daily()

            log.info(f"daily_scheduler complete — next run in 24h")
        except Exception as e:
            log.error(f"daily_scheduler error: {e}")
        await asyncio.sleep(86400)

async def uhee_background():
    """Background: run UHEE evolution cycles."""
    await asyncio.sleep(600)  # wait 10 min on startup
    while not kill_check():
        try:
            data = await get_data_for_uhee()
            if data is not None:
                result = uhee_engine.run_cycle(data, "scraped_trends")
                log.info(
                    f"UHEE cycle {uhee_engine.cycle_count}: "
                    f"best={result['stats']['best']:.3f}"
                )
                await event_bus.publish_async("evolution_completed", {
                    "cycle": uhee_engine.cycle_count,
                    "best_fitness": result["stats"]["best"],
                    "top_hypotheses": len(result["top_hypotheses"]),
                    "generation": uhee_engine.cycle_count,
                })
                await code_gen.evolve_strategies(result)
                # Save top hypotheses to DB
                for hyp_dict in result.get("top_hypotheses", [])[:3]:
                    try:
                        hyp = Hypothesis(
                            id=hyp_dict.get("id", ""),
                            type=HypothesisType(hyp_dict.get("type", "correlation")),
                            antecedent=hyp_dict.get("antecedent", {}),
                            consequent=hyp_dict.get("consequent", {}),
                            fitness=hyp_dict.get("fitness", 0),
                        )
                        await aiw_memory_store.save_hypothesis(
                            uhee_engine.cycle_count, hyp
                        )
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"uhee_background error: {e}")

        # Use MetaGovernor interval if available
        interval = 3600
        if al_qalb and al_qalb.meta_governor:
            interval = al_qalb.meta_governor.global_config.get(
                "uhee_evolution_interval", 3600
            )
        await asyncio.sleep(interval)

async def watchdog():
    """
    Background watchdog: restarts crashed tasks automatically.
    The guardian of system reliability.
    """
    required_tasks = {
        "tier_monitor": tier_monitor,
        "daily_scheduler": daily_scheduler,
        "uhee_background": uhee_background,
    }
    while not kill_check():
        await asyncio.sleep(30)
        current_tasks = {t.get_name(): t
                         for t in asyncio.all_tasks()
                         if not t.done()}
        for name, coro_fn in required_tasks.items():
            if name not in current_tasks:
                log.warning(f"⚠ Task '{name}' died! Restarting...")
                asyncio.create_task(coro_fn(), name=name)
                await event_bus.publish_async("task_restarted", {"task": name})
                try:
                    await bot.send_message(
                        OWNER_ID,
                        f"⚠ Task '{name}' di-restart otomatis"
                    )
                except Exception:
                    pass

# =============================================================================
# LAYER 59: TELEGRAM BOT DEFINITION & ALL HANDLERS (Al Qalb)
# =============================================================================
bot = AsyncTeleBot(TOKEN)
pending_approvals: Dict[int, Dict] = {}
_pending_counter = 0

# ---------- COMMAND: /start ----------
@bot.message_handler(commands=["start", "help"])
async def cmd_start(m):
    if not is_owner(m):
        return
    await bot.send_message(
        m.chat.id,
        "🤖 ADAPTIVE AUTONOMOUS ECONOMIC INTELLIGENCE SYSTEM\n"
        "CORE BRAIN: FinancialIntel + OpIntel + U-HEE + MetaGovernor + LocalBrain\n\n"
        "⚡ TIER SYSTEM:\n"
        f"  Tier 1 (< ${TIER_1_MAX_USD}): Basic\n"
        f"  Tier 2 (${TIER_1_MAX_USD}–${TIER_2_MAX_USD}): + Meme Scanner\n"
        f"  Tier 3 (> ${TIER_2_MAX_USD}): Full Auto Trading\n\n"
        "📋 COMMANDS:\n"
        "/tier_status — Cek tier & balance\n"
        "/balance — Cek SOL balance\n"
        "/withdraw — Withdraw SOL\n"
        "/meme_scan — Scan meme coins\n"
        "/trade_start — Mulai auto trading\n"
        "/trade_stop — Stop trading\n"
        "/trade_positions — Posisi aktif\n"
        "/wealth_cycle — Jalankan earner+money\n"
        "/money_status — Status money engine\n"
        "/status — Status lengkap sistem\n"
        "/kill_switch — Emergency stop\n"
        "/mylicense — Cek lisensi\n\n"
        "🧠 KNOWLEDGE:\n"
        "/load_knowledge <topik> — Ambil pengetahuan dari AI eksternal (Graph Reasoning)\n"
        "/show_knowledge — Lihat isi Local Brain\n"
        "/forget_topic <topik> — Hapus topik dari brain\n"
        "/reset_knowledge — Reset seluruh Local Brain\n\n"
        "🔬 DEEP RESEARCH:\n"
        "/deep_research <topik> — Riset internal berbasis Local Brain + MetaGovernor\n\n"
        "🔬 AI & EVOLUSI:\n"
        "/upgrade <deskripsi> — Self-modify kode\n"
        "/evolve_now — Evolusi pengetahuan\n"
        "/kerjakan <instruksi> — Eksekusi dari brain\n"
        "/meta_status — Status MetaGovernor\n\n"
        "💬 Chat bebas untuk tanya-jawab dengan Local Brain + AI.",
        parse_mode=None
    )

# ---------- COMMAND: /meta_status ----------
@bot.message_handler(commands=["meta_status"])
async def cmd_meta_status(m):
    if not is_owner(m):
        return
    if not al_qalb or not al_qalb.meta_governor:
        return await bot.send_message(m.chat.id, "MetaGovernor belum aktif.")
    mg = al_qalb.meta_governor
    state = mg.state
    brain_state = await mg.analyze_brain_state()
    msg = (
        f"🧬 META GOVERNOR STATUS\n\n"
        f"Mode: {state['mode']}\n"
        f"Consciousness: {state['consciousness_level']:.2f}\n"
        f"Ideas generated: {len(mg.idea_history)}\n"
        f"UHEE interval: {mg.global_config['uhee_evolution_interval']}s\n\n"
        f"🧠 BRAIN STATE:\n"
        f"Nodes: {brain_state.get('total_nodes', 0)}\n"
        f"Richness: {brain_state.get('richness_score', 0):.2f}\n"
        f"Gaps: {', '.join(brain_state.get('knowledge_gaps', [])) or 'none'}\n"
        f"Dominant: {brain_state.get('dominant_topic', 'N/A')}"
    )
    await bot.send_message(m.chat.id, msg, parse_mode=None)

# ---------- COMMAND: /tier_status ----------
@bot.message_handler(commands=["tier_status"])
async def cmd_tier_status(m):
    if not is_owner(m):
        return
    await tier_ctrl.update_balance()
    sol_bal = get_real_balance(BOT_WALLET_ADDRESS)
    sol_price = await get_sol_price_usd()
    await bot.send_message(
        m.chat.id,
        f"📊 INCOME TIER STATUS\n\n"
        f"{tier_ctrl.tier_description()}\n\n"
        f"SOL: {sol_bal:.6f} (${sol_bal * sol_price:.2f})",
        parse_mode=None
    )

# ---------- COMMAND: /balance ----------
@bot.message_handler(commands=["balance"])
async def cmd_balance(m):
    if not is_owner(m):
        return
    bal = get_real_balance(BOT_WALLET_ADDRESS)
    price = await get_sol_price_usd()
    binance_usdt = binance_trader.get_account_balance("USDT") if binance_trader.active else 0
    await bot.send_message(
        m.chat.id,
        f"💰 BALANCE\n"
        f"SOL: {bal:.6f}\n"
        f"USD: ${bal * price:.2f}\n"
        f"Binance USDT: ${binance_usdt:.2f}",
        parse_mode=None
    )

# ---------- COMMAND: /withdraw ----------
@bot.message_handler(commands=["withdraw"])
async def cmd_withdraw(m):
    if not is_owner(m):
        return
    if kill_check():
        return await bot.send_message(m.chat.id, "❌ Kill switch aktif.")
    if not PRIVATE_KEY_BOT or not WALLET_TARGET:
        return await bot.send_message(
            m.chat.id, "❌ PRIVATE_KEY_BOT / WALLET_TARGET belum diset."
        )
    bal = get_real_balance(BOT_WALLET_ADDRESS)
    amount = bal - MIN_RESERVE_SOL
    if amount <= 0:
        return await bot.send_message(
            m.chat.id, f"❌ Saldo tidak cukup: {bal:.6f} SOL"
        )
    global _pending_counter
    if amount >= BIG_TX_THRESHOLD:
        _pending_counter += 1
        pid = _pending_counter
        pending_approvals[pid] = {
            "chat_id": m.chat.id,
            "to_address": WALLET_TARGET,
            "amount_sol": amount,
        }
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("✅ Approve", callback_data=f"approve_{pid}"),
            InlineKeyboardButton("❌ Deny", callback_data=f"deny_{pid}")
        )
        await bot.send_message(
            SECOND_ADMIN_ID,
            f"⚠ APPROVAL #{pid}\n"
            f"Withdraw {amount:.4f} SOL → {WALLET_TARGET[:20]}...",
            reply_markup=markup
        )
        return await bot.send_message(m.chat.id, f"⏳ Pending approval #{pid}")
    try:
        sig = send_sol_tx(WALLET_TARGET, amount)
        audit_log("withdraw", f"amount={amount} to={WALLET_TARGET} sig={sig}")
        with sqlite3.connect(DB_FILE_ALQALB) as conn:
            conn.execute(
                "INSERT INTO income_ledger VALUES (?,?,?)",
                (time.time(), -amount, sig)
            )
        await bot.send_message(
            m.chat.id,
            f"✅ Withdraw sukses!\nJumlah: {amount:.6f} SOL\nTx: {sig}"
        )
    except Exception as e:
        await bot.send_message(m.chat.id, f"❌ Error: {e}")

# ---------- CALLBACK: Approval ----------
@bot.callback_query_handler(
    func=lambda c: c.data.startswith("approve_") or c.data.startswith("deny_")
)
async def handle_approval(call):
    if call.from_user.id not in (OWNER_ID, SECOND_ADMIN_ID):
        return
    action, pid_str = call.data.split("_", 1)
    pid = int(pid_str)
    item = pending_approvals.pop(pid, None)
    if not item:
        return await bot.answer_callback_query(call.id, "Expired!")
    if action == "approve":
        try:
            sig = send_sol_tx(item["to_address"], item["amount_sol"])
            audit_log("approved_tx", f"pid={pid} sig={sig}")
            await bot.send_message(item["chat_id"], f"✅ TX Approved!\nSig: {sig}")
        except Exception as e:
            await bot.send_message(item["chat_id"], f"❌ TX Failed: {e}")
    else:
        await bot.send_message(item["chat_id"], f"✗ Request #{pid} ditolak.")
    await bot.answer_callback_query(call.id, "Done")

# ---------- COMMAND: /status ----------
@bot.message_handler(commands=["status"])
async def cmd_status(m):
    if not is_owner(m):
        return
    bal = get_real_balance(BOT_WALLET_ADDRESS)
    price = await get_sol_price_usd()
    stats = aiw_core.task_manager.get_stats()
    health = await op_intel.get_system_health()
    ev = auto_trader.trade_logger.get_expected_value(min_trades=20)
    ev_str = (f"{ev.get('expected_value_pct', 0):+.2f}%"
              if ev.get("status") == "ok" else "N/A")
    await bot.send_message(
        m.chat.id,
        f"📊 SYSTEM STATUS\n\n"
        f"{tier_ctrl.tier_description()}\n\n"
        f"💰 Balance: {bal:.4f} SOL (${bal * price:.2f})\n"
        f"🔴 Kill Switch: {'ON' if KILL_SWITCH else 'OFF'}\n\n"
        f"🤖 Trader: {'ACTIVE' if auto_trader.active else 'INACTIVE'} "
        f"| Positions: {len(auto_trader.positions)}\n"
        f"📈 Expected Value: {ev_str}\n\n"
        f"📋 AIW — Pending: {stats['pending']} "
        f"Running: {stats['running']} Done: {stats['completed']}\n\n"
        f"🧠 CORE BRAIN:\n"
        f"   UHEE Cycles: {uhee_engine.cycle_count}\n"
        f"   Code Evolutions: {code_gen.version}\n"
        f"   Strategies: {len(code_gen.loaded_strategies)}\n"
        f"   Brain Nodes: {len(local_brain.knowledge_nodes)}\n\n"
        f"⚙ System: {health.get('status', 'unknown')} "
        f"| Errors/h: {health.get('errors_last_hour', 0)}",
        parse_mode=None
    )

# ---------- COMMAND: /kill_switch ----------
@bot.message_handler(commands=["kill_switch"])
async def cmd_kill(m):
    global KILL_SWITCH
    if not is_owner(m):
        return
    KILL_SWITCH = not KILL_SWITCH
    if KILL_SWITCH:
        auto_trader.stop()
    state = "AKTIF 🔴" if KILL_SWITCH else "OFF ✅"
    audit_log("kill_switch", f"state={state} by={m.from_user.id}")
    await bot.send_message(
        m.chat.id,
        f"🔴 KILL_SWITCH: {state}\n"
        f"{'Semua engine dihentikan.' if KILL_SWITCH else 'Sistem aktif kembali.'}",
        parse_mode=None
    )

# ---------- COMMAND: /meme_scan ----------
@bot.message_handler(commands=["meme_scan"])
async def cmd_meme_scan(m):
    if not is_owner(m):
        return
    if tier_ctrl.get_tier() < 2:
        return await bot.send_message(
            m.chat.id,
            f"⚠ Meme scanner butuh Tier 2 (balance > ${TIER_1_MAX_USD})"
        )
    await bot.send_message(m.chat.id, "🔍 Scanning meme coins...")
    signals = await scan_meme_coins()
    if not signals:
        return await bot.send_message(m.chat.id, "😔 Tidak ada signal.")
    msg = f"🎯 MEME SIGNALS ({len(signals)} found)\n\n"
    for sig in signals[:5]:
        icon = "🔥" if sig["score"] >= 70 else "📌"
        msg += (
            f"{icon} {sig.get('symbol', '?')} — {sig.get('name', '')}\n"
            f"   Score: {sig['score']}/100\n"
            f"   MC: ${sig.get('market_cap', 0):,.0f}\n"
            f"   Vol: ${sig.get('volume_h24', 0):,.0f}\n"
            f"   Buy: {sig.get('buy_ratio', 0):.0%}\n\n"
        )
    await bot.send_message(m.chat.id, msg, parse_mode=None)

# ---------- COMMAND: /trade_start ----------
@bot.message_handler(commands=["trade_start"])
async def cmd_trade_start(m):
    if not is_owner(m):
        return
    if tier_ctrl.get_tier() < 3:
        return await bot.send_message(
            m.chat.id,
            f"⚠ AutoTrader butuh Tier 3 (balance > ${TIER_2_MAX_USD})"
        )
    # Load knowledge if first time
    with sqlite3.connect(DB_FILE_ALQALB) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM trading_knowledge")
        if cur.fetchone()[0] == 0:
            await bot.send_message(
                m.chat.id, "🔄 First time: Loading trading knowledge..."
            )
            await trading_knowledge.load_from_ai(m.chat.id)
    # Set intelligence layer
    auto_trader.intel_layer = IntelligenceLayer(
        financial_intel, op_intel, uhee_engine
    )
    asyncio.create_task(
        auto_trader.scan_and_trade(m.chat.id),
        name="auto_trader_scan"
    )
    await bot.send_message(
        m.chat.id, "✅ Auto Trader STARTED (Tier 3) — Solana & Binance"
    )

# ---------- COMMAND: /trade_stop ----------
@bot.message_handler(commands=["trade_stop"])
async def cmd_trade_stop(m):
    if not is_owner(m):
        return
    auto_trader.stop()
    await bot.send_message(m.chat.id, "⏹ Auto Trader stopped.")

# ---------- COMMAND: /trade_positions ----------
@bot.message_handler(commands=["trade_positions"])
async def cmd_trade_positions(m):
    if not is_owner(m):
        return
    if not auto_trader.positions:
        return await bot.send_message(m.chat.id, "📭 Tidak ada posisi aktif.")
    msg = "📊 OPEN POSITIONS\n\n"
    for mint, pos in auto_trader.positions.items():
        age = (time.time() - pos.get("ts", time.time())) / 3600
        entry = pos.get("entry_price", 0)
        msg += (
            f"🔹 {mint[:20]}...\n"
            f"   Buy: {pos.get('buy_sol', 0):.4f} SOL\n"
            f"   Entry: {entry:.8f}\n"
            f"   Age: {age:.1f}h\n\n"
        )
    await bot.send_message(m.chat.id, msg, parse_mode=None)

# ---------- COMMAND: /wealth_cycle ----------
@bot.message_handler(commands=["wealth_cycle"])
async def cmd_wealth_cycle(m):
    if not is_owner(m):
        return
    asyncio.create_task(auto_earner.run_all_earners(m.chat.id))
    asyncio.create_task(auto_money_engine.run_money_cycle(m.chat.id))
    await bot.send_message(
        m.chat.id, "💰 Wealth cycle dimulai (background)..."
    )

# ---------- COMMAND: /money_status ----------
@bot.message_handler(commands=["money_status"])
async def cmd_money_status(m):
    if not is_owner(m):
        return
    real_sol = get_real_balance(BOT_WALLET_ADDRESS)
    await bot.send_message(
        m.chat.id,
        f"💵 MONEY ENGINE STATUS\n"
        f"Active: {MONEY_LAYER_ENABLED}\n"
        f"Real Balance: {real_sol:.6f} SOL\n"
        f"Acc. Profit: {auto_money_engine.total_profit_sol:.6f} SOL\n"
        f"Executions: {auto_money_engine.execution_count}",
        parse_mode=None
    )

# ---------- COMMAND: /mylicense ----------
@bot.message_handler(commands=["mylicense"])
async def cmd_license(m):
    lic = check_license(m.from_user.id)
    if lic["active"]:
        exp = time.strftime("%d/%m/%Y", time.localtime(lic["expires_at"]))
        await bot.send_message(
            m.chat.id,
            f"✅ Lisensi aktif\nPlan: {lic['plan'].upper()}\nExpires: {exp}"
        )
    else:
        await bot.send_message(m.chat.id, "❌ Tidak ada lisensi aktif.")

# ---------- COMMAND: /load_knowledge ----------
@bot.message_handler(commands=["load_knowledge"])
async def cmd_load_knowledge(m):
    if not is_owner(m):
        return
    topic = m.text[16:].strip()
    if not topic:
        return await bot.send_message(m.chat.id, "Format: /load_knowledge <topik>")
    await bot.send_message(
        m.chat.id,
        f"💡 Memproses topik: {topic}\nMembangun knowledge graph..."
    )
    if al_qalb:
        await al_qalb._auto_research(topic, m.chat.id)

# ---------- COMMAND: /show_knowledge ----------
@bot.message_handler(commands=["show_knowledge"])
async def cmd_show_brain(m):
    if not is_owner(m):
        return
    topics = local_brain.get_all_topics()
    if not topics:
        return await bot.send_message(m.chat.id, "🧠 Local brain kosong.")
    msg = f"🧠 LOCAL BRAIN ({len(local_brain.knowledge_nodes)} nodes)\n\n"
    for topic in topics[:15]:
        nodes = local_brain.get_topic_summary(topic)
        msg += f"📌 {topic.upper()} ({len(nodes)} node)\n"
        for node in nodes[:2]:
            ep = node.get("epistemic_type", "?")
            stmt = node.get("statement", "")[:70]
            msg += f"  [{ep}] {stmt}...\n"
        msg += "\n"
    chunks = [msg[i:i+3800] for i in range(0, len(msg), 3800)]
    for chunk in chunks[:3]:
        await bot.send_message(m.chat.id, chunk, parse_mode=None)

# ---------- COMMAND: /forget_topic ----------
@bot.message_handler(commands=["forget_topic"])
async def cmd_forget_topic(m):
    if not is_owner(m):
        return
    topic = m.text[14:].strip()
    if not topic:
        return await bot.send_message(m.chat.id, "Format: /forget_topic <topik>")
    deleted = local_brain.delete_topic(topic)
    await bot.send_message(
        m.chat.id,
        f"🗑 Menghapus {deleted} node tentang '{topic}'."
    )

# ---------- COMMAND: /reset_knowledge ----------
@bot.message_handler(commands=["reset_knowledge"])
async def cmd_reset_brain(m):
    if not is_owner(m):
        return
    count = local_brain.reset_brain()
    await bot.send_message(
        m.chat.id, f"🧠 Local Brain di-reset! {count} node dihapus."
    )

# ---------- COMMAND: /upgrade ----------
@bot.message_handler(commands=["upgrade"])
async def cmd_upgrade(m):
    if not is_owner(m):
        return
    desc = m.text[8:].strip()
    if not desc:
        return await bot.send_message(m.chat.id, "Format: /upgrade <deskripsi kode>")

    await bot.send_message(m.chat.id, "🛠 Memproses upgrade kode...")
    main_file = Path(__file__).resolve()

    # Backup first
    code_gen._backup_main()

    prompt = (
        f"Buatkan kode Python untuk: {desc}\n"
        f"Requirements: async-compatible, no file I/O, no subprocess.\n"
        f"Hanya output kode saja tanpa penjelasan."
    )
    new_code = await ai_hybrid(prompt)

    if not code_gen.validate(new_code):
        return await bot.send_message(m.chat.id, "❌ Kode tidak valid/tidak aman.")

    content = main_file.read_text(encoding="utf-8")
    marker = "# AUTO-INJECTION-POINT"
    if marker not in content:
        return await bot.send_message(m.chat.id, "❌ Marker tidak ditemukan.")

    new_content = content.replace(marker, f"{new_code}\n{marker}")
    main_file.write_text(new_content, encoding="utf-8")

    audit_log("code_upgrade", f"desc={desc[:80]}")
    await bot.send_message(m.chat.id, "✅ Upgrade berhasil!")
    await cmd_evolve_now(m)

# ---------- COMMAND: /evolve_now ----------
@bot.message_handler(commands=["evolve_now"])
async def cmd_evolve_now(m):
    if not is_owner(m):
        return
    if al_qalb:
        await al_qalb.consolidate_memory()
    await bot.send_message(m.chat.id, "🧬 Evolusi pengetahuan selesai.")

# ---------- COMMAND: /kerjakan ----------
@bot.message_handler(commands=["kerjakan"])
async def cmd_kerjakan(m):
    if not is_owner(m):
        return
    text = m.text[9:].strip()
    if not text:
        return await bot.send_message(m.chat.id, "Format: /kerjakan <instruksi>")
    # Check local brain for procedure
    proc = local_brain.get_procedure(text)
    if proc:
        await bot.send_message(
            m.chat.id,
            f"✅ Prosedur dari Local Brain:\n{proc['statement']}"
        )
    else:
        # Fall back to AI with brain context
        nodes = local_brain.query(text, limit=5)
        context = json.dumps(nodes[:3], ensure_ascii=False) if nodes else "[]"
        prompt = (
            f"Berdasarkan konteks Local Brain:\n{context}\n\n"
            f"Instruksi: {text}\n\n"
            f"Berikan langkah konkret untuk melaksanakan instruksi ini."
        )
        resp = await ai_hybrid(prompt)
        await bot.send_message(m.chat.id, resp[:4000], parse_mode=None)

# ---------- COMMAND: /deep_research ----------
@bot.message_handler(commands=["meneliti", "researching", "deep_research"])
async def cmd_deep_research(m):
    if not is_owner(m):
        return
    topic = " ".join(m.text.split()[1:]).strip()
    if not topic:
        return await bot.send_message(
            m.chat.id, "Format: /deep_research <topik>"
        )

    await bot.send_message(
        m.chat.id,
        f"🔬 Deep Research dimulai untuk '{topic}'\n"
        f"Menggali Local Brain dan berkolaborasi dengan MetaGovernor..."
    )

    # Panggil fungsi deep research internal
    if al_qalb and al_qalb.meta_governor:
        result = await al_qalb.deep_research_internal(topic, m.chat.id)
        # Kirim hasilnya (format bisa panjang, di-split)
        chunks = [result[i:i+3800] for i in range(0, len(result), 3800)]
        for chunk in chunks[:5]:
            await bot.send_message(m.chat.id, chunk, parse_mode=None)
    else:
        await bot.send_message(m.chat.id, "MetaGovernor belum siap.")

# ---------- CORE TEXT HANDLER (catch-all) ----------
@bot.message_handler(func=lambda m: True)
async def core_handler(m):
    if not is_owner(m):
        return
    text = m.text.strip() if m.text else ""
    if not text or text.startswith("/"):
        return

    if not rate_check(m.from_user.id):
        return await bot.send_message(
            m.chat.id, "⚠ Rate limit — tunggu sebentar."
        )

    # Detect deep research intent
    research_triggers = [
        "riset mendalam", "analisis mendalam", "penelitian", "tolong riset",
        "cari info", "apa itu", "jelaskan", "bagaimana cara",
    ]
    if any(t in text.lower() for t in research_triggers):
        await handle_deep_research(text, m.chat.id)
        return

    # Route to AL QALB cognitive engine
    if al_qalb:
        response = await al_qalb.process_user_input(text, m.chat.id)
        # Split long responses
        if len(response) <= 4000:
            await bot.send_message(m.chat.id, response, parse_mode=None)
        else:
            for i in range(0, len(response), 3800):
                await bot.send_message(
                    m.chat.id, response[i:i+3800], parse_mode=None
                )
    else:
        # Fallback before AL QALB init
        resp = await ai_hybrid(text)
        await bot.send_message(m.chat.id, resp[:4000], parse_mode=None)

# =============================================================================
# LAYER 60: DATABASE INITIALIZATION (Al Qalb)
# =============================================================================
def init_db_alqalb():
    """Initialize all database tables for Al Qalb."""
    with sqlite3.connect(DB_FILE_ALQALB) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL, kind TEXT, content TEXT
        );
        CREATE TABLE IF NOT EXISTS knowledge (
            ts REAL, topic TEXT, content TEXT
        );
        CREATE TABLE IF NOT EXISTS income_ledger (
            ts REAL, amount_sol REAL, note TEXT
        );
        CREATE TABLE IF NOT EXISTS trade_signals_v2 (
            id TEXT PRIMARY KEY,
            ts REAL, mint TEXT, symbol TEXT, score INTEGER,
            market_cap REAL, volume REAL, price REAL,
            buy_ratio REAL, flow_score REAL,
            market_condition TEXT, result TEXT,
            entry_price REAL, exit_price REAL,
            profit_percent REAL, duration_min REAL,
            reason TEXT, simulated INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS licenses (
            user_id INTEGER PRIMARY KEY,
            plan TEXT, created_at REAL, expires_at REAL,
            key TEXT
        );
        CREATE TABLE IF NOT EXISTS risk_state (
            key TEXT PRIMARY KEY, value REAL, date TEXT
        );
        CREATE TABLE IF NOT EXISTS trading_knowledge (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT, content TEXT, loaded_at REAL
        );
        CREATE TABLE IF NOT EXISTS hypotheses (
            cycle INTEGER, data TEXT, ts REAL
        );
        """)
    log.info("Al Qalb database initialized.")

def check_license_alqalb(user_id: int) -> dict:
    try:
        with sqlite3.connect(DB_FILE_ALQALB) as conn:
            cur = conn.execute(
                "SELECT plan, expires_at, key FROM licenses WHERE user_id=?",
                (user_id,)
            )
            row = cur.fetchone()
            if row and row[1] > time.time():
                return {"active": True, "plan": row[0],
                        "expires_at": row[1], "key": row[2]}
    except Exception:
        pass
    return {"active": False}

def create_license_alqalb(user_id: int, plan: str, days: int = 30) -> str:
    key = generate_license_key(user_id)
    expires = time.time() + days * 86400
    with sqlite3.connect(DB_FILE_ALQALB) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO licenses VALUES (?,?,?,?,?)",
            (user_id, plan, time.time(), expires, key)
        )
    audit_log("license_created", f"user={user_id} plan={plan} days={days}")
    return key

# Alias for backward compatibility
check_license = check_license_alqalb
create_license = create_license_alqalb

# =============================================================================
# LAYER 61: MAIN LOOP (Fully Integrated QND + Al Qalb)
# =============================================================================
async def main_async():
    """
    System startup sequence for fully integrated QND + Al Qalb:
    1. Init DB & memory stores (both QND and Al Qalb)
    2. Migrate old knowledge
    3. Init AL QALB with MetaGovernor 2.0
    4. Start QND MetaGovernor and Trinitas controllers
    5. Start background tasks (tier, daily, uhee, watchdog)
    6. Start AIW core engine
    7. Start Telegram polling
    """
    global al_qalb, research_orchestrator

    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 5 + "AL QALB QUANTUM ULTIMATE — FULLY INTEGRATED" + " " * 5 + "║")
    print("║" + " " * 10 + "Quantum Neural DNA + Economic Intelligence" + " " * 10 + "║")
    print("║" + " " * 12 + "Trinitas Bridge + MetaGovernor 2.0" + " " * 12 + "║")
    print("║" + " " * 14 + "405B Target | 2-bit Quantization" + " " * 14 + "║")
    print("╚" + "═" * 68 + "╝")

    log.info("=" * 60)
    log.info("  AL QALB QUANTUM ULTIMATE — STARTING")
    log.info("=" * 60)

    # Step 1: Initialize QND components
    dna_np = QuantumSparseDNA_NP(dna_size=256, latent_dim=128)
    mm_np = ThreeLevelMemoryManagerNP(l1_size_mb=20, l2_size_mb=100)
    governor_qnd = MetaGovernorQND(dna_np, mm_np)
    governor_qnd.run()

    global research_orchestrator
    research_orchestrator = ResearchOrchestrator(governor_qnd)

    # Step 2: Initialize Al Qalb database
    init_db_alqalb()
    await aiw_memory_store.initialize()
    log.info("✓ Al Qalb database initialized")

    # Step 3: Migrate old knowledge
    migrate_knowledge_from_database()
    auto_migrate_old_knowledge()
    log.info(f"✓ Knowledge migrated — {len(local_brain.knowledge_nodes)} nodes")

    # Step 4: Initialize AL QALB + MetaGovernor 2.0
    al_qalb = AlQalb()
    meta_gov = MetaGovernor(al_qalb)
    al_qalb.meta_governor = meta_gov
    await al_qalb.start()
    log.info("✓ AL QALB cognitive engine started")

    # Step 5: Start Trinitas controllers
    await claw.start()
    await manus.start()
    log.info("✓ Trinitas Bridge controllers started")

    # Wire up event subscribers (after controllers are alive)
    event_bus.subscribe("trade_opened", claw.on_trade_opened)
    event_bus.subscribe("trade_closed", claw.on_trade_closed)
    event_bus.subscribe("emergency_stop", claw.on_emergency)
    event_bus.subscribe("manus_research_request", manus.on_research_request)

    # Step 6: Wallet initialization
    await auto_money_engine.initialize_wallets(count=1)
    log.info("✓ Wallet initialized")

    # Step 7: Start all background tasks
    asyncio.create_task(tier_monitor(), name="tier_monitor")
    asyncio.create_task(daily_scheduler(), name="daily_scheduler")
    asyncio.create_task(uhee_background(), name="uhee_background")
    asyncio.create_task(watchdog(), name="watchdog")
    log.info("✓ Background tasks started")

    # Step 8: Start AIW Core Engine
    asyncio.create_task(aiw_core.run(), name="aiw_core")
    log.info("✓ AIW Core Engine started")

    # Step 9: Publish system_started event
    await event_bus.publish_async("system_started", {
        "timestamp": time.time(),
        "version": "ULTIMATE",
        "brain_nodes": len(local_brain.knowledge_nodes),
    })

    # Step 10: Notify owner
    try:
        await bot.send_message(
            OWNER_ID,
            f"🚀 AL QALB QUANTUM ULTIMATE started!\n"
            f"Brain: {len(local_brain.knowledge_nodes)} nodes\n"
            f"Tier: {tier_ctrl.tier_description()}\n"
            f"Trinitas: Claw={'🟢' if claw.active else '🔴'} Manus={'🟢' if manus.active else '🔴'}"
        )
    except Exception:
        pass

    log.info("🚀 AL QALB QUANTUM ULTIMATE with EVENT BUS started!")

    # Step 11: Start Telegram polling (with auto-reconnect)
    while True:
        try:
            await bot.infinity_polling(
                timeout=120,
                request_timeout=90,
                logger_level=logging.WARNING,
            )
        except Exception as e:
            log.error(f"Telegram polling error: {e}")
            await asyncio.sleep(10)

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n✅ AL QALB QUANTUM ULTIMATE stopped by user.")
    except Exception as e:
        log.critical(f"Fatal error: {e}")
        raise

# =============================================================================
# AUTO-INJECTION-POINT
# (New code injected via /upgrade command is inserted above here)
# =============================================================================

if __name__ == "__main__":
    main()
```

.