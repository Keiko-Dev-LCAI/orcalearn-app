#!/usr/bin/env python3
"""
OrcaLearn – Homeschool dApp Backend
Port 8182 | Served via Cloudflare Tunnel → orcalearn.ai

AI lesson generation via Lightchain AIVM (decentralized workers, ~0.022 LCAI/call).
Requires LIGHTCHAIN_PRIVATE_KEY env var (same wallet as contract-explainer).

Run: python3 ~/Desktop/orcalearn-server.py
"""

import sys
sys.path.insert(0, '/home/keiko/pylibs')

from http.server import HTTPServer, BaseHTTPRequestHandler
import json, os, threading, time, secrets, base64, uuid as _uuid_mod
from urllib.parse import urlparse, parse_qs, quote as url_quote

# ── Config ────────────────────────────────────────────────────────────────
PORT       = 8182
HOME       = os.path.expanduser('~')
DATA_DIR   = HOME

PROFILES_FILE  = os.path.join(DATA_DIR, 'orcalearn-profiles.json')
PLANS_FILE     = os.path.join(DATA_DIR, 'orcalearn-plans.json')
PROGRESS_FILE  = os.path.join(DATA_DIR, 'orcalearn-progress.json')
STATS_FILE     = os.path.join(DATA_DIR, 'orcalearn-stats.json')
PLAN_CACHE_FILE= os.path.join(DATA_DIR, 'orcalearn-plan-cache.json')
QUIZ_FILE      = os.path.join(DATA_DIR, 'orcalearn-quiz-results.json')

MIME = {
    '.html': 'text/html; charset=utf-8',
    '.css':  'text/css',
    '.js':   'application/javascript',
    '.json': 'application/json',
    '.png':  'image/png',
    '.ico':  'image/x-icon',
}

FREE_PLANS    = 3       # free AI plans per wallet before LCAI required
LCAI_PER_PLAN = 5       # LCAI cost per plan beyond free tier (informational — enforced on-chain)

SERVER_START      = time.time()
MAINTENANCE_FLAG  = os.path.expanduser("~/MAINTENANCE_MODE")
_data_lock        = threading.Lock()

_ORCALEARN_MAINTENANCE_HTML = b"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OrcaLearn - Coming Soon</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{background:#0a1628;color:#e8f4f8;
      font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
      min-height:100vh;display:flex;align-items:center;justify-content:center}
    .c{text-align:center;max-width:540px;padding:48px 32px}
    .icon{width:80px;height:80px;
      background:linear-gradient(135deg,#00d4ff,#0077aa);
      border-radius:22px;display:inline-flex;align-items:center;
      justify-content:center;font-size:40px;margin-bottom:20px;
      box-shadow:0 0 48px rgba(0,212,255,.25)}
    h1{font-size:2.4rem;font-weight:700;color:#00d4ff;margin-bottom:10px}
    .sub{font-size:1rem;color:#7ab0c5;margin-bottom:32px}
    .card{background:rgba(0,212,255,.05);
      border:1px solid rgba(0,212,255,.18);
      border-radius:14px;padding:28px 32px;
      font-size:1rem;color:#c8dde8;line-height:1.75}
    .dot{display:inline-block;width:9px;height:9px;
      border-radius:50%;background:#00d4ff;margin-right:10px;
      vertical-align:middle;animation:blink 1.8s ease-in-out infinite}
    @keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
    .foot{margin-top:28px;font-size:.82rem;color:#3d6e80}
  </style>
</head>
<body>
  <div class="c">
    <div class="icon">&#x1F40B;</div>
    <h1>OrcaLearn</h1>
    <p class="sub">AI-powered homeschool curriculum on the blockchain</p>
    <div class="card">
      <span class="dot"></span><strong>Coming Soon</strong><br><br>
      We&rsquo;re rebuilding OrcaLearn for a better, more private experience.
      The new version features smarter AI lesson generation, improved
      student profiles, and a cleaner interface.<br><br>Check back soon.
    </div>
    <p class="foot">orcalearn.ai &nbsp;&middot;&nbsp; Maintenance in progress</p>
  </div>
</body>
</html>
"""

# ════════════════════════════════════════════════════════════════════════
# AIVM CLIENT — Lightchain Decentralized Inference
# ════════════════════════════════════════════════════════════════════════

AIVM_GATEWAY = "https://chat-api.mainnet.lightchain.ai"
AIVM_RELAY   = "wss://relay.mainnet.lightchain.ai/ws"
AIVM_RPC     = "https://rpc.mainnet.lightchain.ai"
AIVM_JOB_REG = "0xfB15F90298e4CcD7106E76fFB5e520315cC42B0b"
AIVM_JOB_FEE = 20_000_000_000_000_000   # 0.02 LCAI in wei
AIVM_CHAIN_ID = 9200

AIVM_ABI = [
    {
        "name": "createSession", "type": "function", "stateMutability": "payable",
        "inputs": [
            {"name": "paramsHash",     "type": "bytes32"},
            {"name": "worker",         "type": "address"},
            {"name": "encWorkerKey",   "type": "bytes"},
            {"name": "ephemeralPubKey","type": "bytes"},
            {"name": "initState",      "type": "bytes"},
            {"name": "expiry",         "type": "uint256"},
        ],
        "outputs": [{"name": "sessionId", "type": "uint256"}],
    },
    {
        "name": "submitJob", "type": "function", "stateMutability": "payable",
        "inputs": [
            {"name": "sessionId",  "type": "uint256"},
            {"name": "promptHash", "type": "bytes32"},
        ],
        "outputs": [{"name": "jobId", "type": "uint256"}],
    },
    {
        "anonymous": False, "name": "SessionCreated", "type": "event",
        "inputs": [
            {"indexed": True,  "name": "sessionId",     "type": "uint256"},
            {"indexed": True,  "name": "user",           "type": "address"},
            {"indexed": True,  "name": "paramsHash",     "type": "bytes32"},
            {"indexed": False, "name": "worker",         "type": "address"},
            {"indexed": False, "name": "encWorkerKey",   "type": "bytes"},
            {"indexed": False, "name": "ephemeralPubKey","type": "bytes"},
        ],
    },
    {
        "anonymous": False, "name": "JobSubmitted", "type": "event",
        "inputs": [
            {"indexed": True,  "name": "jobId",     "type": "uint256"},
            {"indexed": True,  "name": "sessionId", "type": "uint256"},
            {"indexed": False, "name": "worker",    "type": "address"},
        ],
    },
    {
        "anonymous": False, "name": "JobCompleted", "type": "event",
        "inputs": [
            {"indexed": True,  "name": "jobId",          "type": "uint256"},
            {"indexed": True,  "name": "worker",          "type": "address"},
            {"indexed": False, "name": "responseHash",    "type": "bytes32"},
            {"indexed": False, "name": "ciphertextHash",  "type": "bytes32"},
        ],
    },
]


def _decode_pubkey(s):
    """Accept hex (with/without 0x) or base64; return 65-byte uncompressed P-256 point."""
    if isinstance(s, (bytes, bytearray)):
        return bytes(s)
    s = s.strip()
    if s.startswith('0x') or s.startswith('0X'):
        b = bytes.fromhex(s[2:])
    elif len(s) == 130 and all(c in '0123456789abcdefABCDEF' for c in s):
        b = bytes.fromhex(s)
    else:
        b = base64.b64decode(s)
    if len(b) != 65:
        raise ValueError(f"pubkey decode: expected 65 bytes, got {len(b)}")
    return b


def _ecdh_wrap(session_key: bytes, peer_pub_bytes: bytes) -> bytes:
    """ECDH-wrap session_key for peer P-256 pubkey."""
    from cryptography.hazmat.primitives.asymmetric.ec import (
        generate_private_key, ECDH, EllipticCurvePublicNumbers, SECP256R1
    )
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.backends import default_backend

    x = int.from_bytes(peer_pub_bytes[1:33], 'big')
    y = int.from_bytes(peer_pub_bytes[33:65], 'big')
    peer_pub = EllipticCurvePublicNumbers(x, y, SECP256R1()).public_key(default_backend())

    ephem_priv = generate_private_key(SECP256R1(), default_backend())
    shared = ephem_priv.exchange(ECDH(), peer_pub)

    pub_nums = ephem_priv.public_key().public_numbers()
    ephem_pub_bytes = (b'\x04' +
                       pub_nums.x.to_bytes(32, 'big') +
                       pub_nums.y.to_bytes(32, 'big'))

    nonce  = secrets.token_bytes(12)
    ct_tag = AESGCM(shared).encrypt(nonce, session_key, None)
    return ephem_pub_bytes + nonce + ct_tag


def _aes_encrypt(key: bytes, plaintext: bytes) -> bytes:
    """AES-256-GCM encrypt. Returns nonce(12) || ct || tag(16)."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    nonce = secrets.token_bytes(12)
    return nonce + AESGCM(key).encrypt(nonce, plaintext, None)


def _aes_decrypt(key: bytes, blob: bytes) -> bytes:
    """AES-256-GCM decrypt nonce(12) || ct || tag(16)."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    if len(blob) < 28:
        raise ValueError("ciphertext too short")
    return AESGCM(key).decrypt(blob[:12], blob[12:], None)


class AIVMClient:
    """Runs LLM inference through the Lightchain decentralized worker network."""

    def __init__(self, private_key: str):
        import requests as _req
        from web3 import Web3
        from eth_account import Account

        self._req      = _req
        self._w3       = Web3(Web3.HTTPProvider(AIVM_RPC))
        self._account  = Account.from_key(private_key)
        self._registry = self._w3.eth.contract(
            address=Web3.to_checksum_address(AIVM_JOB_REG),
            abi=AIVM_ABI,
        )
        self._jwt     = None
        self._jwt_exp = 0
        print(f"  [AIVM] wallet: {self._account.address}")

    def _get_jwt(self) -> str:
        from eth_account.messages import encode_defunct
        if self._jwt and time.time() < self._jwt_exp - 30:
            return self._jwt
        r = self._req.get(
            f"{AIVM_GATEWAY}/api/auth/challenge",
            params={"address": self._account.address}, timeout=15,
        )
        r.raise_for_status()
        message = r.json()["message"]
        sig = self._account.sign_message(encode_defunct(text=message))
        r2 = self._req.post(
            f"{AIVM_GATEWAY}/api/auth/verify",
            json={"message": message, "signature": "0x" + sig.signature.hex()},
            timeout=15,
        )
        r2.raise_for_status()
        v = r2.json()
        self._jwt = v["token"]
        exp_str = v["expiresAt"][:19].replace("T", " ")
        self._jwt_exp = time.mktime(time.strptime(exp_str, "%Y-%m-%d %H:%M:%S"))
        return self._jwt

    def _auth_headers(self):
        return {
            "Authorization": f"Bearer {self._get_jwt()}",
            "Accept":        "application/json",
            "Content-Type":  "application/json",
        }

    def run_inference(self, prompt: str, timeout_secs: int = 360) -> str:
        import websocket as _ws
        from web3 import Web3

        req = self._req
        print(f"  [AIVM] starting inference ({len(prompt)} chars)")

        # 1-2. Auth + pick model
        r = req.get(f"{AIVM_GATEWAY}/api/models", timeout=15)
        r.raise_for_status()
        models = r.json().get("models", [])
        model  = next((m for m in models if m["name"] == "llama3-8b"), models[0] if models else None)
        if not model:
            raise RuntimeError("No models available from AIVM gateway")
        model_id = model["id"]
        print(f"  [AIVM] model: {model['name']} id={model_id[:10]}…")

        # 3. Select worker
        r = req.post(
            f"{AIVM_GATEWAY}/api/sessions/select",
            json={"modelId": model_id},
            headers=self._auth_headers(), timeout=15,
        )
        r.raise_for_status()
        sel = r.json()
        print(f"  [AIVM] worker: {sel['worker']}")

        # 4-5. Session key + ECDH wrap
        session_key  = secrets.token_bytes(32)
        enc_worker   = _ecdh_wrap(session_key, _decode_pubkey(sel["workerEncryptionKey"]))
        enc_disputer = _ecdh_wrap(session_key, _decode_pubkey(sel["disputerEncryptionKey"]))

        # 6. Prepare (get dispatcher signature)
        r = req.post(
            f"{AIVM_GATEWAY}/api/sessions/prepare",
            json={
                "modelId":        model_id,
                "encWorkerKey":   base64.b64encode(enc_worker).decode(),
                "encDisputerKey": base64.b64encode(enc_disputer).decode(),
            },
            headers=self._auth_headers(), timeout=15,
        )
        r.raise_for_status()
        prep = r.json()

        # 7. createSession on-chain
        def _h(s): return s[2:] if isinstance(s, str) and s[:2].lower() == '0x' else s
        params_hash = bytes.fromhex(_h(model_id).zfill(64))
        sig_bytes   = bytes.fromhex(_h(prep["signature"]))
        gas_price = self._w3.eth.gas_price
        nonce_val = self._w3.eth.get_transaction_count(self._account.address)

        tx = self._registry.functions.createSession(
            params_hash,
            Web3.to_checksum_address(prep["worker"]),
            enc_worker,
            enc_disputer,
            sig_bytes,
            prep["expiry"],
        ).build_transaction({
            "from":     self._account.address,
            "nonce":    nonce_val,
            "gas":      1_000_000,
            "gasPrice": gas_price,
            "value":    0,
            "chainId":  AIVM_CHAIN_ID,
        })
        signed  = self._account.sign_transaction(tx)
        tx_hash = self._w3.eth.send_raw_transaction(signed.raw_transaction)
        print(f"  [AIVM] createSession tx: {tx_hash.hex()}")
        receipt1 = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
        if receipt1.status != 1:
            raise RuntimeError("createSession reverted on-chain")

        session_id = None
        for log in receipt1.logs:
            try:
                evt = self._registry.events.SessionCreated().process_log(log)
                session_id = evt["args"]["sessionId"]
                break
            except Exception:
                pass
        if session_id is None:
            raise RuntimeError("SessionCreated event not found in receipt")
        print(f"  [AIVM] sessionId: {session_id}")

        # 8. Open relay BEFORE submitting job
        relay_token = None
        deadline = time.time() + 120
        while time.time() < deadline:
            r = req.get(
                f"{AIVM_GATEWAY}/api/sessions/{session_id}/token",
                headers=self._auth_headers(), timeout=10,
            )
            if r.status_code == 200:
                d = r.json()
                if d.get("token"):
                    relay_token = d["token"]
                    break
            time.sleep(1)
        if not relay_token:
            raise RuntimeError("Relay token not ready within 30s")

        chunks   = []
        ws_ready = threading.Event()
        ws_err   = [None]

        def _on_message(ws_obj, message):
            try:
                frame = json.loads(message)
                payload = frame.get("payload")
                if not payload:
                    return
                blob = base64.b64decode(payload)
                try:
                    pt = _aes_decrypt(session_key, blob)
                    chunks.append(pt.decode("utf-8", errors="replace"))
                except Exception:
                    pass
            except Exception:
                pass

        def _on_open(ws_obj):
            ws_ready.set()

        def _on_error(ws_obj, err):
            ws_err[0] = err
            ws_ready.set()

        ws = _ws.WebSocketApp(
            f"{AIVM_RELAY}?token={url_quote(relay_token)}",
            on_message=_on_message,
            on_open=_on_open,
            on_error=_on_error,
        )
        ws_thread = threading.Thread(target=ws.run_forever, daemon=True)
        ws_thread.start()
        ws_ready.wait(timeout=15)
        if ws_err[0]:
            raise RuntimeError(f"WebSocket failed: {ws_err[0]}")
        print("  [AIVM] relay connected")

        # 9. Encrypt prompt + upload blob
        cipher = _aes_encrypt(session_key, prompt.encode("utf-8"))
        r = req.post(
            f"{AIVM_GATEWAY}/api/blobs",
            json={"data": base64.b64encode(cipher).decode()},
            headers=self._auth_headers(), timeout=15,
        )
        r.raise_for_status()
        blob_hashes = r.json().get("blobHashes", [])
        if not blob_hashes:
            raise RuntimeError("No blob hash returned from gateway")
        prompt_hash = bytes.fromhex(_h(blob_hashes[0]).zfill(64))

        # 10. submitJob (pay 0.02 LCAI)
        nonce_val2 = self._w3.eth.get_transaction_count(self._account.address)
        tx2 = self._registry.functions.submitJob(
            session_id,
            prompt_hash,
        ).build_transaction({
            "from":     self._account.address,
            "nonce":    nonce_val2,
            "gas":      500_000,
            "gasPrice": gas_price,
            "value":    AIVM_JOB_FEE,
            "chainId":  AIVM_CHAIN_ID,
        })
        signed2  = self._account.sign_transaction(tx2)
        tx_hash2 = self._w3.eth.send_raw_transaction(signed2.raw_transaction)
        print(f"  [AIVM] submitJob tx: {tx_hash2.hex()}")
        receipt2 = self._w3.eth.wait_for_transaction_receipt(tx_hash2, timeout=90)
        if receipt2.status != 1:
            raise RuntimeError("submitJob reverted — check LCAI balance")

        job_id = None
        for log in receipt2.logs:
            try:
                evt = self._registry.events.JobSubmitted().process_log(log)
                job_id = evt["args"]["jobId"]
                break
            except Exception:
                pass
        if job_id is None:
            raise RuntimeError("JobSubmitted event not found in receipt")
        print(f"  [AIVM] jobId: {job_id}")

        # 11. Poll for JobCompleted on-chain (also collecting relay chunks)
        # Fix: Web3.keccak().hex() returns WITHOUT 0x — must add it manually
        job_completed_topic = "0x" + Web3.keccak(
            text="JobCompleted(uint256,address,bytes32,bytes32)"
        ).hex()
        job_id_topic = "0x" + hex(job_id)[2:].zfill(64)

        done     = False
        deadline = time.time() + timeout_secs
        while time.time() < deadline and not done:
            time.sleep(5)

            # Return early if relay already delivered the answer
            if chunks:
                print(f"  [AIVM] relay data arrived ({len(chunks)} chunks), returning early")
                done = True
                break

            try:
                head = self._w3.eth.block_number
                logs = self._w3.eth.get_logs({
                    "address":   Web3.to_checksum_address(AIVM_JOB_REG),
                    "fromBlock": receipt2.blockNumber,
                    "toBlock":   head,
                    "topics":    [job_completed_topic, job_id_topic],
                })
                if logs:
                    done = True
                    print(f"  [AIVM] JobCompleted on-chain!")
            except Exception as e:
                print(f"  [AIVM] log poll error (retrying): {e}")

        time.sleep(4)  # grace period for final relay frames
        ws.close()

        result = "".join(chunks)
        if result:
            print(f"  [AIVM] inference done (relay data), {len(result)} chars")
            return result

        if not done:
            raise RuntimeError(f"Timeout after {timeout_secs}s waiting for JobCompleted")

        print(f"  [AIVM] inference done, {len(result)} chars")
        return result


_aivm_client = None


def get_aivm_client():
    global _aivm_client
    pk = os.environ.get("LIGHTCHAIN_PRIVATE_KEY", "").strip()
    if not pk:
        return None
    if _aivm_client is None:
        try:
            _aivm_client = AIVMClient(pk)
        except Exception as e:
            print(f"  [AIVM] init failed: {e}")
            return None
    return _aivm_client


def run_inference(prompt: str, timeout: int = 300) -> str:
    client = get_aivm_client()
    if client:
        try:
            return client.run_inference(prompt, timeout_secs=timeout)
        except Exception as e:
            print(f"  [AIVM] failed: {e}")
    raise RuntimeError("AI inference unavailable — LIGHTCHAIN_PRIVATE_KEY not set or AIVM unreachable")


# ════════════════════════════════════════════════════════════════════════
# DATA HELPERS
# ════════════════════════════════════════════════════════════════════════

def _load(path, default):
    try:
        with open(path) as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): return default

def _save(path, data):
    with open(path, 'w') as f: json.dump(data, f, indent=2)

def load_profiles():  return _load(PROFILES_FILE, {})
def save_profiles(d): _save(PROFILES_FILE, d)
def load_plans():     return _load(PLANS_FILE, {})
def save_plans(d):    _save(PLANS_FILE, d)
def load_progress():  return _load(PROGRESS_FILE, {})
def save_progress(d): _save(PROGRESS_FILE, d)
def load_stats():     return _load(STATS_FILE, {"total_plans": 0, "total_wallets": 0})
def save_stats(d):    _save(STATS_FILE, d)
def load_plan_cache(): return _load(PLAN_CACHE_FILE, {})
def save_plan_cache(d): _save(PLAN_CACHE_FILE, d)
def load_quiz_results(): return _load(QUIZ_FILE, {})
def save_quiz_results(d): _save(QUIZ_FILE, d)

def normalize_address(addr: str) -> str:
    return addr.lower().strip()

def is_valid_address(addr: str) -> bool:
    import re
    return bool(addr and re.match(r'^0x[0-9a-fA-F]{40}$', addr.strip()))


# ── Lesson plan job queue (async generation) ─────────────────────────────
_jobs: dict = {}
_jobs_lock  = threading.Lock()

def _cleanup_old_jobs():
    cutoff = time.time() - 3600
    with _jobs_lock:
        stale = [k for k, v in _jobs.items() if v.get("ts", 0) < cutoff]
        for k in stale: del _jobs[k]


# ════════════════════════════════════════════════════════════════════════
# PROMPT BUILDER
# ════════════════════════════════════════════════════════════════════════

SUBJECTS = {
    "math":    "Mathematics",
    "reading": "Reading & Language Arts",
    "science": "Science",
    "history": "History & Social Studies",
    "writing": "Writing & Composition",
    "art":     "Art & Creativity",
    "music":   "Music",
    "pe":      "Physical Education",
}

def build_lesson_plan_prompt(subject: str, grade: int, goals: str, days: int) -> str:
    subject_name = SUBJECTS.get(subject.lower(), subject.title())
    grade_desc = {
        1: "1st grade (age 6-7, early reader, counting to 100)",
        2: "2nd grade (age 7-8, beginning chapter books, basic addition/subtraction)",
        3: "3rd grade (age 8-9, multiplication introduction, paragraph writing)",
        4: "4th grade (age 9-10, fractions introduction, multi-paragraph writing)",
        5: "5th grade (age 10-11, decimals, research skills)",
        6: "6th grade (age 11-12, pre-algebra, essay writing)",
        7: "7th grade (age 12-13, algebra introduction, literary analysis)",
        8: "8th grade (age 13-14, algebra, argumentative essays)",
    }.get(grade, f"grade {grade}")

    return f"""You are an experienced homeschool curriculum designer. Create a detailed {days}-day lesson plan for a {grade_desc} student learning {subject_name}.

Parent's goals and notes: {goals if goals else "Standard curriculum progression."}

Format your response as a structured lesson plan with exactly {days} days. For each day include:
- Day number and title
- Learning objectives (2-3 bullet points)
- Main activity or lesson (3-5 sentences explaining what to do)
- Materials needed (brief list)
- Optional extension activity (1 sentence)

Keep language clear and parent-friendly. Be specific and practical — parents should be able to follow this without prior teaching experience. Assume 45-60 minutes per day.

After the {days} days, add a short "Assessment Ideas" section with 2-3 ways to check understanding.

Begin the lesson plan now:"""


# ════════════════════════════════════════════════════════════════════════
# HTTP SERVER
# ════════════════════════════════════════════════════════════════════════

class Handler(BaseHTTPRequestHandler):

    # ── Maintenance mode ─────────────────────────────────────────────────────

    def _serve_maintenance_page(self) -> bool:
        """Serve Coming Soon page if ~/MAINTENANCE_MODE exists. Returns True if served."""
        if not os.path.exists(MAINTENANCE_FLAG):
            return False
        html = _ORCALEARN_MAINTENANCE_HTML
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(html)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(html)
        return True

    def log_message(self, fmt, *args):
        pass  # suppress default Apache-style logs

    def _send_json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, msg, code=400):
        self._send_json({"error": msg}, code)

    def _read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        if not length: return {}
        try: return json.loads(self.rfile.read(length))
        except Exception: return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    # ── Static file serving ───────────────────────────────────────────
    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        qs     = parse_qs(parsed.query)

        if path in ('', '/'):
            return self._serve_file('/home/keiko/Desktop/orcalearn/orcalearn-v2.html')
        if path == '/orcalearn-v2.html':
            return self._serve_file('/home/keiko/Desktop/orcalearn/orcalearn-v2.html')
        if path == '/orcalearn.html':
            return self._serve_file('/home/keiko/Desktop/orcalearn/orcalearn-v2.html')
        if path == '/orcamail-logo.png':
            return self._serve_file('/home/keiko/Desktop/orcamail-logo.png')
        if path == '/orcalearn-logo.png':
            return self._serve_file('/home/keiko/Desktop/orcalearn/orcalearn-logo.png')

        if path == '/api/health':
            uptime = int(time.time() - SERVER_START)
            h, rem = divmod(uptime, 3600); m = rem // 60
            self._send_json({"ok": True, "uptime": uptime,
                             "uptimeLabel": f"{h}h {m}m"})
            return

        if path == '/api/stats':
            stats = load_stats()
            self._send_json({"totalPlans": stats.get("total_plans", 0),
                             "totalWallets": stats.get("total_wallets", 0)})
            return

        if path == '/api/profiles':
            address = qs.get('address', [''])[0].strip()
            if not is_valid_address(address):
                self._send_error("Invalid address"); return
            address = normalize_address(address)
            with _data_lock:
                profiles = load_profiles()
            self._send_json({"profiles": profiles.get(address, [])})
            return

        if path == '/api/plans':
            address = qs.get('address', [''])[0].strip()
            if not is_valid_address(address):
                self._send_error("Invalid address"); return
            address = normalize_address(address)
            with _data_lock:
                plans = load_plans()
            self._send_json({"plans": plans.get(address, [])})
            return

        if path == '/api/progress':
            address  = qs.get('address', [''])[0].strip()
            student  = qs.get('studentId', [''])[0].strip()
            if not is_valid_address(address):
                self._send_error("Invalid address"); return
            address = normalize_address(address)
            with _data_lock:
                progress = load_progress()
            key = f"{address}:{student}" if student else address
            self._send_json({"progress": progress.get(key, {})})
            return

        if path == '/api/job':
            job_id = qs.get('id', [''])[0].strip()
            with _jobs_lock:
                job = _jobs.get(job_id)
            if not job:
                self._send_error("Job not found", 404); return
            self._send_json(job)
            return

        if path == '/api/quiz/results':
            address  = qs.get('address',   [''])[0].strip()
            student  = qs.get('studentId', [''])[0].strip()
            if not is_valid_address(address):
                self._send_error("Invalid address"); return
            address = normalize_address(address)
            key = f"{address}:{student}" if student else address
            with _data_lock:
                quiz_results = load_quiz_results()
            self._send_json({"results": quiz_results.get(key, [])})
            return

        if path == '/api/plan-count':
            address = qs.get('address', [''])[0].strip()
            if not is_valid_address(address):
                self._send_error("Invalid address"); return
            address = normalize_address(address)
            with _data_lock:
                plans = load_plans()
            count = len(plans.get(address, []))
            self._send_json({"count": count, "freePlans": FREE_PLANS,
                             "remaining": max(0, FREE_PLANS - count)})
            return

        self._send_error("Not found", 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')

        if path == '/api/profiles':
            self._handle_save_profile(); return

        if path == '/api/plan/generate':
            self._handle_generate_plan(); return

        if path == '/api/progress/update':
            self._handle_update_progress(); return

        if path == '/api/plans/delete':
            self._handle_delete_plan(); return

        if path == '/api/quiz/save':
            self._handle_save_quiz(); return

        self._send_error("Not found", 404)

    # ── Serve static file ─────────────────────────────────────────────
    def _serve_file(self, fpath):
        import mimetypes, re as _re
        try:
            with open(fpath, 'rb') as f: data = f.read()
            ext  = os.path.splitext(fpath)[1]
            mime = MIME.get(ext, mimetypes.guess_type(fpath)[0] or 'application/octet-stream')
            # v2 is live — no upgrade banner needed
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', str(len(data)))
            # Prevent browser caching so changes are always picked up immediately
            self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
            self.send_header('Pragma', 'no-cache')
            self.end_headers()
            self.wfile.write(data)
        except FileNotFoundError:
            self._send_error("File not found", 404)

    # ── POST /api/profiles ────────────────────────────────────────────
    def _handle_save_profile(self):
        body    = self._read_body()
        address = body.get("address", "")
        profile = body.get("profile", {})

        if not is_valid_address(address):
            self._send_error("Invalid address"); return
        if not profile.get("name"):
            self._send_error("Profile name required"); return

        address = normalize_address(address)
        student_id = profile.get("id") or str(_uuid_mod.uuid4())[:8]
        profile["id"] = student_id

        with _data_lock:
            profiles = load_profiles()
            wallet_profiles = profiles.get(address, [])
            # Update existing or append
            existing = next((i for i, p in enumerate(wallet_profiles) if p["id"] == student_id), None)
            if existing is not None:
                wallet_profiles[existing] = profile
            else:
                wallet_profiles.append(profile)
                # Track new wallets in stats
                stats = load_stats()
                if address not in profiles:
                    stats["total_wallets"] = stats.get("total_wallets", 0) + 1
                    save_stats(stats)
            profiles[address] = wallet_profiles
            save_profiles(profiles)

        self._send_json({"ok": True, "studentId": student_id})

    # ── POST /api/plan/generate ───────────────────────────────────────
    def _handle_generate_plan(self):
        body      = self._read_body()
        address   = body.get("address", "")
        student_id= body.get("studentId", "")
        subject   = body.get("subject", "")
        grade     = int(body.get("grade", 3))
        goals     = body.get("goals", "")
        days      = min(int(body.get("days", 5)), 10)

        if not is_valid_address(address):
            self._send_error("Invalid address"); return
        if not subject:
            self._send_error("Subject required"); return
        if not (1 <= grade <= 12):
            self._send_error("Grade must be 1–12"); return

        address = normalize_address(address)

        # Check cache (same subject+grade+days+goals fingerprint)
        import hashlib
        cache_key = hashlib.md5(
            f"{subject}|{grade}|{days}|{goals.lower().strip()}".encode()
        ).hexdigest()[:12]

        with _data_lock:
            cache = load_plan_cache()
            cached = cache.get(cache_key)

        # Start async job
        job_id = str(_uuid_mod.uuid4())[:12]
        with _jobs_lock:
            _jobs[job_id] = {"status": "pending", "ts": time.time()}

        def _run():
            try:
                if cached:
                    result_text = cached["text"]
                    print(f"  [plan] cache hit: {cache_key}")
                else:
                    prompt = build_lesson_plan_prompt(subject, grade, goals, days)
                    result_text = run_inference(prompt, timeout=300)
                    # Cache the result
                    with _data_lock:
                        c = load_plan_cache()
                        c[cache_key] = {"text": result_text, "ts": time.time()}
                        # Keep cache to 200 entries
                        if len(c) > 200:
                            oldest = sorted(c.items(), key=lambda x: x[1].get("ts", 0))[:50]
                            for k, _ in oldest: del c[k]
                        save_plan_cache(c)

                # Save plan to wallet's plan list
                plan_obj = {
                    "planId":    job_id,
                    "studentId": student_id,
                    "subject":   subject,
                    "grade":     grade,
                    "days":      days,
                    "goals":     goals,
                    "text":      result_text,
                    "createdAt": int(time.time() * 1000),
                    "cacheKey":  cache_key,
                }
                with _data_lock:
                    plans = load_plans()
                    wallet_plans = plans.get(address, [])
                    wallet_plans.append(plan_obj)
                    plans[address] = wallet_plans
                    save_plans(plans)
                    stats = load_stats()
                    stats["total_plans"] = stats.get("total_plans", 0) + 1
                    save_stats(stats)

                with _jobs_lock:
                    _jobs[job_id] = {"status": "done", "ts": time.time(),
                                     "planId": job_id, "text": result_text}
            except Exception as e:
                print(f"  [plan] generation error: {e}")
                with _jobs_lock:
                    _jobs[job_id] = {"status": "error", "ts": time.time(), "error": str(e)}

        threading.Thread(target=_run, daemon=True).start()
        self._send_json({"ok": True, "jobId": job_id})

    # ── POST /api/progress/update ─────────────────────────────────────
    def _handle_update_progress(self):
        # Write endpoint disabled during upgrade — no personal data written to disk
        self._send_json(
            {"error": "Progress saving temporarily disabled during upgrade. Check back soon!"},
            503,
        )
        return

    def _handle_update_progress_DISABLED(self):
        body      = self._read_body()
        address   = body.get("address", "")
        student_id= body.get("studentId", "")
        plan_id   = body.get("planId", "")
        day       = body.get("day")
        completed = body.get("completed", True)

        if not is_valid_address(address):
            self._send_error("Invalid address"); return
        if not (student_id and plan_id):
            self._send_error("studentId and planId required"); return

        address = normalize_address(address)
        key     = f"{address}:{student_id}"

        with _data_lock:
            progress = load_progress()
            entry    = progress.get(key, {})
            plan_prog = entry.get(plan_id, {"completedDays": [], "startedAt": int(time.time() * 1000)})
            if day is not None:
                days = plan_prog.get("completedDays", [])
                if completed and day not in days:
                    days.append(day)
                    # Track activity date for streak calculation
                    today = time.strftime('%Y-%m-%d')
                    existing = entry.get('_activityDates', [])
                    if today not in existing:
                        existing = sorted(set(existing + [today]))[-365:]
                    entry['_activityDates'] = existing
                elif not completed and day in days:
                    days.remove(day)
                plan_prog["completedDays"] = days
                plan_prog["lastUpdated"] = int(time.time() * 1000)
            entry[plan_id] = plan_prog
            progress[key]  = entry
            save_progress(progress)

        self._send_json({"ok": True})

    # ── POST /api/quiz/save ───────────────────────────────────────────
    def _handle_save_quiz(self):
        # Write endpoint disabled during upgrade — no personal data written to disk
        self._send_json(
            {"error": "Score saving temporarily disabled during upgrade. Check back soon!"},
            503,
        )
        return

    def _handle_save_quiz_DISABLED(self):
        body       = self._read_body()
        address    = body.get("address", "")
        student_id = body.get("studentId", "")
        result     = body.get("result", {})

        if not is_valid_address(address):
            self._send_error("Invalid address"); return
        if not student_id:
            self._send_error("studentId required"); return
        if not result:
            self._send_error("result required"); return

        address = normalize_address(address)
        key     = f"{address}:{student_id}"
        result["savedAt"]   = int(time.time() * 1000)
        result["studentId"] = student_id

        with _data_lock:
            quiz_results = load_quiz_results()
            lst = quiz_results.get(key, [])
            lst.insert(0, result)
            if len(lst) > 50: lst = lst[:50]
            quiz_results[key] = lst
            save_quiz_results(quiz_results)

        self._send_json({"ok": True})

    # ── POST /api/plans/delete ────────────────────────────────────────
    def _handle_delete_plan(self):
        body    = self._read_body()
        address = body.get("address", "")
        plan_id = body.get("planId", "")

        if not is_valid_address(address):
            self._send_error("Invalid address"); return
        if not plan_id:
            self._send_error("planId required"); return

        address = normalize_address(address)
        with _data_lock:
            plans = load_plans()
            before = len(plans.get(address, []))
            plans[address] = [p for p in plans.get(address, []) if p["planId"] != plan_id]
            if len(plans[address]) < before:
                save_plans(plans)
                stats = load_stats()
                stats["total_plans"] = max(0, stats.get("total_plans", 0) - 1)
                save_stats(stats)

        self._send_json({"ok": True})


# ════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print(f"OrcaLearn backend starting on port {PORT}…")
    aivm = get_aivm_client()
    if aivm:
        print(f"  AI: Lightchain AIVM (wallet {aivm._account.address})")
    else:
        print("  AI: UNAVAILABLE — set LIGHTCHAIN_PRIVATE_KEY to enable lesson generation")

    server = HTTPServer(('0.0.0.0', PORT), Handler)
    print(f"  Ready: http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
