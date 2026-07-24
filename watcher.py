#!/usr/bin/env python3
"""
Liquity v2 redemption watcher — deterministic, no-agent, self-gating.

Watches a wallet's Liquity v2 troves (all three branches: WETH, wstETH, rETH)
and reports when one is hit by a redemption: debt retired, collateral redeemed,
fee kept, post-redemption trove state, zombie flag. Liquidations of watched
troves are reported too (same event stream, higher severity).

The script is deliberately a plain cron job with no agent: it prints a report
to stdout ONLY when something happened, and is otherwise completely silent.
Pipe stdout into any notifier you like (a Telegram bot, ntfy, email, a Slack
webhook, ...) and you get a message exactly when a watched trove is redeemed or
liquidated — and nothing the rest of the time.

Set the RPC endpoint first (any Ethereum mainnet JSON-RPC works):
  export LIQUITY_WATCH_RPC=https://eth-mainnet.example/v2/KEY

Usage:
  python watcher.py setup 0xWALLET [0xWALLET2 ...]
      one-time: verify contracts, enumerate every watched wallet's troves,
      write config.json + state.json anchored at the current finalized block
  python watcher.py run
      cron entrypoint: scan (last_block, finalized] for redemptions/liquidations
      of watched troves; print reports only when something happened
  python watcher.py run --from-block N --to-block M --verbose
      replay a historical range without touching state (verification/testing)
  python watcher.py status
      human-readable dump of config, state and current trove positions

Address provenance: docs.liquity.org contract-addresses page, cross-verified
on-chain against the CollateralRegistry's own wiring
(getTroveManager/getToken/troveNFT). See README.md.
"""

import json
import os
import sys
import time

from web3 import Web3

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(HERE, "config.json")
STATE_FILE = os.path.join(HERE, "state.json")

# Ethereum mainnet JSON-RPC endpoint. Any node/provider that supports
# eth_getLogs over ~5k-block ranges works (your own node, Alchemy, Infura, ...).
# No default — set LIQUITY_WATCH_RPC in the environment.
RPC_URL = os.environ.get("LIQUITY_WATCH_RPC", "")

# --- Liquity v2 mainnet (current deployment, NOT legacy) --------------------
COLLATERAL_REGISTRY = "0xf949982B91C8c61e952B3bA942cbbfaef5386684"
BOLD_TOKEN = "0x6440f144b7e50D6a8439336510312d2F54beB01D"
BRANCHES = [
    # name, TroveManager, TroveNFT
    ("WETH",   "0x7bcb64B2c9206a5B699eD43363f6F98D4776Cf5A", "0x1A0FC0b843aFD9140267D25d4E575Cb37a838013"),
    ("wstETH", "0xA2895d6A3bf110561Dfe4b71cA539d84e1928B22", "0x857aECeBF75f1012DC18E15020C97096aeA31b04"),
    ("rETH",   "0xb2B2ABEb5C357a234363FF5D180912D319e3e19e", "0x7ae430E25b67f19B431e1D1Dc048a5BCF24C0873"),
]

# ITroveManager.Status enum
STATUS = {0: "nonExistent", 1: "ACTIVE", 2: "closed by owner", 3: "CLOSED BY LIQUIDATION", 4: "ZOMBIE"}
# ITroveEvents.Operation enum values we react to
OP_LIQUIDATE = 5
OP_REDEEM = 6

LOG_CHUNK = 5000          # eth_getLogs chunk size (safe across most RPC providers)
FAIL_ALERT_AT = 3         # consecutive failures before alerting (~15 min at 5-min cadence)
FAIL_REALERT_EVERY = 36   # then re-alert every N failures (~3 h)

TM_ABI = [
    {"name": "getTroveIdsCount", "type": "function", "stateMutability": "view",
     "inputs": [], "outputs": [{"type": "uint256"}]},
    {"name": "getTroveFromTroveIdsArray", "type": "function", "stateMutability": "view",
     "inputs": [{"type": "uint256"}], "outputs": [{"type": "uint256"}]},
    {"name": "getTroveStatus", "type": "function", "stateMutability": "view",
     "inputs": [{"type": "uint256"}], "outputs": [{"type": "uint8"}]},
    {"name": "getLatestTroveData", "type": "function", "stateMutability": "view",
     "inputs": [{"type": "uint256"}],
     "outputs": [{"type": "tuple", "components": [
         {"name": "entireDebt", "type": "uint256"},
         {"name": "entireColl", "type": "uint256"},
         {"name": "redistBoldDebtGain", "type": "uint256"},
         {"name": "redistCollGain", "type": "uint256"},
         {"name": "accruedInterest", "type": "uint256"},
         {"name": "recordedDebt", "type": "uint256"},
         {"name": "annualInterestRate", "type": "uint256"},
         {"name": "weightedRecordedDebt", "type": "uint256"},
         {"name": "accruedBatchManagementFee", "type": "uint256"},
         {"name": "lastInterestRateAdjTime", "type": "uint256"},
     ]}]},
    # events (ITroveEvents) — signatures verified against on-chain logs, see README
    {"name": "TroveOperation", "type": "event", "anonymous": False, "inputs": [
        {"name": "_troveId", "type": "uint256", "indexed": True},
        {"name": "_operation", "type": "uint8", "indexed": False},
        {"name": "_annualInterestRate", "type": "uint256", "indexed": False},
        {"name": "_debtIncreaseFromRedist", "type": "uint256", "indexed": False},
        {"name": "_debtIncreaseFromUpfrontFee", "type": "uint256", "indexed": False},
        {"name": "_debtChangeFromOperation", "type": "int256", "indexed": False},
        {"name": "_collIncreaseFromRedist", "type": "uint256", "indexed": False},
        {"name": "_collChangeFromOperation", "type": "int256", "indexed": False},
    ]},
    {"name": "TroveUpdated", "type": "event", "anonymous": False, "inputs": [
        {"name": "_troveId", "type": "uint256", "indexed": True},
        {"name": "_debt", "type": "uint256", "indexed": False},
        {"name": "_coll", "type": "uint256", "indexed": False},
        {"name": "_stake", "type": "uint256", "indexed": False},
        {"name": "_annualInterestRate", "type": "uint256", "indexed": False},
        {"name": "_snapshotOfTotalCollRedist", "type": "uint256", "indexed": False},
        {"name": "_snapshotOfTotalDebtRedist", "type": "uint256", "indexed": False},
    ]},
    {"name": "RedemptionFeePaidToTrove", "type": "event", "anonymous": False, "inputs": [
        {"name": "_troveId", "type": "uint256", "indexed": True},
        {"name": "_ETHFee", "type": "uint256", "indexed": False},
    ]},
    {"name": "Redemption", "type": "event", "anonymous": False, "inputs": [
        {"name": "_attemptedBoldAmount", "type": "uint256", "indexed": False},
        {"name": "_actualBoldAmount", "type": "uint256", "indexed": False},
        {"name": "_ETHSent", "type": "uint256", "indexed": False},
        {"name": "_ETHFee", "type": "uint256", "indexed": False},
        {"name": "_price", "type": "uint256", "indexed": False},
        {"name": "_redemptionPrice", "type": "uint256", "indexed": False},
    ]},
]
NFT_ABI = [
    {"name": "ownerOf", "type": "function", "stateMutability": "view",
     "inputs": [{"type": "uint256"}], "outputs": [{"type": "address"}]},
]


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def connect():
    if not RPC_URL:
        raise SystemExit(
            "Set the LIQUITY_WATCH_RPC environment variable to a mainnet JSON-RPC URL, "
            "e.g. export LIQUITY_WATCH_RPC=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY")
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 60}))
    cid = w3.eth.chain_id
    if cid != 1:
        raise RuntimeError(f"unexpected chainId {cid} (want 1 mainnet)")
    return w3


def fmt(wei, dec=4):
    return f"{wei / 1e18:,.{dec}f}"


def short_id(trove_id):
    h = f"{trove_id:064x}"
    return f"0x{h[:6]}…{h[-4:]}"


def short_addr(addr):
    return f"{addr[:6]}…{addr[-4:]}"


def config_wallets(cfg):
    """Watched wallets, supporting the multi-wallet `wallets` list and the
    legacy single `wallet` key (older config.json files)."""
    if cfg.get("wallets"):
        return cfg["wallets"]
    if cfg.get("wallet"):
        return [cfg["wallet"]]
    return []


def enumerate_troves(w3, wallets):
    """Exact wallets→troves resolution: enumerate every open trove per branch
    (only ~200 exist protocol-wide) and keep the ones owned by any watched
    wallet. Returns (troves_by_branch, owner_by_trove_id)."""
    want = {w.lower() for w in wallets}
    out = {}
    owners = {}
    for name, tm_addr, nft_addr in BRANCHES:
        tm = w3.eth.contract(address=tm_addr, abi=TM_ABI)
        nft = w3.eth.contract(address=nft_addr, abi=NFT_ABI)
        n = tm.functions.getTroveIdsCount().call()
        mine = []
        for i in range(n):
            tid = tm.functions.getTroveFromTroveIdsArray(i).call()
            try:
                owner = nft.functions.ownerOf(tid).call()
            except Exception:
                continue
            if owner.lower() in want:
                mine.append(tid)
                owners[tid] = owner
        out[name] = mine
    return out, owners


def hx(b):
    """HexBytes → 0x-prefixed hex string (this env's hexbytes .hex() is unprefixed)."""
    s = b.hex()
    return s if s.startswith("0x") else "0x" + s


def _event_sig(abi):
    types = ",".join(i["type"] for i in abi["inputs"])
    return f"{abi['name']}({types})"


def decode_receipt_events(w3, tm, receipt, tm_addr):
    """Decode this branch TroveManager's events out of a tx receipt."""
    by_name = {}
    topic_map = {}
    for ev_name in ("TroveUpdated", "RedemptionFeePaidToTrove", "Redemption"):
        ev = getattr(tm.events, ev_name)()
        topic_map[hx(w3.keccak(text=_event_sig(ev.abi)))] = ev
    for lg in receipt["logs"]:
        if lg["address"].lower() != tm_addr.lower():
            continue
        ev = topic_map.get(hx(lg["topics"][0]))
        if ev is None:
            continue
        dec = ev.process_log(lg)
        by_name.setdefault(dec["event"], []).append(dec)
    return by_name


def scan_range(w3, watch, from_block, to_block, owners=None, verbose=False):
    """Scan (from_block..to_block) for redeemCollateral/liquidate TroveOperations
    on watched troves. Returns list of report strings."""
    owners = owners or {}
    reports = []
    for name, tm_addr, nft_addr in BRANCHES:
        ids = watch.get(name, [])
        if not ids:
            continue
        tm = w3.eth.contract(address=tm_addr, abi=TM_ABI)
        op_ev = tm.events.TroveOperation()
        topic0 = hx(w3.keccak(text=_event_sig(op_ev.abi)))
        id_topics = ["0x" + f"{tid:064x}" for tid in ids]
        logs = []
        start = from_block
        while start <= to_block:
            end = min(start + LOG_CHUNK - 1, to_block)
            logs += w3.eth.get_logs({"fromBlock": start, "toBlock": end,
                                     "address": tm_addr, "topics": [topic0, id_topics]})
            start = end + 1
        if verbose:
            print(f"[debug] {name}: {len(logs)} TroveOperation logs on watched troves "
                  f"in {from_block}-{to_block}", file=sys.stderr)
        for lg in logs:
            dec = op_ev.process_log(lg)
            op = dec["args"]["_operation"]
            if op not in (OP_REDEEM, OP_LIQUIDATE):
                continue
            owner = owners.get(dec["args"]["_troveId"])
            reports.append(report_event(w3, tm, tm_addr, name, dec, lg, owner))
    return reports


def report_event(w3, tm, tm_addr, branch, dec, lg, owner=None):
    a = dec["args"]
    tid = a["_troveId"]
    who = f", owner {short_addr(owner)}" if owner else ""
    txh = hx(lg["transactionHash"])
    debt_retired = -a["_debtChangeFromOperation"]
    coll_removed = -a["_collChangeFromOperation"]
    receipt = w3.eth.get_transaction_receipt(txh)
    evs = decode_receipt_events(w3, tm, receipt, tm_addr)

    fee_kept = sum(e["args"]["_ETHFee"] for e in evs.get("RedemptionFeePaidToTrove", [])
                   if e["args"]["_troveId"] == tid)
    red = evs.get("Redemption", [])
    red_price = red[0]["args"]["_redemptionPrice"] if red else None

    # current state (head), authoritative for "where does this leave me"
    status = tm.functions.getTroveStatus(tid).call()
    now = tm.functions.getLatestTroveData(tid).call()
    now_debt, now_coll, now_rate = now[0], now[1], now[6]

    if a["_operation"] == OP_LIQUIDATE:
        lines = [f"🚨 Liquity v2: trove {short_id(tid)} ({branch} branch{who}) was LIQUIDATED",
                 f"  debt cleared: {fmt(debt_retired, 2)} BOLD",
                 f"  collateral taken: {fmt(coll_removed)} {branch}"]
    else:
        lines = [f"🔔 Liquity v2 redemption hit trove {short_id(tid)} ({branch} branch{who})",
                 f"  debt retired: {fmt(debt_retired, 2)} BOLD",
                 f"  collateral redeemed: {fmt(coll_removed)} {branch}"]
        if fee_kept:
            lines.append(f"  redemption fee kept by trove: +{fmt(fee_kept)} {branch}")
        if red_price:
            lines.append(f"  redemption price used: ${fmt(red_price, 2)}/{branch}")
    lines.append(f"  trove now: {fmt(now_debt, 2)} BOLD debt, {fmt(now_coll)} {branch}, "
                 f"rate {now_rate / 1e16:.2f}% — {STATUS.get(status, status)}")
    if status == 4:
        lines.append("  ⚠️ trove is now a ZOMBIE (debt below 2,000 BOLD minimum): out of the "
                     "redemption queue until you top up, adjust or close it")
    lines.append(f"  block {lg['blockNumber']}, https://etherscan.io/tx/{txh}")
    return "\n".join(lines)


def cmd_setup(wallets, from_block=None):
    w3 = connect()
    wallets = [Web3.to_checksum_address(w) for w in wallets]
    # sanity: registry wiring still matches our pinned branch addresses
    reg = w3.eth.contract(address=COLLATERAL_REGISTRY, abi=[
        {"name": "getTroveManager", "type": "function", "stateMutability": "view",
         "inputs": [{"type": "uint256"}], "outputs": [{"type": "address"}]}])
    for i, (name, tm_addr, _) in enumerate(BRANCHES):
        onchain = reg.functions.getTroveManager(i).call()
        assert onchain.lower() == tm_addr.lower(), f"registry mismatch on branch {name}"
    troves, owners = enumerate_troves(w3, wallets)
    anchor = from_block if from_block is not None else w3.eth.get_block("finalized")["number"]
    save_json(CONFIG_FILE, {"wallets": wallets, "created": time.strftime("%Y-%m-%d %H:%M:%S")})
    save_json(STATE_FILE, {"last_block": anchor,
                           "troves": {k: [hex(t) for t in v] for k, v in troves.items()},
                           "consecutive_failures": 0})
    total = sum(len(v) for v in troves.values())
    print(f"watching {len(wallets)} wallet(s) from block {anchor}:")
    for w in wallets:
        print(f"  {w}")
    for name, tm_addr, _ in BRANCHES:
        tm = w3.eth.contract(address=tm_addr, abi=TM_ABI)
        for tid in troves[name]:
            d = tm.functions.getLatestTroveData(tid).call()
            st = tm.functions.getTroveStatus(tid).call()
            print(f"  {name} trove {short_id(tid)} ({short_addr(owners[tid])}): "
                  f"{fmt(d[0], 2)} BOLD debt, {fmt(d[1])} {name}, "
                  f"rate {d[6] / 1e16:.2f}% — {STATUS.get(st, st)}")
    if total == 0:
        print("  (no open troves found for these wallets right now — the watcher will pick "
              "up any trove they open or receive later)")


def cmd_run(from_block=None, to_block=None, verbose=False):
    replay = from_block is not None
    cfg = load_json(CONFIG_FILE)
    state = load_json(STATE_FILE)
    if not cfg or not state:
        print("[liquity-watch] ALERT: not configured — run `watcher.py setup <wallet>` "
              "or pause the cron.")
        return
    try:
        w3 = connect()
        wallets = config_wallets(cfg)
        head = to_block if to_block is not None else w3.eth.get_block("finalized")["number"]
        start = from_block if replay else state["last_block"] + 1
        if head < start:
            return  # nothing new final yet
        current, owners = enumerate_troves(w3, wallets)
        # union with previously-known troves so events on a trove that was
        # closed/transferred inside the range are still caught
        watch = {}
        for name, _, _ in BRANCHES:
            prev = {int(x, 16) for x in state["troves"].get(name, [])}
            watch[name] = sorted(prev | set(current.get(name, [])))
        reports = scan_range(w3, watch, start, head, owners=owners, verbose=verbose)
        for r in reports:
            print(r)
            print()
    except Exception as e:
        state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
        n = state["consecutive_failures"]
        save_json(STATE_FILE, state)
        if n == FAIL_ALERT_AT or (n > FAIL_ALERT_AT and (n - FAIL_ALERT_AT) % FAIL_REALERT_EVERY == 0):
            print(f"[liquity-watch] ALERT: {n} consecutive failures, watcher is blind. "
                  f"last error: {type(e).__name__}: {e}")
        return
    if not replay:
        state.update({"last_block": head,
                      "troves": {k: [hex(t) for t in v] for k, v in current.items()},
                      "consecutive_failures": 0})
        save_json(STATE_FILE, state)


def cmd_status():
    cfg = load_json(CONFIG_FILE)
    state = load_json(STATE_FILE)
    if not cfg:
        print("not configured (no config.json)")
        return
    w3 = connect()
    head = w3.eth.get_block("finalized")["number"]
    wallets = config_wallets(cfg)
    print(f"wallets ({len(wallets)}): {', '.join(wallets)}")
    print(f"last scanned block: {state['last_block']} (finalized head {head}, "
          f"lag {head - state['last_block']})")
    print(f"consecutive failures: {state.get('consecutive_failures', 0)}")
    troves, owners = enumerate_troves(w3, wallets)
    for name, tm_addr, _ in BRANCHES:
        tm = w3.eth.contract(address=tm_addr, abi=TM_ABI)
        for tid in troves[name]:
            d = tm.functions.getLatestTroveData(tid).call()
            st = tm.functions.getTroveStatus(tid).call()
            print(f"  {name} trove {short_id(tid)} ({short_addr(owners[tid])}): "
                  f"{fmt(d[0], 2)} BOLD debt, {fmt(d[1])} {name}, "
                  f"rate {d[6] / 1e16:.2f}% — {STATUS.get(st, st)}")
    if not any(troves.values()):
        print("  no open troves for these wallets")


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return
    cmd, rest = args[0], args[1:]

    def flag(name):
        if name in rest:
            i = rest.index(name)
            v = rest[i + 1]
            del rest[i:i + 2]
            return int(v)
        return None

    if cmd == "setup":
        fb = flag("--from-block")
        if not rest:
            print("usage: watcher.py setup 0xWALLET [0xWALLET2 ...]")
            sys.exit(1)
        cmd_setup(rest, from_block=fb)
    elif cmd == "run":
        fb, tb = flag("--from-block"), flag("--to-block")
        cmd_run(from_block=fb, to_block=tb, verbose="--verbose" in rest)
    elif cmd == "status":
        cmd_status()
    else:
        print(f"unknown command {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
