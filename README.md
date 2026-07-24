# Liquity v2 redemption watcher

A small, deterministic, agent-free watcher that monitors one or more wallets'
[Liquity v2](https://www.liquity.org/) troves (all three mainnet branches:
WETH, wstETH, rETH) and prints an alert when a trove is hit by a **redemption**
(debt retired, collateral redeemed, fee kept, post-state, zombie flag) or a
**liquidation**. Watch a single borrower, or pass a whole list of addresses
(e.g. a front-end tracking all its users) — alerts name the affected wallet.

It is intentionally just a Python script and a cron line — no server, no agent,
no database. The script prints to stdout **only when something happened** and is
otherwise silent, so you can wire it to any notifier (a Telegram bot, ntfy,
email, a Slack webhook, ...) and get a message exactly when a watched trove is
redeemed or liquidated, and nothing the rest of the time.

## Why watch for redemptions?

In Liquity v2, redemptions hit the troves paying the **lowest interest rate**
first (not the lowest collateral ratio, as in v1). A redemption swaps some of
your collateral for BOLD at face value, which for a healthy borrower is roughly
neutral — but it changes your position, can push you into "zombie" status, and
is a signal your interest rate is now the cheapest in the branch. Borrowers and
front-ends (e.g. self-repaying / automation tooling) generally want to know the
moment it happens. This script gives you that with a few lines of cron.

## How it works

1. **Every run** it re-enumerates trove ownership exactly: each branch
   TroveManager's `TroveIds` array is walked (~200 open troves protocol-wide)
   and filtered by `TroveNFT.ownerOf == wallet`. No NFT-transfer tracking, no
   trove-id derivation assumptions — the watched set is always ground truth.
2. It scans `eth_getLogs` from `last_block+1` to the **finalized** head
   (reorg-safe) for `TroveOperation(troveId indexed, ...)` events on the
   watched trove ids (union of current and previously-known, so a trove
   closed/transferred mid-range is still covered), on each branch
   TroveManager, in 5,000-block chunks (outages self-heal by catch-up).
3. For `operation == redeemCollateral (6)` or `liquidate (5)` it decodes the
   same-tx `RedemptionFeePaidToTrove`, `TroveUpdated` and `Redemption` events
   plus current `getLatestTroveData` / `getTroveStatus`, and prints a report.
   State (`state.json`) is only advanced after a successful scan
   (at-least-once delivery).
4. **Failure policy:** exceptions increment a counter; alert at 3 consecutive
   failures (~15 min blind at a 5-min cadence), re-alert every 36 (~3 h),
   silent otherwise. The counter resets on success.

Notes on the v2 mechanics reflected in the report:

- Redemptions route through the `CollateralRegistry` across branches and hit the
  **lowest interest rate** troves first.
- The redeemed borrower **keeps the redemption fee** portion of collateral
  (`RedemptionFeePaidToTrove`), so the collateral lost is slightly less than
  `debt / price` — the report shows both.
- A redemption that leaves debt below `MIN_DEBT` (2,000 BOLD) makes the trove a
  **zombie** (out of the redemption queue until the owner tops up, adjusts or
  closes it) — this is flagged.

## Install

Requires Python 3.9+.

```bash
git clone https://github.com/YOUR_USERNAME/liquity-redemption-watch.git
cd liquity-redemption-watch
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

Set an Ethereum **mainnet** JSON-RPC endpoint. Any node or provider that
supports `eth_getLogs` over ~5,000-block ranges works — your own node, or a
provider such as Alchemy or Infura:

```bash
export LIQUITY_WATCH_RPC=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
```

## Usage

```bash
# 1. configure: verify contracts, find the wallets' troves, anchor at
#    finalized head. Writes config.json + state.json (both gitignored).
#    Pass one address, or several to watch a whole set.
python watcher.py setup 0xYOURWALLET [0xWALLET2 0xWALLET3 ...]

# 2. normal tick — silent unless a watched trove was redeemed/liquidated.
python watcher.py run

# 3. inspect config, state and current positions.
python watcher.py status

# 4. replay a historical block range without touching state (verification).
python watcher.py run --from-block N --to-block M --verbose
```

Example alert:

```
🔔 Liquity v2 redemption hit trove 0xaaaaaa…bbbb (WETH branch, owner 0x1234…cdef)
  debt retired: 1,234.56 BOLD
  collateral redeemed: 0.4210 WETH
  redemption fee kept by trove: +0.0031 WETH
  redemption price used: $2,950.12/WETH
  trove now: 3,010.00 BOLD debt, 1.9800 WETH, rate 5.50% — ACTIVE
  block 20000000, https://etherscan.io/tx/0x0000000000000000000000000000000000000000000000000000000000000000
```

(All values above are placeholders for illustration.)

## Deploy as a cron job

Because the script is silent on quiet ticks, the simplest deployment is a
plain cron entry that pipes any output to your notifier of choice. For example,
using a Telegram bot every 5 minutes:

```cron
*/5 * * * * cd /path/to/liquity-redemption-watch && \
  LIQUITY_WATCH_RPC=https://eth-mainnet.example/v2/KEY \
  .venv/bin/python watcher.py run 2>&1 | \
  { read -r line && { printf '%s\n' "$line"; cat; } | \
    curl -s -X POST "https://api.telegram.org/bot$TG_TOKEN/sendMessage" \
      --data-urlencode "chat_id=$TG_CHAT" --data-urlencode "text=$(cat)"; }
```

(Any transport works — swap the `curl` for `ntfy`, `mail`, a Slack webhook,
etc. The only contract is: **no output = no notification**.) You can also run it
under systemd with an `OnCalendar` timer if you prefer.

To change the watched set, re-run `setup` with the new address(es) — it replaces
`config.json` and re-anchors `state.json`. No cron changes needed.

Enumeration walks every open trove protocol-wide (~200) once per branch per run
regardless of how many wallets you watch, so watching many addresses costs
essentially the same as watching one.

## Contract addresses (Liquity v2 mainnet)

Source: [docs.liquity.org](https://docs.liquity.org/) contract addresses
(current deployment). Cross-verified on-chain: every address has code, and
`CollateralRegistry.getTroveManager(i)` / `getToken(i)` + `TroveManager.troveNFT()`
reproduce these addresses exactly. `setup` re-asserts the registry wiring every
time it runs.

| contract | address |
|---|---|
| CollateralRegistry | `0xf949982B91C8c61e952B3bA942cbbfaef5386684` |
| BOLD token | `0x6440f144b7e50D6a8439336510312d2F54beB01D` |
| WETH TroveManager / TroveNFT | `0x7bcb64B2c9206a5B699eD43363f6F98D4776Cf5A` / `0x1A0FC0b843aFD9140267D25d4E575Cb37a838013` |
| wstETH TroveManager / TroveNFT | `0xA2895d6A3bf110561Dfe4b71cA539d84e1928B22` / `0x857aECeBF75f1012DC18E15020C97096aeA31b04` |
| rETH TroveManager / TroveNFT | `0xb2B2ABEb5C357a234363FF5D180912D319e3e19e` / `0x7ae430E25b67f19B431e1D1Dc048a5BCF24C0873` |

Event signatures were taken from
[`liquity/bold`](https://github.com/liquity/bold) `ITroveEvents.sol` and
verified against live logs on the deployed contracts (`TroveOperation` topic0
`0x962110f2...`). If you point the watcher at a different Liquity v2 deployment
(e.g. a fork or a testnet), replace the addresses in `watcher.py` accordingly.

## Files

| file | purpose |
|---|---|
| `watcher.py` | the whole watcher (setup / run / status) |
| `config.json` | generated by `setup`: the watched wallet(s) — **gitignored** |
| `state.json` | generated by `setup`/`run`: last scanned block + trove ids — **gitignored** |
| `config.example.json` | shows the shape of `config.json` |

## License

MIT — see [LICENSE](LICENSE).
