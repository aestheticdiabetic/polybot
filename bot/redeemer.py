"""
redeemer.py — Polygon position redemption via Alchemy webhooks.
Watches for market resolution events on-chain and automatically
redeems winning shares back to USDC.
"""
import asyncio
import json
import logging
import os
import time
from typing import Dict, Set
from aiohttp import web

from config import ALCHEMY_RPC, ALCHEMY_API_KEY, PRIVATE_KEY, FUNDER_ADDRESS, SIM

log = logging.getLogger("redeemer")

# Polymarket CTF Exchange contract on Polygon
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# PositionSettled event topic
POSITION_SETTLED_TOPIC = "0x1f99a5a5c09e05db7e498f1d73d29ee06aa1c7462db9946b36a0e2e6e9f46e62"

# Persist redeemed set across restarts so we never re-submit gas for old positions
REDEEMED_LOG = "/app/logs/redeemed.json"


class Redeemer:
    def __init__(self, state_manager):
        self.state = state_manager
        self._redeemed: Set[str] = self._load_redeemed()
        self._web3 = None
        self._app = None
        self.stats = {
            "redemptions_completed": 0,
            "redemptions_failed": 0,
            "usdc_redeemed": 0.0,
            "gas_spent_usdc": 0.0,
        }

    def _load_redeemed(self) -> Set[str]:
        """Load persisted set of already-redeemed condition IDs."""
        try:
            if os.path.exists(REDEEMED_LOG):
                with open(REDEEMED_LOG, "r") as f:
                    data = json.load(f)
                loaded = set(data)
                log.info(f"Loaded {len(loaded)} previously redeemed condition IDs from disk")
                return loaded
        except Exception as e:
            log.warning(f"Could not load redeemed log: {e}")
        return set()

    def _save_redeemed(self):
        """Persist the redeemed set so restarts don't re-submit old positions."""
        try:
            os.makedirs(os.path.dirname(REDEEMED_LOG), exist_ok=True)
            with open(REDEEMED_LOG, "w") as f:
                json.dump(list(self._redeemed), f)
        except Exception as e:
            log.warning(f"Could not save redeemed log: {e}")

    async def start(self):
        if SIM.enabled:
            log.info("Redeemer in SIM mode — no on-chain calls")
            return

        await self._init_web3()
        # Start webhook server for Alchemy notifications
        asyncio.get_event_loop().create_task(self._start_webhook_server())
        # Also poll as fallback
        asyncio.get_event_loop().create_task(self._poll_loop())
        log.info("Redeemer started — Alchemy webhook + fallback polling active")

    async def _init_web3(self):
        try:
            from web3 import Web3
            self._web3 = Web3(Web3.HTTPProvider(ALCHEMY_RPC))
            if self._web3.is_connected():
                log.info(f"Web3 connected to Polygon via Alchemy")
            else:
                log.warning("Web3 connection failed — will retry")
        except ImportError:
            log.error("web3 not installed — install with: pip install web3")
        except Exception as e:
            log.error(f"Web3 init error: {e}")

    # ── Alchemy webhook receiver ──────────────────────────────────

    async def _start_webhook_server(self):
        """
        Lightweight HTTP server to receive Alchemy 'mined transaction'
        webhooks. Configure in Alchemy dashboard:
          URL: http://<your-vps-ip>:8082/webhook
          Network: Polygon Mainnet
          Type: Mined Transaction
          Filter: address = CTF_EXCHANGE
        """
        app = web.Application()
        app.router.add_post("/webhook", self._handle_webhook)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", 8082)
        await site.start()
        log.info("Alchemy webhook server listening on :8082/webhook")

    async def _handle_webhook(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
            # Alchemy sends activity array
            for activity in payload.get("activity", []):
                await self._process_activity(activity)
        except Exception as e:
            log.error(f"Webhook handler error: {e}")
        return web.Response(text="ok")

    async def _process_activity(self, activity: dict):
        """Check if this on-chain event is a market resolution we care about."""
        logs = activity.get("log", {})
        topics = logs.get("topics", [])

        if not topics or topics[0].lower() != POSITION_SETTLED_TOPIC.lower():
            return

        condition_id = activity.get("condition_id") or logs.get("data", "")[:66]
        if condition_id and condition_id not in self._redeemed:
            log.info(f"Resolution detected on-chain: {condition_id}")
            await self._redeem(condition_id)

    # ── Fallback polling ─────────────────────────────────────────

    async def _poll_loop(self):
        """
        Poll the Polymarket Data API every 30s to find resolved positions
        that need redemption. Acts as fallback if webhook misses an event.
        """
        import aiohttp
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    url = f"https://data-api.polymarket.com/positions"
                    params = {"user": FUNDER_ADDRESS, "redeemable": "true", "limit": 100}
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        positions = await r.json()

                for pos in (positions or []):
                    cid = pos.get("conditionId")
                    if not cid or cid in self._redeemed:
                        continue
                    if not pos.get("redeemable"):
                        continue
                    # Skip zero-share losers — no USDC to recover, gas is wasted
                    size = float(pos.get("size", 0) or 0)
                    if size == 0:
                        log.debug(f"Skipping zero-share position {cid} — nothing to redeem")
                        # Mark as handled so we never check it again
                        self._redeemed.add(cid)
                        self._save_redeemed()
                        continue
                    log.info(f"Redeemable position found (poll): {pos.get('title', cid)} size={size}")
                    await self._redeem(cid)
            except Exception as e:
                log.debug(f"Poll error: {e}")
            await asyncio.sleep(30)

    # ── Redemption ───────────────────────────────────────────────

    async def _redeem(self, condition_id: str):
        """Call redeemPositions on the CTF contract."""
        if condition_id in self._redeemed:
            return

        if not self._web3:
            # Don't mark as redeemed — web3 wasn't ready, safe to retry later
            log.warning(f"Cannot redeem {condition_id} — web3 not initialised")
            return

        # Mark as redeemed BEFORE submitting to prevent duplicate submissions
        # across restarts. Even a failed/0-return tx means the position is settled.
        self._redeemed.add(condition_id)
        self._save_redeemed()

        try:
            success, amount = await asyncio.get_event_loop().run_in_executor(
                None, self._do_redeem_tx, condition_id
            )
            if success:
                self.stats["redemptions_completed"] += 1
                self.stats["usdc_redeemed"] += amount
                self.state.record_redemption(condition_id, amount)
                log.info(f"Redeemed {condition_id} → ${amount:.4f} USDC")
                # Wait before processing next redemption to avoid nonce collisions
                await asyncio.sleep(2)
            else:
                self.stats["redemptions_failed"] += 1
                # Do NOT discard — tx was submitted, position is settled on-chain.
                # Re-submitting would just waste more gas.
                log.warning(f"Redemption tx failed for {condition_id} — skipping retry to save gas")
        except Exception as e:
            log.error(f"Redemption error for {condition_id}: {e}")
            # Do NOT discard from _redeemed — the tx may have been broadcast.
            # Operator should investigate rather than auto-retry.

    def _do_redeem_tx(self, condition_id: str) -> tuple[bool, float]:
        """
        Execute redeemPositions() on-chain via Alchemy RPC.
        Returns (success, usdc_amount).
        """
        try:
            from web3 import Web3
            from eth_account import Account

            w3 = self._web3
            account = Account.from_key(PRIVATE_KEY)

            # Minimal CTF ABI for redeemPositions
            ctf_abi = [{
                "name": "redeemPositions",
                "type": "function",
                "inputs": [
                    {"name": "collateralToken", "type": "address"},
                    {"name": "parentCollectionId", "type": "bytes32"},
                    {"name": "conditionId", "type": "bytes32"},
                    {"name": "indexSets", "type": "uint256[]"},
                ],
                "outputs": [],
                "stateMutability": "nonpayable",
            }]

            USDC_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            ctf = w3.eth.contract(address=CTF_CONTRACT, abi=ctf_abi)

            nonce = w3.eth.get_transaction_count(account.address)
            gas_price = w3.eth.gas_price

            tx = ctf.functions.redeemPositions(
                USDC_POLYGON,
                b'\x00' * 32,          # parentCollectionId = 0
                bytes.fromhex(condition_id.replace("0x", "")),
                [1, 2],                # index sets for binary market
            ).build_transaction({
                "from": account.address,
                "nonce": nonce,
                "gas": 300_000,
                "gasPrice": int(gas_price * 1.1),  # 10% priority bump
            })

            signed = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            if receipt.status == 1:
                gas_cost_matic = receipt.gasUsed * gas_price / 1e18
                # Approximate MATIC→USDC (rough — update with live price if needed)
                gas_cost_usdc = gas_cost_matic * 0.85
                self.stats["gas_spent_usdc"] += gas_cost_usdc
                return True, 0.0    # actual USDC amount requires event parsing
            return False, 0.0
        except Exception as e:
            log.error(f"Redemption tx error: {e}")
            return False, 0.0
