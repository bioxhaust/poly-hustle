"""Merge balanced Yes+No pairs back into collateral ($1 per pair).

This wallet is a Polymarket Deposit Wallet (SIGNATURE_TYPE=3, POLY_1271): a
smart-contract wallet. Only the wallet itself can move its outcome tokens, so a
merge is a gasless wallet batch signed by the owner key and submitted through
Polymarket's relayer. The sanctioned way to do that from Python is the unified
SDK (`polymarket-client`), `SecureClient.merge_positions`, which picks the route
(V2 Router, CtfCollateralAdapter, or NegRiskCtfCollateralAdapter for neg-risk
markets) and checks the mergeable balance. The relayer needs either a Relayer
API key (RELAYER_API_KEY + RELAYER_API_KEY_ADDRESS) or a Builder API key
(BUILDER_API_KEY / BUILDER_SECRET / BUILDER_PASSPHRASE). Without one, merging
is unavailable and the bot falls back to selling pairs.
"""

import os

from eth_abi import encode
from eth_utils import keccak, to_checksum_address

MERGE_ENABLED = os.getenv("MERGE_ENABLED", "1") != "0"
MERGE_MIN_SHARES = float(os.getenv("MERGE_MIN_SHARES", "1"))
MERGE_WAIT_SECONDS = float(os.getenv("MERGE_WAIT_SECONDS", "120"))
# Fallback when merge is unavailable: sell both legs at taker prices only if
# their combined bids return at least 1 - this tolerance per pair.
PAIR_SELL_TOLERANCE = float(os.getenv("PAIR_SELL_TOLERANCE", "0.01"))

PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
CTF_ADAPTER = "0xAdA100Db00Ca00073811820692005400218FcE1f"
NEG_RISK_CTF_ADAPTER = "0xadA2005600Dec949baf300f4C6120000bDB6eAab"
MERGE_SIG = "mergePositions(address,bytes32,bytes32,uint256[],uint256)"
MERGE_SELECTOR = keccak(text=MERGE_SIG)[:4]


def floor2(x):
    return int(x * 100 + 1e-9) / 100


def merge_amount(yes_size, no_size):
    """Shares to merge: the balanced part, to the cent. None below MERGE_MIN_SHARES."""
    amount = floor2(min(float(yes_size), float(no_size)))
    return amount if amount + 1e-9 >= MERGE_MIN_SHARES else None


def to_units(shares):
    """Outcome tokens use 6 decimals."""
    return int(round(float(shares) * 1_000_000))


def build_ctf_merge_call(condition_id, shares, neg_risk=False):
    """The CTF-adapter wallet call, for logging and dry runs. The SDK builds the live one."""
    cid = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)
    if len(cid) != 32:
        raise ValueError("condition id must be 32 bytes")
    data = MERGE_SELECTOR + encode(
        ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
        [to_checksum_address(PUSD), b"\x00" * 32, cid, [1, 2], to_units(shares)],
    )
    target = NEG_RISK_CTF_ADAPTER if neg_risk else CTF_ADAPTER
    return {"target": to_checksum_address(target), "value": "0", "data": "0x" + data.hex()}


def relayer_api_key(env=os.environ):
    """Relayer or Builder key from the environment, or None. Never logs values."""
    try:
        from polymarket import BuilderApiKey, RelayerApiKey
    except ImportError:
        return None
    if env.get("RELAYER_API_KEY") and env.get("RELAYER_API_KEY_ADDRESS"):
        return RelayerApiKey(key=env["RELAYER_API_KEY"], address=env["RELAYER_API_KEY_ADDRESS"])
    if env.get("BUILDER_API_KEY") and env.get("BUILDER_SECRET") and env.get("BUILDER_PASSPHRASE"):
        return BuilderApiKey(key=env["BUILDER_API_KEY"], secret=env["BUILDER_SECRET"],
                             passphrase=env["BUILDER_PASSPHRASE"])
    return None


class Merger:
    """Lazily built SDK client. `available()` is False when merge cannot run safely."""

    def __init__(self, env=os.environ, factory=None):
        self.env = env
        self.factory = factory
        self._client = None
        self.reason = None

    def available(self):
        if not MERGE_ENABLED:
            self.reason = "MERGE_ENABLED=0"
            return False
        if self.env.get("SIGNATURE_TYPE", "1") != "3":
            self.reason = "merge is wired for the Deposit Wallet (SIGNATURE_TYPE=3) only"
            return False
        if not self.env.get("PRIVATE_KEY") or not self.env.get("FUNDER_ADDRESS"):
            self.reason = "PRIVATE_KEY or FUNDER_ADDRESS missing"
            return False
        if self.factory is None and relayer_api_key(self.env) is None:
            self.reason = "no relayer/builder API key or polymarket-client not installed"
            return False
        self.reason = None
        return True

    def _sdk(self):
        if self._client is None:
            if self.factory is not None:
                self._client = self.factory()
            else:
                from polymarket import SecureClient
                self._client = SecureClient.create(
                    private_key=self.env["PRIVATE_KEY"], wallet=self.env["FUNDER_ADDRESS"],
                    api_key=relayer_api_key(self.env))
        return self._client

    def merge(self, condition_id, shares, dry_run=False, neg_risk=False, progress=None):
        """Merge `shares` pairs. Returns a dict with ok, tx and the built call. Blocking.

        The SDK's wait() has no timeout; the caller enforces MERGE_WAIT_SECONDS and
        reconciles from balances. `progress` receives the relayer transaction id as
        soon as the batch is submitted, so a timeout can still be traced.
        """
        if dry_run:
            call = build_ctf_merge_call(condition_id, shares, neg_risk)
            return {"ok": True, "dry_run": True, "call": call, "tx": None}
        handle = self._sdk().merge_positions(
            condition_id=condition_id, amount=to_units(shares),
            metadata=f"poly-hustle merge {shares:g}")
        if progress is not None:
            progress["transaction_id"] = getattr(handle, "transaction_id", None)
            progress["submitted"] = True
        outcome = handle.wait(timeout=MERGE_WAIT_SECONDS) if _accepts_timeout(handle.wait) else handle.wait()
        tx = getattr(outcome, "transaction_hash", None)
        return {"ok": bool(tx), "dry_run": False, "tx": tx}


def _accepts_timeout(fn):
    try:
        import inspect
        return "timeout" in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False


def pair_sell_ok(yes_bid, no_bid):
    """Fallback check: selling one Yes and one No at these bids returns >= 1 - tolerance."""
    if yes_bid is None or no_bid is None:
        return False
    return float(yes_bid) + float(no_bid) >= 1 - PAIR_SELL_TOLERANCE - 1e-9
