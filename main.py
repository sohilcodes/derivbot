import asyncio
import json
import websockets
import numpy as np
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Deriv Algo Bot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Config ───────────────────────────────────────────────────
DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3"
APP_ID = "019ede2f-47e1-7750-bd54-5af51d303bbf"          # Replace with your Deriv App ID
API_TOKEN = "pat_e64c75760702e74ddf6d555d5f3f9fbc89e7dd0dda2bf07b4416cce7b59741e5"    # Replace with your Deriv API Token
SYMBOL = "frxEURUSD"
STAKE = 1.0
DURATION = 5
DURATION_UNIT = "m"
RSI_PERIOD = 14
MA_PERIOD = 20

# ─── State ────────────────────────────────────────────────────
bot_state = {
    "running": False,
    "balance": 0.0,
    "trades": [],
    "ticks": [],
    "last_signal": None,
    "profit_loss": 0.0,
    "wins": 0,
    "losses": 0,
}

# ─── Indicators ───────────────────────────────────────────────
def calculate_rsi(prices: list, period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices[-period-1:])
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains)
    avg_loss = np.mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_ma(prices: list, period: int = 20) -> float:
    if len(prices) < period:
        return prices[-1] if prices else 0
    return np.mean(prices[-period:])

def get_signal(ticks: list) -> Optional[str]:
    if len(ticks) < max(RSI_PERIOD, MA_PERIOD) + 1:
        return None
    rsi = calculate_rsi(ticks, RSI_PERIOD)
    ma = calculate_ma(ticks, MA_PERIOD)
    current_price = ticks[-1]

    logger.info(f"RSI: {rsi:.2f} | MA: {ma:.5f} | Price: {current_price:.5f}")

    if rsi < 30 and current_price < ma:
        return "CALL"
    elif rsi > 70 and current_price > ma:
        return "PUT"
    return None

# ─── WebSocket Bot ────────────────────────────────────────────
async def run_bot():
    ws_url = f"{DERIV_WS_URL}?app_id={APP_ID}"
    logger.info("Connecting to Deriv WebSocket...")

    try:
        async with websockets.connect(ws_url) as ws:
            # Authorize
            await ws.send(json.dumps({"authorize": API_TOKEN}))
            auth_resp = json.loads(await ws.recv())
            if "error" in auth_resp:
                logger.error(f"Auth failed: {auth_resp['error']['message']}")
                bot_state["running"] = False
                return
            bot_state["balance"] = auth_resp["authorize"]["balance"]
            logger.info(f"Authorized. Balance: {bot_state['balance']}")

            # Subscribe to ticks
            await ws.send(json.dumps({
                "ticks": SYMBOL,
                "subscribe": 1
            }))

            trade_in_progress = False

            while bot_state["running"]:
                try:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                except asyncio.TimeoutError:
                    logger.warning("Tick timeout, continuing...")
                    continue

                # Handle tick
                if msg.get("msg_type") == "tick":
                    price = float(msg["tick"]["quote"])
                    bot_state["ticks"].append(price)
                    if len(bot_state["ticks"]) > 200:
                        bot_state["ticks"] = bot_state["ticks"][-200:]

                    if not trade_in_progress:
                        signal = get_signal(bot_state["ticks"])
                        if signal:
                            bot_state["last_signal"] = signal
                            logger.info(f"Signal: {signal} — Placing trade...")
                            trade_in_progress = True

                            await ws.send(json.dumps({
                                "buy": 1,
                                "price": STAKE,
                                "parameters": {
                                    "amount": STAKE,
                                    "basis": "stake",
                                    "contract_type": signal,
                                    "currency": "USD",
                                    "duration": DURATION,
                                    "duration_unit": DURATION_UNIT,
                                    "symbol": SYMBOL,
                                }
                            }))

                # Handle buy response
                elif msg.get("msg_type") == "buy":
                    if "error" in msg:
                        logger.error(f"Trade error: {msg['error']['message']}")
                        trade_in_progress = False
                    else:
                        contract_id = msg["buy"]["contract_id"]
                        buy_price = msg["buy"]["buy_price"]
                        logger.info(f"Trade placed! Contract ID: {contract_id} | Buy price: {buy_price}")

                        trade = {
                            "id": contract_id,
                            "signal": bot_state["last_signal"],
                            "stake": STAKE,
                            "buy_price": buy_price,
                            "time": datetime.now().strftime("%H:%M:%S"),
                            "status": "open"
                        }
                        bot_state["trades"].append(trade)

                        # Subscribe to contract update
                        await ws.send(json.dumps({
                            "proposal_open_contract": 1,
                            "contract_id": contract_id,
                            "subscribe": 1
                        }))

                # Handle contract result
                elif msg.get("msg_type") == "proposal_open_contract":
                    poc = msg.get("proposal_open_contract", {})
                    if poc.get("is_sold") == 1:
                        profit = poc.get("profit", 0)
                        status = "win" if profit > 0 else "loss"
                        bot_state["profit_loss"] += profit
                        if profit > 0:
                            bot_state["wins"] += 1
                        else:
                            bot_state["losses"] += 1

                        # Update trade in list
                        for t in bot_state["trades"]:
                            if t["id"] == poc.get("contract_id"):
                                t["status"] = status
                                t["profit"] = profit
                                break

                        logger.info(f"Trade {status.upper()} | Profit: {profit} | Total P&L: {bot_state['profit_loss']:.2f}")

                        # Update balance
                        await ws.send(json.dumps({"balance": 1}))
                        trade_in_progress = False

                elif msg.get("msg_type") == "balance":
                    bot_state["balance"] = msg["balance"]["balance"]

    except Exception as e:
        logger.error(f"Bot error: {e}")
        bot_state["running"] = False

# ─── API Routes ───────────────────────────────────────────────
class BotConfig(BaseModel):
    token: str
    app_id: str

@app.get("/")
def root():
    return {"status": "Deriv Bot API running"}

@app.post("/start")
async def start_bot(config: BotConfig):
    global API_TOKEN, APP_ID
    if bot_state["running"]:
        return {"message": "Bot already running"}
    API_TOKEN = config.token
    APP_ID = config.app_id
    bot_state["running"] = True
    bot_state["trades"] = []
    bot_state["ticks"] = []
    bot_state["profit_loss"] = 0.0
    bot_state["wins"] = 0
    bot_state["losses"] = 0
    asyncio.create_task(run_bot())
    return {"message": "Bot started"}

@app.post("/stop")
async def stop_bot():
    bot_state["running"] = False
    return {"message": "Bot stopped"}

@app.get("/status")
async def get_status():
    total_trades = bot_state["wins"] + bot_state["losses"]
    win_rate = (bot_state["wins"] / total_trades * 100) if total_trades > 0 else 0
    return {
        "running": bot_state["running"],
        "balance": bot_state["balance"],
        "profit_loss": round(bot_state["profit_loss"], 2),
        "wins": bot_state["wins"],
        "losses": bot_state["losses"],
        "win_rate": round(win_rate, 1),
        "last_signal": bot_state["last_signal"],
        "recent_trades": bot_state["trades"][-10:],
        "tick_count": len(bot_state["ticks"]),
  }
                                      
