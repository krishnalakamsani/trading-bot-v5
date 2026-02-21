import React, { useState, useEffect, useRef, useCallback } from "react";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import axios from "axios";
import { Toaster } from "@/components/ui/sonner";
import { toast } from "sonner";
import Dashboard from "@/pages/Dashboard";
import TradesAnalysis from "@/pages/TradesAnalysis";
import Settings from "@/pages/Settings";
import "@/App.css";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || '';
export const API = `${BACKEND_URL}/api`;

const getWsUrl = () => {
  if (!BACKEND_URL || BACKEND_URL === '') {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${window.location.host}`;
  }
  return BACKEND_URL.replace('https://', 'wss://').replace('http://', 'ws://');
};
const WS_BASE = getWsUrl();

export const AppContext = React.createContext();

function App() {
  const [botStatus, setBotStatus] = useState({
    is_running: false,
    mode: "paper",
    market_status: "closed",
    daily_max_loss_triggered: false,
    trading_enabled: true,
    selected_index: "NIFTY",
    candle_interval: 5,
    mds_score: 0,
    mds_slope: 0,
    mds_acceleration: 0,
    mds_stability: 0,
    mds_confidence: 0,
    mds_is_choppy: false,
    mds_direction: "NONE",
  });

  const [marketData, setMarketData] = useState({
    ltp: 0,
    mds_score: 0,
    mds_direction: "NONE",
    mds_confidence: 0,
    selected_index: "NIFTY",
  });

  // Real-time candle history built from WebSocket `candle` messages
  const [candleHistory, setCandleHistory] = useState([]);

  const [position, setPosition] = useState(null);
  const [trades, setTrades] = useState([]);
  const [summary, setSummary] = useState({
    total_trades: 0,
    total_pnl: 0,
    max_drawdown: 0,
    daily_stop_triggered: false,
  });
  const [logs, setLogs] = useState([]);
  const [config, setConfig] = useState({
    order_qty: 1,
    max_trades_per_day: 5,
    daily_max_loss: 2000,
    max_loss_per_trade: 0,
    initial_stoploss: 50,
    trail_start_profit: 10,
    trail_step: 5,
    target_points: 0,
    risk_per_trade: 0,
    indicator_type: "score_mds",
    supertrend_period: 7,
    supertrend_multiplier: 4,
    macd_fast: 12,
    macd_slow: 26,
    macd_signal: 9,
    macd_confirmation_enabled: true,
    min_trade_gap: 0,
    trade_only_on_flip: true,
    htf_filter_enabled: true,
    htf_filter_timeframe: 60,
    min_hold_seconds: 15,
    min_order_cooldown_seconds: 15,
    max_trade_duration_seconds: 0,
    has_credentials: false,
    mode: "paper",
    selected_index: "NIFTY",
    candle_interval: 5,
    lot_size: 65,
    strike_interval: 50,
    trading_enabled: true,
    bypass_market_hours: false,
  });

  const [indices, setIndices] = useState([]);
  const [timeframes, setTimeframes] = useState([]);
  const [wsConnected, setWsConnected] = useState(false);

  const wsRef = useRef(null);
  const reconnectTimerRef = useRef(null);
  const retryDelayRef = useRef(3000);
  const configRef = useRef(config);

  useEffect(() => { configRef.current = config; }, [config]);

  // ── Initial REST load ────────────────────────────────────────────────────
  const fetchData = useCallback(async () => {
    try {
      const [statusRes, positionRes, tradesRes, summaryRes, logsRes, configRes, indicesRes, timeframesRes] =
        await Promise.all([
          axios.get(`${API}/status`),
          axios.get(`${API}/position`),
          axios.get(`${API}/trades`),
          axios.get(`${API}/summary`),
          axios.get(`${API}/logs?limit=100`),
          axios.get(`${API}/config`),
          axios.get(`${API}/indices`),
          axios.get(`${API}/timeframes`),
        ]);
      setBotStatus(prev => ({ ...prev, ...statusRes.data }));
      setPosition(positionRes.data);
      setTrades(tradesRes.data);
      setSummary(summaryRes.data);
      setLogs(logsRes.data);
      setConfig(configRes.data);
      setIndices(indicesRes.data);
      setTimeframes(timeframesRes.data);
    } catch (err) {
      console.error("[App] fetchData error:", err);
    }
  }, []);

  // ── Single WebSocket ──────────────────────────────────────────────────────
  useEffect(() => {
    fetchData();

    const handleMessage = (msg) => {
      switch (msg.type) {

        // Raw tick — update LTP immediately, no chart update
        case "tick": {
          const { ltp } = msg.data;
          setMarketData(prev => ({ ...prev, ltp }));
          break;
        }

        // Closed candle — append to chart history
        case "candle": {
          const { open, high, low, close, ts } = msg.data;
          const time = new Date(ts * 1000).toLocaleTimeString("en-IN", {
            hour: "2-digit", minute: "2-digit", second: "2-digit",
          });
          setCandleHistory(prev => [...prev, { time, open, high, low, close, price: close }].slice(-120));
          break;
        }

        // Full bot state snapshot — position, P&L, signals, daily stats
        case "state_update": {
          const u = msg.data;
          const cfg = configRef.current;

          setMarketData(prev => ({
            ...prev,
            ltp: u.index_ltp ?? prev.ltp,
            mds_score: u.mds_score ?? prev.mds_score,
            mds_direction: u.mds_direction ?? prev.mds_direction,
            mds_confidence: u.mds_confidence ?? prev.mds_confidence,
            selected_index: u.selected_index ?? prev.selected_index,
          }));

          setBotStatus(prev => ({
            ...prev,
            is_running: u.is_running,
            mode: u.mode,
            market_status: u.market_status ?? prev.market_status,
            trading_enabled: u.trading_enabled,
            selected_index: u.selected_index,
            candle_interval: u.candle_interval,
            daily_max_loss_triggered: u.daily_max_loss_triggered ?? prev.daily_max_loss_triggered,
            mds_score: u.mds_score ?? prev.mds_score,
            mds_slope: u.mds_slope ?? prev.mds_slope,
            mds_acceleration: u.mds_acceleration ?? prev.mds_acceleration,
            mds_stability: u.mds_stability ?? prev.mds_stability,
            mds_confidence: u.mds_confidence ?? prev.mds_confidence,
            mds_is_choppy: u.mds_is_choppy ?? prev.mds_is_choppy,
            mds_direction: u.mds_direction ?? prev.mds_direction,
          }));

          setSummary(prev => ({
            ...prev,
            total_trades: u.daily_trades ?? prev.total_trades,
            total_pnl: u.daily_pnl ?? prev.total_pnl,
            max_drawdown: u.max_drawdown ?? prev.max_drawdown,
            daily_stop_triggered: u.daily_max_loss_triggered ?? prev.daily_stop_triggered,
          }));

          if (u.position) {
            const qty = u.position.qty ?? (cfg.order_qty * cfg.lot_size);
            setPosition({
              has_position: true,
              ...u.position,
              entry_price: u.entry_price,
              current_ltp: u.current_option_ltp,
              trailing_sl: u.trailing_sl,
              qty,
              unrealized_pnl: (u.current_option_ltp - u.entry_price) * qty,
            });
          } else {
            setPosition({ has_position: false });
          }

          // Re-subscribe to TickEngine if index changed remotely
          if (u.selected_index && u.selected_index !== configRef.current.selected_index) {
            const ws = wsRef.current;
            if (ws?.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify({ type: "subscribe", index: u.selected_index, interval: u.candle_interval || 5 }));
            }
          }
          break;
        }

        // Daily loss limit hit
        case "daily_stop_triggered": {
          toast.error(msg.data?.message || "Daily loss limit reached. Trading stopped.", { duration: 10000 });
          setSummary(prev => ({ ...prev, daily_stop_triggered: true }));
          setBotStatus(prev => ({ ...prev, daily_max_loss_triggered: true }));
          break;
        }

        // Trade closed — refresh trades list
        case "trade_closed": {
          axios.get(`${API}/trades`).then(r => setTrades(r.data)).catch(() => {});
          break;
        }

        case "heartbeat":
        case "ack":
        default:
          break;
      }
    };

    const connect = () => {
      // Guard: never open a second socket while one is OPEN or CONNECTING
      const state = wsRef.current?.readyState;
      if (state === WebSocket.OPEN || state === WebSocket.CONNECTING) return;

      const token = process.env.REACT_APP_WS_TOKEN || '';
      const url = token ? `${WS_BASE}/ws?token=${token}` : `${WS_BASE}/ws`;
      const ws = new WebSocket(url);

      ws.onopen = () => {
        setWsConnected(true);
        retryDelayRef.current = 3000;
        console.log("[WS] Connected");
        // Subscribe to current index immediately on open
        const cfg = configRef.current;
        ws.send(JSON.stringify({
          type: "subscribe",
          index: cfg.selected_index || "NIFTY",
          interval: cfg.candle_interval || 5,
        }));
      };

      ws.onmessage = (event) => {
        try { handleMessage(JSON.parse(event.data)); }
        catch (e) { console.warn("[WS] Parse error:", e); }
      };

      ws.onclose = () => {
        setWsConnected(false);
        console.log(`[WS] Disconnected — retrying in ${retryDelayRef.current}ms`);
        reconnectTimerRef.current = setTimeout(connect, retryDelayRef.current);
        retryDelayRef.current = Math.min(retryDelayRef.current * 2, 30000);
      };

      ws.onerror = (err) => { console.warn("[WS] Error:", err); };

      wsRef.current = ws;
    };

    connect();

    return () => {
      clearTimeout(reconnectTimerRef.current);
      wsRef.current?.close();
    };
  }, [fetchData]);

  // Resend subscribe when user changes index or timeframe
  const resubscribe = useCallback((index, interval) => {
    const ws = wsRef.current;
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "subscribe", index, interval }));
      setCandleHistory([]);  // clear stale candles from previous index
    }
  }, []);

  // ── Bot controls ─────────────────────────────────────────────────────────
  const startBot = async () => {
    try { const r = await axios.post(`${API}/bot/start`); toast.success(r.data.message); fetchData(); }
    catch (e) { toast.error(e.response?.data?.detail || "Failed to start bot"); }
  };

  const stopBot = async () => {
    try { const r = await axios.post(`${API}/bot/stop`); toast.success(r.data.message); fetchData(); }
    catch (e) { toast.error(e.response?.data?.detail || "Failed to stop bot"); }
  };

  const squareOff = async () => {
    try { const r = await axios.post(`${API}/bot/squareoff`); toast.success(r.data.message); fetchData(); }
    catch (e) { toast.error(e.response?.data?.detail || "Failed to square off"); }
  };

  const updateConfig = async (newConfig) => {
    try {
      await axios.post(`${API}/config/update`, newConfig);
      toast.success("Configuration updated");
      await new Promise(r => setTimeout(r, 300));
      fetchData();
    } catch (e) { toast.error(e.response?.data?.detail || "Failed to update config"); }
  };

  const setMode = async (mode) => {
    try { await axios.post(`${API}/config/mode?mode=${mode}`); toast.success(`Mode changed to ${mode}`); fetchData(); }
    catch (e) { toast.error(e.response?.data?.detail || "Failed to change mode"); }
  };

  const setSelectedIndex = async (indexName) => {
    try {
      await axios.post(`${API}/config/update`, { selected_index: indexName });
      resubscribe(indexName, configRef.current.candle_interval || 5);
      toast.success(`Index changed to ${indexName}`);
      fetchData();
    } catch (e) { toast.error(e.response?.data?.detail || "Failed to change index"); }
  };

  const setTimeframe = async (interval) => {
    try {
      await axios.post(`${API}/config/update`, { candle_interval: interval });
      resubscribe(configRef.current.selected_index || "NIFTY", interval);
      toast.success("Timeframe updated");
      fetchData();
    } catch (e) { toast.error(e.response?.data?.detail || "Failed to change timeframe"); }
  };

  const refreshLogs = async () => {
    try { const r = await axios.get(`${API}/logs?limit=100`); setLogs(r.data); }
    catch (e) { console.error("Failed to refresh logs:", e); }
  };

  return (
    <AppContext.Provider value={{
      botStatus, marketData, niftyData: marketData,
      candleHistory,
      position, trades, summary, logs,
      config, indices, timeframes, wsConnected,
      startBot, stopBot, squareOff, updateConfig,
      setMode, setSelectedIndex, setTimeframe, refreshLogs, fetchData,
    }}>
      <div className="min-h-screen bg-white">
        <Toaster position="top-right" richColors />
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="/analysis" element={<TradesAnalysis />} />
          </Routes>
        </BrowserRouter>
      </div>
    </AppContext.Provider>
  );
}

export default App;
