import { useEffect, useRef, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

type TickMessage = {
  type: string;
  symbol: string;
  tsUnixMs: number;
  price: number;
  size: number;
};

type TickMap = Record<string, TickMessage>;
type PriceHistory = Record<string, TickMessage[]>;

const SYMBOL_COLORS: Record<string, string> = {
  AAPL: "#00ff99",
  MSFT: "#00aaff",
  GOOG: "#ff9900",
  NVDA: "#ff4466",
  TSLA: "#cc44ff",
  AMZN: "#ffcc00",
};

const MAX_HISTORY = 60;

export default function App() {
  const [symbolInput, setSymbolInput] = useState("AAPL,MSFT,TSLA,NVDA,AMZN");
  const [connectionStatus, setConnectionStatus] = useState("Disconnected");
  const [latestTicks, setLatestTicks] = useState<TickMap>({});
  const [priceHistory, setPriceHistory] = useState<PriceHistory>({});
  const [activeSymbols, setActiveSymbols] = useState<string[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);

  const socketRef = useRef<WebSocket | null>(null);
  const tickBufferRef = useRef<TickMap>({});

  // WebSocket setup
  useEffect(() => {
    const socket = new WebSocket("ws://localhost:5176/ws/marketdata");
    socketRef.current = socket;

    socket.onopen = () => {
      console.log("WebSocket opened");
      setConnectionStatus("Connected");
    };

    socket.onclose = (event) => {
      console.log("WebSocket closed", event);
      setConnectionStatus("Disconnected");
    };

    socket.onerror = (event) => {
      console.log("WebSocket error", event);
      setConnectionStatus("Error");
    };

    socket.onmessage = (event) => {
      try {
        const message: TickMessage = JSON.parse(event.data);
        if (message.type !== "tick") return;
        tickBufferRef.current[message.symbol] = message;
      } catch {
        // ignore malformed messages
      }
    };

    return () => {
      socket.close();
    };
  }, []);

  // Flush buffer to state every 100ms
  useEffect(() => {
    const interval = setInterval(() => {
      const buffer = tickBufferRef.current;
      if (Object.keys(buffer).length === 0) return;

      const snapshot = { ...buffer };

      setLatestTicks((prev) => ({ ...prev, ...snapshot }));

      setPriceHistory((prev) => {
        const next = { ...prev };
        for (const symbol of Object.keys(snapshot)) {
          const existing = next[symbol] ?? [];
          const updated = [...existing, snapshot[symbol]];
          next[symbol] =
            updated.length > MAX_HISTORY
              ? updated.slice(updated.length - MAX_HISTORY)
              : updated;
        }
        return next;
      });

      setActiveSymbols((prev) => {
        const incoming = Object.keys(snapshot);
        const newSymbols = incoming.filter((s) => !prev.includes(s));
        return newSymbols.length > 0 ? [...prev, ...newSymbols] : prev;
      });
    }, 100);

    return () => clearInterval(interval);
  }, []);

  function handleSubscribe() {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      console.log("Socket is not open");
      return;
    }

    const symbols = symbolInput
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter((s) => s.length > 0);

    const payload = { type: "set_symbols", symbols };
    console.log("Sending subscribe payload:", payload);
    socket.send(JSON.stringify(payload));
  }

  const visibleSymbols = selectedSymbol ? [selectedSymbol] : activeSymbols;

  // Normalize prices to % change from first tick in history window
  const chartData = (() => {
    if (visibleSymbols.length === 0) return [];

    const maxLen = Math.max(
      ...visibleSymbols.map((s) => (priceHistory[s] ?? []).length)
    );

    if (maxLen === 0) return [];

    const baselines: Record<string, number> = {};
    for (const symbol of visibleSymbols) {
      const history = priceHistory[symbol] ?? [];
      if (history.length > 0) {
        baselines[symbol] = history[0].price;
      }
    }

    return Array.from({ length: maxLen }, (_, i) => {
      const point: Record<string, number | string> = { index: i };
      for (const symbol of visibleSymbols) {
        const history = priceHistory[symbol] ?? [];
        const offset = history.length - maxLen;
        const tick = history[offset + i];
        const baseline = baselines[symbol];
        if (tick !== undefined && baseline !== undefined) {
          point[symbol] = parseFloat(
            (((tick.price - baseline) / baseline) * 100).toFixed(4)
          );
        }
      }
      return point;
    });
  })();

  const rows = Object.values(latestTicks).sort((a, b) =>
    a.symbol.localeCompare(b.symbol)
  );

  return (
    <div
      style={{
        padding: "24px",
        fontFamily: "Arial, sans-serif",
        backgroundColor: "#0f1117",
        minHeight: "100vh",
        color: "#e0e0e0",
      }}
    >
      <h1 style={{ color: "#00ff99", marginBottom: "4px" }}>
        Trading Analytics
      </h1>

      <p
        style={{
          color: connectionStatus === "Connected" ? "#00ff99" : "#ff4466",
          marginBottom: "16px",
        }}
      >
        ● {connectionStatus}
      </p>

      <div
        style={{
          marginBottom: "16px",
          display: "flex",
          gap: "8px",
          alignItems: "center",
        }}
      >
        <input
          value={symbolInput}
          onChange={(e) => setSymbolInput(e.target.value)}
          style={{
            width: "300px",
            backgroundColor: "#1a1d27",
            border: "1px solid #333",
            color: "#e0e0e0",
            padding: "6px 10px",
            borderRadius: "4px",
          }}
        />
        <button
          onClick={handleSubscribe}
          style={{
            backgroundColor: "#00ff99",
            color: "#0f1117",
            border: "none",
            padding: "6px 16px",
            borderRadius: "4px",
            fontWeight: "bold",
            cursor: "pointer",
          }}
        >
          Subscribe
        </button>
      </div>

      {/* Symbol filter buttons */}
      <div
        style={{
          display: "flex",
          gap: "8px",
          marginBottom: "16px",
          flexWrap: "wrap",
        }}
      >
        <button
          onClick={() => setSelectedSymbol(null)}
          style={filterButtonStyle(selectedSymbol === null, "#00ff99")}
        >
          All
        </button>
        {activeSymbols.map((symbol) => (
          <button
            key={symbol}
            onClick={() =>
              setSelectedSymbol(selectedSymbol === symbol ? null : symbol)
            }
            style={filterButtonStyle(
              selectedSymbol === symbol,
              SYMBOL_COLORS[symbol] ?? "#ffffff"
            )}
          >
            {symbol}
          </button>
        ))}
      </div>

      {/* Chart */}
      <div
        style={{
          backgroundColor: "#1a1d27",
          borderRadius: "8px",
          padding: "16px",
          marginBottom: "24px",
        }}
      >
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData}>
            <XAxis
              dataKey="index"
              tick={{ fill: "#888", fontSize: 11 }}
              tickFormatter={() => ""}
            />
            <YAxis
              tick={{ fill: "#888", fontSize: 11 }}
              domain={["auto", "auto"]}
              width={70}
              tickFormatter={(v) => `${v > 0 ? "+" : ""}${v.toFixed(2)}%`}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#1a1d27",
                border: "1px solid #333",
                color: "#e0e0e0",
              }}
              formatter={(value, name) => {
                const num = Number(value ?? 0);
                return [
                  `${num > 0 ? "+" : ""}${num.toFixed(3)}%`,
                  String(name ?? ""),
                ];
              }}
            />
            <Legend wrapperStyle={{ color: "#e0e0e0" }} />
            {visibleSymbols.map((symbol) => (
              <Line
                key={symbol}
                type="monotone"
                dataKey={symbol}
                stroke={SYMBOL_COLORS[symbol] ?? "#ffffff"}
                dot={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Watchlist table */}
      <table
        style={{
          borderCollapse: "collapse",
          width: "100%",
          maxWidth: "700px",
        }}
      >
        <thead>
          <tr>
            <th style={thStyle}>Symbol</th>
            <th style={thStyle}>Price</th>
            <th style={thStyle}>Size</th>
            <th style={thStyle}>Timestamp</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((tick) => (
            <tr key={tick.symbol} style={{ backgroundColor: "#1a1d27" }}>
              <td
                style={{
                  ...tdStyle,
                  color: SYMBOL_COLORS[tick.symbol] ?? "#ffffff",
                  fontWeight: "bold",
                }}
              >
                {tick.symbol}
              </td>
              <td style={tdStyle}>${tick.price.toFixed(2)}</td>
              <td style={tdStyle}>{tick.size}</td>
              <td style={tdStyle}>
                {new Date(tick.tsUnixMs).toLocaleTimeString()}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function filterButtonStyle(
  active: boolean,
  color: string
): React.CSSProperties {
  return {
    backgroundColor: active ? color : "#1a1d27",
    color: active ? "#0f1117" : color,
    border: `1px solid ${color}`,
    padding: "4px 12px",
    borderRadius: "4px",
    cursor: "pointer",
    fontWeight: "bold",
    fontSize: "13px",
  };
}

const thStyle: React.CSSProperties = {
  border: "1px solid #333",
  padding: "8px",
  textAlign: "left",
  backgroundColor: "#1a1d27",
  color: "#888",
};

const tdStyle: React.CSSProperties = {
  border: "1px solid #333",
  padding: "8px",
  color: "#e0e0e0",
};