import { useEffect, useRef, useState } from "react";

type TickMessage = {
  type: string;
  symbol: string;
  tsUnixMs: number;
  price: number;
  size: number;
};

type TickMap = Record<string, TickMessage>;

export default function App() {
  const [symbolInput, setSymbolInput] = useState("AAPL,MSFT");
  const [connectionStatus, setConnectionStatus] = useState("Disconnected");
  const [latestTicks, setLatestTicks] = useState<TickMap>({});

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

  // Flush buffer to state on a fixed interval 
  useEffect(() => {
    const interval = setInterval(() => {
      const buffer = tickBufferRef.current;
      if (Object.keys(buffer).length === 0) return;
      setLatestTicks((prev) => ({ ...prev, ...buffer }));
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

  const rows = Object.values(latestTicks).sort((a, b) =>
    a.symbol.localeCompare(b.symbol)
  );

  return (
    <div style={{ padding: "24px", fontFamily: "Arial, sans-serif" }}>
      <h1>Trading Analytics Dashboard</h1>
      <p>
        <strong>WebSocket status:</strong> {connectionStatus}
      </p>
      <div style={{ marginBottom: "16px" }}>
        <label>
          Symbols (comma-separated):{" "}
          <input
            value={symbolInput}
            onChange={(e) => setSymbolInput(e.target.value)}
            style={{ width: "300px", marginRight: "8px" }}
          />
        </label>
        <button onClick={handleSubscribe}>Subscribe</button>
      </div>
      <table style={{ borderCollapse: "collapse", width: "100%", maxWidth: "700px" }}>
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
            <tr key={tick.symbol}>
              <td style={tdStyle}>{tick.symbol}</td>
              <td style={tdStyle}>{tick.price.toFixed(2)}</td>
              <td style={tdStyle}>{tick.size}</td>
              <td style={tdStyle}>{new Date(tick.tsUnixMs).toLocaleTimeString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const thStyle: React.CSSProperties = {
  border: "1px solid #ccc",
  padding: "8px",
  textAlign: "left",
  backgroundColor: "#f5f5f5",
};

const tdStyle: React.CSSProperties = {
  border: "1px solid #ccc",
  padding: "8px",
};