import { useState } from "react";

type Props = {
  busy: boolean;
  error: string | null;
  onLogin: (username: string, password: string) => void;
};

export default function LoginPage({ busy, error, onLogin }: Props) {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");

  function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!username || !password || busy) return;
    onLogin(username, password);
  }

  return (
    <div className="login-panel">
      <h1 className="page-title" style={{ marginBottom: 6 }}>
        xbyte-collector
      </h1>
      <p className="page-subtitle" style={{ marginBottom: 18 }}>
        Enterprise appliance console
      </p>
      <form onSubmit={submit} className="form-grid">
        <label>
          <span>Username</span>
          <input value={username} onChange={(e) => setUsername(e.target.value)} disabled={busy} />
        </label>
        <label>
          <span>Password</span>
          <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} disabled={busy} />
        </label>
        <button type="submit" disabled={busy || !username || !password}>
          {busy ? "Signing in..." : "Login"}
        </button>
        {error && <div style={{ color: "#b91c1c", fontSize: 13 }}>{error}</div>}
      </form>
    </div>
  );
}
