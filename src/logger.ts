import { appendFileSync, existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

type Level = "fatal" | "error" | "warn" | "info" | "debug" | "trace";

function shouldLog(level: Level, current: Level): boolean {
  const order: Level[] = ["trace", "debug", "info", "warn", "error", "fatal"];
  return order.indexOf(level) >= order.indexOf(current);
}

export class Logger {
  private logPath: string;

  constructor(private level: Level = "info", logDir = join(process.cwd(), "var", "logs"), filename = "xbyte-collector.log") {
    if (!existsSync(logDir)) mkdirSync(logDir, { recursive: true });
    this.logPath = join(logDir, filename);
  }

  log(level: Level, msg: string, meta?: Record<string, unknown>) {
    if (!shouldLog(level, this.level)) return;
    const entry = {
      level,
      msg,
      time: new Date().toISOString(),
      ...(meta ? { meta } : {}),
    };
    const line = `${entry.time} ${JSON.stringify(entry)}\n`;
    try {
      appendFileSync(this.logPath, line, "utf8");
    } catch {
      // swallow file write errors to avoid crashing; still attempt console
    }
    console.log(JSON.stringify(entry));
  }

  fatal(msg: string, meta?: Record<string, unknown>) {
    this.log("fatal", msg, meta);
  }
  error(msg: string, meta?: Record<string, unknown>) {
    this.log("error", msg, meta);
  }
  warn(msg: string, meta?: Record<string, unknown>) {
    this.log("warn", msg, meta);
  }
  info(msg: string, meta?: Record<string, unknown>) {
    this.log("info", msg, meta);
  }
  debug(msg: string, meta?: Record<string, unknown>) {
    this.log("debug", msg, meta);
  }
  trace(msg: string, meta?: Record<string, unknown>) {
    this.log("trace", msg, meta);
  }
}
