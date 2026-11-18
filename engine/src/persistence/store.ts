import fs from "fs";
import path from "path";

// One line per state-changing command, in the order the engine applied it.
// CREATE_ORDER records also carry the fills the engine produced, so replay can
// check it reproduced exactly the same trades.
export interface EngineEvent {
  seq: number;
  ts: number;
  type: "CREATE_ORDER" | "CANCEL_ORDER" | "ADD_BALANCE";
  data: any;
  ok: boolean;
  fills?: any[];
}

export interface Snapshot {
  lastSeq: number; // last event already reflected in this snapshot
  takenAt: number;
  orderbooks: any[];
  balances: [string, any][];
}

// Durable state for the engine: an append-only event log (fsync'd per event)
// plus periodic snapshots written atomically (tmp file + fsync + rename).
export class EngineStore {
  private logPath: string;
  private snapshotPath: string;
  private fd: number | null = null;

  constructor(private dir: string) {
    fs.mkdirSync(dir, { recursive: true });
    this.logPath = path.join(dir, "events.log");
    this.snapshotPath = path.join(dir, "snapshot.json");
  }

  loadSnapshot(): Snapshot | null {
    if (!fs.existsSync(this.snapshotPath)) {
      return null;
    }
    // rename() is atomic, so this is either the old or the new snapshot, never a mix
    return JSON.parse(fs.readFileSync(this.snapshotPath, "utf8"));
  }

  // Reads every complete event. A torn last line (crash mid-append) is cut off
  // so the next append starts on a clean line.
  readEvents(): EngineEvent[] {
    if (!fs.existsSync(this.logPath)) {
      return [];
    }
    const buf = fs.readFileSync(this.logPath, "utf8");
    const events: EngineEvent[] = [];
    let goodBytes = 0;
    let start = 0;
    while (start < buf.length) {
      const end = buf.indexOf("\n", start);
      if (end === -1) {
        break; // no trailing newline: append never finished
      }
      const line = buf.slice(start, end);
      try {
        events.push(JSON.parse(line));
      } catch (e) {
        break;
      }
      start = end + 1;
      goodBytes += Buffer.byteLength(line, "utf8") + 1;
    }
    const total = fs.statSync(this.logPath).size;
    if (goodBytes < total) {
      console.log(`[recovery] dropping ${total - goodBytes} bytes of torn log tail`);
      fs.truncateSync(this.logPath, goodBytes);
    }
    return events;
  }

  append(event: EngineEvent) {
    if (this.fd === null) {
      this.fd = fs.openSync(this.logPath, "a");
    }
    fs.writeSync(this.fd, JSON.stringify(event) + "\n");
    fs.fsyncSync(this.fd); // durable before anyone hears about it
  }

  writeSnapshot(snapshot: Snapshot) {
    const tmp = this.snapshotPath + ".tmp";
    const fd = fs.openSync(tmp, "w");
    fs.writeSync(fd, JSON.stringify(snapshot));
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fs.renameSync(tmp, this.snapshotPath);
    const dirFd = fs.openSync(this.dir, "r");
    fs.fsyncSync(dirFd); // make the rename itself durable
    fs.closeSync(dirFd);
  }
}
