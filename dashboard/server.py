#!/usr/bin/env python3
"""pstack live dashboard: tails Claude Code session transcripts and streams
agent flow (spawns, tool calls, hand-backs) to a local browser page.

Stdlib only. Binds to 127.0.0.1. Run: python3 server.py [--port 8765]
"""
import argparse
import json
import os
import re
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

CLAUDE = Path.home() / ".claude"
PROJECTS = CLAUDE / "projects"
HERE = Path(__file__).resolve().parent
BUILTIN_AGENTS = {"general-purpose", "Explore", "Plan", "claude", "claude-code-guide", "statusline-setup"}
CONFIG_RE = re.compile(r"\.claude/(agents|skills|rules)/([A-Za-z0-9_.\-]+)")
SYSTEM_PREFIXES = ("<system-reminder>", "<task-notification>", "<agent-message", "<command-", "<local-command")
LOCK = threading.RLock()


def ts_ms(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000
    except Exception:
        return time.time() * 1000


def clip(s, n):
    s = " ".join(str(s).split())
    return s if len(s) <= n else s[: n - 1] + "…"


def block_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
    return ""


def tool_summary(name, inp):
    if not isinstance(inp, dict):
        return ""
    if name in ("Read", "Edit", "Write", "NotebookEdit"):
        return inp.get("file_path") or inp.get("notebook_path") or ""
    if name == "Bash":
        return inp.get("description") or inp.get("command") or ""
    if name in ("Grep", "Glob"):
        return inp.get("pattern") or ""
    if name == "Agent":
        return "%s → %s" % (inp.get("subagent_type") or "general-purpose", inp.get("description") or "")
    if name == "Skill":
        return inp.get("skill") or ""
    if name in ("WebFetch", "WebSearch"):
        return inp.get("url") or inp.get("query") or ""
    return json.dumps(inp, ensure_ascii=False)


class Session:
    def __init__(self, sid, project):
        self.id = sid
        self.project = project
        self.title = ""
        self.cwd = ""
        self.start = None
        self.last = 0
        self.events = []
        self.nodes = {"main": self.new_node("main", "main", "Main session")}
        self.tool_index = {}  # tool_use_id -> {ts, name, agent}
        self.meta_to_agent = {}  # tool_use_id -> agentId (from subagent meta)
        self.turn_open = False

    @staticmethod
    def new_node(nid, atype, desc):
        return {"id": nid, "type": atype, "desc": desc, "model": "", "tools": 0, "out": 0, "ctx": 0,
                "first": None, "last": None, "done": False, "reported": False, "ghost": False,
                "error": "", "toolUseId": None, "background": False}

    def add(self, ev):
        ev["i"] = len(self.events)
        self.events.append(ev)
        self.last = max(self.last, ev["ts"])
        if self.start is None or ev["ts"] < self.start:
            self.start = ev["ts"]
        node = self.nodes.get(ev["agent"])
        if node:
            node["first"] = ev["ts"] if node["first"] is None else min(node["first"], ev["ts"])
            node["last"] = ev["ts"] if node["last"] is None else max(node["last"], ev["ts"])

    def node_for(self, agent_id):
        if agent_id not in self.nodes:
            self.nodes[agent_id] = self.new_node(agent_id, "subagent", agent_id[:8])
        return self.nodes[agent_id]

    def snapshot_nodes(self, now):
        out = []
        for n in self.nodes.values():
            n = dict(n)
            parent = "main"
            if n["toolUseId"] and n["toolUseId"] in self.tool_index:
                parent = self.tool_index[n["toolUseId"]]["agent"]
            n["parent"] = None if n["id"] == "main" else parent
            if n["id"] == "main":
                n["status"] = "active" if self.turn_open else "idle"
            elif n["ghost"]:
                n["status"] = "failed"
            elif n["done"]:
                n["status"] = "done"
            elif n["last"] and now - n["last"] > 90_000:
                n["status"] = "stalled"
            else:
                n["status"] = "running"
            out.append(n)
        return out

    def info(self, now):
        subs = [n for n in self.nodes.values() if n["id"] != "main"]
        running = sum(1 for n in self.snapshot_nodes(now) if n["id"] != "main" and n["status"] == "running")
        return {"id": self.id, "project": self.project, "title": self.title, "cwd": self.cwd,
                "last": self.last, "start": self.start, "agents": len(subs), "running": running,
                "events": len(self.events)}


class Store:
    def __init__(self, window_hours):
        self.sessions = {}
        self.files = {}  # path -> {off, buf, sid, agent}
        self.window = window_hours * 3600
        self.last_scan = 0

    def get(self, sid, project):
        if sid not in self.sessions:
            self.sessions[sid] = Session(sid, project)
        return self.sessions[sid]

    def discover(self):
        found = []
        if not PROJECTS.exists():
            return found
        cutoff = time.time() - self.window
        for proj in PROJECTS.iterdir():
            if not proj.is_dir():
                continue
            for p in proj.glob("*.jsonl"):
                found.append((p, p.stem, proj.name, None))
            for p in proj.glob("*/subagents/agent-*.jsonl"):
                found.append((p, p.parent.parent.name, proj.name, p.stem[len("agent-"):]))
        keep = []
        for item in found:
            path = item[0]
            try:
                if str(path) in self.files or path.stat().st_mtime >= cutoff:
                    keep.append(item)
            except OSError:
                pass
        return keep

    def poll(self):
        now = time.time()
        if now - self.last_scan > 2:
            self.last_scan = now
            for path, sid, proj, agent in self.discover():
                key = str(path)
                if key not in self.files:
                    self.files[key] = {"off": 0, "buf": b"", "sid": sid, "proj": proj, "agent": agent, "meta": False}
        with LOCK:
            for key, f in list(self.files.items()):
                self.read_file(key, f)

    def read_file(self, key, f):
        try:
            size = os.path.getsize(key)
        except OSError:
            return
        sess = self.get(f["sid"], f["proj"])
        if f["agent"] and not f["meta"]:
            self.load_meta(key, f, sess)
        if size < f["off"]:
            f["off"], f["buf"] = 0, b""
        if size == f["off"]:
            return
        with open(key, "rb") as fh:
            fh.seek(f["off"])
            chunk = fh.read()
        f["off"] += len(chunk)
        data = f["buf"] + chunk
        lines = data.split(b"\n")
        f["buf"] = lines.pop()
        for line in lines:
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            try:
                self.handle(sess, rec, f["agent"] or "main")
            except Exception as exc:  # never let one odd record kill the tailer
                print("record error:", exc)

    def load_meta(self, key, f, sess):
        meta_path = key[: -len(".jsonl")] + ".meta.json"
        try:
            meta = json.load(open(meta_path))
        except (OSError, ValueError):
            return
        f["meta"] = True
        node = sess.node_for(f["agent"])
        node["type"] = meta.get("agentType") or "subagent"
        node["desc"] = meta.get("description") or node["desc"]
        node["toolUseId"] = meta.get("toolUseId")
        node["background"] = meta.get("requestShape") == "background"
        if node["toolUseId"]:
            sess.meta_to_agent[node["toolUseId"]] = f["agent"]

    # --- record -> events -------------------------------------------------
    def handle(self, sess, rec, agent):
        rtype = rec.get("type")
        ts = ts_ms(rec["timestamp"]) if rec.get("timestamp") else (sess.last or time.time() * 1000)
        if rec.get("cwd") and not sess.cwd:
            sess.cwd = rec["cwd"]
        if rtype == "ai-title":
            sess.title = rec.get("aiTitle") or sess.title
            return
        if rtype == "system":
            if rec.get("subtype") == "turn_duration":
                sess.turn_open = False
                sess.add({"ts": ts, "agent": "main", "kind": "turn_end", "name": "turn",
                          "summary": "turn finished in %.1fs" % (rec.get("durationMs", 0) / 1000)})
            return
        if rtype == "queue-operation":
            self.handle_queue(sess, rec, ts)
            return
        msg = rec.get("message") or {}
        content = msg.get("content")
        if rtype == "user":
            if rec.get("isMeta"):
                return
            if isinstance(content, str):
                self.user_text(sess, agent, ts, content)
            elif isinstance(content, list):
                for b in content:
                    if b.get("type") == "text":
                        self.user_text(sess, agent, ts, b.get("text", ""))
                    elif b.get("type") == "tool_result":
                        self.tool_result(sess, agent, ts, b, rec)
        elif rtype == "assistant" and isinstance(content, list):
            self.assistant(sess, agent, ts, msg, content)

    def user_text(self, sess, agent, ts, text):
        if not text.strip():
            return
        if text.lstrip().startswith(SYSTEM_PREFIXES):
            return
        if agent == "main":
            sess.turn_open = True
        kind = "prompt" if agent == "main" else "task"
        sess.add({"ts": ts, "agent": agent, "kind": kind, "name": "you" if agent == "main" else "task from parent",
                  "summary": clip(text, 220), "detail": text[:2000]})

    def assistant(self, sess, agent, ts, msg, content):
        node = sess.node_for(agent) if agent != "main" else sess.nodes["main"]
        if msg.get("model"):
            node["model"] = msg["model"]
        usage = msg.get("usage") or {}
        node["out"] += usage.get("output_tokens", 0) or 0
        node["ctx"] = (usage.get("input_tokens", 0) or 0) + (usage.get("cache_read_input_tokens", 0) or 0) \
            + (usage.get("cache_creation_input_tokens", 0) or 0) or node["ctx"]
        if msg.get("stop_reason") == "end_turn" and agent != "main":
            node["done"] = True
        if agent == "main":
            sess.turn_open = True
        for b in content:
            bt = b.get("type")
            if bt == "thinking":
                text = b.get("thinking", "")
                if text:
                    sess.add({"ts": ts, "agent": agent, "kind": "thinking", "name": "thinking",
                              "summary": clip(text, 220), "detail": text[:2000]})
            elif bt == "text":
                text = b.get("text", "")
                if text.strip():
                    sess.add({"ts": ts, "agent": agent, "kind": "text", "name": "says",
                              "summary": clip(text, 220), "detail": text[:2000]})
            elif bt == "tool_use":
                self.tool_use(sess, agent, ts, b, node)

    def tool_use(self, sess, agent, ts, b, node):
        name, inp, tid = b.get("name", "?"), b.get("input") or {}, b.get("id")
        node["tools"] += 1
        sess.tool_index[tid] = {"ts": ts, "name": name, "agent": agent}
        ev = {"ts": ts, "agent": agent, "kind": "tool_use", "name": name, "toolUseId": tid,
              "summary": clip(tool_summary(name, inp), 220)}
        cfg = CONFIG_RE.search(json.dumps(inp, ensure_ascii=False))
        if cfg:
            ev["config"] = {"kind": cfg.group(1)[:-1], "name": re.sub(r"\.md$", "", cfg.group(2))}
        if name == "Skill" and inp.get("skill"):
            ev["config"] = {"kind": "skill", "name": inp["skill"]}
        if name == "Agent":
            ev["kind"] = "spawn"
            ev["spawn"] = {"type": inp.get("subagent_type") or "general-purpose", "model": inp.get("model") or "",
                           "desc": inp.get("description") or ""}
            ev["detail"] = str(inp.get("prompt", ""))[:2000]
            ev["config"] = {"kind": "agent", "name": ev["spawn"]["type"]}
        sess.add(ev)

    def tool_result(self, sess, agent, ts, b, rec):
        tid = b.get("tool_use_id")
        info = sess.tool_index.get(tid)
        text = block_text(b.get("content"))
        is_err = bool(b.get("is_error"))
        ev = {"ts": ts, "agent": agent, "kind": "tool_result", "toolUseId": tid, "isError": is_err,
              "name": info["name"] if info else "result", "summary": clip(text, 220), "detail": text[:2000],
              "durationMs": int(ts - info["ts"]) if info else None}
        sess.add(ev)
        if info and info["name"] == "Agent":
            tur = rec.get("toolUseResult")
            agent_id = tur.get("agentId") if isinstance(tur, dict) else None
            if agent_id:
                sess.meta_to_agent.setdefault(tid, agent_id)
                node = sess.node_for(agent_id)
                node["toolUseId"] = tid
            if is_err:
                ghost = Session.new_node(tid, "unregistered", "spawn failed")
                spawn = next((e for e in sess.events if e.get("toolUseId") == tid and e["kind"] == "spawn"), None)
                if spawn:
                    ghost["type"] = spawn["spawn"]["type"]
                    ghost["desc"] = spawn["spawn"]["desc"]
                ghost.update(ghost=True, error=clip(text, 160), toolUseId=tid, first=ts, last=ts)
                sess.nodes[tid] = ghost

    def handle_queue(self, sess, rec, ts):
        if rec.get("operation") != "enqueue":
            return
        content = rec.get("content") or ""
        m = re.search(r'<agent-message from="([^"]+)"', content)
        if m:
            aid = m.group(1)
            node = sess.nodes.get(aid)
            if node:
                node["reported"] = True
                node["done"] = True
            body = re.sub(r"\s+", " ", content.split("\n", 2)[-1])
            sess.add({"ts": ts, "agent": aid, "kind": "handback", "name": "report", "to": "main",
                      "summary": clip(body, 220), "detail": content[:2000]})
            return
        m = re.search(r"<task-id>([^<]+)</task-id>", content)
        if m and m.group(1) in sess.nodes:
            sess.nodes[m.group(1)]["done"] = True
            sess.add({"ts": ts, "agent": m.group(1), "kind": "done", "name": "finished", "summary": "task notification"})


STORE = None


def parse_frontmatter(path):
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return {}
    meta = {}
    if text.startswith("---"):
        for line in text.split("---", 2)[1].splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                meta[k.strip()] = v.strip().strip('"')
    return meta


def setup_info():
    agents, skills, rules = [], [], []
    for p in sorted((CLAUDE / "agents").glob("*.md")):
        m = parse_frontmatter(p)
        agents.append({"name": m.get("name") or p.stem, "desc": clip(m.get("description", ""), 200),
                       "model": m.get("model", ""), "tools": m.get("tools", ""),
                       "background": m.get("background", ""), "file": p.name})
    for p in sorted((CLAUDE / "skills").glob("*/SKILL.md")):
        m = parse_frontmatter(p)
        skills.append({"name": m.get("name") or p.parent.name, "desc": clip(m.get("description", ""), 200),
                       "manual": m.get("disable-model-invocation", "") == "true"})
    for p in sorted((CLAUDE / "rules").glob("*.md")):
        text = p.read_text(errors="replace")
        paths = re.findall(r'^\s*-\s*"?([^"\n]+)"?\s*$', text.split("---", 2)[1], re.M) if text.startswith("---") else []
        rules.append({"name": p.stem, "paths": paths, "always": not paths})
    models = ""
    for cand in (Path.home() / ".agents" / "pstack-models.md",):
        if cand.exists():
            models = cand.read_text(errors="replace")[:3000]
    return {"agents": agents, "skills": skills, "rules": rules, "builtin": sorted(BUILTIN_AGENTS), "models": models}


class Handler(BaseHTTPRequestHandler):
    server_version = "pstack-dashboard"

    def log_message(self, *a):
        pass

    def host_ok(self):
        host = (self.headers.get("Host") or "").split(":")[0]
        return host in ("127.0.0.1", "localhost")

    def send_json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def resolve_session(self, sid):
        with LOCK:
            if sid in (None, "", "latest"):
                if not STORE.sessions:
                    return None
                return max(STORE.sessions.values(), key=lambda s: s.last)
            return STORE.sessions.get(sid)

    def do_GET(self):
        if not self.host_ok():
            self.send_error(403)
            return
        url = urlparse(self.path)
        q = parse_qs(url.query)
        if url.path in ("/", "/index.html"):
            body = (HERE / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        elif url.path == "/api/sessions":
            now = time.time() * 1000
            with LOCK:
                items = sorted((s.info(now) for s in STORE.sessions.values() if s.events),
                               key=lambda i: -i["last"])
            self.send_json(items)
        elif url.path == "/api/setup":
            self.send_json(setup_info())
        elif url.path == "/api/stream":
            self.stream(q.get("session", [None])[0], int(q.get("since", ["0"])[0] or 0))
        else:
            self.send_error(404)

    def stream(self, sid, cursor):
        sess = self.resolve_session(sid)
        if sess is None:
            self.send_json({"error": "no session"}, 404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        last_beat = 0
        last_sent_nodes = None
        try:
            while True:
                with LOCK:
                    now = time.time() * 1000
                    new = sess.events[cursor:]
                    nodes = sess.snapshot_nodes(now)
                    payload = {"session": sess.info(now), "events": new, "nodes": nodes, "turnOpen": sess.turn_open,
                               "now": now, "live": cursor > 0 or None}
                nodes_key = json.dumps(nodes, sort_keys=True)
                if new or nodes_key != last_sent_nodes or time.time() - last_beat > 15:
                    self.wfile.write(("data: %s\n\n" % json.dumps(payload)).encode())
                    self.wfile.flush()
                    last_beat = time.time()
                    last_sent_nodes = nodes_key
                    cursor += len(new)
                time.sleep(0.4)
        except (BrokenPipeError, ConnectionResetError, OSError):
            return


def poller(interval):
    while True:
        try:
            STORE.poll()
        except Exception as exc:
            print("poll error:", exc)
        time.sleep(interval)


def main():
    global STORE
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--hours", type=float, default=12, help="only track sessions modified in the last N hours")
    args = ap.parse_args()
    STORE = Store(args.hours)
    STORE.poll()
    threading.Thread(target=poller, args=(0.5,), daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    srv.daemon_threads = True
    print("pstack dashboard → http://127.0.0.1:%d  (tracking %d sessions)" % (args.port, len(STORE.sessions)))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
