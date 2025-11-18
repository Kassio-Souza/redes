#!/usr/bin/env python3
import argparse, json, socket, time, re, sys
from typing import Any, Dict

def build_line(case: Dict[str, Any]) -> bytes:
    mode = case.get("mode", "json")
    if mode == "json":
        payload = case["send"]
        # JSON compacto por padrão (uma linha)
        line = json.dumps(payload, separators=(",", ":"))
    elif mode == "raw":
        line = case.get("send", "")
        if not isinstance(line, str):
            line = str(line)
    elif mode == "synth":
        cfg = case.get("synth", {}) or {}
        pat = cfg.get("pattern", "curly_a")
        count = int(cfg.get("count", 0))
        if pat == "curly_a":
            # Ex: "{" + "a"*33000 + "}" -> invalida propositalmente p/ testar limite
            line = "{" + ("a" * count) + "}"
        elif pat == "whitespace":
            line = " " * count
        else:
            raise ValueError(f"Unknown synth pattern: {pat}")
    else:
        raise ValueError(f"Unknown mode: {mode}")
    return (line + "\n").encode("utf-8", errors="replace")

def recv_line(sock: socket.socket, timeout: float) -> str:
    sock.settimeout(timeout)
    buf = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
        if b"\n" in buf:
            line, _ = buf.split(b"\n", 1)
            return line.decode("utf-8", errors="replace")
    # EOF sem newline: devolve tudo que tiver
    return buf.decode("utf-8", errors="replace")

def is_subset(expected: Any, got: Any) -> bool:
    if isinstance(expected, dict):
        if not isinstance(got, dict):
            return False
        for k, v in expected.items():
            if k not in got or not is_subset(v, got[k]):
                return False
        return True
    if isinstance(expected, list):
        if not isinstance(got, list) or len(expected) > len(got):
            return False
        # Subconjunto “posicional” simples
        return all(is_subset(e, g) for e, g in zip(expected, got))
    return expected == got

def check_types(type_spec: Dict[str, str], got_obj: Dict[str, Any]) -> bool:
    mp = {"int": int, "str": str, "list": list, "dict": dict, "float": float, "bool": bool}
    for k, tname in type_spec.items():
        if k not in got_obj:
            return False
        py = mp.get(tname)
        if py is None or not isinstance(got_obj[k], py):
            return False
    return True

def run_case(case: Dict[str, Any], host: str, port: int, timeout: float, default_delay: float) -> bool:
    name = case.get("name", "<no-name>")
    delay = float(case.get("delay", default_delay or 0))
    if delay > 0:
        time.sleep(delay)

    try:
        payload = build_line(case)
    except Exception as e:
        print(f"[{name}] BUILD ERROR: {e}")
        return False

    resp_text = ""
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.sendall(payload)
            resp_text = recv_line(sock, timeout)
    except (ConnectionRefusedError, TimeoutError, socket.timeout) as e:
        print(f"[{name}] NET ERROR: {e}")
        return False
    except Exception as e:
        print(f"[{name}] UNEXPECTED ERROR: {e}")
        return False

    exp = case.get("expect", {})
    ok = True

    # 1) Checagem de regex (sobre texto bruto)
    if "regex" in exp:
        if not re.search(exp["regex"], resp_text, flags=re.S):
            print(f"[{name}] FAIL regex: {exp['regex']}\n  got: {resp_text}")
            ok = False

    # 2) Parse JSON (se der)
    got_obj = None
    try:
        got_obj = json.loads(resp_text)
    except Exception:
        pass

    # 3) Checagens estruturais
    if "status" in exp and got_obj is not None:
        if got_obj.get("status") != exp["status"]:
            print(f"[{name}] FAIL status: expected {exp['status']}, got {got_obj.get('status')}; raw={resp_text}")
            ok = False

    if "equals" in exp and got_obj is not None:
        if got_obj != exp["equals"]:
            print(f"[{name}] FAIL equals:\n  expected={json.dumps(exp['equals'])}\n  got     ={resp_text}")
            ok = False

    if "subset" in exp and got_obj is not None:
        if not is_subset(exp["subset"], got_obj):
            print(f"[{name}] FAIL subset:\n  expected⊆got {json.dumps(exp['subset'])}\n  got={resp_text}")
            ok = False

    if "has" in exp and got_obj is not None:
        for key in exp["has"]:
            if key not in got_obj:
                print(f"[{name}] FAIL has: missing key '{key}' in {resp_text}")
                ok = False

    if "types" in exp and got_obj is not None:
        if not check_types(exp["types"], got_obj):
            print(f"[{name}] FAIL types: spec={exp['types']} got={resp_text}")
            ok = False

    if ok:
        print(f"[{name}] OK")
    return ok

def main():
    ap = argparse.ArgumentParser(description="Rendezvous JSON line tester")
    ap.add_argument("test_file", help="Path to JSON test sequence file")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--timeout", type=float, default=5.0, help="Socket connect/read timeout seconds")
    ap.add_argument("--delay", type=float, default=0.0, help="Default delay (seconds) before each case (can be overridden per-case)")
    args = ap.parse_args()

    with open(args.test_file, "r", encoding="utf-8") as f:
        cases = json.load(f)

    passed = 0
    for case in cases:
        ok = run_case(case, args.host, args.port, args.timeout, args.delay)
        if ok: passed += 1
    total = len(cases)
    print(f"\nSummary: {passed}/{total} passed")
    sys.exit(0 if passed == total else 1)

if __name__ == "__main__":
    main()
