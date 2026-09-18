"""Local integration checks; no AWS access. Run after rebuilding distributed-kv.jar."""
import hashlib
import json
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading

ROOT = Path(__file__).resolve().parents[1]
JAVA = shutil.which("java")
JAR = ROOT / "distributed-kv.jar"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def metadata(folder):
    return {p.parent: json.loads(p.read_text(encoding="utf-8")) for p in (folder / "runs").glob("*/run.json")}


def without_master_log(target_port):
    """Forward real TCP traffic, withholding only LOG_REQUEST to exercise partial verification."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    port = listener.getsockname()[1]

    def forward(source, target, filter_request):
        try:
            with source.makefile("rb") as stream:
                for line in stream:
                    if not (filter_request and line.strip() == b"LOG_REQUEST"):
                        target.sendall(line)
        except OSError:
            pass
        finally:
            try:
                target.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def accept():
        with listener:
            for _ in range(4):
                client, _ = listener.accept()
                upstream = socket.create_connection(("127.0.0.1", target_port))
                def bridge(client=client, upstream=upstream):
                    thread = threading.Thread(target=forward, args=(client, upstream, True), daemon=True)
                    thread.start()
                    forward(upstream, client, False)
                    client.close()
                    upstream.close()
                threading.Thread(target=bridge, daemon=True).start()

    threading.Thread(target=accept, daemon=True).start()
    return port


def main():
    assert JAVA, "Java 17+ required"
    with tempfile.TemporaryDirectory(prefix=".tmp.console-test-", dir=ROOT) as name:
        folder = Path(name).resolve()
        assert folder.is_relative_to(ROOT)
        previous = None
        previous_hash = None
        for mode in ("quiet", "detailed", "no-master-log"):
            detailed = mode == "detailed"
            port = free_port()
            master_dir = folder / ("master-" + mode)
            master_dir.mkdir()
            with (master_dir / "console.txt").open("wb") as output:
                master = subprocess.Popen([JAVA, "-jar", str(JAR), "master", str(port)],
                                          cwd=master_dir, stdout=output, stderr=subprocess.STDOUT)
                try:
                    # Wait for INIT in the log, not a probe connection (Master expects HELLO).
                    import time
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        log = master_dir / "Master.txt"
                        if log.exists() and "INIT" in log.read_text(encoding="utf-8"):
                            break
                        if master.poll() is not None:
                            raise AssertionError("Master startup failed")
                        time.sleep(.05)
                    before = set(metadata(folder))
                    worker_port = without_master_log(port) if mode == "no-master-log" else port
                    args = [JAVA, "-jar", str(JAR), "workers", "127.0.0.1", str(worker_port)]
                    if detailed:
                        args.append("--verbose")
                    result = subprocess.run(args, cwd=folder, input=b"v\nv\n" if mode == "quiet" else b"", capture_output=True, timeout=50)
                    assert result.returncode == (2 if mode == "no-master-log" else 0), (result.returncode, result.stdout, result.stderr)
                    master.wait(timeout=10)
                    assert master.returncode == 0
                    runs = metadata(folder)
                    added = set(runs) - before
                    assert len(added) == 1
                    run = added.pop()
                    assert runs[run]["status"] == ("PARTIAL" if mode == "no-master-log" else "COMPLETED")
                    assert runs[run]["uniqueSuccesses"] == 5000
                    assert (run / "summary.txt").is_file()
                    if mode == "no-master-log":
                        assert not (run / "Master.txt").exists()
                    if mode == "quiet":
                        assert b"ON" in result.stdout and result.stdout.count(b"OFF") == 2
                    worker_logs = "".join((run / f"Worker{i}.txt").read_text(encoding="utf-8") for i in range(1, 5))
                    assert worker_logs.count("| PROC | SUCCESS |") == 5000
                    assert worker_logs.count("| TERMINATE | SUCCESS |") == 4
                    assert (result.stdout.count(b"| PROC | SUCCESS |") == 5000) == detailed
                    if not detailed:
                        assert len(result.stdout.splitlines()) < 100
                    if previous:
                        assert hashlib.sha256((previous / "Worker1.txt").read_bytes()).hexdigest() == previous_hash
                    previous = run
                    previous_hash = hashlib.sha256((run / "Worker1.txt").read_bytes()).hexdigest()
                    print("PASS", mode, "5000 successes, expected verification status, separate preserved logs")
                finally:
                    if master.poll() is None:
                        master.kill()
                        master.wait()

        before = set(metadata(folder))
        result = subprocess.run([JAVA, "-jar", str(JAR), "workers", "127.0.0.1", str(free_port())],
                                cwd=folder, input=b"v\nv\n", capture_output=True, timeout=20)
        assert result.returncode == 1
        assert b"Stream closed" not in result.stdout + result.stderr
        after = metadata(folder)
        failed = (set(after) - before).pop()
        assert after[failed]["status"] == "FAILED"
        assert not (failed / "Master.txt").exists()
        print("PASS refused connection: FAILED, nonzero exit, no stale Master log or closed-stream error")


if __name__ == "__main__":
    main()
