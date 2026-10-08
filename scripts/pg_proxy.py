"""Lightweight TCP Forwarder for PostgreSQL.
Binds to 127.0.0.1:5432 and forwards to WSL PostgreSQL (172.26.16.219:5432)
to ensure seamless localhost access on Windows without requiring Admin elevation.
"""
import sys
import socket
import threading

def forward(src, dst):
    try:
        while True:
            data = src.recv(4096)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            dst.close()
        except Exception:
            pass

def handle_client(client_sock, remote_host, remote_port):
    try:
        remote_sock = socket.create_connection((remote_host, remote_port), timeout=5)
    except Exception as e:
        client_sock.close()
        return
    threading.Thread(target=forward, args=(client_sock, remote_sock), daemon=True).start()
    threading.Thread(target=forward, args=(remote_sock, client_sock), daemon=True).start()

def main():
    remote_host = sys.argv[1] if len(sys.argv) > 1 else "172.26.16.219"
    local_port = 5432
    remote_port = 5432

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(("127.0.0.1", local_port))
    except Exception as e:
        print(f"[PG Proxy] Could not bind to 127.0.0.1:{local_port}: {e}")
        return

    server.listen(20)
    print(f"[PG Proxy] Forwarding 127.0.0.1:{local_port} -> {remote_host}:{remote_port}", flush=True)

    while True:
        client, addr = server.accept()
        threading.Thread(target=handle_client, args=(client, remote_host, remote_port), daemon=True).start()

if __name__ == "__main__":
    main()
