import json
import os
import platform
import subprocess
import threading
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

PROJECT_DIR = os.path.abspath(os.path.dirname(__file__))
COMPOSE_FILE = os.path.join(PROJECT_DIR, "docker-compose.yml")

SERVICES = [
    "zookeeper",
    "kafka",
    "schema-registry",
    "postgres",
    "kafka-connect",
    "minio",
    "minio-init",
    "producer-app",
    "kafka-consumer",
    "spark",
]

HEALTH_URLS = {
    "Kafka Connect": "http://localhost:8083/connectors",
    "Schema Registry": "http://localhost:8081/subjects",
    "MinIO Console": "http://localhost:9001",
}

POSTGRES_CHECK_CMD = [
    "docker", "exec", "postgres",
    "psql", "-U", "postgres", "-d", "business_db",
    "-t", "-c", "SELECT COUNT(*) FROM orders;"
]

KAFKA_TOPICS_CMD = [
    "docker", "exec", "kafka",
    "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"
]

DEBEZIUM_CONNECTOR_PAYLOAD = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "business_db",
        "database.server.name": "dbserver1",
        "topic.prefix": "dbserver1",
        "table.include.list": "public.customers,public.products,public.orders",
        "slot.name": "debezium_slot",
        "publication.name": "dbz_publication",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "include.schema.changes": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true"
    }
}

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Mini Data Platform Control Panel")
        self.geometry("1180x760")
        self.minsize(1000, 680)

        self.status_labels = {}
        self.log_var = tk.StringVar(value="producer-app")
        self.selected_service = tk.StringVar(value="producer-app")

        self._build_ui()
        self.refresh_status()

    def _build_ui(self):
        top = ttk.Frame(self, padding=12)
        top.pack(fill="x")

        ttk.Label(top, text="Mini Data Platform Control Panel", font=("Segoe UI", 16, "bold")).pack(anchor="w")
        ttk.Label(top, text=f"Project directory: {PROJECT_DIR}").pack(anchor="w", pady=(4, 0))

        main = ttk.Panedwindow(self, orient="horizontal")
        main.pack(fill="both", expand=True, padx=12, pady=12)

        left = ttk.Frame(main, padding=8)
        right = ttk.Frame(main, padding=8)
        main.add(left, weight=1)
        main.add(right, weight=2)

        self._build_controls(left)
        self._build_status_panel(left)
        self._build_checks_panel(left)
        self._build_logs_panel(right)

    def _build_controls(self, parent):
        frm = ttk.LabelFrame(parent, text="Controls", padding=10)
        frm.pack(fill="x", pady=(0, 10))

        row1 = ttk.Frame(frm)
        row1.pack(fill="x", pady=3)
        ttk.Button(row1, text="Start platform", command=lambda: self.run_bg(self.compose_up)).pack(side="left", padx=3)
        ttk.Button(row1, text="Stop platform", command=lambda: self.run_bg(self.compose_down)).pack(side="left", padx=3)
        ttk.Button(row1, text="Restart platform", command=lambda: self.run_bg(self.compose_restart)).pack(side="left",
                                                                                                          padx=3)
        ttk.Button(row1, text="Refresh status", command=lambda: self.run_bg(self.refresh_status)).pack(side="left",
                                                                                                       padx=3)

        row2 = ttk.Frame(frm)
        row2.pack(fill="x", pady=3)
        ttk.Button(row2, text="Register Debezium connector", command=lambda: self.run_bg(self.register_connector)).pack(
            side="left", padx=3)
        ttk.Button(row2, text="List Kafka topics", command=lambda: self.run_bg(self.list_kafka_topics)).pack(
            side="left", padx=3)
        ttk.Button(row2, text="Check PostgreSQL data", command=lambda: self.run_bg(self.check_postgres_data)).pack(
            side="left", padx=3)

        row3 = ttk.Frame(frm)
        row3.pack(fill="x", pady=3)
        ttk.Button(row3, text="Open MinIO Console", command=lambda: self.open_url("http://localhost:9001")).pack(
            side="left", padx=3)
        ttk.Button(row3, text="Open Kafka Connect",
                   command=lambda: self.open_url("http://localhost:8083/connectors")).pack(side="left", padx=3)
        ttk.Button(row3, text="Open Schema Registry",
                   command=lambda: self.open_url("http://localhost:8081/subjects")).pack(side="left", padx=3)

    def _build_status_panel(self, parent):
        frm = ttk.LabelFrame(parent, text="Container status", padding=10)
        frm.pack(fill="x", pady=(0, 10))

        for service in SERVICES:
            row = ttk.Frame(frm)
            row.pack(fill="x", pady=2)
            ttk.Label(row, text=service, width=18).pack(side="left")
            label = ttk.Label(row, text="unknown")
            label.pack(side="left")
            self.status_labels[service] = label

    def _build_checks_panel(self, parent):
        frm = ttk.LabelFrame(parent, text="Quick health checks", padding=10)
        frm.pack(fill="both", expand=False)

        self.health_tree = ttk.Treeview(frm, columns=("name", "status", "details"), show="headings", height=6)
        self.health_tree.heading("name", text="Check")
        self.health_tree.heading("status", text="Status")
        self.health_tree.heading("details", text="Details")
        self.health_tree.column("name", width=140)
        self.health_tree.column("status", width=90)
        self.health_tree.column("details", width=260)
        self.health_tree.pack(fill="both", expand=True)

        ttk.Button(frm, text="Run all checks", command=lambda: self.run_bg(self.run_all_checks)).pack(anchor="w", pady=(8, 0))

    def _build_logs_panel(self, parent):
        top = ttk.LabelFrame(parent, text="Logs", padding=10)
        top.pack(fill="both", expand=True)

        controls = ttk.Frame(top)
        controls.pack(fill="x", pady=(0, 8))

        ttk.Label(controls, text="Service:").pack(side="left")
        service_box = ttk.Combobox(controls, textvariable=self.selected_service, values=SERVICES, state="readonly",
                                   width=22)
        service_box.pack(side="left", padx=6)

        ttk.Button(controls, text="Show recent logs", command=lambda: self.run_bg(self.show_logs)).pack(side="left",
                                                                                                        padx=3)
        ttk.Button(controls, text="Tail logs", command=self.tail_logs).pack(side="left", padx=3)
        ttk.Button(controls, text="Clear", command=lambda: self.log_text.delete("1.0", tk.END)).pack(side="left",
                                                                                                     padx=3)

        self.log_text = scrolledtext.ScrolledText(top, wrap="word", font=("Consolas", 10))
        self.log_text.pack(fill="both", expand=True)

    def log(self, text):
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)
        self.update_idletasks()

    def run_bg(self, func):
        thread = threading.Thread(target=self._safe_call, args=(func,), daemon=True)
        thread.start()

    def _safe_call(self, func):
        try:
            func()
        except Exception as e:
            self.after(0, lambda: self.log(f"ERROR: {e}"))

    def run_command(self, cmd, title=None, cwd=PROJECT_DIR):
        if title:
            self.after(0, lambda: self.log(f"\n=== {title} ==="))
        self.after(0, lambda: self.log("$ " + " ".join(cmd)))

        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            shell=False
        )

        if result.stdout:
            self.after(0, lambda: self.log(result.stdout.strip()))
        if result.stderr:
            self.after(0, lambda: self.log(result.stderr.strip()))

        return result

    def compose_up(self):
        self.run_command(["docker", "compose", "up", "--build", "-d"], title="Starting platform")
        self.refresh_status()
        self.run_all_checks()

    def compose_down(self):
        self.run_command(["docker", "compose", "down"], title="Stopping platform")
        self.refresh_status()

    def compose_restart(self):
        self.compose_down()
        self.compose_up()

    def refresh_status(self):
        result = self.run_command(["docker", "compose", "ps", "--format", "json"], title="Refreshing container status")
        mapping = {s: ("not running", "red") for s in SERVICES}

        if result.returncode == 0 and result.stdout.strip():
            lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            for line in lines:
                try:
                    item = json.loads(line)
                    service = item.get("Service")
                    state = item.get("State", "unknown")
                    color = "green" if state.lower() == "running" else "orange"
                    if service in mapping:
                        mapping[service] = (state, color)
                except json.JSONDecodeError:
                    pass

        def update_ui():
            for service, label in self.status_labels.items():
                text, color = mapping.get(service, ("unknown", "black"))
                label.config(text=text, foreground=color)

        self.after(0, update_ui)

    def http_check(self, name, url):
        req = Request(url, headers={"User-Agent": "MiniDataPlatformGUI/1.0"})
        try:
            with urlopen(req, timeout=4) as response:
                code = response.status
                return name, "OK", f"HTTP {code}"
        except HTTPError as e:
            return name, "WARN", f"HTTP {e.code}"
        except URLError as e:
            return name, "FAIL", str(e.reason)
        except Exception as e:
            return name, "FAIL", str(e)

    def run_all_checks(self):
        rows = []

        for name, url in HEALTH_URLS.items():
            rows.append(self.http_check(name, url))

        topics_result = self.run_command(KAFKA_TOPICS_CMD, title="Checking Kafka topics")
        if topics_result.returncode == 0:
            lines = [x for x in topics_result.stdout.splitlines() if x.strip()]
            rows.append(("Kafka topics", "OK", f"{len(lines)} topics"))
        else:
            rows.append(("Kafka topics", "FAIL", "Cannot list topics"))

        pg_result = self.run_command(POSTGRES_CHECK_CMD, title="Checking PostgreSQL rows")
        if pg_result.returncode == 0:
            count = pg_result.stdout.strip() or "0"
            rows.append(("PostgreSQL orders", "OK", f"orders={count}"))
        else:
            rows.append(("PostgreSQL orders", "FAIL", "Query failed"))

        connector_check = self.http_check("Debezium connector endpoint", "http://localhost:8083/connectors")
        rows.append(connector_check)

        def update_tree():
            for item in self.health_tree.get_children():
                self.health_tree.delete(item)
            for row in rows:
                self.health_tree.insert("", tk.END, values=row)

        self.after(0, update_tree)

    def list_kafka_topics(self):
        self.run_command(KAFKA_TOPICS_CMD, title="Kafka topics")

    def check_postgres_data(self):
        self.run_command(POSTGRES_CHECK_CMD, title="PostgreSQL orders count")
        self.run_command([
            "docker", "exec", "postgres",
            "psql", "-U", "postgres", "-d", "business_db",
            "-c", "SELECT * FROM customers LIMIT 5;"
        ], title="Sample customers")

    def register_connector(self):
        self.log("\n=== Registering Debezium connector ===")
        try:
            import urllib.request
            payload = json.dumps(DEBEZIUM_CONNECTOR_PAYLOAD).encode("utf-8")
            req = urllib.request.Request(
                "http://localhost:8083/connectors/",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                body = response.read().decode("utf-8", errors="replace")
                self.log(body)
        except HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code == 409:
                self.log("Connector already exists.")
                self.log(body)
            else:
                self.log(f"HTTPError {e.code}: {body}")
        except Exception as e:
            self.log(f"Connector registration failed: {e}")

    def show_logs(self):
        service = self.selected_service.get()
        self.run_command(["docker", "logs", "--tail", "120", service], title=f"Recent logs: {service}")

    def tail_logs(self):
        service = self.selected_service.get()
        self.log(f"\n=== Tailing logs for {service} ===")
        self.log(
            "Open a separate terminal if you want a permanent live stream. This window shows one snapshot every click.")
        self.show_logs()

    def open_url(self, url):
        import webbrowser
        webbrowser.open(url)

if __name__ == "__main__":
    try:
        subprocess.run(["docker", "version"], capture_output=True, text=True)
    except FileNotFoundError:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Docker not found", "Docker CLI is not available in PATH.")
        raise SystemExit(1)

    app = App()
    app.mainloop()









