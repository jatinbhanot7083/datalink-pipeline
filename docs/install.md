# Installation Guide — DataLink Local-First Medallion Pipeline

Windows 11 fresh-laptop to `make demo` running in **under 30 minutes**.

If you hit anything not covered here, it's a bug in the guide — let Jatin know.

---

## 0. Hardware minimums

| Resource | Minimum | Recommended |
|---|---|---|
| RAM | 16 GB | 32 GB |
| Disk free | 20 GB | 50 GB |
| CPU | 4 cores | 8 cores |
| OS | Windows 11 (Build 22000+) | |

> With <16 GB RAM, run with `DL_ORCHESTRATOR=local_sequential` (the default) and skip `make up-airflow`.

---

## 1. Install WSL2 + Ubuntu

Open **PowerShell as Administrator**:

```powershell
wsl --install -d Ubuntu-22.04
```

Reboot when prompted. On first Ubuntu launch, set a username + password.

**Verify:**
```powershell
wsl -l -v       # Ubuntu-22.04 should be Running, Version 2
```

---

## 2. Install Docker Desktop

1. Download: https://www.docker.com/products/docker-desktop/
2. Install with the **"Use WSL2 based engine"** option checked.
3. Open Docker Desktop → Settings → Resources → WSL Integration → enable Ubuntu-22.04.
4. Start Docker Desktop. Leave it running.

**Verify from Ubuntu (WSL2):**
```bash
docker --version
docker compose version
docker run --rm hello-world
```

---

## 3. System packages in WSL2

Open Ubuntu (WSL2):

```bash
sudo apt update
sudo apt install -y build-essential make git curl unixodbc-dev
```

(`unixodbc-dev` is the SQL Server ODBC driver dep. Not used until Phase 4, but cheap to install now.)

---

## 4. OneDrive decision (important)

This repo **should not live inside your OneDrive folder**. OneDrive sync
can prune "empty" `.git/` subdirectories and corrupt the repo silently.

Options (pick one):

**Recommended — move outside OneDrive:**
```bash
mkdir -p ~/dev
git clone <this-repo> ~/dev/DataPipelinesWithGX
cd ~/dev/DataPipelinesWithGX
```

**Alternative — exclude `.git` from OneDrive sync:**

1. Open the OneDrive app → Settings → Sync and backup → Manage backup → unpick the repo folder.
2. Or: in Windows Explorer, right-click `.git/` → OneDrive → **Always keep on this device** and set to **Available on this device only** (do not free up space).
3. Also exclude: `warehouse.duckdb*`, `.venv/`, `logs/`, `.pytest_cache/`.

---

## 5. Clone + setup

```bash
cd ~/dev        # (or wherever from step 4)
git clone <this-repo>
cd DataPipelinesWithGX
cp .env.example .env
make setup
```

`make setup` will:
1. Install `uv` (Astral's Python package manager) to `~/.local/bin/`.
2. Install Python 3.11 via uv.
3. Create a `.venv/` and install project deps + dev tools.
4. Install pre-commit hooks.

If `uv` isn't on your PATH after install, add it:
```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

---

## 6. Bring up the local stack

```bash
make up
```

This starts 5 containers on `localhost`:
| Service | Port | Purpose |
|---|---|---|
| `sftp` | 2222 | atmoz/sftp — Claims/Membership drop zone |
| `azurite` | 10000-10002 | ADLS Gen2 emulator |
| `sqlserver` | 1433 | SQL Server 2022 (UM operational target) |
| `postgres` | 5432 | PostgreSQL 16 (future UM target) |
| `webhook-stub` | 9000 | Local Teams-webhook replacement |

First boot takes 1-2 minutes while SQL Server initializes. Check with:
```bash
make ps
make logs          # Ctrl-C to stop tailing
```

---

## 7. Verify Phase 1

```bash
make verify-phase-1
```

Exits `0` when:
- All files in the Phase 1 skeleton are present.
- Every environment config (`local`, `dev`, `stage`, `prod`) parses cleanly.
- Unit tests pass (config loader, PHI guard, adapter factory).
- `ruff` and `mypy` pass.
- All docker-compose services are healthy.

If any step fails, run `make verify-phase-1-code` first — that isolates Python issues
from Docker issues.

---

## 8. (Optional) Airflow on kind

Only if you want the full Airflow UI experience:

```bash
# One-time: install kind, kubectl, helm — see infra/kind/README.md
make up-airflow
# browse to http://localhost:8080  (admin / admin_local_only)
make down-airflow
```

The default `local_sequential` orchestrator does not need any of this.

---

## 9. Teardown

```bash
make down          # stop services, keep volumes (fast restart)
make nuke          # stop + delete volumes (full reset)
make down-airflow  # if you opted into the kind cluster
```

---

## Windows + WSL2 gotchas

- **Line endings:** `.gitattributes` forces LF. If you see `^M` in files, your git config overrode it — `git config core.autocrlf false` in this repo.
- **File permissions:** WSL2 shows all files as `777` when accessed via `/mnt/c/...`. Always work inside the WSL2 filesystem (`~/dev/...`), not a Windows drive mount.
- **Docker Desktop memory:** default allocation is 8 GB, but Kubernetes + SQL Server can push past that. In Docker Desktop → Settings → Resources, set memory to ≥ 12 GB if you plan to use `make up-airflow`.
- **Antivirus / corporate VPN:** some tools (e.g., Global Protect, ZScaler) break kind networking. If `kind create cluster` hangs, try disabling the VPN.
