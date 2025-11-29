# Docker Compose Command Fix

## ✅ Solution: Use `docker compose` (Space) Instead of `docker-compose` (Hyphen)

Docker Compose v2 (included with Docker Desktop) uses a **space** instead of a **hyphen**.

## Quick Fix

**Replace this:**
```bash
docker-compose up -d  # ❌ Command not found
```

**With this:**
```bash
docker compose up -d  # ✅ Works!
```

## All Commands Updated

| Old (v1) | New (v2) |
|----------|----------|
| `docker-compose up -d` | `docker compose up -d` |
| `docker-compose ps` | `docker compose ps` |
| `docker-compose logs` | `docker compose logs` |
| `docker-compose down` | `docker compose down` |
| `docker-compose restart` | `docker compose restart` |
| `docker-compose stop` | `docker compose stop` |

## Verify Installation

```bash
# Check Docker Compose version
docker compose version

# Should show: Docker Compose version v2.x.x
```

## Optional: Create Alias

If you prefer the old syntax, add this to `~/.zshrc`:

```bash
alias docker-compose='docker compose'
```

Then reload:
```bash
source ~/.zshrc
```

## Start Your Project

```bash
# Navigate to project directory
cd /Users/pbn/My_project/realtime-us-stock-etl-platform

# Start all services
docker compose up -d

# Check status
docker compose ps
```

---

**All scripts and documentation have been updated to use `docker compose` (space) syntax! ✅**

