# Docker Compose Syntax Update

## ⚠️ Important: Use `docker compose` (Space) Not `docker-compose` (Hyphen)

Docker Compose v2 (included with Docker Desktop) uses a **space** instead of a **hyphen**.

## ✅ Correct Syntax (Docker Compose v2)

```bash
# Start services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f

# Stop services
docker compose down

# Restart services
docker compose restart
```

## ❌ Old Syntax (Docker Compose v1 - Deprecated)

```bash
# This won't work with Docker Desktop
docker-compose up -d  # ❌ Command not found
```

## Quick Fix

If you see `command not found: docker-compose`, use:

```bash
# Replace this:
docker-compose up -d

# With this:
docker compose up -d
```

## Check Your Version

```bash
# Check Docker Compose version
docker compose version

# Should show: Docker Compose version v2.x.x
```

## Create Alias (Optional)

If you prefer the old syntax, create an alias:

```bash
# Add to ~/.zshrc or ~/.bashrc
alias docker-compose='docker compose'

# Then reload
source ~/.zshrc  # or source ~/.bashrc
```

## All Updated Commands

| Old (v1) | New (v2) |
|----------|----------|
| `docker-compose up -d` | `docker compose up -d` |
| `docker-compose ps` | `docker compose ps` |
| `docker-compose logs` | `docker compose logs` |
| `docker-compose down` | `docker compose down` |
| `docker-compose restart` | `docker compose restart` |
| `docker-compose stop` | `docker compose stop` |
| `docker-compose start` | `docker compose start` |

---

**All documentation has been updated to use `docker compose` (space) syntax! ✅**

