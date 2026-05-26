# Async Data Hub

Async Data Hub is a hands-on FastAPI project for learning production-style async backend engineering.

## Run locally with Docker

1. Copy `.env.example` to `.env` if you want to override defaults.
2. Start the stack:

```bash
docker compose up --build
```

3. Open:
   - API docs: `http://localhost:8000/docs`
   - Health endpoint: `http://localhost:8000/api/v1/health`

## Run tests locally

```bash
pip install -e ".[dev]"
pytest
```

## Run the frontend

The lightweight frontend lives in `frontend/` and is intentionally plain HTML, CSS, and JavaScript.

1. Start the backend:

```bash
docker compose up --build
```

2. Serve the frontend from the project root:

```bash
python -m http.server 8080
```

3. Open `http://localhost:8080/frontend/`

The UI defaults to `http://localhost:8000/api/v1` as its API base URL. You can change that value in the page when needed.

## Build order

The project is organized so each feature demonstrates a specific async concept:
- infrastructure and lifespan-managed shared resources
- concurrent aggregation
- SSE streaming
- WebSocket broadcasting
- Redis caching and request coalescing
- durable async PostgreSQL access
- webhook fan-out with limits and retries
- CPU-bound offloading comparisons
