# Async Data Hub


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

## Media streaming samples

The media-streaming endpoints serve files from `sample_media/` (configurable
via `MEDIA_SAMPLES_DIR`). Drop one or more files with these extensions into
that directory: `.mp4`, `.mp3`, `.webm`, `.ogg`, `.wav`, `.m4a`. They become
available at `/api/v1/media/list` and the two streaming endpoints, and show
up in the Media Streaming panel of the frontend after clicking *Refresh list*.

## Build order
