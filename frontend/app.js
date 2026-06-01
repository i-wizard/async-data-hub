const config = {
    apiBaseUrl: "http://localhost:8000/api/v1",
};

const state = {
    stream: null,
    websocket: null,
};

const elements = {
    baseUrl: document.getElementById("base-url"),
    activeStreamCount: document.getElementById("active-stream-count"),
    activeStreamRefresh: document.getElementById("active-stream-refresh"),
    activeStreamNote: document.getElementById("active-stream-note"),
    healthButton: document.getElementById("health-button"),
    healthOutput: document.getElementById("health-output"),
    aggregateForm: document.getElementById("aggregate-form"),
    aggregateQuery: document.getElementById("aggregate-query"),
    aggregateCache: document.getElementById("aggregate-cache"),
    aggregateOutput: document.getElementById("aggregate-output"),
    streamQuery: document.getElementById("stream-query"),
    streamStart: document.getElementById("stream-start"),
    streamStop: document.getElementById("stream-stop"),
    streamOutput: document.getElementById("stream-output"),
    websocketConnect: document.getElementById("websocket-connect"),
    websocketDisconnect: document.getElementById("websocket-disconnect"),
    websocketOutput: document.getElementById("websocket-output"),
    webhookForm: document.getElementById("webhook-form"),
    webhookPayload: document.getElementById("webhook-payload"),
    webhookTargets: document.getElementById("webhook-targets"),
    webhookOutput: document.getElementById("webhook-output"),
    analysisForm: document.getElementById("analysis-form"),
    analysisOutput: document.getElementById("analysis-output"),
    mediaFile: document.getElementById("media-file"),
    mediaRefresh: document.getElementById("media-refresh"),
    mediaProgressive: document.getElementById("media-progressive"),
    mediaSeekable: document.getElementById("media-seekable"),
    remoteMediaUrl: document.getElementById("remote-media-url"),
    remoteMediaLoad: document.getElementById("remote-media-load"),
    remoteMediaPlayer: document.getElementById("remote-media-player"),
    mediaOutput: document.getElementById("media-output"),
};

function syncBaseUrlFromInput() {
    /**
     * Keeps the runtime API base URL editable from the UI because this learning
     * app needs to follow whichever backend origin the user is currently running.
     */
    config.apiBaseUrl = elements.baseUrl.value.trim().replace(/\/+$/, "");
}

function buildHttpUrl(path, queryParams) {
    /**
     * Derives endpoint URLs from one base so REST and SSE callers stay aligned
     * when the backend host or port changes during local development.
     */
    const url = new URL(config.apiBaseUrl + path);
    if (queryParams) {
        Object.entries(queryParams).forEach(([key, value]) => {
            url.searchParams.set(key, value);
        });
    }
    return url.toString();
}

function buildWebSocketUrl(path) {
    /**
     * Converts the configured HTTP base into a WebSocket URL so the frontend
     * does not have to maintain a second hardcoded host configuration.
     */
    const httpUrl = new URL(config.apiBaseUrl + path);
    httpUrl.protocol = httpUrl.protocol === "https:" ? "wss:" : "ws:";
    return httpUrl.toString();
}

async function requestJson(method, path, payload, queryParams) {
    /**
     * Centralizes JSON request handling so the panels all get consistent error
     * behavior and response parsing without repeating fetch boilerplate.
     */
    const requestOptions = {
        method: method,
        headers: {
            "Content-Type": "application/json",
        },
    };

    if (payload !== undefined) {
        requestOptions.body = JSON.stringify(payload);
    } else {
        delete requestOptions.headers["Content-Type"];
    }

    const response = await fetch(buildHttpUrl(path, queryParams), requestOptions);
    const text = await response.text();
    const body = text ? safeParseJson(text) : null;

    if (!response.ok) {
        throw new Error(JSON.stringify(body, null, 2));
    }

    return body;
}

function safeParseJson(rawValue) {
    /**
     * Parses JSON responses defensively so a backend error page or empty body
     * does not crash the frontend before the real problem can be shown.
     */
    try {
        return JSON.parse(rawValue);
    } catch (error) {
        return { raw: rawValue };
    }
}

function setStatus(name, status, message) {
    /**
     * Updates a panel status chip because each async interaction should make its
     * current state explicit instead of leaving the user to infer progress.
     */
    const statusElement = document.querySelector('[data-status-for="' + name + '"]');
    statusElement.textContent = message || status;
    statusElement.classList.remove("is-loading", "is-success", "is-error");

    if (status === "loading") {
        statusElement.classList.add("is-loading");
    } else if (status === "success") {
        statusElement.classList.add("is-success");
    } else if (status === "error") {
        statusElement.classList.add("is-error");
    }
}

function writeOutput(element, value) {
    /**
     * Writes formatted output into a panel so API responses and transport events
     * remain easy to inspect during iterative backend learning.
     */
    if (typeof value === "string") {
        element.textContent = value;
        return;
    }
    element.textContent = JSON.stringify(value, null, 2);
}

function appendOutput(element, line) {
    /**
     * Appends transport logs incrementally because streaming and WebSocket flows
     * are best understood as a timeline rather than a single final payload.
     */
    const nextValue = element.textContent ? element.textContent + "\n\n" + line : line;
    element.textContent = nextValue;
    element.scrollTop = element.scrollHeight;
}

function parseJsonInput(rawValue, fieldName) {
    /**
     * Validates free-form JSON locally so malformed learning inputs fail fast in
     * the browser instead of producing confusing backend validation errors.
     */
    try {
        return JSON.parse(rawValue);
    } catch (error) {
        throw new Error(fieldName + " must be valid JSON.");
    }
}

function parseTargets(rawValue) {
    /**
     * Converts one-target-per-line input into the backend schema because the UI
     * should stay easy to edit while still sending a typed request payload.
     */
    const targets = rawValue
        .split("\n")
        .map((target) => target.trim())
        .filter((target) => target.length > 0);

    if (targets.length === 0) {
        throw new Error("At least one target URL is required.");
    }

    return targets;
}

async function refreshActiveStreams() {
    /**
     * Reads the backend stream counter so the UI can show currently iterated
     * streaming responses while users experiment with SSE and media players.
     */
    try {
        const body = await requestJson("GET", "/streams/active");
        elements.activeStreamCount.textContent = String(body.count);
        elements.activeStreamNote.textContent = "Updated from /streams/active.";
    } catch (error) {
        elements.activeStreamNote.textContent = "Unable to fetch active stream count.";
    }
}

async function fetchHealth() {
    setStatus("health", "loading", "loading");
    try {
        const body = await requestJson("GET", "/health");
        writeOutput(elements.healthOutput, body);
        setStatus("health", "success", "success");
    } catch (error) {
        writeOutput(elements.healthOutput, String(error));
        setStatus("health", "error", "error");
    }
}

async function runAggregation(event) {
    event.preventDefault();
    setStatus("aggregate", "loading", "loading");
    try {
        const body = await requestJson(
            "GET",
            "/aggregate",
            undefined,
            {
                q: elements.aggregateQuery.value.trim(),
                use_cache: String(elements.aggregateCache.checked),
            },
        );
        writeOutput(elements.aggregateOutput, body);
        setStatus("aggregate", "success", body.cached ? "cache-hit" : "fresh");
    } catch (error) {
        writeOutput(elements.aggregateOutput, String(error));
        setStatus("aggregate", "error", "error");
    }
}

function stopStream() {
    /**
     * Closes the active EventSource before creating a new one so repeated
     * learning experiments do not leave duplicate stream subscriptions open.
     */
    if (state.stream) {
        state.stream.close();
        state.stream = null;
        setStatus("stream", "success", "stopped");
        refreshActiveStreams();
    }
}

function startStream() {
    stopStream();
    elements.streamOutput.textContent = "";
    setStatus("stream", "loading", "connecting");

    const query = elements.streamQuery.value.trim();
    const stream = new EventSource(buildHttpUrl("/aggregate/stream", { q: query }));
    state.stream = stream;

    stream.addEventListener("started", (event) => {
        appendOutput(elements.streamOutput, "[started]\n" + event.data);
        setStatus("stream", "success", "streaming");
        refreshActiveStreams();
    });

    stream.addEventListener("source_result", (event) => {
        appendOutput(elements.streamOutput, "[source_result]\n" + event.data);
    });

    stream.addEventListener("completed", (event) => {
        appendOutput(elements.streamOutput, "[completed]\n" + event.data);
        setStatus("stream", "success", "completed");
        stopStream();
        refreshActiveStreams();
    });

    stream.onerror = function () {
        appendOutput(elements.streamOutput, "[error]\nStream closed or failed.");
        setStatus("stream", "error", "error");
        refreshActiveStreams();
    };
}

function disconnectWebSocket() {
    /**
     * Closes the live socket intentionally so the UI state matches the real
     * connection lifecycle instead of keeping stale handles around.
     */
    if (state.websocket) {
        state.websocket.close();
        state.websocket = null;
        setStatus("websocket", "success", "closed");
    }
}

function connectWebSocket() {
    disconnectWebSocket();
    elements.websocketOutput.textContent = "";
    setStatus("websocket", "loading", "connecting");

    const socket = new WebSocket(buildWebSocketUrl("/ws/updates"));
    state.websocket = socket;

    socket.onopen = function () {
        appendOutput(elements.websocketOutput, "[open]\nConnected");
        setStatus("websocket", "success", "connected");
    };

    socket.onmessage = function (event) {
        appendOutput(elements.websocketOutput, "[message]\n" + event.data);
    };

    socket.onerror = function () {
        appendOutput(elements.websocketOutput, "[error]\nWebSocket error");
        setStatus("websocket", "error", "error");
    };

    socket.onclose = function () {
        appendOutput(elements.websocketOutput, "[close]\nDisconnected");
        if (state.websocket === socket) {
            state.websocket = null;
        }
        setStatus("websocket", "success", "closed");
    };
}

async function triggerWebhook(event) {
    event.preventDefault();
    setStatus("webhook", "loading", "loading");
    try {
        const payload = {
            event_name: document.getElementById("webhook-event-name").value.trim(),
            payload: parseJsonInput(elements.webhookPayload.value, "Payload JSON"),
            targets: parseTargets(elements.webhookTargets.value),
        };
        const body = await requestJson("POST", "/trigger-event", payload);
        writeOutput(elements.webhookOutput, body);
        setStatus("webhook", "success", body.status.toLowerCase());
    } catch (error) {
        writeOutput(elements.webhookOutput, String(error));
        setStatus("webhook", "error", "error");
    }
}

function setMediaSources(filename) {
    /**
     * Points both players at the two streaming endpoints so the seek-behavior
     * contrast is visible without the user having to swap URLs manually.
     */
    if (!filename) {
        elements.mediaProgressive.removeAttribute("src");
        elements.mediaSeekable.removeAttribute("src");
        elements.mediaProgressive.load();
        elements.mediaSeekable.load();
        return;
    }
    const encoded = encodeURIComponent(filename);
    elements.mediaProgressive.src = buildHttpUrl("/media/progressive/" + encoded);
    elements.mediaSeekable.src = buildHttpUrl("/media/seekable/" + encoded);
    elements.mediaProgressive.load();
    elements.mediaSeekable.load();
    refreshActiveStreams();
}

async function refreshMediaList() {
    setStatus("media", "loading", "loading");
    try {
        const body = await requestJson("GET", "/media/list");
        const files = Array.isArray(body && body.files) ? body.files : [];
        elements.mediaFile.innerHTML = "";
        if (files.length === 0) {
            const option = document.createElement("option");
            option.value = "";
            option.textContent = "(no files in sample_media)";
            elements.mediaFile.appendChild(option);
            setMediaSources(null);
            writeOutput(elements.mediaOutput, "No files found. Drop one in sample_media/.");
            setStatus("media", "success", "empty");
            return;
        }
        files.forEach((filename) => {
            const option = document.createElement("option");
            option.value = filename;
            option.textContent = filename;
            elements.mediaFile.appendChild(option);
        });
        setMediaSources(files[0]);
        writeOutput(elements.mediaOutput, "Loaded " + files.length + " file(s).");
        setStatus("media", "success", "ready");
    } catch (error) {
        writeOutput(elements.mediaOutput, String(error));
        setStatus("media", "error", "error");
    }
}

function onMediaSelectionChange() {
    setMediaSources(elements.mediaFile.value);
}

function loadRemoteMedia() {
    /**
     * Points the browser player at the proxy endpoint so the backend can
     * demonstrate remote streaming while the UI stays a simple URL launcher.
     */
    const remoteUrl = elements.remoteMediaUrl.value.trim();
    if (!remoteUrl) {
        writeOutput(elements.mediaOutput, "Enter a direct media URL first.");
        setStatus("media", "error", "error");
        return;
    }
    if (remoteUrl.includes("youtube.com/watch") || remoteUrl.includes("youtu.be/")) {
        writeOutput(
            elements.mediaOutput,
            "This endpoint expects a direct media file URL, not a YouTube watch page URL.",
        );
        setStatus("media", "error", "direct-url-only");
        return;
    }

    const proxyUrl = buildHttpUrl("/media/remote", { url: remoteUrl });
    elements.remoteMediaPlayer.src = proxyUrl;
    elements.remoteMediaPlayer.load();
    writeOutput(elements.mediaOutput, {
        proxy_url: proxyUrl,
        source_url: remoteUrl,
    });
    setStatus("media", "success", "remote-ready");
    refreshActiveStreams();
}

async function runAnalysis(event) {
    event.preventDefault();
    setStatus("analysis", "loading", "loading");
    try {
        const payload = {
            number: Number(document.getElementById("analysis-number").value),
            strategy: document.getElementById("analysis-strategy").value,
        };
        const body = await requestJson("POST", "/heavy-analysis", payload);
        writeOutput(elements.analysisOutput, body);
        setStatus("analysis", "success", body.strategy);
    } catch (error) {
        writeOutput(elements.analysisOutput, String(error));
        setStatus("analysis", "error", "error");
    }
}

function registerEventHandlers() {
    /**
     * Wires the panels to their handlers in one place so the app remains easy
     * to extend as new backend features are added later.
     */
    elements.baseUrl.addEventListener("change", syncBaseUrlFromInput);
    elements.activeStreamRefresh.addEventListener("click", refreshActiveStreams);
    elements.healthButton.addEventListener("click", fetchHealth);
    elements.aggregateForm.addEventListener("submit", runAggregation);
    elements.streamStart.addEventListener("click", startStream);
    elements.streamStop.addEventListener("click", stopStream);
    elements.websocketConnect.addEventListener("click", connectWebSocket);
    elements.websocketDisconnect.addEventListener("click", disconnectWebSocket);
    elements.webhookForm.addEventListener("submit", triggerWebhook);
    elements.analysisForm.addEventListener("submit", runAnalysis);
    elements.mediaRefresh.addEventListener("click", refreshMediaList);
    elements.mediaFile.addEventListener("change", onMediaSelectionChange);
    elements.remoteMediaLoad.addEventListener("click", loadRemoteMedia);
    elements.mediaProgressive.addEventListener("play", refreshActiveStreams);
    elements.mediaProgressive.addEventListener("pause", refreshActiveStreams);
    elements.mediaProgressive.addEventListener("ended", refreshActiveStreams);
    elements.mediaSeekable.addEventListener("play", refreshActiveStreams);
    elements.mediaSeekable.addEventListener("pause", refreshActiveStreams);
    elements.mediaSeekable.addEventListener("ended", refreshActiveStreams);
    elements.remoteMediaPlayer.addEventListener("play", refreshActiveStreams);
    elements.remoteMediaPlayer.addEventListener("pause", refreshActiveStreams);
    elements.remoteMediaPlayer.addEventListener("ended", refreshActiveStreams);
    window.addEventListener("beforeunload", function () {
        stopStream();
        disconnectWebSocket();
    });
}

function init() {
    /**
     * Bootstraps the static app with predictable defaults so the first page
     * load is immediately usable against the local backend stack.
     */
    syncBaseUrlFromInput();
    registerEventHandlers();
    writeOutput(elements.healthOutput, "Click 'Fetch health' to verify backend readiness.");
    writeOutput(elements.aggregateOutput, "Submit a query to call GET /aggregate.");
    writeOutput(elements.streamOutput, "Start a stream to observe SSE events.");
    writeOutput(elements.websocketOutput, "Connect to observe broadcast updates.");
    writeOutput(elements.webhookOutput, "Submit a webhook event to inspect delivery attempts.");
    writeOutput(elements.analysisOutput, "Run a CPU strategy comparison.");
    writeOutput(elements.mediaOutput, "Click 'Refresh list' to load files from sample_media/.");
    refreshActiveStreams();
}

init();
