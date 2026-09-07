const REFRESH_INTERVAL_MS = 2000;

const el = (id) => document.getElementById(id);

function formatLatency(ms) {
    if (ms === null || ms === undefined) return "--";
    return `${ms.toFixed(1)} ms`;
}

function formatTimestamp(iso) {
    return new Date(iso).toLocaleString();
}

async function fetchStats() {
    const res = await fetch("/api/v1/dashboard/stats");
    if (!res.ok) throw new Error(`stats fetch failed: ${res.status}`);
    return res.json();
}

async function fetchDlq() {
    const res = await fetch("/api/v1/dashboard/dlq");
    if (!res.ok) throw new Error(`dlq fetch failed: ${res.status}`);
    return res.json();
}

async function replayDlqEntry(dlqId, button) {
    button.disabled = true;
    button.textContent = "Replaying...";
    try {
        const res = await fetch(`/api/v1/dashboard/dlq/${dlqId}/replay`, { method: "POST" });
        if (!res.ok) throw new Error(`replay failed: ${res.status}`);
        await refreshAll();
    } catch (err) {
        console.error(err);
        button.disabled = false;
        button.textContent = "Replay";
    }
}

function renderStats(stats) {
    el("stat-queue-depth").textContent = stats.queue_depth;
    el("stat-pending-retries").textContent = stats.pending_retries;
    el("stat-delivered").textContent = stats.total_delivered;
    el("stat-dlq").textContent = stats.total_dlq;
    el("stat-queued").textContent = stats.total_queued;
    el("stat-retrying").textContent = stats.total_retrying;
    el("stat-latency").textContent = formatLatency(stats.average_latency_ms);
}

function renderDlq(rows) {
    const body = el("dlq-body");
    const empty = el("dlq-empty");
    body.innerHTML = "";

    if (rows.length === 0) {
        empty.classList.remove("hidden");
        return;
    }
    empty.classList.add("hidden");

    for (const row of rows) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td class="px-4 py-2 font-mono text-xs">${row.event_id}</td>
            <td class="px-4 py-2">${row.total_attempts}</td>
            <td class="px-4 py-2 text-rose-400 max-w-xs truncate" title="${row.final_error}">${row.final_error}</td>
            <td class="px-4 py-2 text-slate-400">${formatTimestamp(row.last_attempted_at)}</td>
            <td class="px-4 py-2"></td>
        `;
        const actionCell = tr.querySelector("td:last-child");
        const btn = document.createElement("button");
        btn.className = "replay-btn";
        btn.textContent = "Replay";
        btn.addEventListener("click", () => replayDlqEntry(row.id, btn));
        actionCell.appendChild(btn);
        body.appendChild(tr);
    }
}

async function refreshAll() {
    try {
        const [stats, dlq] = await Promise.all([fetchStats(), fetchDlq()]);
        renderStats(stats);
        renderDlq(dlq);
        el("conn-status").textContent = "connected";
        el("conn-status").className = "px-3 py-1 rounded-full text-xs font-medium bg-emerald-500/20 text-emerald-400";
    } catch (err) {
        console.error(err);
        el("conn-status").textContent = "disconnected";
        el("conn-status").className = "px-3 py-1 rounded-full text-xs font-medium bg-rose-500/20 text-rose-400";
    }
}

el("refresh-dlq").addEventListener("click", refreshAll);

refreshAll();
setInterval(refreshAll, REFRESH_INTERVAL_MS);
