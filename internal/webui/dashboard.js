const tokenKey = "bezorg.token";

const subtitle = document.querySelector("#subtitle");
const banner = document.querySelector("#banner");
const stats = document.querySelector("#stats");
const jobsBody = document.querySelector("#jobs");
const empty = document.querySelector("#empty");
const detail = document.querySelector("#detail");
const detailTitle = document.querySelector("#detail-title");
const detailFacts = document.querySelector("#detail-facts");
const detailBar = document.querySelector("#detail-bar");
const detailProgress = document.querySelector("#detail-progress");
const inflight = document.querySelector("#inflight");
const failures = document.querySelector("#failures");
const log = document.querySelector("#log");
const tokenInput = document.querySelector("#token");
const tokenLabel = document.querySelector("label[for=token]");
const unlockButton = document.querySelector("#unlock");
const lockButton = document.querySelector("#lock");
const cancelButton = document.querySelector("#cancel");
const retryButton = document.querySelector("#retry");

let token = sessionStorage.getItem(tokenKey) || "";
let selected = "";
let lines = [];
let logNext = 0;
let publicRead = false;
let refreshing = false;
let refreshQueued = false;
let authNote = "";

document.querySelector("#auth").addEventListener("submit", (event) => {
  event.preventDefault();
  token = tokenInput.value.trim();
  if (!token) {
    authNote = "";
    showBanner("Paste the token from the bezorgd log.");
    return;
  }
  authNote = "";
  sessionStorage.setItem(tokenKey, token);
  tokenInput.value = "";
  refresh();
});

lockButton.addEventListener("click", () => {
  token = "";
  sessionStorage.removeItem(tokenKey);
  paintAuth();
  refresh();
});

cancelButton.addEventListener("click", () => act("cancel"));
retryButton.addEventListener("click", () => act("retry"));

function showBanner(message) {
  banner.hidden = !message;
  banner.textContent = message || "";
}

function paintAuth() {
  const unlocked = token !== "";
  tokenLabel.hidden = unlocked;
  tokenInput.hidden = unlocked;
  unlockButton.hidden = unlocked;
  lockButton.hidden = !unlocked;
}

function formatBytes(n) {
  n = Number(n) || 0;
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = n;
  let i = 0;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i++;
  }
  const digits = i === 0 || value >= 100 ? 0 : value >= 10 ? 1 : 2;
  return value.toFixed(digits) + " " + units[i];
}

function formatTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
}

async function api(method, path) {
  const headers = new Headers();
  if (token) headers.set("Authorization", "Bearer " + token);
  const response = await fetch(path, { method, headers });
  const data = await response.json().catch(() => ({}));
  return { status: response.status, data };
}

function progressOf(job) {
  const progress = job.progress || {};
  const bytesTotal = Number(progress.bytes_total) || 0;
  const bytesDone = Number(progress.bytes_done) || 0;
  const filesTotal = Number(progress.files_total) || 0;
  const filesDone = Number(progress.files_done) || 0;
  let ratio = 0;
  if (bytesTotal > 0) ratio = bytesDone / bytesTotal;
  else if (filesTotal > 0) ratio = filesDone / filesTotal;
  ratio = Math.max(0, Math.min(1, ratio));
  return { progress, ratio, bytesDone, bytesTotal, filesDone, filesTotal };
}

function renderStats(system, jobs) {
  const counts = { running: 0, queued: 0, failed: 0, completed: 0 };
  for (const job of jobs) {
    if (Object.prototype.hasOwnProperty.call(counts, job.state)) counts[job.state]++;
  }
  const cards = [
    ["Version", system.version || "dev"],
    ["Active", String(system.active ?? 0)],
    ["Queued", String(system.queued ?? counts.queued)],
    ["Completed", String(counts.completed)],
    ["Failed", String(counts.failed)],
  ];
  clear(stats);
  for (const [label, value] of cards) {
    const card = el("div", "card");
    card.append(el("div", "label", label), el("div", "value", value));
    stats.append(card);
  }
}

function renderJobs(jobs) {
  clear(jobsBody);
  empty.hidden = jobs.length !== 0;
  let stillThere = false;
  for (const job of jobs) {
    if (job.id === selected) stillThere = true;
    const row = document.createElement("tr");
    if (job.id === selected) row.className = "selected";
    row.append(
      el("td", "", job.id || ""),
      (() => {
        const cell = document.createElement("td");
        cell.append(el("span", "state state-" + (job.state || ""), job.state || ""));
        return cell;
      })(),
      el("td", "path", remoteLabel(job)),
      el("td", "", progressLabel(job)),
    );
    row.addEventListener("click", () => {
      if (selected !== job.id) {
        selected = job.id;
        lines = [];
        logNext = 0;
        log.textContent = "";
      }
      refresh();
    });
    jobsBody.append(row);
  }
  if (!stillThere) {
    selected = "";
    detail.hidden = true;
  }
}

function remoteLabel(job) {
  const cfg = job.config || {};
  const host = cfg.host || "";
  const remote = cfg.remote || "";
  if (host && remote) return host + " " + remote;
  return host || remote;
}

function progressLabel(job) {
  const view = progressOf(job);
  return view.filesDone + "/" + view.filesTotal + " · " + formatBytes(view.bytesDone);
}

function fact(label, value) {
  detailFacts.append(el("dt", "", label), el("dd", "", value || ""));
}

function renderDetail(job) {
  detail.hidden = false;
  detailTitle.textContent = job.id || "Job";
  const cfg = job.config || {};
  const view = progressOf(job);
  clear(detailFacts);
  fact("State", job.state);
  fact("Protocol", cfg.protocol);
  fact("Host", cfg.host);
  fact("User", cfg.user);
  fact("Remote", cfg.remote);
  fact("Local", cfg.local);
  fact("Started", formatTime(job.started_at));
  fact("Updated", formatTime(job.updated_at));
  if (job.error) fact("Error", job.error);
  detailBar.style.width = Math.round(view.ratio * 100) + "%";
  detailProgress.textContent =
    view.filesDone + "/" + view.filesTotal +
    " files, " + (view.progress.files_skipped || 0) + " skipped, " +
    (view.progress.files_failed || 0) + " failed · " +
    formatBytes(view.bytesDone) + " / " + formatBytes(view.bytesTotal);

  const terminal = job.state === "completed" || job.state === "failed" || job.state === "canceled";
  cancelButton.disabled = !token || terminal;
  retryButton.disabled = !token || !(job.state === "failed" || job.state === "canceled" || job.state === "interrupted");

  clear(inflight);
  const flying = (job.progress && job.progress.in_flight) || [];
  if (flying.length === 0) inflight.append(el("li", "", "None"));
  for (const file of flying) {
    inflight.append(el("li", "", (file.path || "") + " · " + formatBytes(file.have) + " / " + formatBytes(file.size)));
  }
  clear(failures);
  const failed = (job.progress && job.progress.failures) || [];
  if (failed.length === 0) failures.append(el("li", "", "None"));
  for (const file of failed) {
    failures.append(el("li", "", (file.path || "") + " — " + (file.error || "")));
  }
}

function appendLog(chunk) {
  if (!chunk || chunk.length === 0) return;
  lines = lines.concat(chunk);
  if (lines.length > 400) lines = lines.slice(lines.length - 400);
  log.textContent = lines.join("\n");
  log.scrollTop = log.scrollHeight;
}

async function refresh() {
  if (refreshing) {
    refreshQueued = true;
    return;
  }
  refreshing = true;
  paintAuth();
  try {
    const systemCall = await api("GET", "/api/v1/system");
    if (systemCall.status === 401) {
      if (token) {
        token = "";
        sessionStorage.removeItem(tokenKey);
        paintAuth();
        authNote = "That token was rejected.";
      }
      showBanner(authNote || "Paste the token from the bezorgd log. The file is next to the control socket and ends in .web-token.");
      clear(stats);
      clear(jobsBody);
      empty.hidden = true;
      detail.hidden = true;
      return;
    }
    if (systemCall.status !== 200) {
      showBanner(systemCall.data.error || "The dashboard could not read the daemon.");
      return;
    }
    if (token) authNote = "";
    showBanner(authNote);
    publicRead = Boolean(systemCall.data.public_read);
    const mode = publicRead && !token ? " · read only" : "";
    subtitle.textContent = "Started " + formatTime(systemCall.data.started_at) + mode;

    const jobsCall = await api("GET", "/api/v1/jobs");
    if (jobsCall.status !== 200) {
      showBanner(jobsCall.data.error || "Jobs could not be loaded.");
      return;
    }
    const jobs = jobsCall.data.jobs || [];
    renderStats(systemCall.data, jobs);
    renderJobs(jobs);
    const wanted = selected;
    if (!wanted) return;

    const jobCall = await api("GET", "/api/v1/jobs/" + encodeURIComponent(wanted));
    if (wanted !== selected) return;
    if (jobCall.status !== 200) {
      showBanner(jobCall.data.error || "That job could not be loaded.");
      return;
    }
    renderDetail(jobCall.data.job || {});
    const logCall = await api("GET", "/api/v1/jobs/" + encodeURIComponent(wanted) + "/logs?after=" + logNext);
    if (wanted !== selected || logCall.status !== 200) return;
    logNext = logCall.data.next || logNext;
    appendLog(logCall.data.lines || []);
  } catch {
    showBanner("The dashboard lost contact with bezorgd.");
  } finally {
    refreshing = false;
    if (refreshQueued) {
      refreshQueued = false;
      refresh();
    }
  }
}

async function act(name) {
  if (!selected) return;
  if (!token) {
    showBanner("Paste the token to cancel or retry a job.");
    return;
  }
  const result = await api("POST", "/api/v1/jobs/" + encodeURIComponent(selected) + "/" + name);
  if (result.status !== 200) {
    showBanner(result.data.error || "The daemon refused that action.");
    return;
  }
  showBanner("");
  refresh();
}

paintAuth();
refresh();
setInterval(refresh, 2000);
