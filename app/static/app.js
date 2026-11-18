const $ = (sel) => document.querySelector(sel);

// Paged review list state.  `cursor` is bound to one publication of one day;
// it is only ever extended by appending the server's next page.
let dayState = null;

function badge(status) {
  const label = {
    current: "现行",
    stale: "待重算",
    uncomputed: "未计算",
    pending: "排队中",
    running: "运行中",
    done: "完成",
    failed: "失败",
  }[status] || status;
  return `<span class="badge ${status}">${label}</span>`;
}

function esc(text) {
  return String(text ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function pendingRow(row) {
  return `
    <tr><td class="mono">${esc(row.card_id)}</td>
        <td class="mono">${esc(row.tap_time)}</td>
        <td>${esc(row.station)}</td>
        <td>${row.direction === "in" ? "进站" : "出站"}</td>
        <td class="small">${esc(row.reason)}</td></tr>`;
}

function pageFooter() {
  if (!dayState) return "";
  if (dayState.loading) {
    return `<div class="pager"><span class="small">正在读取第 ${dayState.page_no + 1} 页…</span></div>`;
  }
  if (dayState.cursor) {
    return `<div class="pager">
      <button class="load-more" type="button">加载下一页（已显示 ${dayState.rows.length} 条）</button>
      <span class="small">每页最多 ${dayState.limit} 条</span>
    </div>`;
  }
  if (dayState.rows.length) {
    return `<div class="pager"><span class="small">已显示全部 ${dayState.total} 条待核记录</span></div>`;
  }
  return "";
}

function renderPendingTable() {
  const host = $("#pending-host");
  if (!host || !dayState) return;
  host.innerHTML = dayState.rows.length
    ? dayState.rows.map(pendingRow).join("")
    : '<tr><td colspan="5" class="empty">当日无待核记录</td></tr>';
  const footer = $("#pending-pager");
  if (footer) {
    footer.innerHTML = pageFooter();
    footer.querySelectorAll(".load-more").forEach((el) => {
      el.addEventListener("click", () => loadMorePending());
    });
  }
}

function renderDay(data, rows = []) {
  const panel = $("#day-panel");
  panel.hidden = false;
  dayState = {
    date: data.date,
    publication_id: data.publication_id,
    limit: data.page.limit,
    cursor: data.page.next_cursor,
    total: data.pending_total,
    rows: rows.slice(),
    page_no: 1,
    loading: false,
  };
  const head = `
    <div class="panel-head">
      <h3>${esc(data.date)} · UTC 服务日</h3>
      ${badge(data.status)}
      ${data.computed_at ? `<span class="mono small">计算于 ${esc(data.computed_at)}</span>` : ""}
    </div>`;

  if (data.status !== "current") {
    dayState = null;
    const msg = data.status === "stale"
      ? `结果已过期，不能用于排班。${data.last_error ? "原因：" + esc(data.last_error) : ""} 请在下方触发重算。`
      : "该日尚未计算完成。";
    panel.innerHTML = head + `<div class="notice ${data.status}">${msg}</div>` +
      `<button class="recompute" data-date="${esc(data.date)}">重算 ${esc(data.date)}</button>`;
    bindRecompute();
    return;
  }

  const pairs = data.station_pairs.map((row) => `
    <tr><td>${esc(row.from_station)} → ${esc(row.to_station)}</td>
        <td class="num">${row.trips}</td></tr>`).join("");

  panel.innerHTML = head + `
    <div class="stat-row">
      <div class="stat"><div class="k">配对行程</div><div class="v">${data.trip_total}</div></div>
      <div class="stat"><div class="k">待核刷卡</div><div class="v">${data.pending_total}</div></div>
      <div class="stat"><div class="k">站间流向</div><div class="v">${data.station_pairs.length}</div></div>
    </div>
    <div class="cols">
      <div>
        <h3>站间人数（按进站日）</h3>
        <table class="grid">
          <thead><tr><th>区间</th><th class="num">人数</th></tr></thead>
          <tbody>${pairs || '<tr><td colspan="2" class="empty">当日无配对行程</td></tr>'}</tbody>
        </table>
      </div>
      <div>
        <h3>待核记录（不计入客流）</h3>
        <div class="notice">待核清单分页显示，每页最多 ${esc(data.page.limit)} 条；本页与上方统计同属版本 ${esc(data.publication_id)}。补传、更正或重算后旧清单作废，须从现行结果重新开始。</div>
        <table class="grid">
          <thead><tr><th>卡号</th><th>时刻 (UTC)</th><th>站点</th><th>方向</th><th>原因</th></tr></thead>
          <tbody id="pending-host"></tbody>
        </table>
        <div id="pending-pager"></div>
      </div>
    </div>`;
  renderPendingTable();
  bindRecompute();
}

function showStalePanel(date, message, status = null) {
  dayState = null;
  const panel = $("#day-panel");
  panel.hidden = false;
  const action = status === "current"
    ? `<button class="reload-day" type="button" data-date="${esc(date)}">从现行结果重新加载</button>`
    : `<button class="recompute" type="button" data-date="${esc(date)}">重算 ${esc(date)}</button>`;
  panel.innerHTML = `
    <div class="panel-head"><h3>${esc(date)} · UTC 服务日</h3></div>
    <div class="notice stale">${esc(message)} 旧清单已清空，请从现行结果重新开始。</div>
    ${action}`;
  panel.querySelectorAll(".reload-day").forEach((el) => {
    el.addEventListener("click", () => loadDay(el.dataset.date));
  });
  bindRecompute();
}

async function loadDay(date) {
  document.querySelectorAll(".day-tab").forEach((el) => {
    el.classList.toggle("active", el.dataset.date === date);
  });
  const res = await fetch(`/api/days/${date}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    showStalePanel(date, body.detail || "加载失败");
    return;
  }
  const data = await res.json();
  renderDay(data, data.pending);
}

async function loadMorePending() {
  if (!dayState || !dayState.cursor || dayState.loading) return;
  const state = dayState;
  state.loading = true;
  state.page_no += 1;
  renderPendingTable();
  let res;
  try {
    res = await fetch(
      `/api/days/${state.date}?limit=${state.limit}&cursor=${encodeURIComponent(state.cursor)}`,
    );
  } catch (err) {
    state.loading = false;
    state.page_no -= 1;
    renderPendingTable();
    alert(`网络错误：${err}`);
    return;
  }
  if (dayState !== state) return;      // another day was opened meanwhile
  if (res.status === 409) {
    const body = await res.json().catch(() => ({}));
    await refreshTabs();
    showStalePanel(state.date, body.detail || "该日结果已变化，旧清单作废。", body.status);
    return;
  }
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    state.loading = false;
    state.page_no -= 1;
    renderPendingTable();
    alert(body.detail || "翻页失败");
    return;
  }
  state.loading = false;
  if (body.publication_id !== state.publication_id) {
    // Defensive: the server should already have rejected this cursor.
    await refreshTabs();
    showStalePanel(state.date, "该日已换版，旧清单作废。", body.status);
    return;
  }
  state.rows = state.rows.concat(body.pending);
  state.cursor = body.page.next_cursor;
  state.total = body.pending_total;
  renderPendingTable();
}

document.querySelectorAll(".day-tab").forEach((el) => {
  el.addEventListener("click", () => loadDay(el.dataset.date));
});

function bindRecompute() {
  document.querySelectorAll(".recompute").forEach((el) => {
    el.addEventListener("click", async () => {
      el.disabled = true;
      const res = await fetch(`/api/recompute?days=${encodeURIComponent(el.dataset.date)}`, { method: "POST" });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) alert(body.detail || "重算失败");
      await refreshTabs();
      await loadDay(el.dataset.date);
    });
  });
}

async function refreshTabs() {
  const res = await fetch("/api/days");
  if (!res.ok) return;
  const { days } = await res.json();
  const tabs = $("#day-tabs");
  const current = document.querySelector(".day-tab.active")?.dataset.date;
  tabs.innerHTML = days.length
    ? days.map((d) => `
      <button class="day-tab ${d.status === "stale" ? "stale" : ""} ${d.date === current ? "active" : ""}" data-date="${esc(d.date)}">
        <span class="date">${esc(d.date)}</span>
        <span class="badge ${d.status}">${d.status === "current" ? "现行" : "待重算"}</span>
      </button>`).join("")
    : '<p class="empty">尚无数据，请先上传。</p>';
  tabs.querySelectorAll(".day-tab").forEach((el) => {
    el.addEventListener("click", () => loadDay(el.dataset.date));
  });
}

$("#upload-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const fileInput = $("#csv");
  const result = $("#upload-result");
  const btn = $("#upload-btn");
  if (!fileInput.files.length) return;
  const form = new FormData();
  form.append("file", fileInput.files[0]);
  btn.disabled = true;
  result.hidden = false;
  result.className = "result";
  result.textContent = "上传并重算中…";
  try {
    const res = await fetch("/api/uploads", { method: "POST", body: form });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      result.className = "result error";
      result.textContent = body.detail || `上传失败（HTTP ${res.status}）`;
      return;
    }
    const states = Object.entries(body.day_states || {})
      .map(([day, status]) => `${day}: ${status === "current" ? "已转为现行" : "待重算"}`).join("\n");
    result.textContent =
      `批次 #${body.batch_id} 已受理：${body.rows} 行，新增 ${body.inserted}，忽略 ${body.ignored}。\n` +
      `涉及日期：${(body.affected_days || []).join(", ") || "无"}\n${states}`;
    await refreshTabs();
    if (body.affected_days?.length) await loadDay(body.affected_days[0]);
  } catch (err) {
    result.className = "result error";
    result.textContent = `网络错误：${err}`;
  } finally {
    btn.disabled = false;
  }
});
