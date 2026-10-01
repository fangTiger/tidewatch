// 观潮前端：订阅 SSE，实时更新时间线与状态页
(function () {
  const live = document.getElementById("live");
  const source = new EventSource("/stream");
  let disconnected = false;
  source.onopen = () => {
    if (live) live.classList.remove("off");
    if (disconnected) {
      disconnected = false;
      location.reload();
    }
  };
  source.onerror = () => {
    disconnected = true;
    if (live) live.classList.add("off");
  };
  source.addEventListener("resync", () => location.reload());

  // 时间线：收到簇更新后，按当前筛选条件决定插入、替换或移除
  const table = document.getElementById("timeline");
  const requestVersions = new Map();

  function matches(row) {
    const f = table.dataset;
    if (f.category && row.category !== f.category) return false;
    if (f.level && row.level !== f.level) return false;
    if (!f.level && f.showC !== "1" && row.level === "C") return false;
    if (f.asset && !row.assets.includes(f.asset)) return false;
    return true;
  }

  function insertSorted(fresh) {
    const body = table.tBodies[0];
    const empty = body.querySelector("tr.empty");
    if (empty) empty.remove();
    const ts = fresh.dataset.ts;
    const cid = Number(fresh.dataset.cid);
    const before = Array.from(body.rows).find(
      (row) => row.dataset.ts < ts || (row.dataset.ts === ts && Number(row.dataset.cid) < cid)
    );
    body.insertBefore(fresh, before || null);
  }

  if (table) {
    source.addEventListener("cluster.updated", async (e) => {
      const row = JSON.parse(e.data);
      const version = (requestVersions.get(row.cluster_id) || 0) + 1;
      requestVersions.set(row.cluster_id, version);
      if (!matches(row)) {
        const gone = document.getElementById("c" + row.cluster_id);
        if (gone) gone.remove();
        return;
      }
      const resp = await fetch("/fragment/cluster/" + row.cluster_id);
      if (!resp.ok) return;
      const html = (await resp.text()).trim();
      const holder = document.createElement("tbody");
      holder.innerHTML = html;
      const fresh = holder.firstElementChild;
      if (!fresh || requestVersions.get(row.cluster_id) !== version) return;
      fresh.classList.add("flash");
      // 等待期间同簇可能再次更新；只有最新请求能走到这里。
      const existing = document.getElementById("c" + row.cluster_id);
      if (existing) existing.remove();
      // 替换也重新插入，保证代表事件时间变化后仍与服务端排序一致。
      insertSorted(fresh);
    });
  }

  // 状态页：健康事件触发即时刷新，并每 30 秒兜底刷新全部字段与推送统计。
  const collectors = document.getElementById("collectors");
  if (collectors) {
    let statusRequestVersion = 0;

    function collectorRow(state) {
      let row = document.getElementById("s-" + state.name);
      if (row) return row;
      const body = collectors.tBodies[0];
      const empty = body.querySelector("tr.empty");
      if (empty) empty.remove();
      row = document.createElement("tr");
      row.id = "s-" + state.name;
      row.innerHTML = [
        '<td class="source-name"></td>',
        '<td class="status"></td>',
        '<td class="mode"></td>',
        '<td class="last-success"></td>',
        '<td class="last-event"></td>',
        '<td class="failures"></td>',
        '<td class="error"></td>',
      ].join("");
      body.appendChild(row);
      return row;
    }

    function updateStatus(data) {
      data.collectors.forEach((s) => {
        const tr = collectorRow(s);
        tr.className = "st-" + s.status;
        tr.querySelector(".source-name").textContent = s.source_name;
        tr.querySelector(".status").textContent = s.status_text;
        tr.querySelector(".mode").textContent = s.mode_text;
        tr.querySelector(".last-success").textContent = s.last_success;
        tr.querySelector(".last-event").textContent = s.last_event;
        tr.querySelector(".failures").textContent = s.consecutive_failures;
        tr.querySelector(".error").textContent = s.last_error || "";
      });
      Object.entries(data.push_stats).forEach(([status, count]) => {
        const target = document.querySelector(`[data-push-status="${status}"]`);
        if (target) target.textContent = count;
      });
    }

    async function refreshStatus() {
      const version = ++statusRequestVersion;
      try {
        const response = await fetch("/api/status");
        if (!response.ok) return;
        const data = await response.json();
        if (version === statusRequestVersion) updateStatus(data);
      } catch (_) {
        // 连接状态由 EventSource 指示；轮询失败留待下一次重试。
      }
    }

    source.addEventListener("health.changed", refreshStatus);
    setInterval(refreshStatus, 30000);
  }
})();
