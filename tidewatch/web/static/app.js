// 观潮前端：订阅 SSE，实时更新时间线与状态页
(function () {
  const live = document.getElementById("live");
  const source = new EventSource("/stream");
  source.onopen = () => live && live.classList.remove("off");
  source.onerror = () => live && live.classList.add("off");

  // 时间线：收到簇更新后，按当前筛选条件决定插入、替换或移除
  const table = document.getElementById("timeline");
  function matches(row) {
    const f = table.dataset;
    if (f.category && row.category !== f.category) return false;
    if (f.level && row.level !== f.level) return false;
    if (!f.level && f.showC !== "1" && row.level === "C") return false;
    if (f.asset && !row.assets.includes(f.asset)) return false;
    return true;
  }
  if (table) {
    source.addEventListener("cluster.updated", async (e) => {
      const row = JSON.parse(e.data);
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
      if (!fresh) return;
      fresh.classList.add("flash");
      // 必须在 await 之后再查找已有行：等待期间并发的同簇事件可能已经插入了这一行，否则会出现重复行
      const existing = document.getElementById("c" + row.cluster_id);
      if (existing) {
        existing.replaceWith(fresh);
        return;
      }
      const body = table.tBodies[0];
      const empty = body.querySelector("tr.empty");
      if (empty) empty.remove();
      // 按发生时间倒序插入；发生时间相同时按簇 id 倒序（与服务端排序一致）
      const ts = fresh.dataset.ts;
      const cid = Number(fresh.dataset.cid);
      const before = Array.from(body.rows).find(
        (r) => r.dataset.ts < ts || (r.dataset.ts === ts && Number(r.dataset.cid) < cid)
      );
      body.insertBefore(fresh, before || null);
    });
  }

  // 状态页：采集器状态变化
  const collectors = document.getElementById("collectors");
  if (collectors) {
    const text = { normal: "正常", abnormal: "异常", unknown: "未知" };
    source.addEventListener("health.changed", (e) => {
      const s = JSON.parse(e.data);
      const tr = document.getElementById("s-" + s.name);
      if (!tr) return;
      tr.className = "st-" + s.status;
      tr.querySelector(".status").textContent = text[s.status] || s.status;
      tr.querySelector(".mode").textContent = s.mode === "degraded" ? "降级" : "正常";
      tr.querySelector(".failures").textContent = s.consecutive_failures;
      tr.querySelector(".error").textContent = s.last_error || "";
    });
  }
})();
