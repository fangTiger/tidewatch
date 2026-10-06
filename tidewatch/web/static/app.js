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

  // Hyperliquid 行情条与净仓位侧栏：所有外部字段只通过 textContent 写入。
  const marketStrip = document.getElementById("market-strip");
  const netPositions = document.getElementById("net-positions");
  const netUpdated = document.getElementById("net-updated");

  function childWithClass(root, className) {
    return root ? root.querySelector("." + className) : null;
  }

  function updateMarket(data) {
    if (!marketStrip || !data || !Array.isArray(data.tickers)) return;
    data.tickers.forEach((ticker) => {
      const target = Array.from(marketStrip.children).find(
        (element) => element.dataset && element.dataset.symbol === ticker.symbol
      );
      if (!target) return;
      target.dataset.status = ticker.status;
      const price = childWithClass(target, "ticker-price");
      const status = childWithClass(target, "ticker-status");
      if (price) price.textContent = ticker.price || "—";
      if (status) status.textContent = ticker.status;
    });
  }

  function appendText(parent, tag, value, className) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    element.textContent = value == null ? "—" : String(value);
    parent.appendChild(element);
    return element;
  }

  function updateNetPositions(data) {
    if (!netPositions || !data || !Array.isArray(data.net_positions)) return;
    netPositions.innerHTML = "";
    if (!data.net_positions.length) {
      appendText(netPositions, "p", data.enabled === false ? "未启用" : "暂无有效仓位", "empty-state");
    }
    data.net_positions.forEach((item) => {
      const article = document.createElement("article");
      article.className = "net-row";
      article.dataset.coin = item.coin;
      const headline = document.createElement("div");
      appendText(headline, "b", item.coin);
      appendText(headline, "span", item.net_usd, "net-value " + item.direction);
      article.appendChild(headline);
      const detail = document.createElement("dl");
      appendText(detail, "dt", "多");
      appendText(detail, "dd", item.long_usd);
      appendText(detail, "dt", "空");
      appendText(detail, "dd", item.short_usd);
      article.appendChild(detail);
      netPositions.appendChild(article);
    });
    if (netUpdated) netUpdated.textContent = data.net_updated_at || (data.enabled === false ? "未启用" : "暂无有效快照");
  }

  async function refreshHyperliquid() {
    if (!marketStrip && !netPositions) return;
    try {
      const response = await fetch("/api/hyperliquid");
      if (!response.ok) return;
      const data = await response.json();
      updateMarket(data);
      updateNetPositions(data);
    } catch (_) {
      // SSE 的连接标记已经提供断线反馈，下一次健康事件或重连继续刷新。
    }
  }

  source.addEventListener("market.updated", (event) => updateMarket(JSON.parse(event.data)));
  source.addEventListener("whales.net_position.updated", (event) => updateNetPositions(JSON.parse(event.data)));
  source.addEventListener("wallet.updated", refreshHyperliquid);
  source.addEventListener("health.changed", refreshHyperliquid);

  // 钱包详情仓位表：仅处理当前地址，避免其他钱包的广播污染页面。
  const positions = document.getElementById("positions");
  function updatePositions(data) {
    if (!positions || !data || data.address !== positions.dataset.address || !Array.isArray(data.positions)) return;
    const body = positions.tBodies[0];
    body.innerHTML = "";
    if (!data.positions.length) {
      const empty = document.createElement("tr");
      empty.className = "empty";
      const cell = appendText(empty, "td", "当前没有仓位");
      cell.colSpan = 9;
      body.appendChild(empty);
      return;
    }
    data.positions.forEach((item) => {
      const row = document.createElement("tr");
      row.dataset.coin = item.coin;
      row.dataset.dex = item.dex;
      appendText(row, "td", item.coin + " · " + item.dex);
      appendText(row, "td", item.direction === "long" ? "多" : "空", item.direction);
      [item.quantity, item.notional_usd, item.entry_px, item.liquidation_px].forEach((value) => appendText(row, "td", value));
      const risk = appendText(row, "td", item.liquidation_distance);
      const progress = document.createElement("progress");
      progress.max = 100;
      progress.value = item.liquidation_progress || 0;
      risk.appendChild(progress);
      appendText(row, "td", item.leverage);
      appendText(row, "td", item.unrealized_pnl);
      body.appendChild(row);
    });
  }
  source.addEventListener("position.updated", (event) => updatePositions(JSON.parse(event.data)));

  // 画像盈亏曲线：内联 SVG，不依赖前端框架或图表库。
  const pnlChart = document.getElementById("pnl-chart");
  if (pnlChart) {
    try {
      const history = JSON.parse(pnlChart.dataset.history || "[]")
        .map((point) => [Number(point[0]), Number(point[1])])
        .filter((point) => Number.isFinite(point[0]) && Number.isFinite(point[1]));
      if (history.length > 1) {
        const values = history.map((point) => point[1]);
        const low = Math.min(...values);
        const high = Math.max(...values);
        const span = high - low || 1;
        const points = history.map((point, index) => {
          const x = 20 + index * 760 / (history.length - 1);
          const y = 200 - (point[1] - low) * 180 / span;
          return x.toFixed(1) + "," + y.toFixed(1);
        }).join(" ");
        const line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
        line.setAttribute("points", points);
        line.setAttribute("class", "pnl-line");
        pnlChart.appendChild(line);
      }
    } catch (_) {
      // 旧画像若包含损坏历史，保留空图而不影响页面其他区域。
    }
  }
})();
