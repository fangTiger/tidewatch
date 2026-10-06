"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");

class ClassList {
  constructor(element) {
    this.element = element;
  }

  _classes() {
    return new Set(this.element.className.split(/\s+/).filter(Boolean));
  }

  add(name) {
    const classes = this._classes();
    classes.add(name);
    this.element.className = Array.from(classes).join(" ");
  }

  remove(name) {
    const classes = this._classes();
    classes.delete(name);
    this.element.className = Array.from(classes).join(" ");
  }

  contains(name) {
    return this._classes().has(name);
  }
}

class Element {
  constructor(tagName) {
    this.tagName = tagName.toUpperCase();
    this.id = "";
    this.className = "";
    this.dataset = {};
    this.children = [];
    this.parentNode = null;
    this.textContent = "";
    this.classList = new ClassList(this);
    this.tBodies = [];
  }

  appendChild(child) {
    child._detach();
    child.parentNode = this;
    this.children.push(child);
    if (this.tagName === "TABLE" && child.tagName === "TBODY") this.tBodies.push(child);
    return child;
  }

  insertBefore(child, before) {
    child._detach();
    child.parentNode = this;
    const index = before === null ? -1 : this.children.indexOf(before);
    if (index < 0) this.children.push(child);
    else this.children.splice(index, 0, child);
    return child;
  }

  replaceWith(fresh) {
    if (!this.parentNode) return;
    const parent = this.parentNode;
    const index = parent.children.indexOf(this);
    fresh._detach();
    fresh.parentNode = parent;
    parent.children[index] = fresh;
    this.parentNode = null;
  }

  remove() {
    this._detach();
  }

  _detach() {
    if (!this.parentNode) return;
    const siblings = this.parentNode.children;
    const index = siblings.indexOf(this);
    if (index >= 0) siblings.splice(index, 1);
    this.parentNode = null;
  }

  querySelector(selector) {
    if (selector.startsWith(".")) {
      const name = selector.slice(1);
      return this._find((element) => element.classList.contains(name));
    }
    if (selector === "tr.empty") {
      return this._find((element) => element.tagName === "TR" && element.classList.contains("empty"));
    }
    return null;
  }

  _find(predicate) {
    for (const child of this.children) {
      if (predicate(child)) return child;
      const nested = child._find(predicate);
      if (nested) return nested;
    }
    return null;
  }

  get rows() {
    return this.children.filter((child) => child.tagName === "TR");
  }

  get firstElementChild() {
    return this.children[0] || null;
  }

  set innerHTML(html) {
    this.children = [];
    const rowMatch = html.match(/<tr\b([^>]*)>/i);
    if (this.tagName === "TBODY" && rowMatch) {
      const row = new Element("tr");
      const attrs = rowMatch[1];
      row.id = attribute(attrs, "id") || "";
      row.className = attribute(attrs, "class") || "";
      row.dataset.ts = attribute(attrs, "data-ts") || "";
      row.dataset.cid = attribute(attrs, "data-cid") || "";
      this.appendChild(row);
      return;
    }
    for (const match of html.matchAll(/<td class="([^"]+)"><\/td>/g)) {
      const cell = new Element("td");
      cell.className = match[1];
      this.appendChild(cell);
    }
  }
}

function attribute(text, name) {
  const match = text.match(new RegExp(`${name}="([^"]*)"`));
  return match ? match[1] : null;
}

class DocumentStub {
  constructor(roots) {
    this.roots = roots;
  }

  createElement(tagName) {
    return new Element(tagName);
  }

  getElementById(id) {
    for (const root of this.roots) {
      if (root.id === id) return root;
      const found = root._find((element) => element.id === id);
      if (found) return found;
    }
    return null;
  }

  querySelector() {
    return null;
  }
}

class EventSourceStub {
  constructor(url) {
    this.url = url;
    this.listeners = new Map();
    EventSourceStub.instance = this;
  }

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  async emit(type, data) {
    const event = {data: JSON.stringify(data)};
    await Promise.all((this.listeners.get(type) || []).map((listener) => listener(event)));
  }
}

function row(id, ts, cid) {
  const element = new Element("tr");
  element.id = id;
  element.dataset.ts = ts;
  element.dataset.cid = String(cid);
  return element;
}

function statusRow(name) {
  const element = new Element("tr");
  element.id = `s-${name}`;
  for (const className of ["source-name", "status", "mode", "last-success", "last-event", "failures", "error"]) {
    const cell = new Element("td");
    cell.className = className;
    element.appendChild(cell);
  }
  return element;
}

const live = new Element("span");
live.id = "live";
const timeline = new Element("table");
timeline.id = "timeline";
timeline.dataset = {category: "news", level: "", asset: "BTC", showC: "1"};
const timelineBody = new Element("tbody");
timeline.appendChild(timelineBody);
timelineBody.appendChild(row("c1", "2026-10-01T10:00:00Z", 1));
timelineBody.appendChild(row("c3", "2026-10-01T08:00:00Z", 3));

const collectors = new Element("table");
collectors.id = "collectors";
const collectorsBody = new Element("tbody");
collectors.appendChild(collectorsBody);
collectorsBody.appendChild(statusRow("jin10"));

const marketStrip = new Element("section");
marketStrip.id = "market-strip";
const btcTicker = new Element("span");
btcTicker.dataset.symbol = "BTC";
const btcPrice = new Element("span");
btcPrice.className = "ticker-price";
const btcStatus = new Element("small");
btcStatus.className = "ticker-status";
btcTicker.appendChild(btcPrice);
btcTicker.appendChild(btcStatus);
marketStrip.appendChild(btcTicker);

const netPositions = new Element("div");
netPositions.id = "net-positions";
const netUpdated = new Element("small");
netUpdated.id = "net-updated";

const positions = new Element("table");
positions.id = "positions";
positions.dataset.address = "0x1111111111111111111111111111111111111111";
const positionsBody = new Element("tbody");
positions.appendChild(positionsBody);

global.document = new DocumentStub([live, timeline, collectors, marketStrip, netPositions, netUpdated, positions]);
global.EventSource = EventSourceStub;
global.location = {reload() {}};
global.setInterval = () => 1;

const responses = [];
global.fetch = async (url) => {
  const response = responses.shift();
  assert.ok(response, `没有为 ${url} 准备响应`);
  assert.equal(url, response.url);
  return response.value;
};

require(path.resolve(__dirname, "../../tidewatch/web/static/app.js"));

async function testTimeline() {
  responses.push({
    url: "/fragment/cluster/2",
    value: {
      ok: true,
      async text() {
        return '<tr id="c2" class="lv-A" data-ts="2026-10-01T09:00:00Z" data-cid="2"></tr>';
      },
    },
  });
  await EventSourceStub.instance.emit("cluster.updated", {
    cluster_id: 2,
    category: "news",
    level: "A",
    assets: ["BTC"],
  });
  assert.deepEqual(timelineBody.rows.map((item) => item.id), ["c1", "c2", "c3"]);

  responses.push({
    url: "/fragment/cluster/2",
    value: {
      ok: true,
      async text() {
        return '<tr id="c2" class="lv-S" data-ts="2026-10-01T11:00:00Z" data-cid="2"></tr>';
      },
    },
  });
  await EventSourceStub.instance.emit("cluster.updated", {
    cluster_id: 2,
    category: "news",
    level: "S",
    assets: ["BTC"],
  });
  assert.deepEqual(timelineBody.rows.map((item) => item.id), ["c2", "c1", "c3"]);
  assert.equal(timelineBody.rows.filter((item) => item.id === "c2").length, 1);

  await EventSourceStub.instance.emit("cluster.updated", {
    cluster_id: 2,
    category: "exchange",
    level: "S",
    assets: ["BTC"],
  });
  assert.deepEqual(timelineBody.rows.map((item) => item.id), ["c1", "c3"]);
}

async function testStatus() {
  responses.push({
    url: "/api/status",
    value: {
      ok: true,
      async json() {
        return {
          collectors: [{
            name: "jin10",
            source_name: "金十",
            status: "abnormal",
            status_text: "异常",
            mode_text: "正常",
            last_success: "刚刚",
            last_event: "刚刚",
            consecutive_failures: 5,
            last_error: "连接失败",
          }],
          push_stats: {},
        };
      },
    },
  });
  responses.push({
    url: "/api/hyperliquid",
    value: {
      ok: true,
      async json() {
        return {enabled: true, tickers: [{symbol: "BTC", price: "100000", status: "实时"}], net_positions: [], net_updated_at: null};
      },
    },
  });
  await EventSourceStub.instance.emit("health.changed", {name: "jin10", status: "abnormal"});
  const updated = document.getElementById("s-jin10");
  assert.equal(updated.className, "st-abnormal");
  assert.equal(updated.querySelector(".status").textContent, "异常");
}

async function testHyperliquidUpdates() {
  await EventSourceStub.instance.emit("market.updated", {
    enabled: true,
    tickers: [{symbol: "BTC", price: "101234.5", status: "实时"}],
  });
  assert.equal(btcPrice.textContent, "101,234.50");
  assert.equal(btcStatus.textContent, "实时");

  await EventSourceStub.instance.emit("whales.net_position.updated", {
    net_positions: [{coin: "BTC", long_usd: "30000000", short_usd: "12000000", net_usd: "18000000", direction: "long", updated_at: "2026-10-06T04:00:00Z"}],
    net_updated_at: "2026-10-06T04:00:00Z",
  });
  assert.equal(netPositions.children.length, 1);
  assert.equal(netPositions.children[0].dataset.coin, "BTC");
  assert.equal(netPositions.children[0].querySelector(".net-value").textContent, "$18,000,000.00");
  assert.equal(netPositions.children[0].children[1].children[1].textContent, "$30,000,000.00");
  assert.equal(netPositions.children[0].children[1].children[3].textContent, "$12,000,000.00");
  assert.equal(netUpdated.textContent, "2026-10-06T04:00:00Z");

  await EventSourceStub.instance.emit("position.updated", {
    address: positions.dataset.address,
    positions: [{coin: "BTC", dex: "主 dex", direction: "long", quantity: "12345.6789012", notional_usd: "200000", entry_px: "84931.3", liquidation_px: "68604.6106151719", liquidation_distance: "0.2007897133034884465957211340", liquidation_progress: 79.92102866965115, leverage: "6.59", unrealized_pnl: "20000.125"}],
  });
  assert.equal(positionsBody.rows.length, 1);
  assert.equal(positionsBody.rows[0].dataset.coin, "BTC");
  assert.equal(positionsBody.rows[0].children[1].textContent, "多");
  assert.equal(positionsBody.rows[0].children[2].textContent, "12,345.678901");
  assert.equal(positionsBody.rows[0].children[3].textContent, "$200,000.00");
  assert.equal(positionsBody.rows[0].children[4].textContent, "84,931.30");
  assert.equal(positionsBody.rows[0].children[5].textContent, "68,604.61");
  assert.equal(positionsBody.rows[0].children[6].textContent, "20.08%");
  assert.equal(positionsBody.rows[0].children[7].textContent, "6.59×");
  assert.equal(positionsBody.rows[0].children[8].textContent, "$20,000.13");

  await EventSourceStub.instance.emit("position.updated", {
    address: positions.dataset.address,
    positions: [{coin: "ETH", dex: "主 dex", direction: "short", quantity: "0", notional_usd: "9007199254740992.01", entry_px: "0.00001234567", liquidation_px: null, liquidation_distance: "0", liquidation_progress: 0, leverage: "0", unrealized_pnl: "1.005"}],
  });
  assert.equal(positionsBody.rows[0].children[2].textContent, "0");
  assert.equal(positionsBody.rows[0].children[3].textContent, "$9,007,199,254,740,992.01");
  assert.equal(positionsBody.rows[0].children[4].textContent, "0.0000123457");
  assert.equal(positionsBody.rows[0].children[5].textContent, "—");
  assert.equal(positionsBody.rows[0].children[6].textContent, "0.00%");
  assert.equal(positionsBody.rows[0].children[7].textContent, "0.00×");
  assert.equal(positionsBody.rows[0].children[8].textContent, "$1.01");

  await EventSourceStub.instance.emit("position.updated", {
    address: positions.dataset.address,
    positions: [{coin: "SOL", dex: "主 dex", direction: "long", quantity: "-0.0000001", notional_usd: "-0.001", entry_px: "0", liquidation_px: "0", liquidation_distance: "-0.000001", liquidation_progress: 0, leverage: "0", unrealized_pnl: "-0.001"}],
  });
  assert.equal(positionsBody.rows[0].children[2].textContent, "-0");
  assert.equal(positionsBody.rows[0].children[3].textContent, "-$0.00");
  assert.equal(positionsBody.rows[0].children[6].textContent, "-0.00%");
  assert.equal(positionsBody.rows[0].children[8].textContent, "-$0.00");
}

(async () => {
  await testTimeline();
  await testStatus();
  await testHyperliquidUpdates();
  process.stdout.write("app DOM tests passed\n");
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
