function lrGetAudience() {
  const v = localStorage.getItem("lr_audience");
  if (v === "library" || v === "policy") return v;
  return "public";
}

function lrSetAudience(audience) {
  const a = audience === "library" || audience === "policy" ? audience : "public";
  localStorage.setItem("lr_audience", a);
  try {
    window.dispatchEvent(new CustomEvent("lr_audience_changed", { detail: { audience: a } }));
  } catch {
    // no-op
  }
  return a;
}

function lrEscapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function lrFmtNum(n, digits = 1) {
  const v = Number(n);
  if (!Number.isFinite(v)) return "—";
  return v.toFixed(digits);
}

function lrDeltaPill(delta, { goodWhenNegative = false, digits = 1 } = {}) {
  const d = Number(delta);
  if (!Number.isFinite(d) || d === 0) return "";
  const sign = d > 0 ? "+" : "";
  const good = goodWhenNegative ? d < 0 : d > 0;
  const cls = good ? "pos" : "neg";
  return ` <span class="delta ${cls}">${sign}${lrFmtNum(d, digits)}</span>`;
}

function lrPickBlock(blocks, type) {
  return (blocks || []).find((b) => b && b.type === type) || null;
}

function lrToneSummary(compare, audience) {
  const m = compare?.whatif?.metrics || {};
  const d = compare?.delta || {};
  const avg = m.avg_accessibility_score;
  const deserts = m.deserts_count;
  const outreach = m.outreach_count;

  const avgLine =
    `平均可達性 ${lrFmtNum(avg, 1)}` +
    (d.avg_accessibility_score === null || d.avg_accessibility_score === undefined
      ? ""
      : lrDeltaPill(d.avg_accessibility_score, { digits: 1, goodWhenNegative: false }));
  const desertsLine =
    `服務落差 deserts ${lrFmtNum(deserts, 0)}` +
    (d.deserts_count === null || d.deserts_count === undefined
      ? ""
      : lrDeltaPill(d.deserts_count, { digits: 0, goodWhenNegative: true }));
  const outreachLine = `外展候選點位 ${lrFmtNum(outreach, 0)}`;

  if (audience === "library") {
    return [
      `館務觀點：${avgLine}（看低分區域與落差熱點）`,
      `規劃優先順序：${desertsLine}（越少越好）`,
      `可落地方案：${outreachLine}（Top N 供場域合作討論）`,
    ];
  }
  if (audience === "policy") {
    return [
      `公平性指標：${desertsLine}（越少越好，建議用 by city 追蹤）`,
      `整體可達性：${avgLine}（越高越好，仍需看分布）`,
      `政策工具箱：${outreachLine}（用於跨局處/場域合作）`,
    ];
  }
  return [
    `現在的狀況：${avgLine}`,
    `服務落差：${desertsLine}（越少越好）`,
    `我們可以做什麼：${outreachLine}（外展據點建議）`,
  ];
}

function lrRenderNarrative(compare, { audience, includeAssumptions = true, includeNextSteps = true } = {}) {
  const a = audience || lrGetAudience();
  const blocks = compare?.narrative_blocks || [];
  const assumptionsBlock = lrPickBlock(blocks, "assumptions");
  const nextStepsBlock = lrPickBlock(blocks, "next_steps");
  const interpretationBlock = lrPickBlock(blocks, "interpretation");

  const summaryItems = lrToneSummary(compare, a);
  const summaryHtml = `
    <div class="narrative-block">
      <div class="narrative-title">${a === "policy" ? "決策摘要" : a === "library" ? "館務摘要" : "大眾摘要"}</div>
      <ul class="brief-bullets">${summaryItems.map((x) => `<li>${x}</li>`).join("")}</ul>
    </div>
  `;

  const assumptionsHtml =
    includeAssumptions && assumptionsBlock?.body
      ? `
        <div class="narrative-block">
          <div class="narrative-title">${lrEscapeHtml(assumptionsBlock.title || "本次假設")}</div>
          <div class="muted">${lrEscapeHtml(assumptionsBlock.body)}</div>
        </div>
      `
      : "";

  const interpretationHtml =
    interpretationBlock?.items?.length
      ? `
        <div class="narrative-block">
          <div class="narrative-title">${lrEscapeHtml(interpretationBlock.title || "如何解讀")}</div>
          <ul class="brief-bullets">${interpretationBlock.items.map((x) => `<li>${lrEscapeHtml(x)}</li>`).join("")}</ul>
        </div>
      `
      : "";

  const nextStepsHtml =
    includeNextSteps && nextStepsBlock?.items?.length
      ? `
        <div class="narrative-block">
          <div class="narrative-title">${lrEscapeHtml(nextStepsBlock.title || "下一步")}</div>
          <ul class="brief-bullets">${nextStepsBlock.items.map((x) => `<li>${lrEscapeHtml(x)}</li>`).join("")}</ul>
        </div>
      `
      : "";

  return summaryHtml + assumptionsHtml + interpretationHtml + nextStepsHtml;
}

function lrInitAudienceTabs(containerId, { onChange } = {}) {
  const root = document.getElementById(containerId);
  if (!root) return null;

  function setActive(audience) {
    const a = lrSetAudience(audience);
    for (const btn of root.querySelectorAll(".audience-tab")) {
      const isActive = btn.getAttribute("data-audience") === a;
      btn.classList.toggle("active", isActive);
      btn.setAttribute("aria-selected", isActive ? "true" : "false");
    }
    onChange?.(a);
    return a;
  }

  for (const btn of root.querySelectorAll(".audience-tab")) {
    btn.addEventListener("click", () => setActive(btn.getAttribute("data-audience")));
  }

  setActive(lrGetAudience());
  return { setActive };
}
