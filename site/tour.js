// Guided tour: spotlights one part of the dashboard at a time and switches tabs as it goes.
// Tour.start(steps) runs it; Tour.offer(steps, key) shows a one-time "take the tour" prompt instead.
// A step is {page?, target?, exact?, title, text}: page is a tab id, target a CSS selector. The target's card is
// highlighted unless exact is set, which spotlights the element itself (for cards taller than the screen).
(function () {
  const STYLE = `
.tour-shade { position: fixed; inset: 0; z-index: 90; }
.tour-ring { position: fixed; z-index: 91; border-radius: 22px; pointer-events: none;
  box-shadow: 0 0 0 200vmax rgba(12, 9, 6, 0.55), 0 0 0 2px var(--s4, #eda100);
  transition: left 0.45s var(--ease, ease), top 0.45s var(--ease, ease), width 0.45s var(--ease, ease), height 0.45s var(--ease, ease); }
.tour-pop { position: fixed; z-index: 92; width: min(360px, calc(100vw - 32px)); border-radius: 22px; padding: 18px 18px 14px;
  background: var(--glass-strong, #fff); outline: none;
  transition: left 0.45s var(--ease, ease), top 0.45s var(--ease, ease), opacity 0.25s ease; }
.tour-pop .tour-count { font-size: 12px; font-weight: 600; color: var(--muted); letter-spacing: 0.02em; }
.tour-pop h3 { font-size: 17px; font-weight: 650; margin: 4px 0 6px; letter-spacing: -0.01em; }
.tour-pop p { margin: 0; font-size: 14px; line-height: 1.55; color: var(--ink-2); }
.tour-dots { display: flex; gap: 5px; margin: 14px 0 12px; }
.tour-dots i { width: 6px; height: 6px; border-radius: 3px; background: var(--grid); transition: width 0.3s ease, background 0.3s ease; }
.tour-dots i.on { width: 18px; background: var(--ink); }
.tour-btns { display: flex; align-items: center; gap: 8px; }
.tour-btns span { flex: 1; }
.tour-btns button, .tour-toast button { font: inherit; font-size: 13px; font-weight: 500; border-radius: 999px; padding: 7px 14px; cursor: pointer;
  border: 1px solid var(--edge); background: var(--glass); color: var(--ink); }
.tour-btns .tour-next, .tour-toast .tour-go { background: var(--ink); color: var(--solid); border-color: var(--ink); }
.tour-btns .tour-skip { border: 0; background: none; padding: 7px 4px; color: var(--ink-2); }
.tour-btns button:disabled { opacity: 0.4; cursor: default; }
.tour-btns button:focus-visible, .tour-toast button:focus-visible { outline: 2px solid var(--s1); outline-offset: 2px; }
.tour-toast { position: fixed; left: 50%; bottom: 84px; transform: translateX(-50%); z-index: 30; display: flex; align-items: center; gap: 10px;
  padding: 8px 8px 8px 16px; border-radius: 999px; font-size: 13px; color: var(--ink); max-width: calc(100vw - 32px);
  animation: tour-in 0.5s var(--ease, ease) both; }
.tour-toast span { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.tour-toast .tour-x { padding: 7px 10px; }
@keyframes tour-in { from { opacity: 0; transform: translate(-50%, 12px); } }
@media (prefers-reduced-motion: reduce) {
  .tour-ring, .tour-pop, .tour-dots i { transition: none; }
  .tour-toast { animation: none; }
}`;

  const reduced = () => matchMedia("(prefers-reduced-motion: reduce)").matches;
  const wait = (ms) => new Promise((r) => setTimeout(r, reduced() ? Math.min(ms, 60) : ms));
  const store = { get: (k) => { try { return localStorage.getItem(k); } catch (e) { return null; } },
    set: (k, v) => { try { localStorage.setItem(k, v); } catch (e) {} } };
  let steps = [], at = 0, ui = null, box = null, doneKey = "tour-done", busy = false;

  function addStyle() {
    if (document.getElementById("tour-style")) return;
    const s = document.createElement("style"); s.id = "tour-style"; s.textContent = STYLE; document.head.appendChild(s);
  }

  function build() {
    addStyle();
    const shade = document.createElement("div"); shade.className = "tour-shade";
    const ring = document.createElement("div"); ring.className = "tour-ring";
    const pop = document.createElement("div");
    pop.className = "tour-pop glass"; pop.tabIndex = -1;
    pop.setAttribute("role", "dialog"); pop.setAttribute("aria-modal", "true"); pop.setAttribute("aria-labelledby", "tour-title");
    pop.innerHTML = '<div class="tour-count" aria-live="polite"></div><h3 id="tour-title"></h3><p></p><div class="tour-dots" aria-hidden="true"></div>' +
      '<div class="tour-btns"><button type="button" class="tour-skip">Skip tour</button><span></span>' +
      '<button type="button" class="tour-back">Back</button><button type="button" class="tour-next">Next</button></div>';
    document.body.append(shade, ring, pop);
    pop.querySelector(".tour-skip").onclick = end;
    pop.querySelector(".tour-back").onclick = () => go(at - 1);
    pop.querySelector(".tour-next").onclick = () => (at === steps.length - 1 ? end() : go(at + 1));
    shade.onclick = () => pop.focus();
    ui = { shade, ring, pop };
    document.addEventListener("keydown", onKey);
    addEventListener("resize", place); addEventListener("scroll", place, { passive: true });
  }

  function onKey(e) {
    if (!ui) return;
    if (e.key === "Escape") end();
    else if (e.key === "ArrowRight") (at === steps.length - 1 ? end() : go(at + 1));
    else if (e.key === "ArrowLeft" && at > 0) go(at - 1);
    else if (e.key === "Tab") {  // keep focus inside the dialog
      const f = [...ui.pop.querySelectorAll("button:not(:disabled)")];
      if (e.shiftKey && document.activeElement === f[0]) { e.preventDefault(); f[f.length - 1].focus(); }
      else if (!e.shiftKey && document.activeElement === f[f.length - 1]) { e.preventDefault(); f[0].focus(); }
    }
  }

  // spotlight the step's card (or the whole screen) and put the dialog where it fits
  function place() {
    if (!ui) return;
    const { ring, pop } = ui, vw = innerWidth, vh = innerHeight, pad = 8, gap = 14;
    const pw = pop.offsetWidth, ph = pop.offsetHeight;
    if (!box) {
      Object.assign(ring.style, { left: vw / 2 + "px", top: vh / 2 + "px", width: "0px", height: "0px" });
      Object.assign(pop.style, { left: (vw - pw) / 2 + "px", top: Math.max(16, (vh - ph) / 2) + "px" });
      return;
    }
    const r = box.getBoundingClientRect();
    const x = r.left - pad, y = r.top - pad, w = r.width + 2 * pad, h = r.height + 2 * pad;
    Object.assign(ring.style, { left: x + "px", top: y + "px", width: w + "px", height: h + "px" });
    let left, top;
    if (vw < 640) { left = (vw - pw) / 2; top = y + h + gap + ph < vh - 80 ? y + h + gap : Math.max(16, y - gap - ph); }
    else if (x + w + gap + pw < vw - 16) { left = x + w + gap; top = y; }
    else if (x - gap - pw > 16) { left = x - gap - pw; top = y; }
    else if (y + h + gap + ph < vh - 16) { left = x; top = y + h + gap; }
    else { left = x; top = y - gap - ph; }
    left = Math.min(Math.max(16, left), vw - pw - 16);
    top = Math.min(Math.max(16, top), vh - ph - 16);
    Object.assign(pop.style, { left: left + "px", top: top + "px" });
  }

  async function go(n) {
    if (busy || n < 0 || n >= steps.length) return;
    busy = true; at = n;
    const s = steps[n], { pop } = ui;
    pop.style.opacity = 0.35;
    if (s.page && location.hash.slice(1) !== s.page) { location.hash = s.page; await wait(650); }
    const el = s.target ? document.querySelector(s.target) : null;
    box = el ? (s.exact ? el : el.closest(".card") || el) : null;
    if (box) {
      const r = box.getBoundingClientRect();
      // tall cards are pinned near the top so the dialog still fits on screen
      const fits = r.height < innerHeight - 260;
      box.scrollIntoView({ block: fits ? "center" : "start", behavior: reduced() ? "auto" : "smooth" });
      await wait(fits ? 450 : 500);
    }
    pop.querySelector(".tour-count").textContent = `${n + 1} of ${steps.length}`;
    pop.querySelector("h3").textContent = s.title;
    pop.querySelector("p").textContent = s.text;
    const dots = pop.querySelector(".tour-dots"); dots.replaceChildren();
    steps.forEach((_, i) => { const d = document.createElement("i"); if (i === n) d.className = "on"; dots.appendChild(d); });
    pop.querySelector(".tour-back").disabled = n === 0;
    pop.querySelector(".tour-next").textContent = n === steps.length - 1 ? "Finish" : "Next";
    place(); pop.style.opacity = 1;
    pop.querySelector(".tour-next").focus({ preventScroll: true });
    busy = false;
  }

  function end() {
    if (!ui) return;
    ui.shade.remove(); ui.ring.remove(); ui.pop.remove(); ui = null; box = null;
    document.removeEventListener("keydown", onKey);
    removeEventListener("resize", place); removeEventListener("scroll", place);
    store.set(doneKey, "1");
    const btn = document.getElementById("tour-btn"); if (btn) btn.focus({ preventScroll: true });
  }

  function start(list) {
    hideOffer();
    if (ui) end();
    steps = list; build(); go(0);
  }

  function hideOffer() { const t = document.querySelector(".tour-toast"); if (t) t.remove(); }

  function offer(list, key) {
    doneKey = key || doneKey;
    const btn = document.getElementById("tour-btn");
    if (btn) btn.onclick = () => start(list);
    if (store.get(doneKey)) return;
    addStyle();
    const t = document.createElement("div"); t.className = "tour-toast glass"; t.setAttribute("role", "status");
    t.innerHTML = '<span>New here? Take a one-minute tour.</span><button type="button" class="tour-go">Start tour</button>' +
      '<button type="button" class="tour-x" aria-label="Dismiss">✕</button>';
    t.querySelector(".tour-go").onclick = () => start(list);
    t.querySelector(".tour-x").onclick = () => { store.set(doneKey, "1"); hideOffer(); };
    document.body.appendChild(t);
  }

  window.Tour = { start, offer };
})();
