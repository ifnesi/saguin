// The interface: a navbar that says what this is answering with, a progress
// bar while the documents are indexed, and a conversation.
//
// React from a vendored UMD build with htm for the templates, so there is no
// npm and no build step - the whole frontend is these two files plus what
// the image vendored.

const html = htm.bind(React.createElement);
const { useState, useEffect, useRef, useMemo } = React;
const { createPortal } = ReactDOM;

// Identifies a popup stack entry independent of its position - a plain
// incrementing counter is enough for one page's lifetime, and position
// alone breaks the moment two documents are opened before the first one's
// fetch has resolved.
let popupSeq = 0;

// A plain cookie, as asked - not localStorage. A year is long enough that
// "survives a restart" means what it should without the cookie outliving
// the machine it was set on.
function setCookie(name, value) {
  document.cookie = `${name}=${encodeURIComponent(value)}; max-age=${365 * 24 * 3600}; path=/; samesite=lax`;
}

// Context windows are always sized in Ks by the people who set them - 8192,
// 131072 - so "8K" and "128K" is what an operator typed, where "8,192" is
// what they'd have to convert back in their head to check it.
function formatKB(tokens) {
  const k = tokens / 1024;
  return (Number.isInteger(k) ? k : k.toFixed(1)) + "K";
}

// A native <select> rather than a hand-built menu: keyboard and screen
// reader behaviour come for free, and the corpus is a flat list of a dozen
// files with nothing about it that needs a custom widget. It always shows
// as reset to the placeholder - picking the same document twice in a row
// must reopen it, not silently do nothing because the value did not change.
function DocsMenu({ docs, onSelect }) {
  return html`
    <select class="pill docs-menu" value=""
            onChange=${e => { const v = e.target.value; if (v) onSelect(v); e.target.value = ""; }}>
      <option value="" disabled>Documents ▾</option>
      ${docs.map(d => html`<option key=${d} value=${d}>${d}</option>`)}
    </select>`;
}

// The questions people actually arrive with, grouped the way they meet the
// broker. Picking one posts it into the chat through the same path as
// typing it - there are no canned answers, so a shortcut answer is exactly
// as grounded, cited and versioned as a typed one, and goes stale with
// nothing when the documents move. The list leans towards questions the
// corpus answers well, several of them the eval's own traps rephrased.
const QUESTIONS = [
  { group: "Getting started", items: [
    "What is saguin, in one paragraph?",
    "How is saguin different from a plain MQTT broker?",
    "When should I not use saguin?",
    "Does my existing MQTT client work?",
    "Can an MQTT 3.1.1 client connect?",
  ]},
  { group: "Channels", items: [
    "What is an append channel?",
    "What is a latest channel?",
    "What is a queue channel?",
    "What happens to a topic no channel claims?",
    "Can a wildcard subscription cross into a channel?",
    "How does a channel claim the topic space?",
    "Why was my publish refused with 0x87?",
    "What does saguin add to every delivery?",
  ]},
  { group: "Queues and workers", items: [
    "How does a worker acknowledge a job?",
    "What happens when a worker dies holding a job?",
    "How do retries and backoff work?",
    "When does work go to the dead-letter channel?",
    "How do I read the dead-letter channel?",
    "Can two workers get the same job?",
    "Why can't a plain MQTT subscriber acknowledge a job?",
    "What happens to in-flight jobs on restart?",
  ]},
  { group: "Replay and seek", items: [
    "How do I replay a channel from the beginning?",
    "How do I replay the last 12 hours?",
    "How do I see the reply to a seek?",
    "What does below-floor mean?",
    "What happens when retention passes my position?",
  ]},
  { group: "Key/value and retained", items: [
    "How do I read one value without subscribing?",
    "How do I delete a key?",
    "How are retained messages different here?",
    "Why was my retained publish refused?",
  ]},
  { group: "Security", items: [
    "How do I turn on TLS?",
    "How do I set up mutual TLS?",
    "How do I generate the certificate and CA files?",
    "How do I add users and passwords?",
    "How do I restrict a device to its own topics?",
    "Can I keep my existing password file?",
    "Why is there no skip-verify?",
    "What does a refused client see?",
  ]},
  { group: "Connections and browsers", items: [
    "How long may a socket wait before sending CONNECT?",
    "How do I stop silent sockets consuming every connection slot?",
    "What is the largest CONNECT packet saguin accepts?",
    "How do I limit a fleet reconnect storm?",
    "How do I restrict which web pages may use the WebSocket listener?",
    "Why does same_origin fail behind a TLS-terminating proxy?",
  ]},
  { group: "Bridges and the copy", items: [
    "How do I read another broker into a channel?",
    "How do I set up a disaster-recovery copy?",
    "Which end connects, the source or the copy?",
    "What does a copy not carry?",
    "How far behind is my copy?",
    "What happens when the link goes down?",
  ]},
  { group: "Operations and storage", items: [
    "How do I monitor saguin with Prometheus?",
    "Which metric should I alert on?",
    "What does /health actually check?",
    "How do I back up a running broker?",
    "What survives a crash, and a power cut?",
    "How do I move a channel between memory and SQLite?",
    "Can I open the SQLite file while the broker runs?",
    "How do I check a configuration without starting the broker?",
    "How do I enable QoS 2, and where do unfinished publishes wait?",
    "Does a refused reconnect cancel an existing delayed Will?",
  ]},
];

// The same native <select>-as-pill as the documents menu, for the same
// reasons - and <optgroup> is what makes sixty entries navigable without
// a custom widget. It resets to the placeholder so the same question can
// be asked twice in a row.
function QuestionsMenu({ onAsk }) {
  return html`
    <select class="pill docs-menu" value=""
            onChange=${e => { const v = e.target.value; if (v) onAsk(v); e.target.value = ""; }}>
      <option value="" disabled>Common questions ▾</option>
      ${QUESTIONS.map(g => html`<optgroup key=${g.group} label=${g.group}>
        ${g.items.map(q => html`<option key=${q} value=${q}>${q}</option>`)}
      </optgroup>`)}
    </select>`;
}

// Sun in dark mode, moon in light mode - the icon names what a click gets
// you, not the state you are already in, which is the reading GitHub's own
// toggle trained people on.
const SUN = html`<svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4">
  <circle cx="8" cy="8" r="3.4"/>
  <path d="M8 1v1.6M8 13.4V15M2.6 8H1M15 8h-1.6M3.5 3.5l1.1 1.1M11.4 11.4l1.1 1.1M12.5 3.5l-1.1 1.1M4.6 11.4l-1.1 1.1" stroke-linecap="round"/>
</svg>`;
const MOON = html`<svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4">
  <path d="M13.8 9.9A6 6 0 1 1 6.1 2.2a5 5 0 0 0 7.7 7.7Z" stroke-linejoin="round"/>
</svg>`;
const EXPAND = html`<svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4">
  <path d="M6 2H2v4M10 2h4v4M6 14H2v-4M10 14h4v-4" stroke-linecap="round" stroke-linejoin="round"/>
</svg>`;
const CONTRACT = html`<svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4">
  <path d="M2 6h4V2M14 6h-4V2M2 10h4v4M14 10h-4v4" stroke-linecap="round" stroke-linejoin="round"/>
</svg>`;

function Navbar({ status, docs, onSelectDoc, onAsk, theme, onToggleTheme, wide, onToggleWidth }) {
  const llm = status.llm || {};
  // Where the model runs is stated, always: every answer comes from
  // saguin's own documents, but with a cloud model the question, recent user
  // questions used as context, and retrieved excerpts leave this machine.
  const where = llm.local ? "local" : "cloud";
  const pill = llm.ok
    ? html`<span class=${"pill" + (llm.local ? " local" : "")}>${llm.provider}: ${llm.model} (${where})</span>`
    : html`<span class="pill bad" title=${llm.detail}>no model - ${llm.provider}</span>`;

  // The context window, beside the model. It is the number that decides
  // whether the model actually saw the excerpts under its own answer: too
  // small and the oldest tokens are dropped without a word, which is an
  // answer citing text it never read.
  const ctx = llm.context
    ? html`<span class="pill" title=${"Context window sent to the model: " +
        llm.context.toLocaleString() + " tokens. " +
        (llm.excerpts || 0) + " excerpts are retrieved per search; if answers ignore them, " +
        "raise SAGUIN_LLM_CONTEXT."}>ctx ${formatKB(llm.context)}</span>`
    : null;

  return html`
    <nav>
      <img src="/saguin.png" alt="saguin" />
      <span class="brand">
        <span class="name">Sagüin</span>
        ${status.commit && html`<span class="version">docs @ ${status.commit}</span>`}
      </span>
      ${docs && docs.length > 0 && html`<${DocsMenu} docs=${docs} onSelect=${onSelectDoc} />`}
      ${onAsk && html`<${QuestionsMenu} onAsk=${onAsk} />`}
      <span class="spacer"></span>
      <span class="llm-info">
        ${pill}
        ${ctx}
      </span>
      <span class="icon-group">
        <button type="button" class="icon-btn" onClick=${onToggleWidth}
          title=${wide ? "Use the normal width" : "Use the full width"} aria-label="Toggle page width">
          ${wide ? CONTRACT : EXPAND}
        </button>
        <button type="button" class="icon-btn" onClick=${onToggleTheme}
          title=${theme === "dark" ? "Switch to light mode" : "Switch to dark mode"} aria-label="Toggle theme">
          ${theme === "dark" ? SUN : MOON}
        </button>
      </span>
    </nav>`;
}

function Progress({ status }) {
  // Hidden once ready, as the item asks - but an error keeps it on screen,
  // because a bar that vanishes on failure leaves a page with nothing on it
  // and no reason.
  if (status.ready) return null;
  const failed = !!status.error;
  return html`
    <div class=${"bar" + (failed ? " err" : "")}>
      <div class="stage">
        <span>${failed ? status.error : status.stage}</span>
        <span>${status.total ? status.done + " / " + status.total : ""}</span>
      </div>
      ${!failed && html`<div class="track"><div class="fill" style=${{ width: status.percent + "%" }}></div></div>`}
    </div>`;
}

// GitHub-style shortcodes the model sometimes writes - ":point_right:",
// ":octocat:" - which marked has no idea what to do with, since it is not
// GitHub rendering this. Only the ones actually seen in answers are covered;
// an unmapped shortcode is left as literal text rather than guessed at.
const EMOJI = {
  point_right: "👉", point_left: "👈", point_up: "☝️", point_down: "👇",
  warning: "⚠️", white_check_mark: "✅", x: "❌", heavy_check_mark: "✔️",
  bulb: "💡", lock: "🔒", unlock: "🔓", gear: "⚙️", rocket: "🚀",
  octocat: "🐙", memo: "📝", books: "📚", package: "📦", link: "🔗",
  fire: "🔥", tada: "🎉", thumbsup: "👍", thumbsdown: "👎",
  question: "❓", exclamation: "❗", information_source: "ℹ️",
};

function withEmoji(markdown) {
  return markdown.replace(/:([a-z0-9_+-]+):/gi, (whole, name) => EMOJI[name] || whole);
}

// A heading's id, the way GitHub's own renderer would make one - lowercase,
// spaces to hyphens, punctuation dropped - because these documents' own
// cross-links (`docs/invariants.md#invariant-15`) were written expecting
// that convention, on the assumption GitHub is what renders them. marked
// stopped assigning heading ids itself several major versions back, so
// without this every anchor in every link is a name nothing in the
// rendered page actually has.
function slugify(text) {
  return (text || "").toLowerCase().trim()
    .replace(/[^\w\- ]+/g, "")
    .replace(/\s+/g, "-");
}

// An absolute http(s) link leaves the corpus entirely - every relative one
// is either ours to intercept or, once resolved, ours to leave alone. `_blank`
// is a real DOM attribute set once after render rather than a click-time
// `window.open`, so native behaviour (middle-click, ctrl-click, "open in new
// tab" from the context menu) all keep working exactly as it would anywhere
// else; a JS-driven interception would have to reimplement each of those.
function markExternalLinks(container) {
  if (!container) return;
  container.querySelectorAll("a[href]").forEach(a => {
    if (/^https?:\/\//i.test(a.getAttribute("href") || "")) {
      a.target = "_blank";
      a.rel = "noopener noreferrer";
    }
  });
}

function assignHeadingIds(container) {
  if (!container) return;
  const seen = new Map();
  container.querySelectorAll("h1,h2,h3,h4,h5,h6").forEach(h => {
    const slug = slugify(h.textContent);
    const n = seen.get(slug) || 0;
    seen.set(slug, n + 1);
    h.id = n === 0 ? slug : `${slug}-${n}`;
  });
}

// A markdown link's raw href, resolved against the document it was written
// in - not against this page's URL, which is what the browser would do by
// default and exactly why these links 404ed. `docs/rfcs/0001-....md` links
// to `../invariants.md`: relative to *that file's own directory*
// (`docs/rfcs/`), which resolves to `docs/invariants.md` - never to
// whatever path this single-page app happens to be serving at the time.
// Returns null for anything not ours to handle: an absolute URL, a mailto,
// anything already outside the relative-path shape a corpus cross-link
// takes.
function resolveDocPath(href, baseDoc) {
  if (!href) return null;
  const hashIdx = href.indexOf("#");
  const pathPart = hashIdx === -1 ? href : href.slice(0, hashIdx);
  const anchor = hashIdx === -1 ? "" : href.slice(hashIdx + 1);
  if (/^[a-z][a-z0-9+.-]*:/i.test(pathPart) || pathPart.startsWith("//")) return null;
  if (pathPart === "") return { path: null, anchor }; // a same-document "#anchor"
  const baseDir = baseDoc ? baseDoc.split("/").slice(0, -1) : [];
  const out = [];
  for (const seg of baseDir.concat(pathPart.split("/"))) {
    if (seg === "" || seg === ".") continue;
    if (seg === "..") out.pop();
    else out.push(seg);
  }
  return { path: out.join("/"), anchor };
}

// One click handler shared by every popup and every answer: a link to a
// document this agent has indexed opens that document here rather than
// asking the browser to navigate to a path this single-page app does not
// serve; a link to a heading - in the same document or another - scrolls
// there once the content exists to scroll to. Anything else (an external
// URL, a mailto, a relative path outside the corpus) is left alone.
function docLinkHandler(baseDoc, docs, onNavigate, containerRef) {
  return (e) => {
    const a = e.target.closest && e.target.closest("a");
    if (!a) return;
    const resolved = resolveDocPath(a.getAttribute("href"), baseDoc);
    if (!resolved) return;
    if (resolved.path === null || resolved.path === baseDoc) {
      if (resolved.anchor) {
        e.preventDefault();
        const target = containerRef.current &&
          containerRef.current.querySelector("#" + CSS.escape(resolved.anchor));
        target && target.scrollIntoView({ behavior: "smooth", block: "start" });
      }
      return;
    }
    if (docs.has(resolved.path)) {
      e.preventDefault();
      onNavigate(resolved.path, resolved.anchor);
    }
    // Else: a relative path outside the indexed corpus. Left to the
    // browser, unchanged from today - out of scope here, since there is no
    // document to open for it.
  };
}

// The full text of one document, or one reference's excerpt - both shown
// the same way, both navigable the same way. `chunk.document` is the
// actual corpus-relative path (`docs/rfcs/0001-....md`); `chunk.citation`
// is what titles the popup, which for a reference is a heading breadcrumb
// and for a whole document is the same path.
//
// One card in the stack - never its own backdrop or its own Escape
// listener, both of which belong to `PopupStack` once, not to every card
// opened inside it. `depth` is how many cards sit on top of this one: 0 is
// the interactive card on top, and each step back cascades it further
// behind - visible, so a reader can see there is somewhere to come back
// to, but inert, so a click cannot land on a card that is not the one
// showing.
function PopupCard({ chunk, docs, onNavigate, onClose, depth, isTop }) {
  const bodyRef = useRef(null);

  // Heading ids exist once the content is in the DOM; the jump to an
  // anchor a caller asked for (arriving here via a cross-document link)
  // has to wait for that same commit.
  useEffect(() => {
    assignHeadingIds(bodyRef.current);
    markExternalLinks(bodyRef.current);
    if (chunk.anchor && bodyRef.current) {
      const target = bodyRef.current.querySelector("#" + CSS.escape(chunk.anchor));
      target && target.scrollIntoView({ block: "start" });
    }
  }, [chunk.text, chunk.anchor]);

  // The chunk is the same markdown the model was handed, not plain text -
  // rendering it verbatim showed bold markers and headings as punctuation
  // instead of formatting.
  const body = { __html: marked.parse(withEmoji(chunk.text || "")) };
  const onLinkClick = docLinkHandler(chunk.document || chunk.citation, docs, onNavigate, bodyRef);
  return html`
    <div class="popup" style=${{
      transform: `translate(calc(-50% + ${depth * 16}px), calc(-50% + ${depth * 16}px)) scale(${1 - depth * 0.04})`,
      zIndex: 100 - depth,
      pointerEvents: isTop ? "auto" : "none",
      filter: isTop ? "none" : `brightness(${Math.max(0.4, 1 - depth * 0.18)})`,
    }} onClick=${e => e.stopPropagation()}>
      <div class="popup-head">
        <strong>${chunk.citation}</strong>
        ${isTop && html`<${CopyButton}
          getHtml=${() => bodyRef.current ? bodyRef.current.innerHTML : ""}
          getText=${() => bodyRef.current ? bodyRef.current.innerText : ""}
        />`}
        <button type="button" class="popup-close" onClick=${onClose} aria-label="Close">×</button>
      </div>
      <div class="popup-body" ref=${bodyRef} onClick=${onLinkClick} dangerouslySetInnerHTML=${body}></div>
    </div>`;
}

// Every open document or reference, cascaded - clicking a link inside one
// pushes another on top rather than replacing it, so Escape (or the ×)
// closes only the newest and returns to the one underneath, however deep
// the reader has gone.
//
// Ported straight to <body>. A `.row` message bubble animates in with a
// `transform`, and CSS makes any transformed element the containing block
// for its `position: fixed` descendants - so rendered in place, this stack
// was "fixed" to that small bubble's box, not the viewport: clipped to it,
// scrolling with the page behind it. A portal renders this subtree as a
// direct child of <body>, outside that ancestor chain entirely, so no
// future animation anywhere in the tree can trap it again.
function PopupStack({ stack, docs, onNavigate, onClose }) {
  useEffect(() => {
    if (stack.length === 0) return;
    const onKey = e => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [stack.length, onClose]);

  if (stack.length === 0) return null;
  return createPortal(
    html`
      <div class="popup-backdrop" onClick=${onClose}>
        ${stack.map((chunk, i) => html`
          <${PopupCard} key=${chunk.id} chunk=${chunk} docs=${docs} onNavigate=${onNavigate}
            onClose=${onClose} depth=${stack.length - 1 - i} isTop=${i === stack.length - 1} />`)}
      </div>`,
    document.body,
  );
}

// Inline every style a paste target needs to see, since Word, Google Docs
// and webmail compose boxes all strip the <link>/<style> this page actually
// uses and keep only inline `style=` attributes. A plain white-background,
// black-text copy of the markdown structure - not this page's dark theme -
// is what belongs in a document nobody else can see the app's CSS in.
function richClipboardHtml(innerHtml) {
  const doc = document.createElement("div");
  doc.innerHTML = innerHtml;
  const style = (sel, decl) => doc.querySelectorAll(sel).forEach(el => Object.assign(el.style, decl));
  style("h1", { fontSize: "20px", margin: "0.6em 0 0.3em", fontWeight: "700" });
  style("h2", { fontSize: "17px", margin: "0.6em 0 0.3em", fontWeight: "700" });
  style("h3", { fontSize: "15px", margin: "0.6em 0 0.3em", fontWeight: "700" });
  style("table", { borderCollapse: "collapse", width: "100%", margin: "0.5em 0" });
  style("th, td", { border: "1px solid #ccc", padding: "6px 8px", textAlign: "left" });
  style("th", { background: "#f0f0f0", fontWeight: "700" });
  style("code", { fontFamily: "Menlo, Consolas, monospace", background: "#f4f4f4", padding: "1px 4px", borderRadius: "3px" });
  style("pre", { fontFamily: "Menlo, Consolas, monospace", background: "#f4f4f4", border: "1px solid #ddd", borderRadius: "6px", padding: "10px", overflow: "auto" });
  style("a", { color: "#1a5fb4" });
  style("strong", { fontWeight: "700" });
  const wrapper = document.createElement("div");
  Object.assign(wrapper.style, {
    background: "#ffffff", color: "#000000",
    fontFamily: "Arial, Helvetica, sans-serif", fontSize: "14px", lineHeight: "1.5",
  });
  wrapper.appendChild(doc);
  return wrapper.outerHTML;
}

// Both formats in one write: `text/html` is what Word, Docs and webmail
// compose boxes read for formatting, `text/plain` is the fallback for
// anywhere that only accepts plain text. Older browsers or a denied
// clipboard permission fall back to plain text alone rather than doing
// nothing and leaving the click looking like it was ignored.
async function copyRich(html, plain) {
  try {
    if (window.ClipboardItem) {
      await navigator.clipboard.write([
        new ClipboardItem({
          "text/html": new Blob([html], { type: "text/html" }),
          "text/plain": new Blob([plain], { type: "text/plain" }),
        }),
      ]);
      return true;
    }
  } catch (e) { /* fall through to plain text */ }
  try {
    await navigator.clipboard.writeText(plain);
    return true;
  } catch (e) {
    return false;
  }
}

function CopyButton({ getHtml, getText }) {
  const [state, setState] = useState("idle"); // idle | copied | failed
  const click = async () => {
    const ok = await copyRich(richClipboardHtml(getHtml()), getText());
    setState(ok ? "copied" : "failed");
    setTimeout(() => setState("idle"), 1500);
  };
  const label = state === "copied" ? "Copied" : state === "failed" ? "Couldn't copy" : "Copy";
  return html`
    <button type="button" class="copy-btn" onClick=${click} title="Copy formatted - pastes into Word, Docs, email" aria-label="Copy response">
      ${state === "idle" && html`
        <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4">
          <rect x="5" y="5" width="9" height="9" rx="1.5"/>
          <path d="M3.5 10.5h-1a1 1 0 0 1-1-1v-7a1 1 0 0 1 1-1h7a1 1 0 0 1 1 1v1"/>
        </svg>`}
      <span>${label}</span>
    </button>`;
}

function Message({ m, docs, onOpenRef, onNavigate, onEditQuestion }) {
  const bodyRef = useRef(null);
  // Called unconditionally, before the early return below, so the Rules of
  // Hooks hold regardless of which role this message turns out to be - a
  // "you" message has nothing in `bodyRef` for it to find, and the guard
  // inside `markExternalLinks` makes that a no-op rather than a special case.
  useEffect(() => { markExternalLinks(bodyRef.current); }, [m.markdown]);
  if (m.role === "you") return html`
    <div class="row you">
      <div class="msg">
        <span class="role">You</span>
        <button type="button" class="copy-btn" onClick=${() => onEditQuestion(m.text)}
          title="Edit and re-ask" aria-label="Edit this question">
          <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4">
            <path d="M10.5 2.5l3 3L5 14H2v-3L10.5 2.5Z" stroke-linejoin="round" stroke-linecap="round"/>
          </svg>
          <span>Edit</span>
        </button>
        ${m.text}
      </div>
    </div>`;
  // While the loop runs: the steps it has taken so far, the last one still
  // going. This is the whole point of streaming - the reader sees it search
  // and read rather than a spinner over nothing on slow hardware.
  if (m.pending) return html`
    <div class="row">
      <div class="msg">
        <span class="role">Sagüin</span>
        <ol class="live-steps">
          ${(m.steps || []).map((t, i, all) => html`
            <li key=${i} class=${i === all.length - 1 ? "doing" : "done"}>${t}</li>`)}
          <li class="doing pulse">thinking…</li>
        </ol>
      </div>
    </div>`;
  const body = { __html: marked.parse(withEmoji(m.markdown || "")) };
  // In the order the backend already put them: sections read in full first,
  // then ones semantic search surfaced, then lines an exact-term grep turned
  // up. Not every reference has a score - an exact match has no similarity
  // number - so sorting by score here would compare against undefined.
  const refs = m.references || [];
  // An answer is not written from any one document, so a relative link in
  // it - usually copied verbatim from an excerpt - is resolved as if
  // written at the corpus root, the same base README.md's own links use.
  const onLinkClick = docLinkHandler("", docs, onNavigate, bodyRef);
  return html`
    <div class="row">
      <div class="msg">
        <span class="role">Sagüin</span>
        <${CopyButton}
          getHtml=${() => bodyRef.current ? bodyRef.current.innerHTML : ""}
          getText=${() => bodyRef.current ? bodyRef.current.innerText : ""}
        />
        <div ref=${bodyRef} onClick=${onLinkClick} dangerouslySetInnerHTML=${body}></div>
        ${refs.length > 0 && html`
          <div class="refs">
            <div><strong>References</strong>${m.commit ? " - saguin docs @ " + m.commit : ""}</div>
            <ol>
              ${refs.map((r, i) => html`
                <li key=${i}>
                  <button type="button" class="ref-link" onClick=${() => onOpenRef(r)}>${r.citation}</button>
                  ${typeof r.score === "number" && html`<span class="ref-score">${r.score.toFixed(3)}</span>`}
                </li>`)}
            </ol>
          </div>`}
        ${m.trace && m.trace.length > 0 && html`
          <details class="trace">
            <summary>What the loop did</summary>
            <ol>${m.trace.map((t, i) => html`<li key=${i}>${t}</li>`)}</ol>
          </details>`}
      </div>
    </div>`;
}

function App() {
  const [status, setStatus] = useState({ stage: "starting", percent: 0, ready: false, llm: {} });
  const [messages, setMessages] = useState([]);
  // The backend issues this bounded capsule after each answer. It contains no
  // generated prose: only two user questions and three retrieved section titles.
  const [context, setContext] = useState(null);
  const [question, setQuestion] = useState("");
  const [busy, setBusy] = useState(false);
  const [docs, setDocs] = useState([]);
  const [popupStack, setPopupStack] = useState([]);
  // The inline script in index.html already set these on <html> before the
  // first paint; read them back rather than re-deciding, so React's first
  // render agrees with what the reader is already looking at.
  const [theme, setTheme] = useState(document.documentElement.dataset.theme || "dark");
  const [wide, setWide] = useState(document.documentElement.dataset.width === "full");
  const end = useRef(null);
  const inputRef = useRef(null);

  // Puts a past question back in the box to edit rather than re-typing it -
  // the input still has to be enabled and empty-ish for this to make sense,
  // so it also focuses it, the same as clicking into it by hand would.
  const editQuestion = (text) => {
    setQuestion(text);
    inputRef.current && inputRef.current.focus();
  };

  const toggleTheme = () => {
    const next = theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    setCookie("saguin_theme", next);
    setTheme(next);
  };
  const toggleWidth = () => {
    const next = wide ? "normal" : "full";
    document.documentElement.dataset.width = next;
    setCookie("saguin_width", next);
    setWide(!wide);
  };
  // A Set once, not a fresh one on every render: `docLinkHandler` is
  // rebuilt per Message render, and membership is checked on every click.
  const docsSet = useMemo(() => new Set(docs), [docs]);

  // Poll once a second only while the corpus is still being indexed - that is
  // what moves the progress bar. Once it is ready the timer stops: the only
  // reason to keep asking is the model's health, and /api/ask already
  // re-checks that and returns the operator's fix if the model has gone, so a
  // forever-timer would ping ollama every second to pre-warn about something
  // the ask path already catches. Health is refreshed instead when the reader
  // comes back to the tab, which is the moment a stale dot would mislead.
  useEffect(() => {
    let t;
    const poll = () => fetch("/api/status").then(r => r.json()).then(s => {
      setStatus(s);
      if (!s.ready) t = setTimeout(poll, 1000);
    }).catch(() => { t = setTimeout(poll, 2000); });
    poll();
    const onFocus = () => poll();
    window.addEventListener("focus", onFocus);
    return () => { clearTimeout(t); window.removeEventListener("focus", onFocus); };
  }, []);

  // The document list is static once the container is built - a file the
  // corpus, not the index, so it does not need to wait on `status.ready`
  // the way a search does.
  useEffect(() => {
    fetch("/api/docs").then(r => r.json()).then(d => setDocs(d.documents || [])).catch(() => {});
  }, []);

  // Two ways a popup opens, one stack behind both: a reference already
  // carries its excerpt, nothing to fetch; a cross-document link or the
  // navbar's dropdown names a path that has to be loaded first. Each opens
  // *on top* of whatever is already open - a link followed from inside one
  // document must not lose the one the reader came from - identified by an
  // id rather than by position, since a second click before the first
  // fetch resolves must not let a slow answer land in the wrong slot.
  const openReference = (ref) => setPopupStack(stack => [...stack, { id: ++popupSeq, ...ref }]);
  const openDocument = (path, anchor) => {
    const id = ++popupSeq;
    setPopupStack(stack => [...stack, { id, citation: path, document: path, text: "Loading…" }]);
    fetch(`/api/docs/${path}`)
      .then(r => r.json())
      .then(d => setPopupStack(stack => stack.map(p => p.id === id
        ? { ...p, citation: d.document || path, document: d.document || path, text: d.text || d.error || "", anchor }
        : p)))
      .catch(err => setPopupStack(stack => stack.map(p => p.id === id
        ? { ...p, text: `Could not load this file (${err}).` }
        : p)));
  };
  const closeTopPopup = () => setPopupStack(stack => stack.slice(0, -1));

  useEffect(() => { end.current && end.current.scrollIntoView({ behavior: "smooth" }); }, [messages]);

  // Indexing and a question in flight are both "the agent is doing
  // something, wait" - shown on the whole page rather than just the input,
  // since indexing happens before there is an input to point at.
  useEffect(() => {
    document.body.classList.toggle("busy", busy || !status.ready);
  }, [busy, status.ready]);

  const llmDown = status.llm && status.llm.ok === false;

  // One path for every question, typed or picked from the shortcut menu,
  // so a shortcut streams, cites and fails exactly as a typed question
  // does. The guards repeat the input's disabled conditions, because a
  // menu pick does not pass through the disabled input.
  const ask = async (q) => {
    if (!q || busy || !status.ready || llmDown) return;
    // The question, and a pending answer beside it that shows the loop's steps
    // as they arrive - so the reader watches it search and read rather than a
    // spinner. `patchLast` updates that pending message in place.
    setMessages(m => [...m, { role: "you", text: q }, { role: "agent", pending: true, steps: [] }]);
    setBusy(true);
    const patchLast = (patch) => setMessages(m => {
      const copy = m.slice();
      copy[copy.length - 1] = { ...copy[copy.length - 1], ...patch };
      return copy;
    });
    try {
      const r = await fetch("/api/ask/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q, context }),
      });
      // The readiness and health checks answer with JSON before the stream
      // opens, so a non-OK response is an ordinary error, not an event stream.
      if (!r.ok) {
        let msg = "something went wrong";
        try { msg = (await r.json()).error || msg; } catch (_) {}
        setContext(null);
        patchLast({ pending: false, steps: undefined, markdown: "**" + msg + "**" });
        return;
      }
      const reader = r.body.getReader();
      const decoder = new TextDecoder();
      const steps = [];
      let buffer = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        // Server-Sent Events are separated by a blank line; a "data:" line
        // carries one JSON event. Parse whole events, keep any partial tail.
        let split;
        while ((split = buffer.indexOf("\n\n")) >= 0) {
          const block = buffer.slice(0, split);
          buffer = buffer.slice(split + 2);
          const line = block.split("\n").find(l => l.startsWith("data:"));
          if (!line) continue;
          const ev = JSON.parse(line.slice(5).trim());
          if (ev.type === "step") {
            steps.push(ev.text);
            patchLast({ steps: steps.slice() });
          } else if (ev.type === "answer") {
            setContext(ev.context || null);
            patchLast({ pending: false, steps: undefined, markdown: ev.markdown,
                        references: ev.references, refused: ev.refused, trace: ev.trace, commit: ev.commit });
          } else if (ev.type === "error") {
            setContext(null);
            patchLast({ pending: false, steps: undefined, markdown: "**" + ev.message + "**" });
          }
        }
      }
    } catch (err) {
      setContext(null);
      patchLast({ pending: false, steps: undefined, markdown: "**" + err + "**" });
    } finally {
      setBusy(false);
    }
  };

  const submit = (e) => {
    e.preventDefault();
    const q = question.trim();
    if (!q || busy) return;
    setQuestion("");
    ask(q);
  };

  return html`
    <${Navbar} status=${status} docs=${docs} onSelectDoc=${openDocument}
      onAsk=${status.ready && !llmDown ? ask : null}
      theme=${theme} onToggleTheme=${toggleTheme} wide=${wide} onToggleWidth=${toggleWidth} />
    <main>
      <${Progress} status=${status} />
      ${llmDown && html`<div class="bar err"><div class="stage"><span>${status.llm.detail}</span></div></div>`}
      ${messages.map((m, i) => html`<${Message} key=${i} m=${m} docs=${docsSet} onOpenRef=${openReference} onNavigate=${openDocument} onEditQuestion=${editQuestion} />`)}
      <div ref=${end}></div>
    </main>
    <${PopupStack} stack=${popupStack} docs=${docsSet} onNavigate=${openDocument} onClose=${closeTopPopup} />
    <form onSubmit=${submit}>
      <div>
        <input type="text" ref=${inputRef} value=${question} placeholder=${status.ready ? "Ask about Sagüin…" : "Indexing the documentation…"}
               maxLength="2000" onInput=${e => setQuestion(e.target.value)} disabled=${!status.ready || llmDown || busy} />
        <button type="submit" disabled=${busy || !status.ready || llmDown}>${busy ? "Thinking…" : "Ask"}</button>
      </div>
    </form>`;
}

ReactDOM.createRoot(document.getElementById("root")).render(html`<${App} />`);
