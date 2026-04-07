#!/usr/bin/env python3
"""Generate plan-structure comparison HTML from JSON explain files.

Usage:
    python3 substrait_test/plot_explain.py                    # all benchmarks
    python3 substrait_test/plot_explain.py tpch               # specific benchmark
"""
import json
import os
import re
import sys
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXPLAIN_ROOT = os.path.join(SCRIPT_DIR, "explain")
METHODS = ["arrow", "substrait"]


def _fmt_time(ms):
    if ms >= 1000:
        return f"{ms / 1000:.1f}s"
    if ms >= 1:
        return f"{ms:.0f}ms"
    return f"{ms * 1000:.0f}us"


def _fmt_rows(n):
    if n >= 1e6:
        return f"{n / 1e6:.1f}M"
    if n >= 1e3:
        return f"{n / 1e3:.1f}K"
    return str(int(n))


def _walk(node, rows, depth):
    label = node.get("Node Type", "?")
    rel = node.get("Relation Name")
    idx = node.get("Index Name")
    alias = node.get("Alias")
    if rel:
        label += f" on {rel}"
        if alias and alias != rel:
            label += f" {alias}"
    if idx:
        label += f" using {idx}"

    loops = node.get("Actual Loops", 1) or 1
    total_time = node.get("Actual Total Time", 0) * loops
    actual_rows = node.get("Actual Rows", 0) * loops

    children = node.get("Plans", [])
    child_time = sum(
        c.get("Actual Total Time", 0) * (c.get("Actual Loops", 1) or 1)
        for c in children
    )
    self_time = max(0, total_time - child_time)

    rows.append({
        "depth": depth,
        "label": label,
        "is_leaf": len(children) == 0,
        "total_time": total_time,
        "self_time": self_time,
        "actual_rows": actual_rows,
        "loops": loops,
    })
    for c in children:
        _walk(c, rows, depth + 1)


def transform_plan(plan_json):
    data = json.loads(plan_json)
    if isinstance(data, list):
        data = data[0]
    plan = data.get("Plan")
    if not plan:
        return [], 0
    rows = []
    _walk(plan, rows, depth=1)
    exec_time = data.get("Execution Time", 0)
    return rows, exec_time


def _esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _self_pct_color(pct):
    if pct > 40:
        return "#c44e52"
    if pct > 10:
        return "#dd8452"
    return "#55a868"


def _table_html(rows, method, exec_time):
    if not rows:
        return f'<div class="method-empty">{method}: no plan</div>'

    root_total = rows[0]["total_time"] if rows else 1
    if root_total <= 0:
        root_total = 1

    hdr_time = _fmt_time(exec_time) if exec_time else ""
    hdr = f"{method.upper()}"
    if hdr_time:
        hdr += f" &mdash; {hdr_time}"

    trs = []
    for r in rows:
        indent_px = (r["depth"] - 1) * 18
        icon = "&#9675;" if r["is_leaf"] else "&#9660;"
        pct = r["self_time"] / root_total * 100 if root_total else 0
        bar_color = _self_pct_color(pct)
        bar_w = min(pct, 100)

        trs.append(
            f'<tr class="node" data-depth="{r["depth"]}">'
            f'<td class="c-node" style="padding-left:{indent_px}px">'
            f'<span class="ic">{icon}</span> {_esc(r["label"])}</td>'
            f'<td class="c-num">{_fmt_time(r["self_time"])}</td>'
            f'<td class="c-bar"><div class="bar-bg">'
            f'<div class="bar-fg" style="width:{bar_w:.1f}%;background:{bar_color}"></div>'
            f'</div><span class="pct-label">{pct:.0f}%</span></td>'
            f'<td class="c-num">{_fmt_rows(r["actual_rows"])}</td>'
            f'</tr>'
        )

    return (
        f'<div class="method-panel">'
        f'<div class="method-hdr">{hdr}</div>'
        f'<table class="plan-table">'
        f'<thead><tr><th class="h-node">Node</th><th class="h-num">Time</th>'
        f'<th class="h-bar">Self%</th><th class="h-num">Rows</th></tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div>'
    )


CSS = """\
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,'Segoe UI',Roboto,monospace;background:#1a1a2e;color:#e0e0e0;padding:12px}
h1{font-size:1.3rem;margin-bottom:10px;color:#fff}
.tabs{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:8px}
.tabs a{padding:4px 12px;border-radius:4px;background:#2a2a4a;color:#aaa;text-decoration:none;font-size:.85rem;cursor:pointer}
.tabs a.active{background:#4a4a8a;color:#fff}
.sf-tabs{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:8px}
.sf-tabs a{padding:3px 10px;border-radius:3px;background:#1e1e3a;color:#888;text-decoration:none;font-size:.75rem;cursor:pointer}
.sf-tabs a.active{background:#3a3a6a;color:#ccc}
.query-panel{display:none}
.query-panel.active{display:block}
.sf-panel{display:none}
.sf-panel.active{display:block}
.methods{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}
.method-panel{background:#16213e;border-radius:6px;overflow:hidden}
.method-hdr{padding:6px 10px;background:#0f3460;font-size:.8rem;font-weight:600;color:#e0e0e0}
.method-empty{padding:20px;color:#666;font-style:italic}
.plan-table{width:100%;border-collapse:collapse;font-size:.75rem;line-height:1.5}
.plan-table th{text-align:left;padding:3px 6px;border-bottom:1px solid #2a2a4a;color:#888;font-weight:500;font-size:.7rem;position:sticky;top:0;background:#16213e}
.plan-table td{padding:2px 6px;border-bottom:1px solid #1a1a2e}
.plan-table tr.node:hover{background:#2a2a4a}
.c-node{white-space:nowrap;max-width:400px;overflow:hidden;text-overflow:ellipsis}
.c-num{text-align:right;font-family:'SF Mono',Menlo,monospace;white-space:nowrap;color:#bbb;width:60px}
.c-bar{width:100px;position:relative}
.bar-bg{background:#1a1a2e;border-radius:2px;height:12px;overflow:hidden}
.bar-fg{height:100%;border-radius:2px;min-width:1px}
.pct-label{font-size:.65rem;color:#888;margin-left:4px}
.h-node{min-width:200px}
.h-num{width:60px;text-align:right}
.h-bar{width:120px}
.ic{opacity:.5;font-size:.6rem;cursor:pointer;user-select:none}
.ic:hover{opacity:1}
.node.collapsed-child{display:none}
"""

JS = """\
function initTabs(container,panelSel,cls){
  container.querySelectorAll('a').forEach(tab=>{
    tab.addEventListener('click',e=>{
      e.preventDefault();
      container.querySelectorAll('a').forEach(t=>t.classList.remove('active'));
      tab.classList.add('active');
      const val=tab.dataset.val;
      const parent=container.parentElement;
      parent.querySelectorAll(':scope > '+panelSel).forEach(p=>{
        p.classList.toggle('active',p.dataset.val===val);
      });
    });
  });
  container.querySelector('a')?.click();
}
document.querySelectorAll('.tabs').forEach(t=>initTabs(t,'.query-panel','active'));
document.querySelectorAll('.sf-tabs').forEach(t=>initTabs(t,'.sf-panel','active'));
document.querySelectorAll('.ic').forEach(ic=>{
  ic.addEventListener('click',function(e){
    e.stopPropagation();
    const tr=this.closest('tr');
    const d=parseInt(tr.dataset.depth);
    const collapsed=this.innerHTML==='\\u25b6';
    this.innerHTML=collapsed?'\\u25bc':'\\u25b6';
    let sib=tr.nextElementSibling;
    while(sib&&parseInt(sib.dataset.depth)>d){
      if(collapsed)sib.classList.remove('collapsed-child');
      else sib.classList.add('collapsed-child');
      sib=sib.nextElementSibling;
    }
  });
});
"""


def _sf_sort_key(sf_str):
    try:
        return float(sf_str)
    except ValueError:
        return float('inf')


def _load_sf_dir(explain_dir):
    query_plans = defaultdict(dict)
    for fname in sorted(os.listdir(explain_dir)):
        m = re.match(r"(Q\d+)_(\w+)\.json$", fname)
        if not m:
            continue
        query, method = m.group(1), m.group(2)
        with open(os.path.join(explain_dir, fname)) as f:
            query_plans[query][method] = f.read()
    return query_plans


def generate_html(benchmark, sf_dirs):
    all_data = {}
    all_queries = set()
    all_methods = set()
    for sf, dirpath in sf_dirs:
        plans = _load_sf_dir(dirpath)
        if plans:
            all_data[sf] = plans
            for q, methods in plans.items():
                all_queries.add(q)
                all_methods.update(methods.keys())

    if not all_data:
        return None

    sfs = sorted(all_data.keys(), key=_sf_sort_key)
    queries = sorted(all_queries)
    present = sorted(
        [m for m in all_methods if m in METHODS],
        key=lambda x: METHODS.index(x))

    title = benchmark.upper()
    query_tabs = "\n".join(
        f'<a href="#" data-val="{q}">{q}</a>' for q in queries)

    query_panels = []
    for q in queries:
        sf_tabs = "\n".join(
            f'<a href="#" data-val="{sf}">SF {sf}</a>' for sf in sfs)

        sf_panels = []
        for sf in sfs:
            methods_html = []
            plans = all_data.get(sf, {}).get(q, {})
            for method in present:
                pjson = plans.get(method)
                if pjson:
                    rows, exec_time = transform_plan(pjson)
                    methods_html.append(_table_html(rows, method, exec_time))
                else:
                    methods_html.append(
                        f'<div class="method-empty">{method}: no plan</div>')
            sf_panels.append(
                f'<div class="sf-panel" data-val="{sf}">'
                f'<div class="methods">{"".join(methods_html)}</div></div>')

        query_panels.append(
            f'<div class="query-panel" data-val="{q}">'
            f'<div class="sf-tabs">{sf_tabs}</div>'
            f'{"".join(sf_panels)}</div>')

    html = (
        f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
        f'<title>{title}</title><style>{CSS}</style></head><body>'
        f'<h1>{title}</h1><div class="tabs">{query_tabs}</div>'
        f'{"".join(query_panels)}<script>{JS}</script></body></html>'
    )

    out_path = os.path.join(EXPLAIN_ROOT, f"{benchmark}.html")
    with open(out_path, "w") as f:
        f.write(html)
    return out_path


def main():
    all_dirs = sorted(
        d for d in os.listdir(EXPLAIN_ROOT)
        if os.path.isdir(os.path.join(EXPLAIN_ROOT, d))
    )

    benchmarks = defaultdict(list)
    for d in all_dirs:
        m = re.match(r"(.+?)_sf(.+)$", d)
        if m:
            bench, sf = m.group(1), m.group(2)
            benchmarks[bench].append((sf, os.path.join(EXPLAIN_ROOT, d)))

    targets = sys.argv[1:]
    if targets:
        benchmarks = {k: v for k, v in benchmarks.items() if k in targets}

    for bench, sf_dirs in sorted(benchmarks.items()):
        out = generate_html(bench, sf_dirs)
        if out:
            print(f"generated {out}")
        else:
            print(f"skip {bench} (no JSON plans)")


if __name__ == "__main__":
    main()
