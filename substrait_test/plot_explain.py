#!/usr/bin/env python3
"""Generate plan-structure comparison HTML from JSON explain files.

Usage:
    python3 substrait_test/plot_explain.py                    # all explain dirs
    python3 substrait_test/plot_explain.py tpch_sf0.01        # specific dir
"""
import json
import os
import re
import sys
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXPLAIN_ROOT = os.path.join(SCRIPT_DIR, "explain")
METHODS = ["pgsql", "arrow", "substrait"]


def _walk(node, rows, depth, in_parallel=False):
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
    workers = node.get("Workers Launched", node.get("Workers Planned"))
    if workers is not None:
        label += f" (workers: {workers})"

    is_gather = label.startswith("Gather")
    children = node.get("Plans", [])
    rows.append({"depth": depth, "label": label, "is_leaf": len(children) == 0,
                 "parallel": in_parallel, "is_gather": is_gather})
    for c in children:
        _walk(c, rows, depth + 1,
              in_parallel=(True if is_gather else in_parallel))


def transform_plan(plan_json):
    data = json.loads(plan_json)
    if isinstance(data, list):
        data = data[0]
    plan = data.get("Plan")
    if not plan:
        return []
    rows = []
    _walk(plan, rows, depth=1, in_parallel=False)
    return rows


def _esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _tree_html(rows, method):
    if not rows:
        return f'<div class="method-empty">{method}: no plan</div>'
    lines = []
    for r in rows:
        indent = "\u00a0" * ((r["depth"] - 1) * 3)
        icon = "\u25cb" if r["is_leaf"] else "\u25bc"
        badge = ""
        if r.get("is_gather"):
            badge = ' <span class="badge-g">G</span>'
        elif r.get("parallel"):
            badge = ' <span class="badge-w">W</span>'
        lines.append(
            f'<div class="node" data-depth="{r["depth"]}">'
            f'{indent}<span class="ic">{icon}</span> {_esc(r["label"])}{badge}</div>'
        )
    return (
        f'<div class="method-panel">'
        f'<div class="method-hdr">{method.upper()}</div>'
        f'<div class="tree">{"".join(lines)}</div>'
        f'</div>'
    )


CSS = """\
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,'Segoe UI',Roboto,monospace;background:#1a1a2e;color:#e0e0e0;padding:12px}
h1{font-size:1.3rem;margin-bottom:10px;color:#fff}
.tabs{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:12px}
.tabs a{padding:4px 12px;border-radius:4px;background:#2a2a4a;color:#aaa;text-decoration:none;font-size:.85rem;cursor:pointer}
.tabs a.active{background:#4a4a8a;color:#fff}
.query-panel{display:none}
.query-panel.active{display:block}
.methods{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:8px}
.method-panel{background:#16213e;border-radius:6px;overflow:hidden}
.method-hdr{padding:6px 10px;background:#0f3460;font-size:.8rem;font-weight:600;color:#e0e0e0}
.method-empty{padding:20px;color:#666;font-style:italic}
.tree{padding:6px 10px;font-size:.78rem;line-height:1.6}
.node{white-space:nowrap}
.node:hover{background:#2a2a4a;border-radius:3px}
.ic{opacity:.5;font-size:.65rem;cursor:pointer;user-select:none}
.ic:hover{opacity:1}
.node.collapsed-child{display:none}
.badge-w{color:#5bc0de;font-size:.65rem;margin-left:4px}
.badge-g{color:#f0ad4e;font-size:.65rem;margin-left:4px}
"""

JS = """\
document.querySelectorAll('.tabs a').forEach(tab=>{
  tab.addEventListener('click',e=>{
    e.preventDefault();
    document.querySelectorAll('.tabs a').forEach(t=>t.classList.remove('active'));
    tab.classList.add('active');
    const q=tab.dataset.query;
    document.querySelectorAll('.query-panel').forEach(p=>{
      p.classList.toggle('active',p.dataset.query===q);
    });
  });
});
document.querySelector('.tabs a')?.click();
document.querySelectorAll('.ic').forEach(ic=>{
  ic.addEventListener('click',function(e){
    e.stopPropagation();
    const div=this.closest('.node');
    const d=parseInt(div.dataset.depth);
    const collapsed=this.textContent==='\\u25b6';
    this.textContent=collapsed?'\\u25bc':'\\u25b6';
    let sib=div.nextElementSibling;
    while(sib&&parseInt(sib.dataset.depth)>d){
      if(collapsed)sib.classList.remove('collapsed-child');
      else sib.classList.add('collapsed-child');
      sib=sib.nextElementSibling;
    }
  });
});
"""


def generate_html(explain_dir):
    query_plans = defaultdict(dict)
    for fname in sorted(os.listdir(explain_dir)):
        m = re.match(r"(Q\d+)_(\w+)\.json$", fname)
        if not m:
            continue
        query, method = m.group(1), m.group(2)
        with open(os.path.join(explain_dir, fname)) as f:
            query_plans[query][method] = f.read()

    if not query_plans:
        return None

    queries = sorted(query_plans.keys())
    present = sorted(
        {m for plans in query_plans.values() for m in plans},
        key=lambda x: METHODS.index(x) if x in METHODS else 99,
    )

    dirname = os.path.basename(explain_dir)
    title = dirname.replace("_", " ").upper()

    tabs = "\n".join(f'<a href="#" data-query="{q}">{q}</a>' for q in queries)

    panels = []
    for q in queries:
        methods_html = []
        for method in present:
            pjson = query_plans[q].get(method)
            if pjson:
                rows = transform_plan(pjson)
                methods_html.append(_tree_html(rows, method))
            else:
                methods_html.append(f'<div class="method-empty">{method}: no plan</div>')
        panels.append(
            f'<div class="query-panel" data-query="{q}">'
            f'<div class="methods">{"".join(methods_html)}</div></div>'
        )

    html = (
        f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
        f'<title>{title}</title><style>{CSS}</style></head><body>'
        f'<h1>{title}</h1><div class="tabs">{tabs}</div>'
        f'{"".join(panels)}<script>{JS}</script></body></html>'
    )

    out_path = os.path.join(EXPLAIN_ROOT, f"{dirname}.html")
    with open(out_path, "w") as f:
        f.write(html)
    return out_path


def main():
    targets = sys.argv[1:]
    if targets:
        dirs = [os.path.join(EXPLAIN_ROOT, t) for t in targets]
    else:
        dirs = sorted(
            os.path.join(EXPLAIN_ROOT, d)
            for d in os.listdir(EXPLAIN_ROOT)
            if os.path.isdir(os.path.join(EXPLAIN_ROOT, d))
        )
    for d in dirs:
        if not os.path.isdir(d):
            print(f"skip {d} (not a directory)")
            continue
        out = generate_html(d)
        if out:
            print(f"generated {out}")
        else:
            print(f"skip {os.path.basename(d)} (no JSON plans)")


if __name__ == "__main__":
    main()
