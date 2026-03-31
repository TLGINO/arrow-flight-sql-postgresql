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


def _sf_sort_key(sf_str):
    """Sort scale factors numerically."""
    try:
        return float(sf_str)
    except ValueError:
        return float('inf')


def _load_sf_dir(explain_dir):
    """Load all query plans from one SF directory."""
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
    """Generate single HTML for a benchmark with all its scale factors."""
    # sf_dirs: list of (sf_label, dir_path)
    # Collect: sf -> query -> method -> json
    all_data = {}  # sf -> {query -> {method -> json}}
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
    present = sorted(all_methods,
                     key=lambda x: METHODS.index(x) if x in METHODS else 99)

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
                    rows = transform_plan(pjson)
                    methods_html.append(_tree_html(rows, method))
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
    # Group SF dirs by benchmark: tpch_sf1 -> benchmark=tpch, sf=1
    all_dirs = sorted(
        d for d in os.listdir(EXPLAIN_ROOT)
        if os.path.isdir(os.path.join(EXPLAIN_ROOT, d))
    )

    # Parse benchmark_sfN.NN pattern
    benchmarks = defaultdict(list)  # benchmark -> [(sf, path)]
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
