#!/usr/bin/env python3
"""Generate pev2 (Dalibo) side-by-side plan comparison HTML.

Arrow vs Substrait plans rendered with Dalibo's pev2 visualizer.

Usage:
    python3 substrait_test/plot_explain_dalibo.py                # all benchmarks
    python3 substrait_test/plot_explain_dalibo.py tpch           # specific benchmark
"""
import json
import os
import re
import sys
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXPLAIN_ROOT = os.path.join(SCRIPT_DIR, "explain")
METHODS = ["arrow", "substrait"]


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
        if method not in METHODS:
            continue
        with open(os.path.join(explain_dir, fname)) as f:
            query_plans[query][method] = f.read()
    return query_plans


def _load_queries(benchmark):
    queries_dir = os.path.join(SCRIPT_DIR, benchmark, "queries", "pgsql")
    result = {}
    if not os.path.isdir(queries_dir):
        return result
    for fname in sorted(os.listdir(queries_dir)):
        m = re.match(r"(\d+)\.sql$", fname)
        if m:
            key = f"Q{m.group(1)}"
            with open(os.path.join(queries_dir, fname)) as f:
                result[key] = f.read()
    return result


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
.split-container{display:flex;gap:8px;height:calc(100vh - 140px)}
.split-pane{flex:1 1 50%;min-width:0;display:flex;flex-direction:column;border-radius:6px;overflow:hidden}
.pane-header{padding:6px 10px;background:#0f3460;font-size:.85rem;font-weight:600;color:#e0e0e0;flex-shrink:0;display:flex;justify-content:space-between;align-items:center}
.pane-body{flex:1;overflow:hidden;background:#16213e}
.pane-body iframe{width:100%;height:100%;border:none}
.pane-empty{display:flex;align-items:center;justify-content:center;color:#666;font-style:italic;background:#16213e}
.fs-btn{background:none;border:1px solid #3a3a6a;color:#aaa;cursor:pointer;font-size:.7rem;padding:2px 8px;border-radius:3px}
.fs-btn:hover{background:#2a2a4a;color:#fff}
.split-pane.fullscreen{position:fixed;top:0;left:0;width:100vw;height:100vh;z-index:9999;border-radius:0;flex-basis:100%}
"""

JS = """\
const mounted=new Set();
function mountPev2(el,planJson,queryText){
  const id=el.id;
  if(mounted.has(id))return;
  mounted.add(id);
  const iframe=document.createElement('iframe');
  el.appendChild(iframe);
  const d=iframe.contentDocument;
  const ePlan=JSON.stringify(planJson);
  const eQuery=JSON.stringify(queryText||'');
  d.open();
  d.write('<!DOCTYPE html><html data-bs-theme="dark"><head>'+
    '<link rel="stylesheet" href="https://unpkg.com/bootstrap@5.3/dist/css/bootstrap.min.css">'+
    '<link rel="stylesheet" href="https://unpkg.com/pev2/dist/pev2.css">'+
    '<style>body{background:#16213e;margin:0}</style>'+
    '</head><body><div id="app" class="p-2"><pev2 :plan-source="plan" :plan-query="query"/></div>'+
    '<script src="https://unpkg.com/vue@3/dist/vue.global.prod.js"><\\/script>'+
    '<script src="https://unpkg.com/pev2/dist/pev2.umd.js"><\\/script>'+
    '<script>const app=Vue.createApp({data(){return{plan:'+ePlan+',query:'+eQuery+'}}});'+
    'app.component("pev2",pev2.Plan);app.mount("#app");<\\/script>'+
    '</body></html>');
  d.close();
}
function mountVisible(){
  const qp=document.querySelector('.query-panel.active');
  if(!qp)return;
  const sp=qp.querySelector('.sf-panel.active');
  if(!sp)return;
  sp.querySelectorAll('[data-pev2]').forEach(el=>{
    const key=el.dataset.pev2;
    const pj=PLAN_DATA[key];
    if(!pj)return;
    const q=key.split('_')[0];
    mountPev2(el,pj,QUERY_SQL[q]||'');
  });
}
function initTabs(container,panelSel){
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
      mountVisible();
    });
  });
  container.querySelector('a')?.click();
}
document.querySelectorAll('.tabs').forEach(t=>initTabs(t,'.query-panel'));
document.querySelectorAll('.sf-tabs').forEach(t=>initTabs(t,'.sf-panel'));
document.querySelectorAll('.fs-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    const pane=btn.closest('.split-pane');
    const on=pane.classList.toggle('fullscreen');
    btn.textContent=on?'\\u2716':'\\u26F6';
  });
});
document.addEventListener('keydown',e=>{
  if(e.key==='Escape'){
    const fs=document.querySelector('.split-pane.fullscreen');
    if(fs){fs.classList.remove('fullscreen');fs.querySelector('.fs-btn').textContent='\\u26F6';}
  }
});
"""


def _esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def generate_html(benchmark, sf_dirs):
    all_data = {}
    all_queries = set()
    for sf, dirpath in sf_dirs:
        plans = _load_sf_dir(dirpath)
        if plans:
            all_data[sf] = plans
            all_queries.update(plans.keys())

    if not all_data:
        return None

    sfs = sorted(all_data.keys(), key=_sf_sort_key)
    queries = sorted(all_queries)
    sql_queries = _load_queries(benchmark)

    # build JS data blobs
    plan_data = {}
    for sf in sfs:
        for q in queries:
            for method in METHODS:
                raw = all_data.get(sf, {}).get(q, {}).get(method)
                if raw:
                    plan_data[f"{q}_{sf}_{method}"] = raw

    plan_blob = json.dumps(plan_data).replace("</", "<\\/")
    sql_blob = json.dumps(sql_queries).replace("</", "<\\/")

    title = benchmark.upper()
    query_tabs = "\n".join(
        f'<a href="#" data-val="{q}">{q}</a>' for q in queries)

    query_panels = []
    for q in queries:
        sf_tabs = "\n".join(
            f'<a href="#" data-val="{sf}">SF {sf}</a>' for sf in sfs)

        sf_panels = []
        for sf in sfs:
            panes = []
            for method in METHODS:
                key = f"{q}_{sf}_{method}"
                eid = f"pev2-{q}-{sf}-{method}"
                if key in plan_data:
                    inner = f'<div class="pane-body" id="{eid}" data-pev2="{key}"></div>'
                else:
                    inner = f'<div class="pane-body pane-empty">{method}: no plan</div>'
                panes.append(
                    f'<div class="split-pane">'
                    f'<div class="pane-header"><span>{method.upper()}</span>'
                    f'<button class="fs-btn">\u26f6</button></div>'
                    f'{inner}</div>')

            sf_panels.append(
                f'<div class="sf-panel" data-val="{sf}">'
                f'<div class="split-container">{"".join(panes)}</div></div>')

        query_panels.append(
            f'<div class="query-panel" data-val="{q}">'
            f'<div class="sf-tabs">{sf_tabs}</div>'
            f'{"".join(sf_panels)}</div>')

    html = (
        f'<!DOCTYPE html><html lang="en" data-bs-theme="dark"><head><meta charset="utf-8">'
        f'<title>{title} Dalibo</title>'
        f'<link rel="stylesheet" href="https://unpkg.com/bootstrap@5.3/dist/css/bootstrap.min.css">'
        f'<link rel="stylesheet" href="https://unpkg.com/pev2/dist/pev2.css">'
        f'<style>{CSS}</style></head><body>'
        f'<h1>{_esc(title)} &mdash; Plan Visualizer</h1>'
        f'<div class="tabs">{query_tabs}</div>'
        f'{"".join(query_panels)}'
        f'<script>const PLAN_DATA={plan_blob};</script>'
        f'<script>const QUERY_SQL={sql_blob};</script>'
        f'<script>{JS}</script></body></html>'
    )

    out_path = os.path.join(EXPLAIN_ROOT, f"{benchmark}_dalibo.html")
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
            print(f"skip {bench} (no plans)")


if __name__ == "__main__":
    main()
