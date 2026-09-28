# Visuals Plan — daggate (Gumroad)

All 1280x720 (Gumroad cover ratio), dark terminal aesthetic, mono font. No Airflow/ASF logos.

## Image 1 — Hero (must explain product in <3s)
- Headline: "Block broken DAGs before they merge"
- Sub: "16 production rules · zero dependencies · one file"
- Composition: left = red terminal output of daggate on bad DAG (real screenshot of `python daggate.py examples/dags/bad_etl_dag.py`); right = green "0 errors" run. Big X/check icons.
- Objection addressed: "what does it do?"
- Alt text: Terminal showing daggate failing a bad Airflow DAG and passing a good one.

## Image 2 — Rule table
- Headline: "What it catches"
- Content: the 16-rule table (code, severity, check) as clean graphic.
- Objection: "is this just 3 trivial checks?"
- Alt: Table of 16 daggate rules.

## Image 3 — CI screenshot
- Headline: "Fails the build, annotates the PR"
- Content: GitHub Actions annotation screenshot (`--format github` output rendered in a PR).
- Objection: "will it fit my workflow?"
- Alt: GitHub pull request annotations produced by daggate.

## Image 4 — Kit contents
- Headline: "Not just a script"
- Content: file-tree graphic: linter, 30 tests, checklist, GH Actions, Jenkins, pre-commit, FAQ.
- Objection: "is $39 worth it vs writing it myself?" Sub-caption: "~2 engineer-days if you build it yourself."
- Alt: Contents of the daggate kit.

## Image 5 — Checklist preview
- Headline: "Plus the review checklist your team will actually use"
- Content: cropped screenshot of PRODUCTION-READINESS-CHECKLIST.md sections 1-3.
- Alt: Production readiness checklist excerpt.

Video storyboard (optional, 45s screen capture): run on bad DAG (red) -> fix 3 lines -> run green -> push -> GH Action passes.
