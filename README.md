# Multi-agent AI sistem za upravljanje energijom u zgradama

Ovaj projekat prikazuje kompletan tok od simulacije podataka, preko AI pipeline-a
(monitoring, predikcija, optimizacija, odluke), do vizuelizacije u Streamlit
dashboardu.

## Istaknuto
- Multi-agent workflow zasnovan na LangGraph (monitor -> predikcija -> optimizacija -> odluka)
- SQLite model podataka (zgrade, stanovi, senzori, anomalije, odluke)
- Feature extraction + clustering za analitiku po jedinicama
- Streamlit dashboard za pregled, analitiku i optimizacione planove

## Struktura projekta
- `agents/` AI agenti (data monitor, prediction, optimization, decision)
- `workflow/` LangGraph workflow i schema stanja
- `scripts/` CLI skripte za DB init, simulaciju i pokretanje pipeline-a
- `db/` SQLite schema i lokacija baze
- `energy-dashboard/` Streamlit UI i stranice
- `utils/` DB helperi i validacija
- `tests/` osnovni testovi/backtest

## Preduvjeti
- Python 3.9+
- SQLite (dolazi uz Python)

## Instalacija
```bash
pip install -r requirements.txt
```

## Brzi start
1) Kreiraj shemu baze:
```bash
python scripts/init_db.py
```

2) Generisi demo podatke (simulacija):
```bash
python scripts/data.py
```

3) Pokreni pipeline (LangGraph workflow):
```bash
python scripts/run_langgraph.py
```

4) Pokreni dashboard:
```bash
streamlit run energy-dashboard/Dashboard.py
```

## Dodatne skripte
- Full pipeline backfill kroz zgrade:
```bash
python scripts/run_decision.py
```

- Sedmicna analiza:
```bash
python scripts/run_weekly_analysis.py
```

- Trening i testiranje modela:
```bash
python scripts/train_models.py
python scripts/test_model.py
```

## Baza podataka
- SQLite fajl: `db/smartbuilding.db`
- Schema: `db/init_db.sql`
- Generator podataka: `scripts/data.py`

## Testovi
```bash
python -m unittest tests/test_backtest.py
```

