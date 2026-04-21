"""DataLink UI package — Streamlit Control Tower + helpers.

The Control Tower at http://localhost:8000 is the single executive URL that
wraps the full medallion pipeline: upload → Bronze → Silver → Gold → ops DBs,
with live row counts, GX + CrewAI status, and deep links to every other
tool in the stack (Airflow, pgAdmin, Adminer, Grafana, GX Data Docs,
Webhook Inbox, Filebrowser, Portainer).
"""
