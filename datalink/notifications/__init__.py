"""Slack/Teams ChatOps integration — Phase 16.4 (Wave 4 #20).

One outbound channel for: DAG failures, anomaly events, AI-proposal-pending
reviews, drift events. Inbound action buttons via Slack Block Kit (approve/
reject/investigate).

Public:
  * ``post_message(channel, text, blocks=None)`` — generic webhook send
  * ``notify_dag_failed(dag_id, run_id, task_id)``
  * ``notify_anomaly(event)``
  * ``notify_proposal_pending(proposal_id)``
  * ``test_connection()`` — used by the UI page to validate webhook config
"""

from datalink.notifications.chatops import (  # noqa: F401
    notify_anomaly,
    notify_dag_failed,
    notify_proposal_pending,
    post_message,
    test_connection,
)
