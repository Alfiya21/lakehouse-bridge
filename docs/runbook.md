# Runbook

## Reconciliation failed
If reconciliation reports a mismatch, compare row counts and per-column null counts in data/reports/reconciliation.json. Re-run the migration for the single table with the mismatch after checking the source partition.

## Quarantine growing
The quarantine table holds orders rejected by silver validation. A quarantine ratio above 5 percent of silver orders raises a warning. Check the reason column: NON_POSITIVE_AMOUNT usually means an upstream sign error, ORPHAN_CUSTOMER means customers arrived late.

## Who may see PII
Only the admin role sees full_name, email and phone. Analyst roles see masked values. analyst_apac additionally sees only region APAC rows.

## Streaming replays
Payment events can be replayed by the source. The streaming job de-duplicates on event_id, so re-running does not change the silver payments table.
