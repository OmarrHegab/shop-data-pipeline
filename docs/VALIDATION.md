# Validation evidence

These results describe the demonstrated local dataset. They are not benchmark
claims or assertions about every possible input.

## Confirmed milestones

| Check | Observed result |
|---|---|
| Initial pipeline | 10 clean orders, 19 units, EUR 1,663.10 |
| Repeat unchanged input | Successful rerun with unchanged totals, confirmed by the operator |
| First complete Airflow run | Eight tasks succeeded on their first attempt; 3m 1s |
| Add five orders | 15 clean orders, 28 units, EUR 2,337.20 |
| SQL reconciliation | `POSTGRES VERIFICATION PASSED` |
| Streamlit integration | Screenshot displays the same totals; AOV EUR 155.81 |
| Five-minute schedule | Schedule loaded and automatic processing confirmed by the operator |

## Additional-order reconciliation

| Metric | Before | New batch | After |
|---|---:|---:|---:|
| Orders | 10 | 5 | 15 |
| Units | 19 | 9 | 28 |
| Revenue EUR | 1663.10 | 674.10 | 2337.20 |

The five-order batch contained two monitors (EUR 399.80), four keyboard units
(EUR 199.60), and three mice (EUR 74.70).

| Country | Orders | Units | Revenue EUR |
|---|---:|---:|---:|
| Egypt | 6 | 9 | 1004.10 |
| France | 4 | 6 | 809.40 |
| Germany | 5 | 13 | 523.70 |

Gold snapshot observed after that load: `20261015T165749Z-051e3f8b`.
There were 2 daily rows, 5 date/country rows, and 7 date/product rows.
The country table above aggregates dates for display.

## Duplicate and invalid-event test — verification pending

Two test records were successfully delivered to Kafka:

- An exact replay of one previously loaded Monitor order.
- A new event/order ID with quantity zero, which violates Silver validation.

Both deliveries were confirmed in partition 0 at offsets 6 and 7. This proves
publication only. The final Silver and PostgreSQL logs for this test have not yet
been recorded in this repository.

Expected after processing the two records once, with no additional events:

```text
INPUT ROWS: 17
CLEAN ORDERS: 15
REJECTED ROWS: 1
DUPLICATE ROWS: 1
TOTAL ORDER VALUE EUR: 2337.20
SILVER VERIFICATION PASSED
```

PostgreSQL should remain at 15 orders, 28 units and EUR 2,337.20. Capture the
`verify_silver` and `verify_postgres` logs from the completed run before marking
this test passed. Repeated publication adds more raw/audit rows.

## Failure recovery — not yet demonstrated

Airflow task retries are configured, but no controlled failure-and-recovery
experiment has been recorded. A future test should use isolated data and document
the injected failure, retry behavior, restored service, and unchanged final totals.

## Screenshot provenance

- `images/dashboard.png`: actual dashboard after the five-order increment.
- `images/airflow-success.png`: actual initial successful eight-task run.
- `images/airflow-schedule.png`: actual five-minute schedule with a run in progress.

The screenshots represent different milestones, not the same single run.
