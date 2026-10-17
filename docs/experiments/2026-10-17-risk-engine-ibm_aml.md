# Risk engine tuning

- Tuned: 2026-10-17T05:47:51+00:00 on `data/processed/ibm_aml` (time split; weights and thresholds chosen on validation only); holdout `None`
- Models: ml `fraud_lgbm:20261017054352-22c237be`, anomaly `anomaly_iforest:20261017054407-22c237be`, graph `graph_lgbm:20261017002114-b656ed9b`
- **Weights:** ml 0.4, anomaly 0.2, graph 0.4 (signals calibrated to P(fraud))
- **Thresholds:** MEDIUM ≥ 2 (best F2 on validation), HIGH = alert ≥ 5 (best F1); rules floor `takeover_pattern` at HIGH and `burst_on_shared_device` at MEDIUM

## Combiners compared (PR-AUC)

| combiner | valid | test |
| --- | --- | --- |
| anomaly alone | 0.0016 | 0.0019 |
| graph alone | 0.041 | 0.0516 |
| ml alone | 0.1413 | 0.1317 |
| spec weights 0.4/0.3/0.3, raw scores | 0.0556 | 0.0567 |
| spec weights, calibrated | 0.199 | 0.1931 |
| tuned weights, calibrated (engine) | 0.2014 | 0.1969 |
| stacking (logistic regression, reference) | 0.2608 | 0.2528 |

## Contribution of each signal (engine PR-AUC without it, weights renormalised)

| variant | valid | test |
| --- | --- | --- |
| all signals | 0.2014 | 0.1969 |
| without anomaly | 0.2013 | 0.1969 |
| without graph | 0.1473 | 0.1379 |
| without ml | 0.0463 | 0.0569 |

## Engine at its thresholds

| set | alert precision | alert recall | alert F1 | alert rate | review (≥ MEDIUM) precision | review recall | rule hits HIGH / MEDIUM |
| --- | --- | --- | --- | --- | --- | --- | --- |
| valid | 0.2552 | 0.3176 | 0.283 | 0.122% | 0.0633 | 0.6201 | 0 / 0 |
| test | 0.2176 | 0.3396 | 0.2652 | 0.198% | 0.0635 | 0.6464 | 0 / 0 |

## Alert recall / false-positive rate by scenario (test)

| scenario | n | recall / FPR |
| --- | --- | --- |
| bipartite | 39 | recall 0.4615 |
| cycle | 46 | recall 0.5217 |
| fan_in | 55 | recall 0.5455 |
| fan_out | 60 | recall 0.6167 |
| gather_scatter | 116 | recall 0.5 |
| laundering_unattributed | 320 | recall 0.0406 |
| normal | 671400 | FPR 0.0016 |
| random | 39 | recall 0.5385 |
| scatter_gather | 102 | recall 0.4314 |
| stack | 77 | recall 0.5844 |
