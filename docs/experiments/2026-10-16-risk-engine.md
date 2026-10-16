# Risk engine tuning

- Tuned: 2026-10-16T23:10:42+00:00 on `data/training/synthetic` (time split; weights and thresholds chosen on validation only); holdout `data/generated`
- Models: ml `fraud_lgbm:20261016230712-97be2705`, anomaly `anomaly_iforest:20261016230716-97be2705`, graph `graph_lgbm:20261016230721-f3e4f4f4`
- **Weights:** ml 0.5, anomaly 0.1, graph 0.4 (signals calibrated to P(fraud))
- **Thresholds:** MEDIUM ≥ 8 (best F2 on validation), HIGH = alert ≥ 23 (best F1); rules floor `takeover_pattern` at HIGH and `burst_on_shared_device` at MEDIUM

## Combiners compared (PR-AUC)

| combiner | valid | test | holdout |
| --- | --- | --- | --- |
| anomaly alone | 0.2665 | 0.2335 | 0.5052 |
| graph alone | 0.2236 | 0.1407 | 0.1975 |
| ml alone | 0.708 | 0.747 | 0.8892 |
| spec weights 0.4/0.3/0.3, raw scores | 0.687 | 0.7173 | 0.8793 |
| spec weights, calibrated | 0.813 | 0.8384 | 0.9267 |
| tuned weights, calibrated (engine) | 0.834 | 0.8506 | 0.932 |
| stacking (logistic regression, reference) | 0.8624 | 0.876 | 0.9309 |

## Contribution of each signal (engine PR-AUC without it, weights renormalised)

| variant | valid | test | holdout |
| --- | --- | --- | --- |
| all signals | 0.834 | 0.8506 | 0.932 |
| without anomaly | 0.8308 | 0.8465 | 0.9305 |
| without graph | 0.7376 | 0.7683 | 0.9013 |
| without ml | 0.4188 | 0.2754 | 0.4466 |

## Engine at its thresholds

| set | alert precision | alert recall | alert F1 | alert rate | review (≥ MEDIUM) precision | review recall | rule hits HIGH / MEDIUM |
| --- | --- | --- | --- | --- | --- | --- | --- |
| valid | 0.8 | 0.7097 | 0.7521 | 0.218% | 0.5286 | 0.8952 | 9 / 12 |
| test | 0.7426 | 0.7576 | 0.75 | 0.200% | 0.4972 | 0.8889 | 8 / 9 |
| holdout | 0.7918 | 0.8755 | 0.8315 | 0.449% | 0.3066 | 0.9962 | 28 / 52 |

## Alert recall / false-positive rate by scenario (holdout)

| scenario | n | recall / FPR |
| --- | --- | --- |
| account_takeover | 24 | recall 1.0 |
| account_takeover_local | 9 | recall 0.7778 |
| app_scam | 18 | recall 0.2222 |
| card_testing | 112 | recall 0.9464 |
| large_purchase | 272 | FPR 0.0368 |
| legit_burst | 1418 | FPR 0.0028 |
| mule_cash_out | 24 | recall 1.0 |
| mule_slow_drip | 78 | recall 0.859 |
| new_device | 298 | FPR 0.0 |
| new_payee | 75 | FPR 0.0133 |
| normal | 62028 | FPR 0.0007 |
| travel | 936 | FPR 0.0032 |
