#!/bin/bash
# 用 2026-10-09 招商数据生产 index_500ms 因子并与 ht_idc 参考对拍。
# 参考数据（权重/阈值/基准 arrow）来自 /tmp/cms_rtf_ref/（拉取过程见 docs/validation/）。
set -euo pipefail
cd "$(dirname "$0")/.."

REF=/tmp/cms_rtf_ref
mkdir -p "$REF/weights"
# 权重目录布局 = csi_{50,300,500,1000}/YYYY-MM-DD.parquet
for i in 50 300 500 1000; do
  mkdir -p "$REF/weights/csi_$i"
  ln -sf "$REF/weight_csi_${i}_2026-10-09.parquet" "$REF/weights/csi_$i/2026-10-09.parquet"
done
mkdir -p "$REF/thresholds"
ln -sf "$REF/threshold_20261009.csv" "$REF/thresholds/threshold_20261009.csv"

PYTHONPATH=python /usr/bin/python3 -m cms_rtf.run_day \
  --day 20261009 \
  --data-root /data/tickl2_data_cms \
  --weights-dir "$REF/weights" \
  --threshold-dir "$REF/thresholds" \
  --out "$REF/out" \
  --compare "$REF/index_500ms_exch.arrow"
