# AGENTS.md — cms_rtf 协作规范

## 这个项目是什么

招商逐笔行情（/data/tickl2_data_cms）的指数 500ms 因子离线批处理。
语义基准 = light_rtf v2 的 index_500ms family（移植其簿内核/流量状态机/聚合），
**不要把语义"创新"带进来**——任何口径改动必须先对照 light_rtf 源文件并更新
docs/design/index_500ms-batch.md 的差异清单。

## 硬性红线

1. **106 列目录顺序不可改**（`python/cms_rtf/catalog.py`），它是对拍契约。
2. 内核（C++）与 Python 之间的二进制布局由 `include/cms_rtf/events.h` 的
   static_assert 和 codec.py 的 dtype 双重守护；两边必须同步改。
3. **不修改线上组件**：不碰 cms_market_gateway 的归档/接收，不碰 ht_idc 的任何文件；
   参考数据（权重/阈值/基准 arrow）只读拷贝。
4. 所有"魔法数字"（会话边界、覆盖门 0.8、1.001/0.999 补价、动态阈值参数）
   都要能指回 light_rtf 源文件行号；新发现写进 docs/validation/。
5. 提交前：ctest 全过 + `python3 -m unittest discover -s tests/python` 全过。
6. 浮点确定性纪律：聚合按 symbol 升序、Float32 量化点在 light_rtf 一致的位置
   （聚合后量化、环内 Float64 求和不做减法）。

## 常用命令

```bash
cmake --build build -j && ctest --test-dir build --output-on-failure
PYTHONPATH=python python3 -m unittest discover -s tests/python -v
bash scripts/run_20261009.sh   # 全量日跑 + 对拍
```
