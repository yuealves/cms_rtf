# docs 索引 — cms_rtf

## 使用（先看这个）

- [how-to.md](how-to.md)：操作手册。新人从这里开始：怎么跑一天、产出物是什么、
  怎么对拍、故障排查。

## 设计

- [design.md](design.md)：批处理架构与对 light_rtf v2 的逐字口径移植
  （只实现交易所网格的原因、CMS 事件映射、流水线、已知的批处理近似边界）。

## 验证记录

- [validation/20261009-field-mapping.md](validation/20261009-field-mapping.md)：
  CMS parquet 字段 → canonical 事件映射的逐条实证（含分片时间范围陷阱）。
- [validation/20261009-compare-results.md](validation/20261009-compare-results.md)：
  全天对拍 ht_idc 参考的分组统计与两个例外列的定性。
