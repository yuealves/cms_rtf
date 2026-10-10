"""cms_rtf：CMS 逐笔行情的指数 500ms 因子离线批处理。

模块：
  catalog    106 列目录与网格常量（对拍契约）
  refdata    指数权重 + 动态阈值（fail-closed）
  codec      CMS parquet → 内核事件流
  aggregate  内核输出 → 106 列指数因子 → Arrow IPC stream
  run_day    日级批处理驱动
  compare    与参考输出对拍
"""
