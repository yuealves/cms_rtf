# 招商机源码部署与实时运行

2026-10-13 用户指定：参考华泰 `scripts/sync_online_bundle.sh`，传 Git 增量，
在招商机编译。链路为既有 `cms_gateway → journal → cms_index500ms_mt`，
不再经过要求最终 END 的离线 exporter。

首次原生部署与真实全天 journal 的验证结果见
[20261013 验收记录](validation/20261013-live-native-deploy.md)：部署代码 `9a4c6dc`，
招商机 8 worker 全天追赶 262.123 秒，C++ 峰值 RSS 3.056 GiB。该次用 10/09
参考文件读取 10/13 journal，验证范围及自动启动状态以记录中的说明为准。

## 增量部署

```bash
# 本开发机；默认经 gy_dev 的 127.0.0.1:15423 隧道连接招商机。
python3 scripts/push_online.py --dry-run
python3 scripts/push_online.py --build-jobs 2
# 如果就在 gy_dev 上执行，添加 --via ''。
```

必须先提交，发送当前 HEAD 而非写死 main。首次 bundle 包含历史；后续以远端
`repo.git/refs/heads/source` 为基础只发送缺少的提交。SHA256 校验、Git 前置校验、
只允许快进；不 hard reset 修改过的 checkout。构建失败时 source ref 可以已推进，
但 current 保持旧版本；修复后重试同一 commit 会重新执行构建检查。

远端布局：

```
/data/cms/rtf/
  repo.git/                  独立裸仓库
  sources/<commit>/build/    每版本原生构建，保留已运行版本
  current -> sources/<commit>
  build-tools -> toolchains/<工具链目录>
  incoming/                 bundle/首次工具包
  config/live.json           外部配置，升级保留
  references/{weights,thresholds}/
  bin/{start,stop,status}.sh
  run/                      锁与 PID 身份记录
  log/build-<commit>.log     完整配置、编译、测试日志
  deployment.json           成功构建回执、commit、Arrow 路径
```

部署先构建，再执行全部 CTest 和 Python unittest；成功后原子切换 current。
运行进程引用已解析的版本目录，部署不会重启它。失败候选与旧版本均保留，不自动清理。

### 首次工具链

招商机原先仅有 Git/Python，没有 GCC/CMake/Make。用 Ubuntu 24.04 开发机生成
用户级 GCC 13/CMake/Make 包（不含 cms_rtf 二进制）：

```bash
python3 scripts/package_build_tools.py --out work/build-tools-ubuntu24-gcc13
python3 scripts/push_online.py --build-tools work/build-tools-ubuntu24-gcc13.tar.gz
```

只解包到 `/data/cms/rtf/toolchains`，不写系统目录、不装系统包；glibc 使用
招商机自己的 2.39。工具包带版本、许可证、逐文件 hash。这是一次性传输。
头文件与 libarrow 复用现有归档 release 的 Python 目录，并在构建回执中固定其
真实版本路径；不使用 gy_dev 的 Arrow 11 二进制或其旧 ABI 编译选项。
旧归档 release 的这个目录是已发布 cms_rtf 的依赖，不能随意删除。

目标机可访问 Ubuntu 镜像时，也可在目标机执行
`python3 scripts/bootstrap_build_tools.py --prefix /data/cms/rtf --refresh-metadata`。
它将 APT 元数据保存在自己的 incoming 目录，校验官方包 hash 后仅用
`dpkg-deb -x` 解包到私有工具目录；不运行 apt install/dpkg -i，也不修改系统
APT 源、系统包数据库或系统安装。后续部署无需重复 bootstrap。

## 参考数据及运行

`config/live.json` 默认 8 workers，待聚合 payload 上限 256 MiB。每日启动前将
四指数权重放入 `references/weights/csi_{50,300,500,1000}/YYYY-MM-DD.parquet`，
阈值放入 `references/thresholds/threshold_YYYYMMDD.csv`。
沿用现有不晚于交易日的最近文件规则；实际所用文件、hash 与成分股数写入
每次运行的 `references.json`，缺失或缺成分阈值直接失败。

```bash
/data/cms/rtf/bin/start.sh
/data/cms/rtf/bin/status.sh
/data/cms/rtf/bin/stop.sh
```

独立锁防止重复启动，stop 经 UID、进程启动 ticks、命令行与 pidfd 校验后
只给本服务发送 SIGTERM。每天从该日日志首帧重建状态；网关重启时仍跟读同一
日期目录的后续 START，不把会话 END 当成全天完成。不删除原始 journal。
正常日 15:31 跟读追平后退出，要求有网关 END；当日无 journal 则空运行退出。
启动过晚而原始页已被归档清理会明确失败，不能只从中间页恢复簿。

输出是单个最终 Arrow stream（106 Float32 因子 + 两个键），没有逐股中间文件：

```
data/index_500ms/YYYYMMDD/runs/<时间-PID>/index_500ms_exch.arrow
data/index_500ms/YYYYMMDD/current -> runs/<时间-PID>
```

实时模式逐批 flush，同名 `live-status.json` 提供已发布端点、source clock、
输入/迟到/中断计数、进程 RSS 峰值和状态。读取正在增长的 Arrow 时，消费者必须
按完整 record batch 读取并等待尾部，不将暂时没有字节视为全天 EOF。
重启创建新 run，追平并发布首批后再切换日期 current，旧结果不覆盖。
失败或盘中人工 stop 保留已写结果，状态标记 failed/stopped，不补造未来窗口。
恢复时读首帧重建，需约一个历史追赶周期；没有跨进程内存快照。

### 盘中延迟监控

journal 入口默认在最终 Arrow 同目录生成 `latency.log`、`latency.csv` 和
`latency.meta.json`。`latency.log` 为“交易所窗口端点 + 关窗至 Arrow flush 的
毫秒数”，可直接 tail。你关心的最后一条行情到因子写出，在 CSV 的
`last_recv_to_write_ms`（网关接收起算）和 `last_read_to_write_ms`（cms_rtf
读到 committed frame 起算）里。当前只输出 exch 因子，尚无 local 因子流。

```bash
# 日期 current 在追平后指向本次 run；启动追赶期间可从 status.sh 查看 attempt。
tail -f /data/cms/rtf/data/index_500ms/20261013/current/latency.log
python3 /data/cms/rtf/current/scripts/summarize_latency.py \
  /data/cms/rtf/data/index_500ms/20261013/current/latency.csv \
  --phase tail --reason watermark
```

统计包含 mean/P50/P95/P99/P999/max，单位全部是 ms。默认排除无当前窗口行情的
空桶；盘中正常水位关窗、收盘定时收尾、启动追赶和离线 replay 分开。历史回放中
`last_recv_to_write_ms` 留空，不能拿历史 recv_ns 减当前时间来报告实时延迟。
catchup 阶段的接收延迟可能包含历史数据年龄；排查时可去掉 phase 过滤查看。
Arrow flush 表示完整 batch 已可读取，没有额外 fsync；详见 design.md 的计时边界。
8 worker 的 gy_dev/招商验证、华泰盘中对照和日志开销见
[延迟日志验收记录](validation/20261013-latency.md)。

`crontab.example` 只提供 09:12 启动示例；部署不会自动修改 crontab 或启动业务。
实际安装定时任务须保留网关原有条目，并按工作区规则先备份。

## 离线复现实盘路径

```bash
build/cms_index500ms_mt --journal <已停止的日期目录> --journal-mode replay \
  --day 20261009 --thresh <thresholds.bin> --ref <reference.bin> \
  --work-dir <新目录> --workers 8 --output-mode stream --out <新文件.arrow>
```

使用 journal 中的原始有效时间（extra_nano 优先）推进定时器，不用回放机器墙钟。
到 EOF 仍要求最后会话有 END，不人为补齐未到时间的未来窗口。
原 `--input` DTYPE 回放及其逐位基准保持不变。实盘迟到策略与无限等待的离线基准
有明确边界，见 design.md；不能把二者的整日逐位相等当成无条件保证。
