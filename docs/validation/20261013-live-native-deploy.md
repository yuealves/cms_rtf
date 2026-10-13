# 20261013：招商机 Git bundle 增量部署与直接 journal 跟读验收

实现并部署的代码为 `9a4c6dc7bf8a233bd6f19ef4e9c45b85d95ba8b4`，
分支 `feat/cms-live-bundle-20261013`。本记录是随后补充的文档提交；线上运行代码
仍以 `/data/cms/rtf/deployment.json` 为准。
[机器可读结果](20261013-live-native-deploy.json) 保存构建回执、实际运行参数、
计数、参考文件 hash、Arrow hash 及本地回归摘要。

## 已解决的部署差距

招商机器可以运行 cms_rtf。此前缺少的是直接跟读正在增长的 journal 的入口、
实盘会话定时器和独立发布流程，不是硬件或二进制兼容方面不可解决的限制。
本次参考 Huatai `light_rtf_v2` 的 `sync_online_bundle.sh`，完成以下链路：

```
本地已提交 HEAD → Git 增量 bundle → 招商独立 worktree → 原生编译与测试
                                                          ↓
既有 cms_gateway → 既有 journal → cms_rtf C++ → 最终 Arrow stream
```

读取、协议解码、计算、聚合和输出都在一个 C++ 进程中；Python 只做启动、
参考文件准备和进程监管。没有离线 exporter，也不生成逐股中间文件。
reader 每次只映射一页 128 MiB，不预载整天 journal。

Huatai 对应依据是 `546aba2` 的 `worker.cpp:119–122`（迟到流量抑制）、
`worker.cpp:303–309`（上午/下午收盘后 60 秒定时器）和 journal source 的空闲
时钟推进方式。详细差异见 [design.md](../design.md) 最后一节；106 列目录和
浮点求和顺序没有改变。原 DTYPE 路径继续保留已验收的离线语义。

## 招商原生构建与发布

目标为 Ubuntu 24.04.4、glibc 2.39、Xeon Gold 6240、16 逻辑 CPU、约 35.2 GiB
内存。原先没有 GCC/CMake/Make，现将官方 Ubuntu 包经 APT hash 验证后只解包
到 `/data/cms/rtf/toolchains/ubuntu24-gcc13-native`。没有安装系统包或修改系统
APT 源。使用 GCC 13.3、CMake 3.28.3 和既有归档 release 的 PyArrow 23.0.1。
Arrow 的头文件和运行库固定在 `/data/cms/archive/releases/cms-archive-0.2.4/python`。

发布到 `/data/cms/rtf`；编译使用 2 jobs。37.491 秒完成构建及测试，
**5 个 CTest、50 个 Python unittest 全部通过，目标机无 skip**，然后切换 current。
构建失败不会激活候选版本，部署不会自动重启进程。

实传源码 bundle 大小：首次到 `a585acf` 为 297,891 B，随后修复到 `eb37a77`
为 2,551 B，再到 `9a4c6dc` 为 685 B。该数字只指 Git 源码 bundle，不含小型
部署 helper、首次工具链及参考文件。工具链准备完成后，常规部署仅传缺少的源码。

本开发机更新命令：

```bash
python3 scripts/push_online.py --build-jobs 2
```

默认经 gy_dev 中转；直接在 gy_dev 执行时增加 `--via ''`。操作及回滚所需的
版本目录布局见 [部署说明](../deploy-live.md)。

## 招商真实全天 journal 运行

2026-10-13 收盘后 20:40:55，通过新组件自己的脚本启动：

```bash
/data/cms/rtf/bin/start.sh --day 20261013
```

实际为 `--journal-mode follow --journal-clock wall --workers 8 --window-batch 1
--pending-mib 256`，直接读取 `/data/cms/data/journal/20261013`。
这是已录制全天数据的追赶验证；跟读追平后按当天结束条件正常退出。
**这不是开盘期间的实时延迟测量。**

| 指标 | 结果 |
|---|---:|
| C++ 内核完整处理时间 | 262.123 秒（4 分 22 秒） |
| 原始行情行数 | 500,061,765 |
| 成分股事件数 | 207,866,215 |
| C++ 进程峰值 RSS | 3,204,472 KiB = 3.056 GiB |
| 输出时间端点 / 行数 | 28,800 / 115,200 |
| 输出列数 | 106 个 Float32 因子 + 2 个键 |
| 最终 Arrow | 201,819,152 B，约 192.47 MiB |
| 逐股中间文件 | 0 B |
| 迟到事件 / 不正常会话重启 | 0 / 0 |

时间来自 C++ `steady_clock`，从 worker 初始化前计时，到 worker 完成、Arrow 关闭和
最终状态写出后结束，包含 journal 读取/解码与计算、聚合、输出；不含 supervisor
准备参考文件、SSH 或事后验证。RSS 来自 C++ 自身 `getrusage(RUSAGE_SELF)`，
包含该进程的 journal 映射驻留页，不含 Python supervisor 和未映射的系统 page cache。

这次 reader 读取/解码累计约 204.02 秒；不能把 262 秒与先前 gy_dev 上仅处理
预导出 DTYPE 的约 36.7 秒当成同条件性能对比。日期、硬件、线程数、关窗调度和
输入阶段均不同。本测试证明完整原生链路可运行；盘中延迟需交易时段另行观察。

SH 全天身份表有 59,785,052 条，容量为 2,615,277,056 B，仍是内存主要组成；
没有删除历史身份来压低峰值。待聚合 payload 各 worker 峰值之和为 117,202,128 B，
不是同一时刻的全局堆快照。

Arrow 按逐端点发布刷新，所以 record batch 元数据多于离线成组刷新；本次
共有 28,318 个 batch，最终文件约 202 MB。除此之外每次运行只有约 101 KB 的
权重/阈值二进制参考输入和小型状态/诊断文件，没有第二份逐股历史落盘。

独立流式读取最终 Arrow，确认目录顺序/类型、完整 500ms 网格、四指数固定行序、
115,200 个唯一键均正确，完整读取至 EOF。SHA256：
`6a8b1ad585cbabeb9b3a6da153a39868385771707a828dca8369aae81d853b75`。
kernel stderr 为空；最终状态 `completed`、exit code 0、`journal_ended=true`、
`caught_up=true`、`input_incomplete=false`，测试进程已退出。

结果目录：
`/data/cms/rtf/data/index_500ms/20261013/runs/204055-15797`，日期下的 current
指向该目录。构建回执、完整日志和旧版本均保留。

## 正确性覆盖和当前运行边界

新增 fixture 覆盖正在增长的页、半写帧、延迟出现下一页、缺中间页、hash 损坏、
会话重启、SIGTERM、只发布已完成端点、显式收盘定时器、迟到流量抑制和既有输出
拒绝覆盖。follow 与 frozen journal replay 在 1/8 worker 下逐位一致；无迟到
数据还与独立 DTYPE 输入路径逐位对照。部署测试覆盖完整/增量 bundle、脏 worktree
拒绝、构建失败不激活、分支分叉拒绝以及真实进程的 start/重复 start/status/stop。

本地 20261009 的 09:35 前样本再次回归：40,271,350 行，16 worker，batch 64；
stream 路径 2.420 秒、614,744 KiB（0.586 GiB），逐股中间文件 0 B。
与旧 blocks 路径及此前已验收 Arrow 的 106 列有效值位模式、null mask、键全部
一致，健康计数一致。这一数字不包含直接 journal reader，不能冒充实盘峰值。

目标机的真实 10/13 journal 验证使用复制的 **10/09 权重和阈值**。项目沿用
不晚于交易日的最近参考文件规则，实际路径和 hash 已记录于 references.json。
因此本次证明部署、运行、输出契约及资源占用，不宣称完成 10/13 因子口径对拍。
正式日常运行前需供给适用的每日参考文件。

既有 gateway、paged、归档脚本和 crontab 未修改；cms_rtf 当前未配置自动开盘
启动，仅提供独立 `bin/start.sh`、`bin/status.sh`、`bin/stop.sh` 和 crontab 示例。
本次验收进程已正常退出，不能将“部署成功”误解为已开启次日自动实盘。
