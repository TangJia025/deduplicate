# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

基于 Redis Bloom Filter 模块的通用去重库，提供 `Check`（仅检查）和 `Write`（检查并写入）两种去重能力。

## 构建 & 测试

```bash
# 运行测试
go test ./...

# 运行单个测试
go test -run TestGenerateKey -v
```

## 架构

### 接口层 (`client.go`)
- `Client` 接口对外暴露 `Check` 和 `Write` 两个方法
- `NewClient(opts ...Option)` 创建实例，使用 function-option 模式

### 核心实现 (`rebloom_client.go`)
- `rebloomClient` 内嵌 `*redis.Client`，通过 `BF.MEXISTS` / `BF.MADD` 命令与 RedisBloom 交互
- `parallelExec` 对多个 key 并发执行去重命令（goroutine per key）
- 使用 CRC32 对 routeKey 哈希取模，将 key 分散到 10000 个虚拟分区
- 按去重天数（Days）生成多条记录，key 格式为 `{busId}_{yyyymmdd}_{partition}`

### 配置 (`option.go`)
- `WithBusId(busId)` — 指定业务 ID
- `WithTimeout(ms)` — 设置 Redis 操作超时

### 业务信息管理
- `init()` 时从配置中心加载各业务的 Redis 连接信息（`mapBusInfo`），并启动后台 goroutine 每 10 秒刷新
- `readFromConfigCenter()` 当前为 mock 实现，返回硬编码的测试数据，实际应替换为从远端配置中心拉取

### 重复判断
- `KeyInfo.Code` 由库填充：`CodeKeyExist` (20000) 表示重复，`CodeKeyNonExist` (20001) 表示不重复
- `BF.MEXISTS` 返回 1 表示存在（重复），`BF.MADD` 返回 0 表示已存在（重复）
