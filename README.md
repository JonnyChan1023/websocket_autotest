# websocket_autotest
base python , autotest 
TODO:
1,数据面自动化，http，长连接消息网关 websocket（python&go）
2,api自动化-工单系统
3,配套后端服务

# autotest_open: 通用长连接开源 Demo

## 1. 项目简介与目标

`autotest_open` 是一个去敏的通用长连接服务开源 Demo。它旨在演示一个经过实战检验的长连接后端架构，保留了其核心的分层设计与组件解耦思想，但彻底移除了所有公司内部的私有依赖、专有协议和业务逻辑。

本项目**仅使用通用的开源依赖库和 Python 标准库**，确保能在任何纯公网环境下独立构建、运行和理解。

核心目标：
-   **展示架构**：清晰呈现一个可扩展、易于维护的长连接服务骨架，包括网关、核心逻辑、控制面和客户端的分离。
-   **技术交流**：提供一个具体的代码范例，用于探讨长连接服务的设计模式，如连接管理、消息分发、协议设计等。
-   **快速启动**：提供一键运行的 Demo，帮助开发者快速理解端到端的完整消息链路。

## 2. 架构概览

项目采用经典的分层架构，各层职责清晰，通过标准接口进行协作。

-   **网关层 (Gateway)**
    -   **TCP 网关** (`gateway/server.py`)：基于 Python `asyncio` 实现的纯标准库 TCP 长连接网关，负责原始的 Socket 连接管理和协议字节流解析。
    -   **WebSocket 网关** (`gateway/ws_server.py`)：基于 `websockets` 库实现，提供标准的 WebSocket 握手、连接管理和消息帧处理。
    -   **职责**：维护客户端物理连接，处理心跳，解析消息帧，并将格式化的消息传递给核心层。网关本身不处理任何业务逻辑。

-   **核心层 (Core)**
    -   **连接注册表 (ConnectionRegistry)** (`core/registry.py`)：一个内存中的注册中心，负责追踪所有在线客户端的状态、ID 和它们所属的连接（TCP 或 WebSocket）。
    -   **消息调度器 (MessageDispatcher)** (`core/dispatcher.py`)：负责将下行消息（单推、群发、广播）分发到正确的客户端连接上。它从注册表获取客户端信息，并将消息写入对应的连接通道。
    -   **职责**：管理客户端的逻辑状态，并根据指令将消息准确地推送给一个或多个客户端。

-   **客户端 (Client)**
    -   **数据客户端 (WsClient)** (`client/ws_client.py`)：一个通用的 WebSocket 客户端，用于模拟真实设备或用户，与网关建立长连接并收发数据。
    -   **控制客户端 (ControlClient)** (`client/control_client.py`)：一个特殊的 WebSocket 客户端，用于向网关发送控制指令，如触发对其他客户端的消息推送、群发或广播。

-   **控制面 (Control Plane)**
    -   **HTTP 控制服务** (`cmd/control_server.py`)：一个基于 Python `http.server` 的极简 HTTP 服务。它提供了一组 RESTful API，允许外部系统（如后台管理、CI/CD 脚本）通过 HTTP 请求来触发消息推送或查询在线状态。

-   **模型层 (Models)**
    -   **消息协议 (Message)** (`models/message.py`)：定义了系统内所有通信的统一数据结构。本项目使用 **JSON Lines** 格式（每行一个 JSON 对象）作为消息的序列化协议，确保了协议的透明性和易读性。

-   **配置层 (Config)**
    -   **集中配置读取** (`internal/config.py`)：提供一个统一的配置加载模块，从环境变量中读取所有可配置项（如监听地址、端口等），为所有组件提供一致的配置视图。

**关系图**:
`外部系统 -> HTTP控制服务 -> 核心层 -> 网关层 <-> 客户端`
`控制客户端 -> 网关层 -> 核心层 -> 网关层 <-> 数据客户端`

## 3. 目录结构

```
autotest_open/
├── cmd/                     # 命令行入口
│   ├── app.py               # 统一 CLI 入口程序
│   ├── control_server.py    # HTTP 控制面服务入口
│   ├── gateway.py           # TCP 网关服务入口
│   ├── ws_client_demo.py    # WebSocket 演示客户端入口
│   └── ws_gateway.py        # WebSocket 网关服务入口
├── client/                  # 客户端实现
│   ├── control_client.py    # WebSocket 控制客户端
│   └── ws_client.py         # 通用 WebSocket 数据客户端
├── core/                    # 核心逻辑 (状态管理与消息调度)
│   ├── dispatcher.py        # 消息调度器
│   └── registry.py          # 连接注册表
├── gateway/                 # 长连接网关实现
│   ├── server.py            # 通用 TCP 网关 (基于 asyncio)
│   └── ws_server.py         # 通用 WebSocket 网关 (基于 websockets)
├── internal/                # 内部辅助模块
│   └── config.py            # 配置管理 (读取环境变量)
├── models/                  # 数据模型
│   └── message.py           # 消息结构定义 (JSON Lines 协议)
├── scripts/                 # 辅助脚本
│   └── run_ws_demo.sh       # WebSocket 一键演示脚本
├── tests/                   # 单元测试
│   └── test_message_model.py
├── Makefile                 # 简化命令 (如 make ws_demo)
├── requirements.txt         # Python 依赖清单
└── README.md                # 本文档
```
*注：项目中不包含 `core/tasks.py`、`cmd/client.py`、`scripts/run_demo.sh` 等已废弃的文件。*

## 4. 构建与运行

### 安装依赖

项目核心 TCP 功能仅依赖 Python 标准库。若要使用 **WebSocket** 相关功能（网关或客户端），需要先安装必要的开源库。

```bash
pip install -r requirements.txt
```
依赖库包括：`websockets` (用于 WS 网关), `websocket-client` (用于 WS 客户端), `requests` (用于客户端的 HTTP 模式)。

### 统一 CLI 入口 (推荐)

项目提供了一个统一的命令行入口，方便启动各个组件。

```bash
# 启动 WebSocket 网关
python3 -m autotest_open.cmd.app ws-gateway

# 启动 TCP 网关
python3 -m autotest_open.cmd.app tcp-gateway

# 启动 HTTP 控制面服务
python3 -m autotest_open.cmd.app control-server

# 启动 WebSocket Demo 客户端
python3 -m autotest_open.cmd.app ws-client-demo
```

### 运行 WebSocket Demo

这是最快验证项目端到端连通性的方式。它会自动在后台启动 WebSocket 网关，然后启动多个客户端进行连接、收发消息并验证。

```bash
# 方式一：使用 Makefile
make ws_demo

# 方式二：直接运行脚本
./scripts/run_ws_demo.sh
```

### 分别启动服务

你也可以在不同的终端中手动启动各个服务，以模拟真实的分布式环境。

1.  **启动 WebSocket 网关**:
    ```bash
    python3 -m autotest_open.cmd.app ws-gateway
    ```
2.  **启动 HTTP 控制面**:
    ```bash
    python3 -m autotest_open.cmd.app control-server
    ```
3.  **启动 TCP 网关** (如果需要):
    ```bash
    python3 -m autotest_open.cmd.app tcp-gateway
    ```

## 5. 客户端使用说明

### WsClient (数据客户端)

`WsClient` 用于模拟普通用户或设备，与网关建立长连接。

-   **建连 URL**：URL 的 query string 仅包含 `client_id`, `device_platform`, `version_code`。
-   **消息发送**：`send_hello()` 和 `send_upstream()` 发送的消息体中不包含 `group_id`。

**代码示例**：
```python
from autotest_open.client.ws_client import WsClient

# URL 中只包含客户端自身标识
url = "ws://127.0.0.1:9100/ws?client_id=user-001&device_platform=python&version_code=101"

client = WsClient(url=url, config={"ack_on": 1, "ack_return": 1})

# 发送 HELLO 消息完成握手和注册
client.send_hello()

# 发送上行数据
client.send_upstream(payload={"action": "ping", "value": 123})

# 接收下行消息
messages = client.receive_messages(timeout=5.0)
print(f"收到消息: {messages}")

client.close()
```

### ControlClient (控制客户端)

`ControlClient` 用于从外部向系统下发控制指令。

-   **初始化**：`ControlClient` 在初始化时**不接受** `group_id` 参数。
-   **下发控制信号**：通过 `send_control_signal()` 方法下发指令。该方法通过 `op` 字段区分操作类型，并通过 `target_client_id` 或 `target_group_id` 参数来指定目标。

**代码示例**：
```python
from autotest_open.client.control_client import ControlClient

# 初始化时不含 group_id
client = ControlClient(
    uri="ws://127.0.0.1:9100/ws",
    client_id="admin-controller-1"
)
client.send_hello() # 控制客户端也需要先注册自己

# 1. 向单个客户端推送 (push)
client.send_control_signal(
    op="push",
    payload={"alert": "你的包裹已到达"},
    target_client_id="user-001"
)

# 2. 向指定群组广播 (group)
client.send_control_signal(
    op="group",
    payload={"promo": "限时优惠活动开始！"},
    target_group_id="vip-users" # group_id 只在这里的 payload 中出现
)

# 3. 向所有在线客户端广播 (broadcast)
client.send_control_signal(
    op="broadcast",
    payload={"notification": "系统将于 5 分钟后进行维护"}
)

# 4. 查询在线状态 (query_online)
client.send_control_signal(op="query_online", payload={})
response = client.receive_messages(timeout=1.0)
print(f"在线状态查询结果: {response}")

client.close()
```

## 6. 去敏声明

本项目经过了严格的去敏处理，以确保其通用性和安全性：
-   **无私有依赖**：所有依赖均为通用开源库（如 `websockets`）或 Python 标准库。
-   **无内部信息**：代码中不包含任何公司名称、内部项目名、内网地址、私有 API 或其他专有标识符。
-   **`group_id` 的使用**：`group_id` **不会**出现在客户端的建连 URL、握手消息 (`hello`) 或普通上行消息 (`upstream`) 中。它仅作为 `ControlClient` 发送控制信号时的一个可选 `payload` 字段，用于指定群组广播的目标。

## 7. 配置项说明

所有配置均通过**环境变量**读取，并提供了合理的默认值。你可以在运行前设置这些变量，或创建一个 `.env` 文件。

-   `APP_GATEWAY_HOST` / `APP_GATEWAY_PORT`: TCP 网关监听地址 (默认: `127.0.0.1:9000`)
-   `APP_WS_GATEWAY_HOST` / `APP_WS_GATEWAY_PORT`: WebSocket 网关监听地址 (默认: `127.0.0.1:9100`)
-   `APP_CONTROL_HOST` / `APP_CONTROL_PORT`: HTTP 控制面监听地址 (默认: `127.0.0.1:8000`)
-   `APP_ACCESS_TOKEN`: 一个用于演示的静态访问令牌 (默认: `demo-token`)

更多配置项请参考 `internal/config.py`。

## 8. 测试与维护

项目包含简单的单元测试，以验证核心模型的行为。

-   **运行单元测试**:
    需要先安装 `pytest` (`pip install pytest`)。
    ```bash
    pytest autotest_open/tests/
    ```

-   **快速验证**:
    推荐使用 `make ws_demo` 或统一的 CLI 入口 `python3 -m autotest_open.cmd.app ...` 来快速启动和验证各个组件功能是否正常。

