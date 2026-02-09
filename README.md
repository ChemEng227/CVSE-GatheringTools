## CVSE 收录信息处理
用于处理 CVSE 收录信息的工具集。使用 [pdm](https://pdm-project.org/en/latest/) 进行环境配置，使用 capnp 作为 RPC 通信协议。
### 环境配置
- 安装 pdm: https://pdm-project.org/en/latest/
- `pdm install` 安装依赖
- `git submodule update --init --recursive` 初始化子模块
### Auth Key
为了使用写入接口，需要在当前目录的 `auth_key` 文件中放置一个有效的 auth key