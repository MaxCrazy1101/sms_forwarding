# ESP-IDF 迁移状态

迁移已完成：固件现在只保留原生 ESP-IDF 工程，不再保留 Arduino fallback sketch。

## 工程边界

- 固件入口：`CMakeLists.txt`、`main/`、`components/`。
- Web UI 源文件：`code/web_src/`。
- 生成的 gzip Web 资源：`code/web_assets.h`、`code/web_assets.cpp`，由 `components/web_assets` 作为 IDF component 链接。
- 构建输出：只写入 `build/`。
- 本机推荐入口：`powershell -ExecutionPolicy Bypass -File tools\idf.ps1 build`。
- 控制台：ESP32-C3 内置 USB Serial/JTAG；UART0 不再承担应用或 bootloader 控制台。
- Windows 烧录/监控：`powershell -ExecutionPolicy Bypass -File tools\idf.ps1 flash -Port COM5` / `monitor -Port COM5`。
- Linux 烧录/监控：固定加载 `/home/alex/code/sdk/esp-idf/6.0/export.sh` 后，使用 `idf.py -B build/idf6 -D SDKCONFIG=build/sdkconfig.idf6 -p /dev/ttyACM0 flash` / `monitor`。

## 当前实现

- `idf_wifi`：STA、失败兜底 SoftAP、captive DNS、轻量 mDNS、SNTP 三服务器、运行中 BOOT 长按配网、重连看门狗。
- `idf_modem`：UART1 单 owner、AT 串行化、模组上电/重启恢复、注册/信号/身份采样、蜂窝 MHTTP、USSD、短信发送 AT 通道。
- `idf_sms`：PDU 收发、`+CMTI`/`+CMT`/`CMGL` 双路径接收、长短信合并、去重、黑名单、管理员命令、收件箱入库、转发入队。
- `idf_push`：HTTP/HTTPS 推送、SMTP over TLS/STARTTLS、转发规则、重试队列、失败冷却、测试推送、Server酱等 10 类通道。
- `idf_web`：Web UI/API、Basic Auth、状态 JSON、日志、短信收发、WiFi 配网、AT 终端、OTA、保号/每日心跳/定时重启调度。
- `idf_config`：NVS 配置读写，保留旧 key 兼容与默认值迁移。

## 双模组控制台边界

- 卡槽1继续使用 UART1：ESP TX=GPIO3、ESP RX=GPIO4、EN=GPIO5。
- 卡槽2预留 UART0 GPIO Matrix 路由：ESP TX=GPIO6、ESP RX=GPIO7、EN=GPIO10。
- GPIO20/21 不连接模组；应用关闭 UART0 控制台不能消除复位早期可能存在的 ROM UART0 输出。
- 烧录、monitor、panic 和 coredump 诊断都走 GPIO18/19 的内置 USB Serial/JTAG。
- 首次迁移必须完整 `flash` 以更新 bootloader；app-only OTA 不能消除旧 bootloader 的 UART0 输出。
- 当前控制台迁移阶段尚未启动卡槽2；不得把预留接线描述成已完成的双卡能力。

## 验证

```powershell
python tools\build_web_assets.py --check
powershell -ExecutionPolicy Bypass -File tools\idf.ps1 build
```

设备侧仍需按场景人工验证：短信收发、长短信合并、推送/邮件实际投递、OTA 上传、WiFi 配网 SoftAP、BOOT 长按配网、保号 MHTTP/USSD、弱信号/无 SIM/漫游场景。
