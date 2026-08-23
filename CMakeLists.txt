cmake_minimum_required(VERSION 3.22.1)

# 原生 ESP-IDF 固件工程入口；Arduino fallback 已移除。
include($ENV{IDF_PATH}/tools/cmake/project.cmake)
set(PROJECT_VER "1.1.5")
project(sms_forwarding_idf)

# UART0 预留给第二个 ML307R；已有 build/sdkconfig.idf6 不会自动接受 defaults 的新选项，
# 因此在配置阶段明确拒绝旧控制台配置，避免生成会向模组 RX 注入日志的固件。
if(NOT CONFIG_ESP_CONSOLE_USB_SERIAL_JTAG OR
   NOT CONFIG_ESP_CONSOLE_SECONDARY_NONE OR
   CONFIG_ESP_CONSOLE_UART)
    message(FATAL_ERROR
        "UART0 is reserved for modem slot 2. Select USB Serial/JTAG as the primary console "
        "and disable the secondary console in build/sdkconfig.idf6.")
endif()
