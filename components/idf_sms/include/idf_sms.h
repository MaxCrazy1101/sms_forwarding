#pragma once

#include <stdint.h>

#include <string>

#include "esp_err.h"

struct IdfSmsStatus {
    uint32_t total = 0;
    uint32_t lastSmsEpoch = 0;
    uint32_t stackFreeBytes = 0;
    bool receiveReady = false;
};

// 在 Web/worker 读取各槽状态前构造双槽运行态；对象放在 BSS，避免 std::string 的
// 空对象指针占用同等大小的 Flash 初始化段。
void idf_sms_init();
esp_err_t idf_sms_start(uint8_t slot);
esp_err_t idf_sms_send_text(uint8_t slot, const std::string& phone, const std::string& text,
                            std::string& message);
esp_err_t idf_sms_enqueue_outgoing(uint8_t slot, const std::string& phone, const std::string& text,
                                   std::string& message);
int idf_sms_outgoing_queue_depth(uint8_t slot);
IdfSmsStatus idf_sms_get_status(uint8_t slot);
