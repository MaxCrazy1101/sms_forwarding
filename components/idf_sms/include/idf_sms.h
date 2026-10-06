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
                            std::string& message, bool track_reply = false);
esp_err_t idf_sms_enqueue_outgoing(uint8_t slot, const std::string& phone, const std::string& text,
                                   std::string& message);
int idf_sms_outgoing_queue_depth(uint8_t slot);
IdfSmsStatus idf_sms_get_status(uint8_t slot);

// 每槽独立回复检测；发件人/正文匹配任一成立，观察不消费普通收件流程。
bool idf_sms_reply_check_begin(uint8_t slot, const std::string& sender_pattern,
                               const std::string& body_pattern);
void idf_sms_reply_check_arm(uint8_t slot, int timeout_seconds);
// 1=匹配成功，0=等待，-1=未开始/超时/非法槽。
int idf_sms_reply_check_poll(uint8_t slot);
void idf_sms_reply_check_cancel(uint8_t slot);
