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

esp_err_t idf_sms_start(uint8_t slot);
esp_err_t idf_sms_send_text(uint8_t slot, const std::string& phone, const std::string& text,
                            std::string& message);
esp_err_t idf_sms_enqueue_outgoing(uint8_t slot, const std::string& phone, const std::string& text,
                                   std::string& message);
int idf_sms_outgoing_queue_depth(uint8_t slot);
IdfSmsStatus idf_sms_get_status(uint8_t slot);
