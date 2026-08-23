#pragma once

#include <stdint.h>

#include <string>

#include "esp_err.h"

esp_err_t idf_push_start(void);

// idf_push 持有设备级重任务 gate：Web 重任务和实际 Push/SMTP 网络事务共用。
// 锁内不得做状态查询或普通队列操作，调用方必须成对 lock/unlock。
esp_err_t idf_push_heavy_operation_init(void);
void idf_push_heavy_operation_lock(void);
void idf_push_heavy_operation_unlock(void);

bool idf_push_enqueue_forward(uint8_t slot, const char* sender, const char* text,
                              const char* timestamp, uint32_t inbox_id);
int idf_push_enqueue_notify(uint8_t slot, const char* title, const char* body, const char* timestamp);
bool idf_push_enqueue_email(const char* subject, const char* body);
int idf_push_forward_queue_depth(void);
int idf_push_retry_queue_depth(void);
int idf_push_email_queue_depth(void);
bool idf_push_busy(void);

bool idf_push_enqueue_test(uint8_t slot, uint8_t channel, std::string& message);
std::string idf_push_test_status_json(uint8_t channel);
