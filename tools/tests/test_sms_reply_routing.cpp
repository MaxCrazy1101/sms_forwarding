#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cctype>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <vector>
#include "idf_sms_reply_check.h"
#include "idf_sms_timestamp.h"

// 测试替身只模拟主机上的 RTOS/外部组件；下方函数和 owner 直接来自生产源码。
using esp_err_t = int;
using TaskHandle_t = void*;
using SemaphoreHandle_t = bool*;
constexpr esp_err_t ESP_OK = 0, ESP_ERR_TIMEOUT = 1, ESP_FAIL = 2,
                      ESP_ERR_INVALID_ARG = 3, ESP_ERR_INVALID_STATE = 4;
constexpr int pdTRUE = 1, portMAX_DELAY = -1;
#define pdMS_TO_TICKS(ms) (ms)
int xSemaphoreTake(SemaphoreHandle_t mutex, int) { assert(mutex && !*mutex); *mutex = true; return pdTRUE; }
void xSemaphoreGive(SemaphoreHandle_t mutex) { assert(mutex && *mutex); *mutex = false; }
void vTaskDelay(int) {}
void idf_logf(const char*, ...) {}
const char* esp_err_to_name(int) { return "TEST_ERROR"; }
static int64_t clock_us = 1000;
int64_t esp_timer_get_time() { return clock_us; }
constexpr size_t MAX_PDU_HEX_CHARS = 600, INDEX_QUEUE_MAX = 8, OUT_SMS_QUEUE_MAX = 3, SEEN_RING_MAX = 32,
                 CONCAT_SLOTS = 5, CONCAT_PARTS = 10;
constexpr uint8_t IDF_MODEM_SLOT_COUNT = 2;
constexpr bool idf_modem_slot_valid(uint8_t slot) { return slot < IDF_MODEM_SLOT_COUNT; }
constexpr int64_t CONCAT_TIMEOUT_US = 900000000, CONCAT_DONE_TTL_US = 600000000;
constexpr uint32_t SMS_SUBMIT_TIMEOUT_MS = 60000;
struct IdfSmsStatus { uint32_t total = 0, lastSmsEpoch = 0; bool receiveReady = false; };
// @生产运行态@
static SmsContext contexts[2];
static bool mutexes[2] = {};
static SmsContext* context_for_slot(uint8_t slot) { return idf_modem_slot_valid(slot) ? &contexts[slot] : nullptr; }
static int inbox[2] = {}, forwarded[2] = {}, sent[2] = {}, pdus[2] = {}, submit_callbacks[2] = {};
struct IdfSmsProcessView { std::string numberBlackList; int tzOffsetMin = 0; };
IdfSmsProcessView idf_config_get_sms_process_view() { return {}; }
std::string idf_config_translate_perl_classes(const std::string& pattern) { return pattern; }
bool number_blacklisted(const std::string&, const std::string&) { return false; }
bool is_admin_sender(const std::string&, const IdfSmsProcessView&) { return false; }
bool process_admin_command(SmsContext&, const std::string&, const std::string&) { return false; }
std::string idf_util_format_epoch_local(uint32_t, int) { return "host-time"; }
std::string idf_util_trim_copy(const std::string& text) {
    size_t begin = text.find_first_not_of(" \r\n\t");
    return begin == std::string::npos ? std::string() : text.substr(begin, text.find_last_not_of(" \r\n\t") - begin + 1);
}
uint32_t idf_inbox_add(uint8_t slot, const char*, const char*, const char*) { return ++inbox[slot]; }
bool idf_push_enqueue_forward(uint8_t slot, const char*, const char*, const char*, uint32_t) { ++forwarded[slot]; return true; }
void idf_sent_add(uint8_t slot, const char*, const char*, bool) { ++sent[slot]; }
void idf_modem_request_reset(uint8_t, bool) {}
void handle_call_urc_line(SmsContext&, const std::string&) {}
static int mode_error = ESP_OK, submit_error = ESP_OK;
static bool ready = true, fast_reply = false;
struct IdfModemReadiness { bool modemReady; };
IdfModemReadiness idf_modem_get_readiness(uint8_t) { return {ready}; }
esp_err_t idf_modem_send_at(uint8_t, const char*, uint32_t, std::string&) { return mode_error; }
bool is_valid_phone_number(const std::string& phone) { return phone == "10086"; }
esp_err_t sms_encode_one(const std::string&, const std::string&, uint16_t, uint8_t, uint8_t,
                          std::string& pdu, int& length, std::string&) { pdu = "0000"; length = 2; return ESP_OK; }
static void process_sms_content(SmsContext&, const char*, const char*, const char*, int64_t, uint32_t, bool = false);
static void handle_decoded_pdu(SmsContext&, const DecodedSms&);
void idf_sms_reply_check_cancel(uint8_t);
static DecodedSms urc_sms;
bool decode_pdu_line(SmsContext& ctx, const std::string& line, int64_t received_us) {
    if (line != "ABCD") return false;
    DecodedSms sms = urc_sms;
    sms.receivedUs = received_us;
    handle_decoded_pdu(ctx, sms);
    return true;
}
esp_err_t idf_modem_send_pdu(uint8_t slot, const char*, const char*, uint32_t, std::string&,
                             void (*on_submit)(uint8_t)) {
    ++pdus[slot];
    if (on_submit) { ++submit_callbacks[slot]; on_submit(slot); }
    if (fast_reply && on_submit) {
        ++clock_us;
        process_sms_content(contexts[slot], "10086", "fast reply", "fast-time", clock_us, 0);
    }
    return submit_error;
}
// @生产函数@

static void prepare(uint8_t slot) {
    assert(idf_sms_reply_check_begin(slot, "^10086$", ""));
    reply_check_submitted(slot);
    idf_sms_reply_check_arm(slot, 5);
}
static DecodedSms part(int ref, int number, const char* body, int64_t received, uint32_t epoch) {
    DecodedSms sms;
    sms.sender = "10086"; sms.text = body; sms.timestamp = std::to_string(ref) + std::to_string(number);
    sms.concat[0] = ref; sms.concat[1] = number; sms.concat[2] = 2;
    sms.receivedUs = received; sms.receivedEpoch = epoch;
    return sms;
}
static void test_slot_and_normal_delivery(uint8_t slot) {
    uint8_t other = 1 - slot;
    prepare(slot);
    int before = forwarded[slot];
    process_sms_content(contexts[other], "10086", "wrong slot", "other-time", clock_us + 1, 0);
    assert(idf_sms_reply_check_poll(slot) == 0);
    process_sms_content(contexts[slot], "10086", "right slot", "right-time", clock_us + 1, 0);
    assert(idf_sms_reply_check_poll(slot) == 1);
    assert(forwarded[slot] == before + 1 && inbox[slot] == forwarded[slot]);
    process_sms_content(contexts[slot], "10086", "right slot", "right-time", clock_us + 1, 0);
    assert(forwarded[slot] == before + 1); // 去重仍生效，matcher 不消费消息。
    idf_sms_reply_check_cancel(slot);
}
static void test_urc_and_index_freshness(uint8_t slot) {
    clock_us += 10000;
    urc_sms.sender = "10086"; urc_sms.text = "URC reply"; urc_sms.timestamp = std::to_string(clock_us);
    const int64_t old_received = clock_us - 1;
    process_urc_text(contexts[slot], "@RXUS:" + std::to_string(old_received) + "\n+CMTI: \"SM\",7\n");
    prepare(slot);
    int index = -1; int64_t received = 0;
    assert(pop_index(contexts[slot], index, received) && index == 7 && received == old_received);
    process_sms_content(contexts[slot], "10086", "queued old CMTI", "old-time", received, 0);
    assert(idf_sms_reply_check_poll(slot) == 0);
    // +CMT 头和 PDU 分两次处理；时间取头的到达时间，不取 worker 处理时间。
    process_urc_text(contexts[slot], "@RXUS:" + std::to_string(old_received) + "\n+CMT: ,2\n");
    process_urc_text(contexts[slot], "@RXUS:" + std::to_string(clock_us + 1) + "\nABCD\n");
    assert(idf_sms_reply_check_poll(slot) == 0);
    urc_sms.timestamp += "new";
    process_urc_text(contexts[slot], "@RXUS:" + std::to_string(clock_us + 1) + "\n+CMT: ,2\nABCD\n");
    assert(idf_sms_reply_check_poll(slot) == 1);
    idf_sms_reply_check_cancel(slot);
}
static void test_multipart_and_backfill(uint8_t slot) {
    clock_us += 10000;
    prepare(slot);
    handle_decoded_pdu(contexts[slot], part(100 + slot, 1, "old fragment ", clock_us - 1, 0));
    handle_decoded_pdu(contexts[slot], part(100 + slot, 2, "new fragment", clock_us + 1, 0));
    assert(idf_sms_reply_check_poll(slot) == 0);
    const uint32_t current_epoch = static_cast<uint32_t>(time(nullptr));
    handle_decoded_pdu(contexts[slot], part(200 + slot, 1, "stored fragment ", 0, current_epoch - 100));
    handle_decoded_pdu(contexts[slot], part(200 + slot, 2, "new fragment", clock_us + 1, current_epoch));
    assert(idf_sms_reply_check_poll(slot) == 0);
    process_sms_content(contexts[slot], "10086", "old backfill", "backfill-old", 0, current_epoch - 100);
    assert(idf_sms_reply_check_poll(slot) == 0);
    process_sms_content(contexts[slot], "10086", "new backfill", "backfill-new", 0, current_epoch);
    assert(idf_sms_reply_check_poll(slot) == 1);
    prepare(slot);
    handle_decoded_pdu(contexts[slot], part(300 + slot, 1, "fresh part ", clock_us + 1, current_epoch));
    handle_decoded_pdu(contexts[slot], part(300 + slot, 2, "fresh finish", 0, current_epoch));
    assert(idf_sms_reply_check_poll(slot) == 1);
    prepare(slot);
    handle_decoded_pdu(contexts[slot], part(400 + slot, 1, "partial reply", clock_us + 1, current_epoch));
    // 超时拼出的缺段消息仍正常转发，但不能证明回复检查成功。
    clock_us += CONCAT_TIMEOUT_US + 1;
    int before = forwarded[slot];
    expire_concat_slots(contexts[slot]);
    assert(forwarded[slot] == before + 1 && idf_sms_reply_check_poll(slot) == -1);
    idf_sms_reply_check_cancel(slot);
}
static void test_submit_boundary_and_failure(uint8_t slot) {
    std::string message;
    clock_us += 10000;
    assert(idf_sms_reply_check_begin(slot, "^10086$", ""));
    fast_reply = true;
    int callbacks_before = submit_callbacks[slot], pdus_before = pdus[slot];
    assert(idf_sms_send_text(slot, "10086", std::string(200, 'x'), message, true) == ESP_OK);
    assert(pdus[slot] == pdus_before + 2 && submit_callbacks[slot] == callbacks_before + 1);
    idf_sms_reply_check_arm(slot, 5);
    assert(idf_sms_reply_check_poll(slot) == 1); // +CMGS 返回前的快速回复保留。
    for (int failure : {ESP_FAIL, ESP_ERR_TIMEOUT}) {
        assert(idf_sms_reply_check_begin(slot, "^10086$", ""));
        submit_error = failure;
        assert(idf_sms_send_text(slot, "10086", "fail", message, true) == failure);
        assert(contexts[slot].replyCheck.poll(clock_us) == IdfSmsReplyCheck::State::Idle);
    }
    submit_error = ESP_OK; fast_reply = false;
    assert(idf_sms_reply_check_begin(slot, "^10086$", ""));
    mode_error = ESP_FAIL;
    assert(idf_sms_send_text(slot, "10086", "fail-mode", message, true) == ESP_FAIL);
    assert(contexts[slot].replyCheck.poll(clock_us) == IdfSmsReplyCheck::State::Idle);
    mode_error = ESP_OK;
    assert(idf_sms_reply_check_begin(slot, "^10086$", ""));
    assert(idf_sms_send_text(slot, "invalid", "text", message, true) == ESP_ERR_INVALID_ARG);
    assert(contexts[slot].replyCheck.poll(clock_us) == IdfSmsReplyCheck::State::Idle);
}
int main() {
    assert(!idf_sms_reply_check_begin(2, "10086", ""));
    assert(idf_sms_reply_check_poll(2) == -1);
    idf_sms_reply_check_cancel(2); idf_sms_reply_check_arm(2, 5);
    for (uint8_t slot = 0; slot < 2; ++slot) { contexts[slot].slot = slot; contexts[slot].statusMutex = &mutexes[slot]; }
    for (uint8_t slot = 0; slot < 2; ++slot) {
        test_slot_and_normal_delivery(slot); test_urc_and_index_freshness(slot);
        test_multipart_and_backfill(slot); test_submit_boundary_and_failure(slot);
        assert(!mutexes[slot]);
    }
    std::puts("dual-slot SMS reply routing/URC/multipart/submit/failure regressions passed");
}
