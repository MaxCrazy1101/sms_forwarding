#include "idf_sms.h"
#include "idf_sms_reply_check.h"
#include "idf_sms_timestamp.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <new>
#include <string>
#include <vector>

#include "esp_log.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "idf_config.h"
#include "idf_inbox.h"
#include "idf_log.h"
#include "idf_modem.h"
#include "idf_push.h"
#include "idf_util.h"
#include "pdulib.h"

static constexpr size_t MAX_PDU_HEX_CHARS = 600;
static constexpr size_t INDEX_QUEUE_MAX = 8;
static constexpr size_t OUT_SMS_QUEUE_MAX = 3;
static constexpr size_t SEEN_RING_MAX = 32;
static constexpr size_t CONCAT_SLOTS = 5;
static constexpr size_t CONCAT_PARTS = 10;
static_assert(CONCAT_PARTS <= 16, "receivedMask 只能记录最多 16 个长短信分段");
// 30s(Arduino 原值)在真实网络下太短：SIM 满被拒收的分段要等 SMSC 按分钟级
// 间隔重投，+CMTI 丢失时也要等轮询兜底，30s 一到就强拼出"[缺失分段]"残句。
// 直投仍可能遇到运营商分钟级重投，等待 15 分钟再带缺段标记转发。
static constexpr int64_t CONCAT_TIMEOUT_US = 15LL * 60LL * 1000LL * 1000LL;
// 存在未拼完的长短信时的加速轮询间隔：缺失分段若已落在 SIM 里(通知丢失)，
// 10s 内即可捞回，而不是等最长 60s 的常规轮询
static constexpr uint32_t CONCAT_HUNT_POLL_MS = 10000;
static constexpr uint32_t SMS_POLL_INTERVAL_MS = 60000;
static constexpr uint32_t SMS_STARTUP_POLL_INTERVAL_MS = 8000;
static constexpr uint32_t SMS_STARTUP_FAST_WINDOW_MS = 120000;
static constexpr uint8_t SMS_CNMI_REASSERT_EVERY = 5;
// 漫游/小区重选时 ML307 的 +CMGS 最终响应可能明显超过 20 秒；网页发送本身已异步，
// 这里允许等到 60 秒，避免 PDU 已交给模组却被固件过早判成失败。
static constexpr uint32_t SMS_SUBMIT_TIMEOUT_MS = 60000;

struct ConcatPart {
    std::string text;
    uint32_t fingerprint = 0;
};

struct ConcatSlot {
    uint16_t ref = 0;
    uint16_t receivedMask = 0;
    uint8_t total = 0;
    uint8_t received = 0;
    bool active = false;
    std::string sender;
    std::string timestamp;
    int64_t lastUs = 0;
    int64_t receivedUs = 0;
    uint32_t receivedEpoch = 0;
    bool needsSmsc = false;
    std::array<ConcatPart, CONCAT_PARTS> parts;
};

struct SmsIndex {
    int index = 0;
    int64_t receivedUs = 0;
};

struct OutgoingSmsJob {
    std::string phone;
    std::string text;
};

struct DecodedSms {
    std::string sender;
    std::string text;
    std::string timestamp;
    int concat[3] = {};
    int64_t receivedUs = 0;
    uint32_t receivedEpoch = 0;
};

// 已成功合并的长短信登记环：SIM 满时部分分段被拒收，SMSC 会整条重投(同 ref)。
struct ConcatDone {
    uint16_t ref = 0;
    uint8_t total = 0;
    bool used = false;
    std::array<uint32_t, CONCAT_PARTS> partFingerprint = {};
    int64_t doneUs = 0;
};

// 所有会影响接收正确性的运行态都归属具体卡槽；两个 worker 只共享无业务状态的代码和 PDU codec。
struct SmsContext {
    uint8_t slot = 0;
    SemaphoreHandle_t statusMutex = nullptr;
    SemaphoreHandle_t outMutex = nullptr;
    TaskHandle_t taskHandle = nullptr;
    IdfSmsStatus status;
    IdfSmsReplyCheck replyCheck;
    bool started = false;
    std::array<SmsIndex, INDEX_QUEUE_MAX> indexQueue = {};
    size_t indexCount = 0;
    std::array<OutgoingSmsJob, OUT_SMS_QUEUE_MAX> outQueue = {};
    size_t outHead = 0;
    size_t outCount = 0;
    std::array<uint32_t, SEEN_RING_MAX> seen = {};
    size_t seenNext = 0;
    size_t seenFilled = 0;
    std::array<ConcatSlot, CONCAT_SLOTS> concat = {};
    std::array<ConcatDone, 4> concatDone = {};
    size_t concatDoneNext = 0;
    std::string urcCarry;
    bool waitPdu = false;
    int64_t urcReceivedUs = 0;
    int64_t waitPduReceivedUs = 0;
    int64_t waitPduUntilUs = 0;
    bool backfillPending = false;
    bool cnmaErrorLogged = false;
    uint8_t nopduStallRounds = 0;
    uint8_t decodeStallRounds = 0;
    int64_t callStartedUs = 0;
    int64_t callLastUrcUs = 0;
    bool callNotified = false;
    bool callSawRing = false;
    std::string callNumber;
    std::atomic<bool> adminSmsBusy{false};
};

static SemaphoreHandle_t s_pdu_mutex = nullptr;
static PDU s_pdu(4096);
alignas(SmsContext) static unsigned char s_sms_storage[sizeof(SmsContext) * IDF_MODEM_SLOT_COUNT];
static SmsContext* s_sms = nullptr;
static std::atomic<bool> s_admin_reset_pending{false};

static SmsContext* context_for_slot(uint8_t slot)
{
    if (!s_sms || !idf_modem_slot_valid(slot)) return nullptr;
    return &s_sms[slot];
}

void idf_sms_init()
{
    if (s_sms) return;
    s_sms = reinterpret_cast<SmsContext*>(s_sms_storage);
    for (uint8_t slot = 0; slot < IDF_MODEM_SLOT_COUNT; ++slot) {
        new (&s_sms[slot]) SmsContext;
    }
}

static void cleanup_start_resources(SmsContext& ctx)
{
    if (ctx.statusMutex) {
        vSemaphoreDelete(ctx.statusMutex);
        ctx.statusMutex = nullptr;
    }
    if (ctx.outMutex) {
        vSemaphoreDelete(ctx.outMutex);
        ctx.outMutex = nullptr;
    }
    ctx.adminSmsBusy.store(false, std::memory_order_relaxed);
}

// SIM 存储索引 0 是合法值；必须严格解析，不能让乱码被宽松转换成 0。
static bool parse_sms_index_token(const char* start, size_t len, int& index)
{
    if (!start || len == 0 || len > 5) return false;
    int parsed = 0;
    for (size_t i = 0; i < len; ++i) {
        unsigned char ch = static_cast<unsigned char>(start[i]);
        if (!isdigit(ch)) return false;
        parsed = parsed * 10 + static_cast<int>(ch - '0');
    }
    index = parsed;
    return true;
}

static bool starts_with(const std::string& text, const char* prefix)
{
    return text.rfind(prefix, 0) == 0;
}

static bool is_hex_string(const std::string& line)
{
    if (line.empty() || line.size() > MAX_PDU_HEX_CHARS || (line.size() & 1U)) return false;
    for (char ch : line) {
        if (!isxdigit(static_cast<unsigned char>(ch))) return false;
    }
    return true;
}

// 仅供本次开机内的去重环使用，不要求跨重启稳定。逐字段混入长度和正文，既保留字段边界，
// 也避免为了计算 slot/sender/time/text 组合键而临时拼接一个大字符串。
static uint32_t fingerprint_add(uint32_t hash, const std::string& text)
{
    hash ^= static_cast<uint32_t>(text.size());
    hash *= 16777619U;
    for (unsigned char ch : text) {
        hash ^= ch;
        hash *= 16777619U;
    }
    return hash;
}

static uint32_t sms_fingerprint(uint8_t slot, const std::string& sender,
                                const std::string& timestamp, const std::string& text)
{
    uint32_t hash = (2166136261U ^ slot) * 16777619U;
    hash = fingerprint_add(hash, sender);
    hash = fingerprint_add(hash, timestamp);
    return fingerprint_add(hash, text);
}

static uint32_t concat_part_fingerprint(const std::string& sender, const std::string& text,
                                        const std::string& timestamp)
{
    uint32_t hash = fingerprint_add(2166136261U, sender);
    hash = fingerprint_add(hash, text);
    return fingerprint_add(hash, timestamp);
}

static bool seen_recently(SmsContext& ctx, uint32_t hash)
{
    // 用 filled 计数而不是排除 0 值：hash 恰为 0 的短信也要参与去重(对齐 Arduino)
    for (size_t i = 0; i < ctx.seenFilled; ++i) {
        if (ctx.seen[i] == hash) return true;
    }
    ctx.seen[ctx.seenNext] = hash;
    ctx.seenNext = (ctx.seenNext + 1) % SEEN_RING_MAX;
    if (ctx.seenFilled < SEEN_RING_MAX) ++ctx.seenFilled;
    return false;
}

static std::string canonical_phone(const std::string& num)
{
    size_t start = 0;
    while (start < num.size() && isspace(static_cast<unsigned char>(num[start]))) ++start;
    bool explicit_cn_prefix = start < num.size() && num[start] == '+';

    std::string out;
    out.reserve(num.size());
    for (size_t i = 0; i < num.size(); ++i) {
        unsigned char ch = static_cast<unsigned char>(num[i]);
        if (isdigit(ch)) out += static_cast<char>(ch);
    }
    if (out.rfind("86", 0) == 0 && out.size() > 2 && (explicit_cn_prefix || out.size() == 13)) {
        return out.substr(2);
    }
    return out;
}

static bool number_blacklisted(const std::string& list, const std::string& sender)
{
    if (list.empty()) return false;
    std::string target = canonical_phone(sender);
    if (target.empty()) return false;
    size_t pos = 0;
    while (pos <= list.size()) {
        size_t end = list.find('\n', pos);
        if (end == std::string::npos) end = list.size();
        std::string line = idf_util_trim_copy(list.substr(pos, end - pos));
        if (!line.empty() && canonical_phone(line) == target) {
            return true;
        }
        if (end == list.size()) break;
        pos = end + 1;
    }
    return false;
}

static bool is_valid_phone_number(const std::string& phone)
{
    if (phone.size() < 3 || phone.size() > 20) return false;
    for (size_t i = 0; i < phone.size(); ++i) {
        char ch = phone[i];
        if (i == 0 && ch == '+') continue;
        if (!isdigit(static_cast<unsigned char>(ch))) return false;
    }
    return true;
}

static bool is_admin_sender(const std::string& sender, const IdfSmsProcessView& cfg)
{
    if (cfg.adminPhone.empty()) return false;
    std::string admin = canonical_phone(cfg.adminPhone);
    return !admin.empty() && canonical_phone(sender) == admin;
}

struct AdminSmsTaskArg {
    uint8_t slot = 0;
    std::string target;
    std::string content;
    std::string command;
};

static void admin_sms_task(void* raw)
{
    AdminSmsTaskArg* arg = static_cast<AdminSmsTaskArg*>(raw);
    if (!arg) {
        vTaskDelete(nullptr);
        return;
    }
    SmsContext* ctx = context_for_slot(arg->slot);
    vTaskDelay(pdMS_TO_TICKS(250));
    std::string send_message;
    esp_err_t err = idf_sms_send_text(arg->slot, arg->target, arg->content, send_message);
    bool ok = (err == ESP_OK);
    std::string subject = ok ? "短信发送成功" : "短信发送失败";
    std::string body = "管理员命令执行结果:\n命令: " + arg->command +
                       "\n目标号码: " + arg->target +
                       "\n短信内容: " + arg->content +
                       "\n执行结果: " + (ok ? "成功" : "失败") +
                       "\n详情: " + send_message;
    idf_push_enqueue_email(subject.c_str(), body.c_str());
    delete arg;
    if (ctx) ctx->adminSmsBusy.store(false, std::memory_order_relaxed);
    vTaskDelete(nullptr);
}

static void admin_reset_task(void*)
{
    int64_t deadline = esp_timer_get_time() + 5LL * 1000LL * 1000LL;
    while ((idf_push_email_queue_depth() > 0 || idf_push_busy()) && esp_timer_get_time() < deadline) {
        vTaskDelay(pdMS_TO_TICKS(100));
    }
    idf_log_line("正在重启ESP32...");
    // RESET 命令语义是"模组+ESP32 都彻底重启"：确保模组断电后再重启 ESP，
    // 避免热启动快路径把它当健康模组沿用
    idf_modem_power_off_all_for_restart();
    esp_restart();
}

static bool process_admin_command(SmsContext& ctx, const std::string& sender, const std::string& text)
{
    std::string cmd = idf_util_trim_copy(text);
    if (!(starts_with(cmd, "SMS:") || cmd == "RESET")) return false;

    idf_logf("slot=%u 处理管理员命令 from=%s", ctx.slot, sender.c_str());
    if (starts_with(cmd, "SMS:")) {
        size_t first = cmd.find(':');
        size_t second = first == std::string::npos ? std::string::npos : cmd.find(':', first + 1);
        if (second == std::string::npos || second <= first + 1) {
            idf_logf("slot=%u SMS命令格式错误", ctx.slot);
            idf_push_enqueue_email("命令执行失败", "SMS命令格式错误，正确格式: SMS:号码:内容");
            return true;
        }
        std::string target = idf_util_trim_copy(cmd.substr(first + 1, second - first - 1));
        std::string content = idf_util_trim_copy(cmd.substr(second + 1));
        idf_logf("slot=%u 管理员命令目标号码: %s", ctx.slot, target.c_str());
        if (!is_valid_phone_number(target)) {
            idf_logf("slot=%u 目标号码非法，拒绝执行", ctx.slot);
            idf_push_enqueue_email("命令执行失败", "SMS命令目标号码非法（应为 3-20 位数字，可带 + 前缀）");
            return true;
        }
        if (content.empty() || content.size() > 300) {
            idf_logf("slot=%u 短信内容为空或超长，拒绝执行", ctx.slot);
            idf_push_enqueue_email("命令执行失败", "SMS命令内容为空或超过 300 字符");
            return true;
        }
        bool expected = false;
        if (!ctx.adminSmsBusy.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            idf_logf("slot=%u 已有管理员短信任务在执行，拒绝新的 SMS 命令", ctx.slot);
            idf_push_enqueue_email("命令执行失败", "已有管理员短信任务在执行，请稍后重试");
            return true;
        }

        AdminSmsTaskArg* arg = new (std::nothrow) AdminSmsTaskArg();
        if (!arg) {
            ctx.adminSmsBusy.store(false, std::memory_order_release);
            idf_logf("slot=%u 管理员短信任务创建失败：内存不足", ctx.slot);
            idf_push_enqueue_email("命令执行失败", "管理员短信任务创建失败：内存不足");
            return true;
        }
        arg->slot = ctx.slot;
        arg->target = target;
        arg->content = content;
        arg->command = cmd;
        if (xTaskCreate(admin_sms_task, "admin_sms", 4096, arg, 3, nullptr) != pdPASS) {
            delete arg;
            ctx.adminSmsBusy.store(false, std::memory_order_release);
            idf_logf("slot=%u 管理员短信任务创建失败", ctx.slot);
            idf_push_enqueue_email("命令执行失败", "管理员短信任务创建失败");
        }
        return true;
    }

    if (esp_timer_get_time() < 60LL * 1000LL * 1000LL) {
        idf_logf("slot=%u 设备刚启动，忽略RESET命令（防重启风暴）", ctx.slot);
        idf_push_enqueue_email("RESET已忽略", "设备启动不足60秒，已忽略RESET命令以防重启风暴。请稍后重试。");
        return true;
    }
    bool expected = false;
    if (!s_admin_reset_pending.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        idf_logf("slot=%u RESET任务已在执行，忽略重复命令", ctx.slot);
        idf_push_enqueue_email("RESET已忽略", "RESET任务已在执行，已忽略重复命令。");
        return true;
    }
    idf_logf("slot=%u 执行设备级 RESET 命令", ctx.slot);
    idf_push_enqueue_email("重启命令已执行", "收到RESET命令，即将重启模组和ESP32...");
    if (xTaskCreate(admin_reset_task, "admin_reset", 3072, nullptr, 3, nullptr) != pdPASS) {
        idf_log_line("RESET任务创建失败，直接重启ESP32");
        esp_restart();
    }
    return true;
}

static void update_status(SmsContext& ctx, bool receive_ready, bool got_sms)
{
    if (!ctx.statusMutex) return;
    if (xSemaphoreTake(ctx.statusMutex, pdMS_TO_TICKS(100)) != pdTRUE) return;
    ctx.status.receiveReady = receive_ready || ctx.status.receiveReady;
    if (got_sms) {
        ++ctx.status.total;
        ctx.status.lastSmsEpoch = static_cast<uint32_t>(time(nullptr));
    }
    xSemaphoreGive(ctx.statusMutex);
}

static void enqueue_index(SmsContext& ctx, int idx, int64_t received_us)
{
    if (idx < 0) {
        ctx.backfillPending = true;  // 非法索引：改由近期 CMGL 兜底(对齐 Arduino storedSmsPending)
        return;
    }
    for (size_t i = 0; i < ctx.indexCount; ++i) {
        if (ctx.indexQueue[i].index == idx) return;
    }
    if (ctx.indexCount < INDEX_QUEUE_MAX) {
        ctx.indexQueue[ctx.indexCount++] = {idx, received_us};
    } else {
        ctx.backfillPending = true;  // 队列满：丢索引但请求 CMGL 兜底，避免最长等 60s
    }
}

static bool pop_index(SmsContext& ctx, int& idx, int64_t& received_us)
{
    if (ctx.indexCount == 0) return false;
    idx = ctx.indexQueue[0].index;
    received_us = ctx.indexQueue[0].receivedUs;
    for (size_t i = 1; i < ctx.indexCount; ++i) ctx.indexQueue[i - 1] = ctx.indexQueue[i];
    --ctx.indexCount;
    return true;
}

static int outgoing_depth_locked(const SmsContext& ctx)
{
    return static_cast<int>(ctx.outCount);
}

static bool pop_outgoing_sms(SmsContext& ctx, OutgoingSmsJob& job)
{
    if (!ctx.outMutex || xSemaphoreTake(ctx.outMutex, pdMS_TO_TICKS(100)) != pdTRUE) return false;
    if (ctx.outCount == 0) {
        xSemaphoreGive(ctx.outMutex);
        return false;
    }
    job = std::move(ctx.outQueue[ctx.outHead]);
    ctx.outQueue[ctx.outHead] = OutgoingSmsJob();
    ctx.outHead = (ctx.outHead + 1) % OUT_SMS_QUEUE_MAX;
    --ctx.outCount;
    xSemaphoreGive(ctx.outMutex);
    return true;
}

static int parse_cmti_index(const std::string& line)
{
    size_t comma = line.rfind(',');
    if (comma == std::string::npos || comma + 1 >= line.size()) return -1;
    std::string num = idf_util_trim_copy(line.substr(comma + 1));
    int index = -1;
    if (!parse_sms_index_token(num.c_str(), num.size(), index)) return -1;
    return index;
}

static int parse_cmgl_index(const std::string& line)
{
    const char* p = strchr(line.c_str(), ':');
    if (!p) return -1;
    while (*++p && isspace(static_cast<unsigned char>(*p))) {}
    const char* start = p;
    while (*p && isdigit(static_cast<unsigned char>(*p))) ++p;
    int index = -1;
    if (!parse_sms_index_token(start, static_cast<size_t>(p - start), index)) return -1;
    while (*p && isspace(static_cast<unsigned char>(*p))) ++p;
    if (*p != ',' && *p != '\0') return -1;
    return index;
}

static void clear_concat_slot(ConcatSlot& slot)
{
    slot.active = false;
    slot.ref = 0;
    slot.receivedMask = 0;
    slot.total = 0;
    slot.received = 0;
    slot.sender.clear();
    slot.timestamp.clear();
    slot.lastUs = 0;
    slot.receivedUs = 0;
    slot.receivedEpoch = 0;
    slot.needsSmsc = false;
    for (auto& part : slot.parts) {
        part.text.clear();
        part.fingerprint = 0;
    }
}

static std::string assemble_concat(const ConcatSlot& slot)
{
    std::string text;
    for (int i = 0; i < slot.total && i < static_cast<int>(CONCAT_PARTS); ++i) {
        if ((slot.receivedMask & (1U << i)) != 0) {
            text += slot.parts[i].text;
        } else {
            // 超时强制合并时标记缺口，收件人能看出内容不完整(对齐 Arduino)
            text += "[缺失分段";
            unsigned number = static_cast<unsigned>(i + 1);
            if (number >= 10) text.push_back(static_cast<char>('0' + number / 10));
            text.push_back(static_cast<char>('0' + number % 10));
            text += "]";
        }
    }
    return text;
}

static void process_sms_content(SmsContext& ctx, const char* sender_raw, const char* text_raw,
                                const char* timestamp_raw, int64_t received_us,
                                uint32_t received_epoch, bool require_smsc = false);

// 已成功合并的长短信登记环：SIM 满时部分分段被拒收，SMSC 会整条重投(同 ref)。
// 槽位合并完成后迟到的重复分段若不识别，会重新开槽并在超时后拼出一条
// "[缺失分段]"幽灵消息。按 (ref,总段数,发件人/正文/时间戳指纹) 组合匹配才丢弃，
// 避免运营商短期复用 ref 时误杀真实新消息。
static constexpr int64_t CONCAT_DONE_TTL_US = 10LL * 60 * 1000 * 1000;

static bool concat_recently_done(SmsContext& ctx, int ref, const std::string& sender, int total,
                                 int part, const std::string& text, const std::string& timestamp)
{
    int64_t now = esp_timer_get_time();
    uint32_t fingerprint = concat_part_fingerprint(sender, text, timestamp);
    for (const auto& d : ctx.concatDone) {
        if (!d.used || now - d.doneUs > CONCAT_DONE_TTL_US) continue;
        if (d.ref == ref && d.total == total && d.partFingerprint[part - 1] == fingerprint) {
            return true;
        }
    }
    return false;
}

static void record_concat_done(SmsContext& ctx, const ConcatSlot& slot)
{
    ConcatDone& d = ctx.concatDone[ctx.concatDoneNext];
    ctx.concatDoneNext = (ctx.concatDoneNext + 1) % ctx.concatDone.size();
    d.used = true;
    d.ref = slot.ref;
    d.total = slot.total;
    d.partFingerprint.fill(0);
    for (int i = 0; i < slot.total && i < static_cast<int>(CONCAT_PARTS); ++i) {
        if ((slot.receivedMask & (1U << i)) != 0) {
            d.partFingerprint[i] = slot.parts[i].fingerprint;
        }
    }
    d.doneUs = esp_timer_get_time();
}

static ConcatSlot& find_concat_slot(SmsContext& ctx, int ref, const std::string& sender, int total)
{
    int64_t now = esp_timer_get_time();
    for (auto& slot : ctx.concat) {
        if (slot.active && slot.ref == ref && slot.sender == sender && slot.total == total) return slot;
    }
    for (auto& slot : ctx.concat) {
        if (!slot.active || now - slot.lastUs > CONCAT_TIMEOUT_US) {
            clear_concat_slot(slot);
            slot.active = true;
            slot.ref = ref;
            slot.total = total;
            slot.sender = sender;
            slot.lastUs = now;
            return slot;
        }
    }
    auto oldest = std::min_element(ctx.concat.begin(), ctx.concat.end(),
        [](const ConcatSlot& a, const ConcatSlot& b) { return a.lastUs < b.lastUs; });
    // 槽位全忙被挤占：已收到的分段不能悄悄扔掉——分段可能已从 SIM 删除，丢了
    // 就再也拿不回。与超时路径一致，先按现状合并转发(缺口有"[缺失分段]"标记)
    {
        std::string partial = assemble_concat(*oldest);
        if (!partial.empty()) {
            idf_logf("slot=%u 长短信槽位耗尽，先合并已收 %d/%d 段再复用槽位",
                     ctx.slot, oldest->received, oldest->total);
            process_sms_content(ctx, oldest->sender.c_str(), partial.c_str(), oldest->timestamp.c_str(), -1, 0);
        }
    }
    clear_concat_slot(*oldest);
    oldest->active = true;
    oldest->ref = ref;
    oldest->total = total;
    oldest->sender = sender;
    oldest->lastUs = now;
    return *oldest;
}

static void process_sms_content(SmsContext& ctx, const char* sender_raw, const char* text_raw,
                                const char* timestamp_raw, int64_t received_us,
                                uint32_t received_epoch, bool require_smsc)
{
    std::string sender = sender_raw ? sender_raw : "";
    std::string text = text_raw ? text_raw : "";
    std::string timestamp = timestamp_raw ? timestamp_raw : "";
    const IdfSmsProcessView cfg = idf_config_get_sms_process_view();

    if (number_blacklisted(cfg.numberBlackList, sender)) {
        idf_logf("slot=%u 短信发件人 %s 在黑名单中，已忽略", ctx.slot, sender.c_str());
        return;
    }

    // 去重键用 PDU 原始时间戳(双通道 URC+CMGL 收到的是同一原始值)；
    // 展示/转发用本地可读时间，未同步时才退回原始数字串
    uint32_t hash = sms_fingerprint(ctx.slot, sender, timestamp, text);
    if (seen_recently(ctx, hash)) {
        idf_logf("slot=%u 重复短信 %s 已忽略", ctx.slot, sender.c_str());
        return;
    }

    // 去重后的完整短信参与验证，不消费短信，后续收件箱/转发行为保持不变。
    if (ctx.statusMutex && xSemaphoreTake(ctx.statusMutex, portMAX_DELAY) == pdTRUE) {
        ctx.replyCheck.observe(sender.c_str(), text.c_str(), esp_timer_get_time(), received_us,
                               received_epoch, static_cast<uint32_t>(time(nullptr)), require_smsc);
        xSemaphoreGive(ctx.statusMutex);
    }

    if (is_admin_sender(sender, cfg)) {
        idf_logf("slot=%u 收到管理员短信，检查命令...", ctx.slot);
        if (process_admin_command(ctx, sender, text)) {
            update_status(ctx, true, true);
            return;
        }
    }

    std::string display_ts = idf_util_format_epoch_local(static_cast<uint32_t>(time(nullptr)), cfg.tzOffsetMin);
    if (display_ts.empty()) display_ts = timestamp;

    uint32_t id = idf_inbox_add(ctx.slot, sender.c_str(), text.c_str(), display_ts.c_str());
    update_status(ctx, true, true);
    // 默认日志展示完整发件人(便于确认是谁)，正文仍不落盘(在收发短信页查看)
    idf_logf("slot=%u 收到短信 id=%u 来自 %s，入队转发中",
             ctx.slot, static_cast<unsigned>(id), sender.c_str());
    if (!idf_push_enqueue_forward(ctx.slot, sender.c_str(), text.c_str(), display_ts.c_str(), id)) {
        idf_logf("slot=%u 转发入口队列已满，短信 id=%u 保持未转发，可稍后手动重发",
                 ctx.slot, static_cast<unsigned>(id));
    }
}

static void handle_decoded_pdu(SmsContext& ctx, const DecodedSms& sms)
{
    const char* sender = sms.sender.c_str();
    const char* text = sms.text.c_str();
    const char* ts = sms.timestamp.c_str();
    int ref = sms.concat[0];
    int part = sms.concat[1];
    int total = sms.concat[2];

    if (total > 1 && part > 0) {
        if (total > static_cast<int>(CONCAT_PARTS) || part > total) {
            idf_logf("slot=%u 长短信分段参数超限 part=%d total=%d，按单条处理",
                     ctx.slot, part, total);
            process_sms_content(ctx, sender, text, ts, -1, 0);
            return;
        }
        if (concat_recently_done(ctx, ref, sender ? sender : "", total, part,
                                 text ? text : "", ts ? ts : "")) {
            idf_logf("slot=%u 长短信分段 ref=%d %d/%d 与近期已合并消息重复，忽略",
                     ctx.slot, ref, part, total);
            return;
        }
        ConcatSlot& concat_slot = find_concat_slot(ctx, ref, sender ? sender : "", total);
        int idx = part - 1;
        uint16_t part_mask = static_cast<uint16_t>(1U << idx);
        if ((concat_slot.receivedMask & part_mask) != 0 &&
            concat_slot.parts[idx].text != (text ? text : "")) {
            // 8 位引用号可能被运营商快速复用；同一分段号正文已变化时不能继续混拼旧消息。
            idf_logf("slot=%u 长短信引用号 ref=%d 已复用，丢弃旧的不完整 %d/%d 段并重新归组",
                     ctx.slot, ref, concat_slot.received, concat_slot.total);
            clear_concat_slot(concat_slot);
            concat_slot.active = true;
            concat_slot.ref = ref;
            concat_slot.total = total;
            concat_slot.sender = sender ? sender : "";
            concat_slot.lastUs = esp_timer_get_time();
        }
        if ((concat_slot.receivedMask & part_mask) == 0) {
            concat_slot.receivedMask |= part_mask;
            concat_slot.parts[idx].text = text ? text : "";
            concat_slot.parts[idx].fingerprint = concat_part_fingerprint(
                concat_slot.sender, concat_slot.parts[idx].text, ts ? ts : "");
            // 混合直推和存储片段时，保留最早到达证据及最早 SMSC 时间。
            // 未知到达时间片段要求 SMSC 校验，不能覆盖已经确认的旧片段。
            if (concat_slot.received == 0) {
                concat_slot.receivedUs = sms.receivedUs;
                concat_slot.needsSmsc = sms.receivedUs == 0;
            } else {
                if (sms.receivedUs > 0) {
                    concat_slot.receivedUs = concat_slot.receivedUs > 0
                        ? std::min(concat_slot.receivedUs, sms.receivedUs) : sms.receivedUs;
                }
                concat_slot.needsSmsc = concat_slot.needsSmsc || sms.receivedUs == 0;
            }
            concat_slot.receivedEpoch = concat_slot.received == 0
                ? sms.receivedEpoch : std::min(concat_slot.receivedEpoch, sms.receivedEpoch);
            concat_slot.received++;
            concat_slot.lastUs = esp_timer_get_time();
            if (concat_slot.timestamp.empty()) concat_slot.timestamp = ts ? ts : "";
            idf_logf("slot=%u 收到长短信分段 ref=%d %d/%d", ctx.slot, ref, part, total);
        }
        if (concat_slot.received >= concat_slot.total) {
            record_concat_done(ctx, concat_slot);
            std::string full = assemble_concat(concat_slot);
            process_sms_content(ctx, concat_slot.sender.c_str(), full.c_str(), concat_slot.timestamp.c_str(),
                                concat_slot.receivedUs, concat_slot.receivedEpoch, concat_slot.needsSmsc);
            clear_concat_slot(concat_slot);
        }
        return;
    }

    process_sms_content(ctx, sender, text, ts, sms.receivedUs, sms.receivedEpoch);
}

static bool decode_pdu_line(SmsContext& ctx, const std::string& line, int64_t received_us = 0)
{
    if (!is_hex_string(line)) return false;
    if (!s_pdu_mutex) return false;
    // 解码器忙(网页发短信正在编码)时重试而不是放弃：返回 false 会被调用方
    // 当成"PDU 损坏"——直推短信直接丢失，存储短信还会被误删
    bool locked = false;
    for (int attempt = 0; attempt < 3 && !locked; ++attempt) {
        locked = xSemaphoreTake(s_pdu_mutex, pdMS_TO_TICKS(2000)) == pdTRUE;
        if (!locked) idf_logf("slot=%u PDU 解码器忙，等待重试...", ctx.slot);
    }
    if (!locked) return false;
    DecodedSms decoded;
    if (!s_pdu.decodePDU(line.c_str())) {
        xSemaphoreGive(s_pdu_mutex);
        idf_logf("slot=%u PDU 解析失败(hex=%u)", ctx.slot, static_cast<unsigned>(line.size()));
        return false;
    }
    decoded.sender = s_pdu.getSender();
    decoded.text = s_pdu.getText();
    decoded.timestamp = s_pdu.getTimeStamp();
    decoded.receivedUs = received_us;
    decoded.receivedEpoch = idf_sms_timestamp_epoch(decoded.timestamp.c_str());
    int* concat = s_pdu.getConcatInfo();
    if (concat) {
        decoded.concat[0] = concat[0];
        decoded.concat[1] = concat[1];
        decoded.concat[2] = concat[2];
    }
    xSemaphoreGive(s_pdu_mutex);
    // PDU 解码器是全局复用对象；业务处理放到锁外，避免入库/转发时阻塞网页发短信编码。
    handle_decoded_pdu(ctx, decoded);
    return true;
}

// 是否有等待补齐的长短信槽位(仅 sms_task 内访问，无需加锁)
static bool concat_waiting(const SmsContext& ctx)
{
    for (const auto& slot : ctx.concat) {
        if (slot.active && slot.received < slot.total) return true;
    }
    return false;
}

static void expire_concat_slots(SmsContext& ctx)
{
    int64_t now = esp_timer_get_time();
    for (auto& slot : ctx.concat) {
        if (!slot.active || now - slot.lastUs <= CONCAT_TIMEOUT_US) continue;
        std::string full = assemble_concat(slot);
        if (!full.empty()) {
            idf_logf("slot=%u 长短信等待超时，已合并现有 %d/%d 段",
                     ctx.slot, slot.received, slot.total);
            process_sms_content(ctx, slot.sender.c_str(), full.c_str(), slot.timestamp.c_str(), -1, 0);
        }
        clear_concat_slot(slot);
    }
}

// ===== 来电通知 =====
// +CLIP 是不阻塞的主路径；缺失时等 3 秒再用 CLCC 查询一次，兼容不主动上报 CLIP 的固件。
static constexpr int64_t CALL_GAP_US = 30LL * 1000 * 1000;
static constexpr int64_t CALL_CLCC_DELAY_US = 3LL * 1000 * 1000;

static std::string parse_clip_number(const std::string& line)
{
    size_t q1 = line.find('"');
    if (q1 == std::string::npos) return {};
    size_t q2 = line.find('"', q1 + 1);
    if (q2 == std::string::npos) return {};
    return line.substr(q1 + 1, q2 - q1 - 1);
}

static std::string query_clcc_number(const SmsContext& ctx)
{
    std::string resp;
    if (idf_modem_send_at(ctx.slot, "AT+CLCC", 1500, resp) != ESP_OK) return {};
    size_t pos = 0;
    while (pos < resp.size()) {
        size_t eol = resp.find('\n', pos);
        if (eol == std::string::npos) eol = resp.size();
        std::string line = idf_util_trim_copy(resp.substr(pos, eol - pos));
        int direction = -1;
        int state = -1;
        if (sscanf(line.c_str(), "+CLCC: %*d,%d,%d", &direction, &state) == 2 &&
            direction == 1 && (state == 4 || state == 5)) {
            return parse_clip_number(line);
        }
        pos = eol + 1;
    }
    return {};
}

static void notify_incoming_call(const SmsContext& ctx, const std::string& number)
{
    if (!idf_config_call_notify_enabled(ctx.slot)) return;
    // 黑名单同时作用于来电：命中则不通知(未知号码无从匹配，照常通知)
    if (!number.empty()) {
        const IdfSmsProcessView cfg = idf_config_get_sms_process_view();
        if (number_blacklisted(cfg.numberBlackList, number)) {
            idf_logf("slot=%u 来电 %s 在黑名单中，已忽略", ctx.slot, number.c_str());
            return;
        }
    }
    std::string num = number.empty() ? std::string("未知号码") : number;
    std::string body = "来电：" + num;
    std::string display_ts = idf_util_format_epoch_local(static_cast<uint32_t>(time(nullptr)),
                                                         idf_config_get_tz_offset());
    idf_logf("slot=%u 来电通知：%s，入队转发中", ctx.slot, num.c_str());
    if (!idf_push_enqueue_forward(ctx.slot, num.c_str(), body.c_str(), display_ts.c_str(), 0)) {
        idf_logf("slot=%u 转发入口队列已满，来电通知未能入队", ctx.slot);
    }
}

static void handle_call_urc_line(SmsContext& ctx, const std::string& line)
{
    int64_t now = esp_timer_get_time();
    if (ctx.callLastUrcUs == 0 || now - ctx.callLastUrcUs > CALL_GAP_US) {
        ctx.callStartedUs = now;
        ctx.callNotified = false;
        ctx.callSawRing = false;
        ctx.callNumber.clear();
    }
    ctx.callLastUrcUs = now;
    if (starts_with(line, "+CLIP:")) {
        std::string number = parse_clip_number(line);
        if (!number.empty()) ctx.callNumber = number;
        if (!ctx.callNotified && !ctx.callNumber.empty()) {
            ctx.callNotified = true;
            notify_incoming_call(ctx, ctx.callNumber);
        }
    } else {
        ctx.callSawRing = true;
    }
}

static void flush_pending_call_notify(SmsContext& ctx)
{
    if (!ctx.callSawRing || ctx.callNotified ||
        esp_timer_get_time() - ctx.callStartedUs < CALL_CLCC_DELAY_US) return;
    ctx.callNotified = true;
    ctx.callNumber = query_clcc_number(ctx);
    notify_incoming_call(ctx, ctx.callNumber);
}

static void process_urc_line(SmsContext& ctx, const std::string& raw)
{
    std::string line = idf_util_trim_copy(raw);
    if (line.empty()) return;
    if (starts_with(line, "@RXUS:")) {
        ctx.urcReceivedUs = strtoll(line.c_str() + 6, nullptr, 10);
        return;
    }

    // 来电通知：RING/+CLIP 优先处理并返回，独立于短信 PDU 等待逻辑
    if (line == "RING" || starts_with(line, "+CLIP:")) {
        handle_call_urc_line(ctx, line);
        return;
    }

    if (ctx.waitPdu) {
        if (starts_with(line, "+CMT:")) {
            // 窗口内又来一条 +CMT：前一条的 PDU 已丢(直推短信不落存储无从兜底)，
            // 重臂窗口至少保住新的这条，并记一笔便于排查
            idf_logf("slot=%u 等待直推 PDU 时又收到 +CMT，前一条可能丢失", ctx.slot);
            size_t comma = line.rfind(',');
            idf_logf("slot=%u 收到直推短信头，TPDU 长度=%d", ctx.slot,
                     comma == std::string::npos ? -1 : atoi(line.c_str() + comma + 1));
            ctx.waitPduReceivedUs = ctx.urcReceivedUs;
        ctx.waitPduUntilUs = esp_timer_get_time() + 3LL * 1000LL * 1000LL;
            return;
        }
        if (starts_with(line, "+CMTI:")) {
            enqueue_index(ctx, parse_cmti_index(line), ctx.urcReceivedUs);
            return;
        }
        if (decode_pdu_line(ctx, line, ctx.waitPduReceivedUs)) {
            ctx.waitPdu = false;
            // ML307R 的 +CNMI 文档要求直推短信通过 +CNMA 确认，确认后模组才会可靠地
            // 继续上报同一条长短信的后续分段。存储读取(CMGR/CMGL)不走此分支。
            std::string ack_resp;
            if (idf_modem_send_at(ctx.slot, "AT+CNMA=0", 1500, ack_resp) == ESP_OK) {
                ctx.cnmaErrorLogged = false;
            } else if (!ctx.cnmaErrorLogged) {
                ctx.cnmaErrorLogged = true;
                idf_logf("slot=%u 直推短信确认失败，后续分段可能由 SIM 存储轮询补收", ctx.slot);
            }
            return;
        }
        if (line != "OK" && line != "ERROR") {
            idf_logf("slot=%u 等待直推 PDU 时收到非 PDU 行，已关闭接收窗口", ctx.slot);
            ctx.waitPdu = false;
            ctx.backfillPending = true;  // 若这条其实是被截断的 PDU，兜底轮询还能救回存储中的副本
        }
        return;
    }

    if (starts_with(line, "+CMT:")) {
        size_t comma = line.rfind(',');
        idf_logf("slot=%u 收到直推短信头，TPDU 长度=%d", ctx.slot,
                 comma == std::string::npos ? -1 : atoi(line.c_str() + comma + 1));
        ctx.waitPdu = true;
        ctx.waitPduReceivedUs = ctx.urcReceivedUs;
        ctx.waitPduUntilUs = esp_timer_get_time() + 3LL * 1000LL * 1000LL;  // 3s 窗口
    } else if (starts_with(line, "+CMTI:")) {
        enqueue_index(ctx, parse_cmti_index(line), ctx.urcReceivedUs);
    }
}

// +CMT 后迟迟等不到 PDU 行时关闭窗口，避免后续无关的十六进制样 URC 被误当短信解码
static void expire_wait_pdu_window(SmsContext& ctx)
{
    if (ctx.waitPdu && esp_timer_get_time() > ctx.waitPduUntilUs) {
        ctx.waitPdu = false;
        ctx.backfillPending = true;  // 直推丢失的短信大概率还在存储里，请求兜底补收
    }
}

static void process_urc_text(SmsContext& ctx, const std::string& text)
{
    ctx.urcCarry += text;
    size_t pos = 0;
    while (true) {
        size_t nl = ctx.urcCarry.find('\n', pos);
        if (nl == std::string::npos) break;
        std::string line = ctx.urcCarry.substr(pos, nl - pos);
        process_urc_line(ctx, line);
        pos = nl + 1;
    }
    if (pos > 0) ctx.urcCarry.erase(0, pos);
    if (ctx.urcCarry.size() > MAX_PDU_HEX_CHARS + 128) {
        ctx.urcCarry.clear();
        ctx.waitPdu = false;
    }
}

static bool extract_first_stored_pdu(const std::string& resp, const char* header, std::string& pdu_line)
{
    pdu_line.clear();
    size_t h = resp.find(header);
    if (h == std::string::npos) return false;
    size_t pos = resp.find('\n', h);
    if (pos == std::string::npos) return false;
    ++pos;
    while (pos < resp.size()) {
        size_t nl = resp.find('\n', pos);
        if (nl == std::string::npos) nl = resp.size();
        std::string line = idf_util_trim_copy(resp.substr(pos, nl - pos));
        pos = nl + 1;
        if (line.empty() || line == "OK" || line == "ERROR" ||
            starts_with(line, "+") || starts_with(line, "AT")) {
            continue;
        }
        pdu_line = line;
        return true;
    }
    return false;
}

static void fetch_stored_sms_by_index(SmsContext& ctx, int idx, int64_t received_us)
{
    if (idx < 0) return;
    char cmd[24];
    snprintf(cmd, sizeof(cmd), "AT+CMGR=%d", idx);
    std::string resp;
    esp_err_t err = idf_modem_send_at(ctx.slot, cmd, 3000, resp);
    bool has_header = resp.find("+CMGR:") != std::string::npos;
    std::string pdu_line;
    bool has_pdu_line = extract_first_stored_pdu(resp, "+CMGR:", pdu_line);
    bool decoded = has_pdu_line && decode_pdu_line(ctx, pdu_line, received_us);
    if (has_header) {
        if (decoded) {
            snprintf(cmd, sizeof(cmd), "AT+CMGD=%d", idx);
            std::string ignored;
            idf_modem_send_at(ctx.slot, cmd, 2000, ignored);
        } else if (has_pdu_line) {
            idf_logf("slot=%u PDU 无法解析(索引=%d)，保留 SIM 记录等待后续重试", ctx.slot, idx);
            ctx.backfillPending = true;
        } else {
            idf_logf("slot=%u 索引=%d 的短信缺少 PDU 行，等待 CMGL 兜底", ctx.slot, idx);
            ctx.backfillPending = true;
        }
    } else if (err != ESP_OK) {
        // 不重排队：空槽位(补收已删)/+CMS ERROR 会形成无限重试环，把收发队列全部饿死。
        // 改为请求一次 CMGL 兜底——真有短信它一定还在存储列表里。
        ctx.backfillPending = true;
    }
}

static void backfill_stored_sms(SmsContext& ctx, bool announce)
{
    std::string resp;
    esp_err_t err = idf_modem_send_at(ctx.slot, "AT+CMGL=4", 4000, resp);
    if (err != ESP_OK && resp.find("+CMGL:") == std::string::npos) {
        if (announce) idf_logf("slot=%u SIM 暂存短信读取失败: %s", ctx.slot, esp_err_to_name(err));
        return;
    }

    // 每轮最多处理 5 条就归还控制权：处理间隙主循环能继续排空 URC、检查长短信超时、
    // 发送网页短信。剩余的通过 ctx.backfillPending 在 ~500ms 后接着处理。
    constexpr int BATCH_MAX = 5;
    // 连续多轮"有缺 PDU 行的条目且毫无进展"的计数：损坏记录若只重试不删除，
    // 会形成每 ~200ms 一次 AT+CMGL 的永久轮询，把整个 AT 通道饿死
    int nopdu_idx[BATCH_MAX];
    int decode_fail_idx[BATCH_MAX];
    int nopdu_count = 0;
    int decode_fail_count = 0;
    int processed = 0;
    int handled = 0;
    bool more_left = false;
    size_t pos = 0;
    while (pos < resp.size()) {
        size_t nl = resp.find('\n', pos);
        if (nl == std::string::npos) nl = resp.size();
        std::string line = idf_util_trim_copy(resp.substr(pos, nl - pos));
        pos = nl + 1;
        if (!starts_with(line, "+CMGL:")) continue;
        if (handled >= BATCH_MAX) {
            more_left = true;
            break;
        }

        int idx = parse_cmgl_index(line);
        std::string pdu_line;
        bool has_pdu_line = false;
        while (pos < resp.size()) {
            nl = resp.find('\n', pos);
            if (nl == std::string::npos) nl = resp.size();
            std::string candidate = idf_util_trim_copy(resp.substr(pos, nl - pos));
            if (starts_with(candidate, "+CMGL:")) break;  // PDU 缺失：别把下一条的头当 PDU 吞掉
            pos = nl + 1;
            if (candidate.empty() || candidate == "OK" || candidate == "ERROR" ||
                starts_with(candidate, "+") || starts_with(candidate, "AT")) {
                continue;
            }
            if (!candidate.empty()) {
                pdu_line = candidate;
                has_pdu_line = true;
                break;
            }
        }

        bool decoded = has_pdu_line && decode_pdu_line(ctx, pdu_line);
        if (decoded) ++processed;
        if (idx >= 0 && decoded) {
            char cmd[24];
            snprintf(cmd, sizeof(cmd), "AT+CMGD=%d", idx);
            std::string ignored;
            idf_modem_send_at(ctx.slot, cmd, 2000, ignored);
            ++handled;
        } else if (idx >= 0 && has_pdu_line) {
            idf_logf("slot=%u PDU 无法解析(索引=%d)，保留 SIM 记录等待后续重试", ctx.slot, idx);
            if (decode_fail_count < BATCH_MAX) decode_fail_idx[decode_fail_count++] = idx;
        } else if (idx >= 0) {
            idf_logf("slot=%u CMGL 索引=%d 缺少 PDU 行，暂不删除", ctx.slot, idx);
            if (nopdu_count < BATCH_MAX) nopdu_idx[nopdu_count++] = idx;
        }
    }

    if (nopdu_count > 0) {
        if (handled > 0 || processed > 0) {
            // 本轮有进展：缺行大概率是 8KB 响应截断，删掉已处理的条目后下轮自然恢复
            ctx.nopduStallRounds = 0;
            ctx.backfillPending = true;
        } else if (++ctx.nopduStallRounds >= 3) {
            // 连续 3 轮原地踏步 = 记录本身损坏，删除释放存储，终结轮询死循环
            for (int i = 0; i < nopdu_count; ++i) {
                idf_logf("slot=%u 索引=%d 连续多轮缺少 PDU 行(记录损坏)，删除以恢复正常轮询",
                         ctx.slot, nopdu_idx[i]);
                char cmd[24];
                snprintf(cmd, sizeof(cmd), "AT+CMGD=%d", nopdu_idx[i]);
                std::string ignored;
                idf_modem_send_at(ctx.slot, cmd, 2000, ignored);
            }
            ctx.nopduStallRounds = 0;
        } else {
            ctx.backfillPending = true;
        }
    } else {
        ctx.nopduStallRounds = 0;
    }

    if (decode_fail_count > 0) {
        if (handled > 0 || processed > 0) {
            ctx.decodeStallRounds = 0;
            ctx.backfillPending = true;
        } else if (++ctx.decodeStallRounds >= 3) {
            for (int i = 0; i < decode_fail_count; ++i) {
                idf_logf("slot=%u 索引=%d 连续多轮 PDU 解析失败(记录损坏)，删除以恢复正常轮询",
                         ctx.slot, decode_fail_idx[i]);
                char cmd[24];
                snprintf(cmd, sizeof(cmd), "AT+CMGD=%d", decode_fail_idx[i]);
                std::string ignored;
                idf_modem_send_at(ctx.slot, cmd, 2000, ignored);
            }
            ctx.decodeStallRounds = 0;
        } else {
            ctx.backfillPending = true;
        }
    } else {
        ctx.decodeStallRounds = 0;
    }

    if (more_left) ctx.backfillPending = true;
    if (processed > 0) idf_logf("slot=%u SIM 暂存短信处理并删除 %d 条", ctx.slot, processed);
}

static void sms_task(void* raw)
{
    SmsContext& ctx = *static_cast<SmsContext*>(raw);
    uint64_t last_poll_ms = 0;  // 64 位毫秒：32 位在 49.7 天回绕会重进启动提频窗口
    uint64_t start_ms = static_cast<uint64_t>(esp_timer_get_time()) / 1000ULL;
    uint32_t poll_count = 0;
    uint8_t reassert_step = 0;  // 1=CSMS, 2=CMGF, 3=CPMS, 4=CNMI；拆帧避免一次堆多条 AT
    bool backfill_after_reassert = false;
    bool configured = false;
    bool first_backfill = true;

    while (true) {
        std::string urc;
        if (idf_modem_take_urc(ctx.slot, urc)) process_urc_text(ctx, urc);
        expire_wait_pdu_window(ctx);
        flush_pending_call_notify(ctx);
        expire_concat_slots(ctx);

        if (ctx.statusMutex && xSemaphoreTake(ctx.statusMutex, 0) == pdTRUE) {
            ctx.status.stackFreeBytes = uxTaskGetStackHighWaterMark(nullptr);
            xSemaphoreGive(ctx.statusMutex);
        }

        // eSIM 逻辑通道期间只收 URC，不插入 CPMS、CMGL 或短信发送 AT。
        if (idf_modem_esim_operation_active(ctx.slot)) {
            idf_modem_wait_event(ctx.slot, 500);
            continue;
        }

        IdfModemReadiness modem = idf_modem_get_readiness(ctx.slot);
        if (modem.atReady && !configured) {
            std::string ignored;
            idf_modem_send_at(ctx.slot, "AT+CSMS=1", 1200, ignored);
            idf_modem_send_at(ctx.slot, "AT+CMGF=0", 1200, ignored);
            idf_modem_send_at(ctx.slot, "AT+CNMI=2,2,0,0,0", 1200, ignored);
            configured = true;
            update_status(ctx, true, false);
            idf_logf("slot=%u 短信接收(PDU/存储通知)已配置", ctx.slot);
        }

        if (configured && modem.modemReady) {
            if (reassert_step != 0) {
                std::string ignored;
                if (reassert_step == 1) {
                    idf_modem_send_at(ctx.slot, "AT+CSMS=1", 1200, ignored);
                    reassert_step = 2;
                } else if (reassert_step == 2) {
                    idf_modem_send_at(ctx.slot, "AT+CMGF=0", 1200, ignored);
                    reassert_step = 3;
                } else if (reassert_step == 3) {
                    // CPMS 也要重申：模组自发复位后存储会回落固件默认，
                    // SM 容量为 0 的 eSIM 上接收会静默死亡(只 CMGF/CNMI 救不回来)
                    idf_modem_reassert_sms_storage(ctx.slot);
                    reassert_step = 4;
                } else {
                    idf_modem_send_at(ctx.slot, "AT+CNMI=2,2,0,0,0", 1200, ignored);
                    reassert_step = 0;
                    backfill_after_reassert = true;
                }
                vTaskDelay(pdMS_TO_TICKS(120));
                continue;
            }

            if (backfill_after_reassert) {
                backfill_after_reassert = false;
                backfill_stored_sms(ctx, false);
                last_poll_ms = static_cast<uint64_t>(esp_timer_get_time()) / 1000ULL;
                vTaskDelay(pdMS_TO_TICKS(200));
                continue;
            }

            int idx = -1;
            int64_t received_us = 0;
            if (pop_index(ctx, idx, received_us)) {
                fetch_stored_sms_by_index(ctx, idx, received_us);
                vTaskDelay(pdMS_TO_TICKS(200));
                continue;
            }

            OutgoingSmsJob out;
            if (pop_outgoing_sms(ctx, out)) {
                std::string send_message;
                idf_logf("slot=%u 网页短信出队发送: %s len=%u", ctx.slot,
                         out.phone.c_str(), static_cast<unsigned>(out.text.size()));
                idf_sms_send_text(ctx.slot, out.phone, out.text, send_message);
                vTaskDelay(pdMS_TO_TICKS(200));
                continue;
            }

            if (ctx.backfillPending) {
                ctx.backfillPending = false;
                backfill_stored_sms(ctx, false);
                last_poll_ms = static_cast<uint64_t>(esp_timer_get_time()) / 1000ULL;
                vTaskDelay(pdMS_TO_TICKS(200));
                continue;
            }

            uint64_t now_ms = static_cast<uint64_t>(esp_timer_get_time()) / 1000ULL;
            uint32_t interval = (now_ms - start_ms < SMS_STARTUP_FAST_WINDOW_MS)
                ? SMS_STARTUP_POLL_INTERVAL_MS
                : SMS_POLL_INTERVAL_MS;
            // 长短信缺段等待期加速轮询：缺失分段可能已静默落在 SIM 里
            // (+CMTI 混进大响应被截断丢失)，主动补收而不是干等常规轮询
            if (concat_waiting(ctx) && interval > CONCAT_HUNT_POLL_MS) interval = CONCAT_HUNT_POLL_MS;
            if (first_backfill || now_ms - last_poll_ms >= interval) {
                if (!first_backfill && (poll_count % SMS_CNMI_REASSERT_EVERY) == 0) {
                    reassert_step = 1;
                    ++poll_count;
                    last_poll_ms = now_ms;
                    continue;
                }
                backfill_stored_sms(ctx, first_backfill);
                first_backfill = false;
                last_poll_ms = now_ms;
                ++poll_count;
            }
        }

        // 事件等待：URC 到达/网页短信入队立即唤醒；无事件 500ms 兜底(与原轮询节奏一致)
        idf_modem_wait_event(ctx.slot, 500);
    }
}

esp_err_t idf_sms_start(uint8_t slot)
{
    SmsContext* context = context_for_slot(slot);
    if (!context) return ESP_ERR_INVALID_ARG;
    SmsContext& ctx = *context;
    if (ctx.started) return ESP_OK;
    ctx.slot = slot;
    cleanup_start_resources(ctx);
    if (!s_pdu_mutex) s_pdu_mutex = xSemaphoreCreateMutex();
    ctx.statusMutex = xSemaphoreCreateMutex();
    ctx.outMutex = xSemaphoreCreateMutex();
    if (!s_pdu_mutex || !ctx.statusMutex || !ctx.outMutex) {
        cleanup_start_resources(ctx);
        return ESP_ERR_NO_MEM;
    }
    const char* task_name = slot == 0 ? "idf_sms0" : "idf_sms1";
    BaseType_t ok = xTaskCreate(sms_task, task_name, 8192, &ctx, 3, &ctx.taskHandle);
    if (ok != pdPASS) {
        cleanup_start_resources(ctx);
        ctx.taskHandle = nullptr;
        return ESP_ERR_NO_MEM;
    }
    ctx.started = true;
    return ESP_OK;
}

// —— 长短信发送辅助 ——
// 保守判定"纯 GSM7"：仅 ASCII 可打印字符与 \r\n。其余(含中文)按 UCS2 处理。
// 保守的意义：切分假设 GSM7 但实际落入 UCS2 会导致该段超长编码失败，反之只是少装几个字
static bool sms_text_basic_gsm7(const std::string& text)
{
    for (unsigned char c : text) {
        if (c == '\r' || c == '\n') continue;
        if (c < 0x20 || c >= 0x7F) return false;
        // 反引号不属于 GSM 03.38；PDU 编码器会为整段改用 UCS2。
        // 若这里误判为 GSM7，71~160 字符的混合文本会直到编码阶段才超长失败。
        if (c == '`') return false;
    }
    return true;
}

// 按每段配额切分，不打断 UTF-8 字符。gsm7 按 septet 计(扩展表字符占 2)，
// UCS2 按 UTF-16 码元计(BMP 外字符即代理对占 2)
static std::vector<std::string> sms_split_parts(const std::string& text, bool gsm7, size_t per_part)
{
    std::vector<std::string> parts;
    size_t pos = 0;
    while (pos < text.size()) {
        size_t units = 0;
        size_t end = pos;
        while (end < text.size()) {
            unsigned char lead = static_cast<unsigned char>(text[end]);
            size_t clen = (lead >= 0xF0) ? 4 : (lead >= 0xE0) ? 3 : (lead >= 0xC0) ? 2 : 1;
            if (end + clen > text.size()) clen = text.size() - end;
            size_t u;
            if (gsm7) u = (clen == 1 && strchr("[]{}\\^~|", text[end])) ? 2 : 1;
            else u = (clen == 4) ? 2 : 1;
            if (units + u > per_part) break;
            units += u;
            end += clen;
        }
        if (end == pos) break;  // 防御：不应发生(单字符不会超过任何配额)
        parts.push_back(text.substr(pos, end - pos));
        pos = end;
    }
    return parts;
}

// 编码单个(或长短信某段)PDU；csms=0 表示普通单条
static esp_err_t sms_encode_one(const std::string& phone, const std::string& body,
                                uint16_t csms, uint8_t total, uint8_t part,
                                std::string& sms_pdu, int& pdu_len, std::string& message)
{
    if (!s_pdu_mutex || xSemaphoreTake(s_pdu_mutex, pdMS_TO_TICKS(3000)) != pdTRUE) {
        message = "PDU 编码器忙";
        return ESP_ERR_TIMEOUT;
    }
    s_pdu.setSCAnumber();
    pdu_len = s_pdu.encodePDU(phone.c_str(), body.c_str(), csms, total, part);
    if (pdu_len >= 0) sms_pdu.assign(s_pdu.getSMS(), strlen(s_pdu.getSMS()));
    xSemaphoreGive(s_pdu_mutex);
    if (pdu_len < 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "PDU 编码失败(%d)", pdu_len);
        message = buf;
        return ESP_FAIL;
    }
    return ESP_OK;
}

static std::string sms_response_error_line(const std::string& response)
{
    static const char* kTokens[] = {"+CMS ERROR", "+CME ERROR", "ERROR"};
    size_t pos = 0;
    while (pos < response.size()) {
        size_t end = response.find('\n', pos);
        if (end == std::string::npos) end = response.size();
        std::string line = idf_util_trim_copy(response.substr(pos, end - pos));
        for (const char* token : kTokens) {
            if (line.find(token) != std::string::npos) return line;
        }
        pos = end + 1;
    }
    return {};
}

static std::string sms_submit_failure_detail(esp_err_t err, const std::string& response)
{
    std::string error_line = sms_response_error_line(response);
    if (!error_line.empty()) return error_line;
    if (err == ESP_ERR_TIMEOUT) {
        if (response.find('>') != std::string::npos) {
            return "短信提交超时（已提交 PDU，但模组未返回 +CMGS/OK，发送结果未知）";
        }
        return "等待短信输入提示符超时（PDU 尚未提交）";
    }
    if (err == ESP_OK) return "模组响应格式异常";
    return esp_err_to_name(err);
}

static void reply_check_submitted(uint8_t slot)
{
    SmsContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->statusMutex) return;
    xSemaphoreTake(ctx->statusMutex, portMAX_DELAY);
    ctx->replyCheck.submitted(esp_timer_get_time(), static_cast<uint32_t>(time(nullptr)));
    xSemaphoreGive(ctx->statusMutex);
}

esp_err_t idf_sms_send_text(uint8_t slot, const std::string& phone_raw, const std::string& text_raw,
                            std::string& message, bool track_reply)
{
    message.clear();
    if (!context_for_slot(slot)) {
        message = "非法模组 slot";
        return ESP_ERR_INVALID_ARG;
    }
    // 任一发送失败都取消 matcher，避免遗留快速回复或等待状态。
    struct ReplySendGuard {
        uint8_t slot;
        bool cancel;
        ~ReplySendGuard() { if (cancel) idf_sms_reply_check_cancel(slot); }
    } reply_guard{slot, track_reply};
    std::string phone = idf_util_trim_copy(phone_raw);
    std::string text = idf_util_trim_copy(text_raw);
    if (!is_valid_phone_number(phone) || text.empty() || text.size() > 300) {
        message = "号码或内容无效";
        return ESP_ERR_INVALID_ARG;
    }
    IdfModemReadiness modem = idf_modem_get_readiness(slot);
    if (!modem.modemReady) {
        message = "模组尚未注册网络";
        return ESP_ERR_INVALID_STATE;
    }

    // 单条容量：GSM7 160 septet / UCS2 70 码元；超出则切分长短信。
    // 分段配额扣除 7 字节 UDH(16 位参考号)：GSM7 152 septet / UCS2 66 码元
    bool gsm7 = sms_text_basic_gsm7(text);
    std::vector<std::string> parts = sms_split_parts(text, gsm7, gsm7 ? 160 : 70);
    if (parts.empty()) {
        message = "号码或内容无效";
        return ESP_ERR_INVALID_ARG;
    }
    if (parts.size() > 1) parts = sms_split_parts(text, gsm7, gsm7 ? 152 : 66);

    uint16_t csms = 0;
    if (parts.size() > 1) {
        // 16 位参考号从熵源取初值后自增，0 保留为"非长短信"
        static std::atomic<uint16_t> s_csms_ref{static_cast<uint16_t>(esp_timer_get_time() & 0x7FFF)};
        csms = s_csms_ref.fetch_add(1, std::memory_order_relaxed);
        if (csms == 0) csms = s_csms_ref.fetch_add(1, std::memory_order_relaxed);
    }

    esp_err_t err = ESP_OK;
    // 热插拔/模组内部协议栈复位可能把 CMGF 恢复成文本模式。文本模式同样会返回
    // `>`，但随后把十六进制 PDU 当普通正文处理，最终表现为无 +CMGS 的超时。
    // 每次发送前重申一次 PDU 模式，代价很小，却能直接封住这一类静默状态漂移。
    std::string mode_resp;
    err = idf_modem_send_at(slot, "AT+CMGF=0", 3000, mode_resp);
    if (err != ESP_OK) {
        message = "无法恢复短信 PDU 模式: " + sms_submit_failure_detail(err, mode_resp);
        idf_sent_add(slot, phone.c_str(), text.c_str(), false);
        idf_logf("slot=%u 发送前短信模式配置失败，触发模组硬重启: %s", slot, message.c_str());
        idf_modem_request_reset(slot, true);
        return err;
    }

    for (size_t i = 0; i < parts.size(); ++i) {
        std::string sms_pdu;
        int pdu_len = -1;
        // encodePDU 要求 csms/numparts/partnumber 三者同为 0(单条)或同非 0(分段)
        err = sms_encode_one(phone, parts[i], csms,
                             csms ? static_cast<uint8_t>(parts.size()) : 0,
                             csms ? static_cast<uint8_t>(i + 1) : 0,
                             sms_pdu, pdu_len, message);
        if (err != ESP_OK) {
            idf_sent_add(slot, phone.c_str(), text.c_str(), false);
            return err;
        }
        char cmd[32];
        snprintf(cmd, sizeof(cmd), "AT+CMGS=%d", pdu_len);
        std::string resp;
        err = idf_modem_send_pdu(slot, cmd, sms_pdu.c_str(), SMS_SUBMIT_TIMEOUT_MS, resp,
                                 track_reply && i + 1 == parts.size() ? reply_check_submitted : nullptr);
        if (err != ESP_OK) {
            std::string detail = sms_submit_failure_detail(err, resp);
            if (parts.size() > 1) {
                char prefix[64];
                snprintf(prefix, sizeof(prefix), "长短信第 %u/%u 段发送失败: ",
                         static_cast<unsigned>(i + 1), static_cast<unsigned>(parts.size()));
                message = std::string(prefix) + detail;
            } else {
                message = detail;
            }
            if (err == ESP_ERR_TIMEOUT) {
                // Ctrl+Z 后超时属于结果不确定态，不能自动重发（否则可能重复扣费/重复送达）；
                // 但必须把模组拉回干净状态，保证下一条短信不被残留 CMGS 会话污染。
                idf_logf("slot=%u 短信提交超时，自动硬重启模组恢复短信通道；本条不自动重发", slot);
                idf_modem_request_reset(slot, true);
            }
            idf_sent_add(slot, phone.c_str(), text.c_str(), false);
            idf_logf("slot=%u 网页发送短信失败: %s", slot, message.c_str());
            return err;
        }
        // 分段之间稍作停顿，给模组喘息并让 URC 有机会被处理
        if (i + 1 < parts.size()) vTaskDelay(pdMS_TO_TICKS(300));
    }

    reply_guard.cancel = false;
    idf_sent_add(slot, phone.c_str(), text.c_str(), true);
    if (parts.size() > 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "长短信发送成功(共 %u 段)", static_cast<unsigned>(parts.size()));
        message = buf;
    } else {
        message = "短信发送成功";
    }
    idf_logf("slot=%u 网页发送短信成功: %s len=%u parts=%u", slot, phone.c_str(),
             static_cast<unsigned>(text.size()), static_cast<unsigned>(parts.size()));
    return ESP_OK;
}

esp_err_t idf_sms_enqueue_outgoing(uint8_t slot, const std::string& phone_raw,
                                   const std::string& text_raw, std::string& message)
{
    SmsContext* context = context_for_slot(slot);
    if (!context) {
        message = "非法模组 slot";
        return ESP_ERR_INVALID_ARG;
    }
    SmsContext& ctx = *context;
    std::string phone = idf_util_trim_copy(phone_raw);
    std::string text = idf_util_trim_copy(text_raw);
    message.clear();
    if (phone.empty()) {
        message = "错误：请输入目标号码";
        return ESP_ERR_INVALID_ARG;
    }
    if (!is_valid_phone_number(phone)) {
        message = "错误：目标号码非法（3-20 位数字，可带 + 前缀）";
        return ESP_ERR_INVALID_ARG;
    }
    if (text.empty()) {
        message = "错误：请输入短信内容";
        return ESP_ERR_INVALID_ARG;
    }
    if (text.size() > 300) {
        message = "错误：短信内容超过 300 字符";
        return ESP_ERR_INVALID_SIZE;
    }
    if (!ctx.outMutex || xSemaphoreTake(ctx.outMutex, pdMS_TO_TICKS(100)) != pdTRUE) {
        message = "发送队列繁忙，请稍后再试";
        return ESP_ERR_TIMEOUT;
    }
    if (ctx.outCount >= OUT_SMS_QUEUE_MAX) {
        xSemaphoreGive(ctx.outMutex);
        message = "发送队列已满，请稍后再试";
        return ESP_ERR_NO_MEM;
    }

    size_t tail = (ctx.outHead + ctx.outCount) % OUT_SMS_QUEUE_MAX;
    ctx.outQueue[tail].phone = phone;
    ctx.outQueue[tail].text = text;
    ++ctx.outCount;
    int depth = outgoing_depth_locked(ctx);
    xSemaphoreGive(ctx.outMutex);

    idf_modem_signal_event(slot);  // 唤醒对应短信任务立即出队发送，不等轮询周期
    idf_logf("slot=%u 网页短信已入队，当前待发=%d", slot, depth);
    message = "已加入发送队列，请稍后在已发送列表查看结果";
    return ESP_OK;
}

int idf_sms_outgoing_queue_depth(uint8_t slot)
{
    SmsContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->outMutex || xSemaphoreTake(ctx->outMutex, pdMS_TO_TICKS(100)) != pdTRUE) return 0;
    int n = outgoing_depth_locked(*ctx);
    xSemaphoreGive(ctx->outMutex);
    return n;
}

IdfSmsStatus idf_sms_get_status(uint8_t slot)
{
    IdfSmsStatus copy;
    SmsContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->statusMutex) return copy;
    if (xSemaphoreTake(ctx->statusMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        copy = ctx->status;
        xSemaphoreGive(ctx->statusMutex);
    }
    return copy;
}

bool idf_sms_reply_check_begin(uint8_t slot, const std::string& sender_pattern,
                               const std::string& body_pattern)
{
    SmsContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->statusMutex) return false;
    xSemaphoreTake(ctx->statusMutex, portMAX_DELAY);
    bool ok = ctx->replyCheck.begin(idf_config_translate_perl_classes(sender_pattern),
                                    idf_config_translate_perl_classes(body_pattern));
    xSemaphoreGive(ctx->statusMutex);
    return ok;
}

void idf_sms_reply_check_arm(uint8_t slot, int timeout_seconds)
{
    SmsContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->statusMutex) return;
    xSemaphoreTake(ctx->statusMutex, portMAX_DELAY);
    ctx->replyCheck.arm(esp_timer_get_time(), timeout_seconds);
    xSemaphoreGive(ctx->statusMutex);
}

int idf_sms_reply_check_poll(uint8_t slot)
{
    SmsContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->statusMutex) return -1;
    xSemaphoreTake(ctx->statusMutex, portMAX_DELAY);
    auto state = ctx->replyCheck.poll(esp_timer_get_time());
    xSemaphoreGive(ctx->statusMutex);
    return state == IdfSmsReplyCheck::State::Matched ? 1 :
           state == IdfSmsReplyCheck::State::Waiting ? 0 : -1;
}

void idf_sms_reply_check_cancel(uint8_t slot)
{
    SmsContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->statusMutex) return;
    xSemaphoreTake(ctx->statusMutex, portMAX_DELAY);
    ctx->replyCheck.cancel();
    xSemaphoreGive(ctx->statusMutex);
}
