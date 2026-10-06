#include "idf_modem.h"
#include "idf_modem_sampling.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <atomic>

#include "driver/gpio.h"
#include "driver/uart.h"
#include "esp_log.h"
#include "esp_random.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "idf_config.h"
#include "idf_log.h"
#include "idf_util.h"
#include "nvs.h"
#include "lwip/inet.h"
#include "lwip/sockets.h"

static const char* TAG = "idf_modem";

#define MODEM_LOG_LINE(ctx, message) \
    idf_logf("slot=%u %s", (ctx).hardware.slot, (message))
#define MODEM_LOGF(ctx, format, ...) \
    idf_logf("slot=%u " format, (ctx).hardware.slot, __VA_ARGS__)

struct ModemHardware {
    uint8_t slot;
    uart_port_t uart;
    gpio_num_t tx;
    gpio_num_t rx;
    gpio_num_t en;
    const char* task_name;
};

static constexpr ModemHardware MODEM_HARDWARE[IDF_MODEM_SLOT_COUNT] = {
    {0, UART_NUM_1, GPIO_NUM_3, GPIO_NUM_4, GPIO_NUM_5, "idf_modem0"},
    {1, UART_NUM_0, GPIO_NUM_6, GPIO_NUM_7, GPIO_NUM_10, "idf_modem1"},
};

static constexpr int MODEM_BAUD = 115200;
static constexpr int UART_RX_BUF = 4096;
static constexpr int MODEM_POWERDOWN_MS = 1200;
static constexpr int MODEM_POWERUP_MIN_MS = 1500;
static constexpr int MODEM_POWERUP_MAX_MS = 6000;
static constexpr uint32_t CELLULAR_KEEPALIVE_MIN_BYTES = 48UL * 1024UL;
static constexpr uint32_t CELLULAR_HTTP_TIMEOUT_MS = 90000UL;
static constexpr uint32_t CELLULAR_PDP_READY_TIMEOUT_MS = 12000UL;
static constexpr uint32_t MODEM_DATA_MODE_RETRY_GAP_MS = 10000UL;
static constexpr uint8_t MODEM_DATA_MODE_RETRY_MAX = 3;
// SIM 锁状态与身份字段首次缺失时快速补采，连续无变化后逐步退避；既照顾启动较慢的卡，
// 也避免不支持 ICCID/COPS/CNUM 等命令的卡长期高频占用 AT 通道。
static constexpr uint32_t MODEM_RETRY_DELAYS_MS[] = {
    30000UL, 60000UL, 120000UL, 300000UL, 600000UL,
};
static constexpr size_t MODEM_RETRY_DELAY_COUNT =
    sizeof(MODEM_RETRY_DELAYS_MS) / sizeof(MODEM_RETRY_DELAYS_MS[0]);

static constexpr uint32_t modem_retry_delay_ms(uint8_t level)
{
    size_t index = level < MODEM_RETRY_DELAY_COUNT ? level : MODEM_RETRY_DELAY_COUNT - 1;
    return MODEM_RETRY_DELAYS_MS[index];
}
static_assert(modem_retry_delay_ms(0) == 30000UL, "首次补采应在 30 秒后执行");
static_assert(modem_retry_delay_ms(99) == 600000UL, "补采退避上限应为 10 分钟");
// 展示采样受 AT 空闲门控；注册后先采一次，缺失身份后台退避重试，手动刷新绕过采样间隔。
static constexpr uint32_t SIGNAL_INTERVAL_WEB_MS = 10000UL;
static constexpr uint32_t SIGNAL_DETAIL_INTERVAL_WEB_MS = 30000UL;
static constexpr uint32_t SIM_CHECK_INTERVAL_MS = 15000UL;  // SIM 热插拔检测轮询间隔
static constexpr int64_t WEB_POLL_ACTIVE_WINDOW_US = 15LL * 1000LL * 1000LL;
static constexpr size_t URC_BUFFER_MAX = 8192;

struct ModemContext {
    explicit ModemContext(const ModemHardware& value) : hardware(value) {}

    const ModemHardware& hardware;
    SemaphoreHandle_t at_mutex = nullptr;
    SemaphoreHandle_t status_mutex = nullptr;
    SemaphoreHandle_t urc_mutex = nullptr;
    // UART 驱动事件队列：URC 一到就唤醒模组任务抓取，替代固定 500ms 轮询延时
    QueueHandle_t uart_evt_queue = nullptr;
    // 模组事件信号：新 URC 入缓冲/网页短信入队时唤醒短信任务，消除轮询等待
    SemaphoreHandle_t event_sem = nullptr;
    TaskHandle_t task_handle = nullptr;
    IdfModemStatus status;
    std::string urc_buffer;
    // 普通 AT 响应与异步 URC 共用 UART。按行持续提取短信/来电 URC，避免 +CMT 头和
    // 后续 PDU 落在两个读取周期时，PDU 被误吞进下一条 AT 响应。
    std::string uart_line_carry;
    int64_t uart_line_received_us = 0;
    bool uart_wait_cmt_pdu = false;
    int64_t uart_wait_cmt_until_us = 0;
    bool started = false;
    std::atomic<int> reset_request{0};  // 1=AT软重启，2=EN硬重启；由模组任务执行
    bool data_mode_retry_pending = false;
    uint8_t data_mode_retry_count = 0;
    TickType_t next_data_mode_retry = 0;
    std::atomic<int> logged_sms_storage_code{-1};  // -1=未知，0=MT，1=ME，2=SM
    bool identity_static_attempted = false;
    bool identity_network_attempted = false;
    std::atomic<int64_t> last_web_poll_us{-WEB_POLL_ACTIVE_WINDOW_US};
    IdfModemSampling status_sampling;
    std::atomic<uint32_t> esim_operation_depth{0};
    std::atomic<int> sim_unlock_request{0};  // 1=重查/自动 PIN，2=用户确认后的单次 PUK
    std::string last_pin_attempt_key;
    bool sms_storage_pending = false;
    bool reinit_pending = false;
};

static ModemContext s_modems[IDF_MODEM_SLOT_COUNT] = {
    ModemContext(MODEM_HARDWARE[0]),
    ModemContext(MODEM_HARDWARE[1]),
};
static std::atomic<void (*)(uint8_t)> s_sim_identity_hook{nullptr};

static ModemContext* context_for_slot(uint8_t slot)
{
    if (!idf_modem_slot_valid(slot)) return nullptr;
    return &s_modems[slot];
}

static void signal_event(ModemContext& ctx)
{
    if (ctx.event_sem) xSemaphoreGive(ctx.event_sem);
}

static void cleanup_start_resources(ModemContext& ctx)
{
    if (ctx.at_mutex) {
        vSemaphoreDelete(ctx.at_mutex);
        ctx.at_mutex = nullptr;
    }
    if (ctx.status_mutex) {
        vSemaphoreDelete(ctx.status_mutex);
        ctx.status_mutex = nullptr;
    }
    if (ctx.urc_mutex) {
        vSemaphoreDelete(ctx.urc_mutex);
        ctx.urc_mutex = nullptr;
    }
    if (ctx.event_sem) {
        vSemaphoreDelete(ctx.event_sem);
        ctx.event_sem = nullptr;
    }
    ctx.urc_buffer.clear();
    ctx.uart_line_carry.clear();
    ctx.uart_wait_cmt_pdu = false;
    ctx.uart_wait_cmt_until_us = 0;
}

static bool parse_long_token(const std::string& value, long& out)
{
    std::string text = idf_util_trim_copy(value);
    if (text.empty()) return false;
    errno = 0;
    char* end = nullptr;
    long parsed = strtol(text.c_str(), &end, 10);
    if (end == text.c_str() || errno == ERANGE) return false;
    while (*end && isspace(static_cast<unsigned char>(*end))) ++end;
    if (*end != '\0') return false;
    out = parsed;
    return true;
}

static bool parse_comma_longs(const std::string& text, long* values, int max_values, int& count)
{
    count = 0;
    size_t start = 0;
    while (start < text.size() && count < max_values) {
        size_t comma = text.find(',', start);
        std::string part = text.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        part = idf_util_trim_copy(part);
        long value = 0;
        if (part.empty() || !parse_long_token(part, value)) return false;
        values[count++] = value;
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return count > 0;
}

// tick 回绕安全的超时窗口：`now + timeout` 在 49.7 天(1000Hz tick)回绕时会溢出，
// 使 `now < deadline` 永远为假，所有 AT 读循环瞬间退出。统一改用无符号差值判断。
struct TickDeadline {
    TickType_t start;
    TickType_t span;
    explicit TickDeadline(uint32_t ms) : start(xTaskGetTickCount()), span(pdMS_TO_TICKS(ms)) {}
    bool expired() const { return static_cast<TickType_t>(xTaskGetTickCount() - start) >= span; }
    void restart(uint32_t ms) { start = xTaskGetTickCount(); span = pdMS_TO_TICKS(ms); }
};

// AT 最终结果码：1=OK，-1=ERROR/+CMS ERROR/+CME ERROR(27.005/27.007 定义的失败终结码)，0=未结束。
// 不要求末尾必须再跟 CRLF：部分长命令的最后一个 UART 块可能恰好止于 "OK"。
static int at_final_result(const std::string& resp)
{
    size_t pos = 0;
    while (pos < resp.size()) {
        size_t end = resp.find_first_of("\r\n", pos);
        if (end == std::string::npos) end = resp.size();
        std::string line = idf_util_trim_copy(resp.substr(pos, end - pos));
        if (line == "OK") return 1;
        if (line == "ERROR" || line.rfind("+CMS ERROR", 0) == 0 ||
            line.rfind("+CME ERROR", 0) == 0) return -1;
        pos = end;
        while (pos < resp.size() && (resp[pos] == '\r' || resp[pos] == '\n')) ++pos;
    }
    return 0;
}

static bool has_cmgs_result(const std::string& resp)
{
    size_t pos = resp.find("+CMGS:");
    if (pos == std::string::npos) return false;
    pos += strlen("+CMGS:");
    while (pos < resp.size() && isspace(static_cast<unsigned char>(resp[pos]))) ++pos;
    if (pos >= resp.size() || !isdigit(static_cast<unsigned char>(resp[pos]))) return false;
    while (pos < resp.size() && isdigit(static_cast<unsigned char>(resp[pos]))) ++pos;
    return true;
}

// 取包含 token 的那一整行(不同 URC 混在同一段响应里时不能只取"第一有效行")
static std::string line_containing(const std::string& resp, size_t pos)
{
    size_t start = resp.rfind('\n', pos);
    start = (start == std::string::npos) ? 0 : start + 1;
    size_t end = resp.find('\n', pos);
    if (end == std::string::npos) end = resp.size();
    std::string line = resp.substr(start, end - start);
    size_t s = 0;
    while (s < line.size() && isspace(static_cast<unsigned char>(line[s]))) ++s;
    size_t e = line.size();
    while (e > s && isspace(static_cast<unsigned char>(line[e - 1]))) --e;
    return line.substr(s, e - s);
}

static bool line_is_payload(const std::string& line, const char* cmd)
{
    if (line.empty() || line == "OK" || line == "ERROR") return false;
    if (cmd && line == cmd) return false;
    return true;
}

static std::string first_payload_line(const std::string& resp, const char* cmd = nullptr)
{
    size_t pos = 0;
    while (pos < resp.size()) {
        size_t end = resp.find('\n', pos);
        if (end == std::string::npos) end = resp.size();
        std::string line = idf_util_trim_copy(resp.substr(pos, end - pos));
        if (line_is_payload(line, cmd)) return line;
        pos = end + 1;
    }
    return {};
}

static std::string first_digits_line(const std::string& resp, size_t min_len, size_t max_len)
{
    size_t pos = 0;
    while (pos < resp.size()) {
        size_t end = resp.find('\n', pos);
        if (end == std::string::npos) end = resp.size();
        std::string line = idf_util_trim_copy(resp.substr(pos, end - pos));
        bool digits = !line.empty();
        for (char ch : line) digits = digits && isdigit(static_cast<unsigned char>(ch));
        if (digits && line.size() >= min_len && line.size() <= max_len) return line;
        pos = end + 1;
    }
    return {};
}

static std::string first_digit_run(const std::string& resp, size_t min_len, size_t max_len)
{
    size_t start = std::string::npos;
    for (size_t i = 0; i <= resp.size(); ++i) {
        bool digit = i < resp.size() && isdigit(static_cast<unsigned char>(resp[i]));
        if (digit && start == std::string::npos) {
            start = i;
        } else if (!digit && start != std::string::npos) {
            size_t len = i - start;
            if (len >= min_len && len <= max_len) return resp.substr(start, len);
            start = std::string::npos;
        }
    }
    return {};
}

static bool is_iccid_text(const std::string& value)
{
    if (value.size() < 15 || value.size() > 22) return false;
    bool seen_digit = false;
    bool padding = false;
    uint8_t padding_count = 0;
    for (char ch : value) {
        if (isdigit(static_cast<unsigned char>(ch))) {
            if (padding) return false;
            seen_digit = true;
        } else if (ch == 'F' || ch == 'f') {
            if (!seen_digit) return false;
            padding = true;
            if (++padding_count > 1) return false;
        } else {
            return false;
        }
    }
    return seen_digit;
}

static bool is_imei_text(const std::string& value)
{
    if (value.size() < 14 || value.size() > 17) return false;
    for (char ch : value) {
        if (!isdigit(static_cast<unsigned char>(ch))) return false;
    }
    return true;
}

static std::string parse_imei_response(const std::string& resp)
{
    size_t pos = 0;
    while (pos < resp.size()) {
        size_t end = resp.find('\n', pos);
        if (end == std::string::npos) end = resp.size();
        std::string line = idf_util_trim_copy(resp.substr(pos, end - pos));
        std::string candidate;
        if (line.rfind("+CGSN:", 0) == 0 || line.rfind("+GSN:", 0) == 0) {
            candidate = idf_util_trim_copy(line.substr(line.find(':') + 1));
            if (candidate.size() >= 2U && candidate.front() == '"' && candidate.back() == '"') {
                candidate = candidate.substr(1, candidate.size() - 2U);
            }
        } else if (is_imei_text(line)) {
            candidate = line;
        }
        if (is_imei_text(candidate)) return candidate;
        pos = end + 1U;
    }
    return {};
}

static bool is_imsi_text(const std::string& value)
{
    if (value.size() < 14 || value.size() > 16) return false;
    for (char ch : value) {
        if (!isdigit(static_cast<unsigned char>(ch))) return false;
    }
    return true;
}

static std::string first_quoted(const std::string& line, size_t start = 0)
{
    size_t q1 = line.find('"', start);
    if (q1 == std::string::npos) return {};
    size_t q2 = line.find('"', q1 + 1);
    if (q2 == std::string::npos) return {};
    return line.substr(q1 + 1, q2 - q1 - 1);
}

static void set_phase(ModemContext& ctx, const char* phase)
{
    if (xSemaphoreTake(ctx.status_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        ctx.status.phase = phase;
        if (strcmp(phase, "powering") == 0 || strcmp(phase, "failed") == 0) {
            ctx.status.atReady = false;
            ctx.status.modemReady = false;
        } else if (strcmp(phase, "at_ready") == 0 || strcmp(phase, "registering") == 0 ||
                   strcmp(phase, "sim_locked") == 0) {
            ctx.status.atReady = true;
            ctx.status.modemReady = false;
        } else if (strcmp(phase, "ready") == 0) {
            ctx.status.atReady = true;
            ctx.status.modemReady = true;
        }
        xSemaphoreGive(ctx.status_mutex);
    }
}

static void update_status(ModemContext& ctx, const IdfModemStatus& patch, bool identity = false,
                          bool signal = false, bool sim = false)
{
    if (xSemaphoreTake(ctx.status_mutex, pdMS_TO_TICKS(500)) != pdTRUE) return;
    ctx.status.started = patch.started || ctx.status.started;
    bool phase_patch = !patch.phase.empty() && patch.phase != "off";
    if (phase_patch) {
        ctx.status.phase = patch.phase;
        if (patch.phase == "powering" || patch.phase == "failed") {
            ctx.status.atReady = false;
            ctx.status.modemReady = false;
        } else if (patch.phase == "at_ready" || patch.phase == "registering" || patch.phase == "sim_locked") {
            ctx.status.atReady = true;
            ctx.status.modemReady = false;
        } else if (patch.phase == "ready") {
            ctx.status.atReady = true;
            ctx.status.modemReady = true;
        }
    }
    if (patch.atReady) ctx.status.atReady = true;
    bool carries_registration = patch.ceregStat >= 0 || patch.modemReady ||
                                patch.phase == "ready" || patch.phase == "registering" ||
                                patch.phase == "sim_locked" || patch.phase == "failed";
    if (carries_registration) ctx.status.modemReady = patch.modemReady;
    if (patch.ceregStat >= 0) ctx.status.ceregStat = patch.ceregStat;
    if (patch.csq >= 0) ctx.status.csq = patch.csq;
    if (patch.csq >= 0 || patch.ber != 99) ctx.status.ber = patch.ber;
    if (patch.rsrp != 999) ctx.status.rsrp = patch.rsrp;
    if (patch.rsrq != 999) ctx.status.rsrq = patch.rsrq;
    if (patch.sinr != 999) ctx.status.sinr = patch.sinr;
    if (!patch.mfr.empty()) ctx.status.mfr = patch.mfr;
    if (!patch.model.empty()) ctx.status.model = patch.model;
    if (!patch.fwver.empty()) ctx.status.fwver = patch.fwver;
    if (!patch.imei.empty() && is_imei_text(patch.imei)) ctx.status.imei = patch.imei;
    if (!patch.iccid.empty() && is_iccid_text(patch.iccid)) ctx.status.iccid = patch.iccid;
    if (!patch.imsi.empty() && is_imsi_text(patch.imsi)) ctx.status.imsi = patch.imsi;
    if (!patch.operatorName.empty()) ctx.status.operatorName = patch.operatorName;
    if (!patch.apnSim.empty()) ctx.status.apnSim = patch.apnSim;
    if (!patch.cellIp.empty()) ctx.status.cellIp = patch.cellIp;
    if (!patch.phone.empty()) ctx.status.phone = patch.phone;
    if (sim) {
        ctx.status.simState = patch.simState;
        ctx.status.simCredentialMatched = patch.simCredentialMatched;
        ctx.status.simUnlockMessage = patch.simUnlockMessage;
    }
    if (identity) ctx.status.identityFresh = true;
    if (signal) ctx.status.signalFresh = true;
    xSemaphoreGive(ctx.status_mutex);
}

static bool startup_info_complete(ModemContext& ctx)
{
    bool complete = false;
    if (xSemaphoreTake(ctx.status_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        complete = ctx.status.signalFresh &&
                   ctx.status.identityFresh &&
                   !ctx.status.mfr.empty() &&
                   !ctx.status.model.empty() &&
                   !ctx.status.fwver.empty() &&
                   is_imei_text(ctx.status.imei) &&
                   is_iccid_text(ctx.status.iccid) &&
                   is_imsi_text(ctx.status.imsi) &&
                   !ctx.status.operatorName.empty() &&
                   ctx.identity_network_attempted;
        xSemaphoreGive(ctx.status_mutex);
    }
    return complete;
}

// ICCID/运营商等字段可能被 SIM 或网络长期拒绝返回；完成一轮采样后即可进入 ready，
// 缺失字段仍由 startup_info_complete() 驱动后台补采，不能让概览永远停在“读取中”。
static IdfModemStatus get_status(ModemContext& ctx)
{
    IdfModemStatus copy;
    if (!ctx.status_mutex) return copy;
    if (xSemaphoreTake(ctx.status_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        copy = ctx.status;
        xSemaphoreGive(ctx.status_mutex);
    }
    copy.sampleRequested = ctx.status_sampling.requested();
    copy.sampleCompleted = ctx.status_sampling.completed();
    copy.sampleRunning = ctx.status_sampling.running();
    return copy;
}

struct ModemRuntimeStatus {
    int ceregStat = -1;
    int rsrp = 999;
    int rsrq = 999;
    int sinr = 999;
    bool atReady = false;
    bool modemReady = false;
    bool simReady = false;
    bool cellIpEmpty = true;
    bool samplingDone = false;
};

// 高频状态机只读取紧凑运行态，避免每轮深拷贝完整状态中的字符串。
static ModemRuntimeStatus get_runtime_status(ModemContext& ctx)
{
    ModemRuntimeStatus state;
    if (!ctx.status_mutex) return state;
    if (xSemaphoreTake(ctx.status_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        state.ceregStat = ctx.status.ceregStat;
        state.rsrp = ctx.status.rsrp;
        state.rsrq = ctx.status.rsrq;
        state.sinr = ctx.status.sinr;
        state.atReady = ctx.status.atReady;
        state.modemReady = ctx.status.modemReady;
        state.simReady = ctx.status.simState == "ready";
        state.cellIpEmpty = ctx.status.cellIp.empty();
        state.samplingDone = ctx.status.signalFresh && ctx.status.identityFresh;
        xSemaphoreGive(ctx.status_mutex);
    }
    return state;
}

static bool startup_sampling_done(ModemContext& ctx)
{
    return get_runtime_status(ctx).samplingDone;
}

static void reset_identity_sampling_state(ModemContext& ctx)
{
    ctx.identity_static_attempted = false;
    ctx.identity_network_attempted = false;
}

static void set_status_cell_ip(ModemContext& ctx, const std::string& ip)
{
    if (xSemaphoreTake(ctx.status_mutex, pdMS_TO_TICKS(500)) != pdTRUE) return;
    ctx.status.cellIp = ip;
    xSemaphoreGive(ctx.status_mutex);
}

static std::string read_nvs_string(nvs_handle_t nvs, const char* key, size_t max_len)
{
    size_t len = 0;
    esp_err_t err = nvs_get_str(nvs, key, nullptr, &len);
    if (err != ESP_OK || len == 0 || len > max_len + 1) return {};
    std::string value(len, '\0');
    err = nvs_get_str(nvs, key, value.data(), &len);
    if (err != ESP_OK) return {};
    if (!value.empty() && value.back() == '\0') value.pop_back();
    return value;
}

static void save_identity_cache(ModemContext& ctx, const std::string& imei, const std::string& iccid)
{
    bool valid_imei = is_imei_text(imei);
    bool valid_iccid = is_iccid_text(iccid);
    if (!valid_imei && !valid_iccid) return;
    nvs_handle_t nvs = 0;
    if (nvs_open("sms_config", NVS_READWRITE, &nvs) != ESP_OK) return;
    const char* imei_key = ctx.hardware.slot == 0 ? "m0Imei" : "m1Imei";
    const char* iccid_key = ctx.hardware.slot == 0 ? "m0Iccid" : "m1Iccid";
    std::string old_imei = read_nvs_string(nvs, imei_key, 32);
    std::string old_iccid = read_nvs_string(nvs, iccid_key, 32);
    esp_err_t err = ESP_OK;
    bool changed = false;
    if (valid_imei && imei != old_imei) {
        err = nvs_set_str(nvs, imei_key, imei.c_str());
        changed = err == ESP_OK;
    }
    if (err == ESP_OK && valid_iccid && iccid != old_iccid) {
        err = nvs_set_str(nvs, iccid_key, iccid.c_str());
        changed = changed || err == ESP_OK;
    }
    if (err == ESP_OK && changed) err = nvs_commit(nvs);
    nvs_close(nvs);
    if (err == ESP_OK && changed) MODEM_LOG_LINE(ctx, "模组身份信息已写入缓存");
}

static void append_urc_text(ModemContext& ctx, const std::string& raw, int64_t received_us)
{
    if (raw.empty() || !ctx.urc_mutex) return;
    // 时间元数据与 URC 同锁入队；短信 worker 延迟处理不能把旧消息当成新回复。
    char stamp[48];
    snprintf(stamp, sizeof(stamp), "@RXUS:%lld\n", static_cast<long long>(received_us));
    std::string text = stamp;
    text += raw;
    if (xSemaphoreTake(ctx.urc_mutex, pdMS_TO_TICKS(100)) != pdTRUE) return;
    if (text.size() >= URC_BUFFER_MAX) {
        // 长 AT 响应里夹 URC 时，整段转存会突破缓冲上限；保留尾部且尽量从完整行开始。
        size_t start = text.size() - URC_BUFFER_MAX;
        size_t nl = text.find('\n', start);
        if (nl != std::string::npos && nl + 1 < text.size()) start = nl + 1;
        ctx.urc_buffer.assign(text.data() + start, text.size() - start);
    } else {
        size_t need = ctx.urc_buffer.size() + text.size();
        if (need > URC_BUFFER_MAX) {
            size_t drop = need - URC_BUFFER_MAX;
            ctx.urc_buffer.erase(0, std::min(drop, ctx.urc_buffer.size()));
            size_t nl = ctx.urc_buffer.find('\n');
            if (nl != std::string::npos && nl + 1 < ctx.urc_buffer.size()) ctx.urc_buffer.erase(0, nl + 1);
        }
        ctx.urc_buffer += text;
    }
    xSemaphoreGive(ctx.urc_mutex);
    signal_event(ctx);  // 立即唤醒短信任务处理，不等它的轮询周期
}

static void append_capped(std::string& out, const uint8_t* data, size_t len, size_t cap);

static bool looks_like_pdu_line(const std::string& line)
{
    if (line.size() < 32 || (line.size() & 1) != 0) return false;
    return std::all_of(line.begin(), line.end(), [](unsigned char ch) { return isxdigit(ch); });
}

static void preserve_uart_urc_line(ModemContext& ctx, const std::string& raw, int64_t received_us)
{
    std::string line = idf_util_trim_copy(raw);
    if (line.empty()) return;
    if (ctx.uart_wait_cmt_pdu && esp_timer_get_time() > ctx.uart_wait_cmt_until_us) {
        ctx.uart_wait_cmt_pdu = false;
    }

    bool cmt = line.rfind("+CMT:", 0) == 0;
    bool standalone = line.rfind("+CMTI:", 0) == 0 || line.rfind("+CLIP:", 0) == 0 || line == "RING";
    if (cmt || standalone || (ctx.uart_wait_cmt_pdu && looks_like_pdu_line(line))) {
        append_urc_text(ctx, line + "\r\n", received_us);
    }
    if (cmt) {
        ctx.uart_wait_cmt_pdu = true;
        ctx.uart_wait_cmt_until_us = esp_timer_get_time() + 3LL * 1000LL * 1000LL;
    } else if (ctx.uart_wait_cmt_pdu && looks_like_pdu_line(line)) {
        ctx.uart_wait_cmt_pdu = false;
    }
}

static void preserve_uart_urcs(ModemContext& ctx, const uint8_t* data, size_t len)
{
    const int64_t received_us = esp_timer_get_time();
    for (size_t i = 0; i < len; ++i) {
        char ch = static_cast<char>(data[i]);
        if (ch == '\r' || ch == '\n') {
            if (!ctx.uart_line_carry.empty()) preserve_uart_urc_line(ctx, ctx.uart_line_carry, ctx.uart_line_received_us);
            ctx.uart_line_carry.clear();
        } else if (ctx.uart_line_carry.size() < 768) {
            if (ctx.uart_line_carry.empty()) ctx.uart_line_received_us = received_us;
            ctx.uart_line_carry += ch;
        } else {
            ctx.uart_line_carry.clear();
            ctx.uart_wait_cmt_pdu = false;
        }
    }
}

static void capture_pending_uart_locked(ModemContext& ctx, uint32_t max_ms)
{
    // RX 缓冲为空时立即返回：该函数在每条 AT 命令前都会执行，
    // 空转等待 20ms×2 会拖慢所有 AT 操作(健康探测/补收批量删除/身份采样)
    size_t buffered = 0;
    if (uart_get_buffered_data_len(ctx.hardware.uart, &buffered) == ESP_OK && buffered == 0) return;

    uint8_t buf[128];
    // 静默窗口(有数据就续期)之上必须再加总时长硬上限：模组连续吐数据
    // (下载中途中止的 MHTTP 载荷、复位横幅、错误波特率乱码)时，纯续期
    // 循环会长期占住 AT 通道；按行提取器本身已有固定长度上限，不在这里重复缓存。
    TickDeadline hard_deadline(std::max<uint32_t>(max_ms, 1000));
    TickDeadline quiet(max_ms);
    do {
        int got = uart_read_bytes(ctx.hardware.uart, buf, sizeof(buf), pdMS_TO_TICKS(20));
        if (got > 0) {
            preserve_uart_urcs(ctx, buf, static_cast<size_t>(got));
            quiet.restart(40);
        }
    } while (!quiet.expired() && !hard_deadline.expired());
}

// 返回是否成功抢到 AT 通道锁并完成抓取；false=通道正被长任务占用
static bool poll_unsolicited_uart(ModemContext& ctx, uint32_t max_ms)
{
    if (!ctx.at_mutex) return false;
    if (xSemaphoreTakeRecursive(ctx.at_mutex, 0) != pdTRUE) return false;
    capture_pending_uart_locked(ctx, max_ms);
    xSemaphoreGiveRecursive(ctx.at_mutex);
    return true;
}

static void handle_uart_event_error(ModemContext& ctx, const uart_event_t& evt)
{
    if (evt.type == UART_FIFO_OVF || evt.type == UART_BUFFER_FULL) {
        MODEM_LOG_LINE(ctx, evt.type == UART_FIFO_OVF
                                ? "模组 UART 硬件 FIFO 溢出，本次接收数据可能不完整"
                                : "模组 UART 接收缓冲已满，本次接收数据可能不完整");
    } else if (evt.type == UART_PARITY_ERR) {
        MODEM_LOG_LINE(ctx, "模组 UART 奇偶校验错误，本次接收数据可能损坏");
    } else if (evt.type == UART_FRAME_ERR) {
        MODEM_LOG_LINE(ctx, "模组 UART 帧错误，本次接收数据可能损坏");
    }
}

static bool at_channel_idle_now(ModemContext& ctx)
{
    if (ctx.esim_operation_depth.load(std::memory_order_relaxed) != 0) return false;
    if (!ctx.at_mutex) return false;
    if (xSemaphoreTakeRecursive(ctx.at_mutex, 0) != pdTRUE) return false;
    xSemaphoreGiveRecursive(ctx.at_mutex);
    return true;
}

static void append_capped(std::string& out, const uint8_t* data, size_t len, size_t cap)
{
    if (!data || len == 0 || cap == 0) return;
    if (len >= cap) {
        out.assign(reinterpret_cast<const char*>(data + len - cap), cap);
        return;
    }
    size_t need = out.size() + len;
    if (need > cap) out.erase(0, need - cap);
    out.append(reinterpret_cast<const char*>(data), len);
}

static esp_err_t send_at(ModemContext& ctx, const std::string& cmd, uint32_t timeout_ms,
                         std::string& response)
{
    if (!ctx.started) return ESP_ERR_INVALID_STATE;
    if (xSemaphoreTakeRecursive(ctx.at_mutex, pdMS_TO_TICKS(timeout_ms + 500)) != pdTRUE) return ESP_ERR_TIMEOUT;

    capture_pending_uart_locked(ctx, 30);
    std::string wire = cmd;
    wire += "\r\n";
    uart_write_bytes(ctx.hardware.uart, wire.data(), wire.size());

    response.clear();
    response.reserve(512);
    // 响应上限 8KB：满存储的 AT+CMGL 可达十几 KB，无上限会造成堆峰值风险；
    // 截断后 OK/ERROR 终结符仍通过重叠扫描窗口检测，漏收的短信由下轮轮询补齐。
    constexpr size_t MAX_RESPONSE = 8192;
    TickDeadline deadline(timeout_ms);
    uint8_t buf[128];
    std::string scan;  // 跨块重叠扫描窗口，保证被截断/跨块的终结符也能被识别
    esp_err_t ret = ESP_ERR_TIMEOUT;
    while (!deadline.expired()) {
        int got = uart_read_bytes(ctx.hardware.uart, buf, sizeof(buf), pdMS_TO_TICKS(80));
        if (got > 0) {
            preserve_uart_urcs(ctx, buf, static_cast<size_t>(got));
            size_t room = MAX_RESPONSE > response.size() ? MAX_RESPONSE - response.size() : 0;
            if (room > 0) response.append(reinterpret_cast<const char*>(buf), std::min<size_t>(room, got));
            scan.append(reinterpret_cast<const char*>(buf), got);
            int final_code = at_final_result(scan);
            if (final_code != 0) {
                ret = final_code > 0 ? ESP_OK : ESP_FAIL;
                break;
            }
            if (scan.size() > 32) scan.erase(0, scan.size() - 32);
        }
    }
    xSemaphoreGiveRecursive(ctx.at_mutex);
    return ret;
}

static esp_err_t send_at_until(ModemContext& ctx, const std::string& cmd, const char* token,
                               uint32_t timeout_ms, std::string& response)
{
    if (!ctx.started || !token || !*token) return ESP_ERR_INVALID_STATE;
    if (xSemaphoreTakeRecursive(ctx.at_mutex, pdMS_TO_TICKS(timeout_ms + 500)) != pdTRUE) return ESP_ERR_TIMEOUT;

    capture_pending_uart_locked(ctx, 30);
    std::string wire = cmd;
    wire += "\r\n";
    uart_write_bytes(ctx.hardware.uart, wire.data(), wire.size());

    response.clear();
    response.reserve(512);
    constexpr size_t MAX_RESPONSE = 4096;
    TickDeadline deadline(timeout_ms);
    uint8_t buf[128];
    std::string scan;
    esp_err_t ret = ESP_ERR_TIMEOUT;
    while (!deadline.expired()) {
        int got = uart_read_bytes(ctx.hardware.uart, buf, sizeof(buf), pdMS_TO_TICKS(100));
        if (got > 0) {
            preserve_uart_urcs(ctx, buf, static_cast<size_t>(got));
            append_capped(response, buf, static_cast<size_t>(got), MAX_RESPONSE);
            scan.append(reinterpret_cast<const char*>(buf), got);
            if (response.find(token) != std::string::npos || scan.find(token) != std::string::npos) {
                ret = ESP_OK;
                break;
            }
            if (at_final_result(scan) < 0) {
                ret = ESP_FAIL;
                break;
            }
            if (scan.size() > 64) scan.erase(0, scan.size() - 64);
        }
    }
    xSemaphoreGiveRecursive(ctx.at_mutex);
    return ret;
}

static esp_err_t send_pdu(ModemContext& ctx, const std::string& cmgs_cmd, const char* pdu,
                          uint32_t timeout_ms, std::string& response, void (*on_submit)(uint8_t))
{
    if (!ctx.started || !pdu) return ESP_ERR_INVALID_STATE;
    if (xSemaphoreTakeRecursive(ctx.at_mutex, pdMS_TO_TICKS(timeout_ms + 2000)) != pdTRUE) return ESP_ERR_TIMEOUT;

    capture_pending_uart_locked(ctx, 30);
    std::string wire = cmgs_cmd;
    wire += "\r\n";
    uart_write_bytes(ctx.hardware.uart, wire.data(), wire.size());

    response.clear();
    response.reserve(512);
    constexpr size_t MAX_RESPONSE = 4096;
    uint8_t buf[128];
    TickDeadline prompt_deadline(5000);
    bool got_prompt = false;
    esp_err_t ret = ESP_ERR_TIMEOUT;
    std::string scan;
    while (!prompt_deadline.expired()) {
        int got = uart_read_bytes(ctx.hardware.uart, buf, sizeof(buf), pdMS_TO_TICKS(80));
        if (got > 0) {
            preserve_uart_urcs(ctx, buf, static_cast<size_t>(got));
            append_capped(response, buf, static_cast<size_t>(got), MAX_RESPONSE);
            scan.append(reinterpret_cast<const char*>(buf), got);
            if (response.find('>') != std::string::npos || scan.find('>') != std::string::npos) {
                got_prompt = true;
                break;
            }
            if (at_final_result(scan) < 0) {
                ret = ESP_FAIL;  // 提示符阶段就报错(如未注册的 +CMS ERROR)，按失败而非超时上报
                break;
            }
            if (scan.size() > 64) scan.erase(0, scan.size() - 64);
        }
    }

    if (got_prompt) {
        size_t pdu_len = strlen(pdu);
        uart_write_bytes(ctx.hardware.uart, pdu, pdu_len);
        // Ctrl+Z 提交 PDU；encodePDU 生成的缓冲已自带 0x1A 结尾，
        // 避免重复发送在命令模式下多注入一个孤立控制字符
        if (pdu_len == 0 || static_cast<uint8_t>(pdu[pdu_len - 1]) != 0x1A) {
            const uint8_t end = 0x1A;
            uart_write_bytes(ctx.hardware.uart, &end, 1);
        }
        // 通知调用方实际提交边界，早于 +CMGS 应答，且不把等待提示符算作已发送。
        if (on_submit) on_submit(ctx.hardware.slot);
        TickDeadline deadline(timeout_ms);
        scan.clear();
        while (!deadline.expired()) {
            int got = uart_read_bytes(ctx.hardware.uart, buf, sizeof(buf), pdMS_TO_TICKS(120));
            if (got > 0) {
                preserve_uart_urcs(ctx, buf, static_cast<size_t>(got));
                append_capped(response, buf, static_cast<size_t>(got), MAX_RESPONSE);
                scan.append(reinterpret_cast<const char*>(buf), got);
                // 官方手册定义 +CMGS:<mr> 即网络已接受 SMS-SUBMIT；某些固件的尾随 OK
                // 可能没有完整 CRLF，不能因此把已经发送成功的短信等到超时。
                if (has_cmgs_result(response) || has_cmgs_result(scan)) {
                    ret = ESP_OK;
                    break;
                }
                int final_code = at_final_result(scan);
                if (final_code != 0) {
                    ret = final_code > 0 ? ESP_OK : ESP_FAIL;
                    break;
                }
                if (scan.size() > 64) scan.erase(0, scan.size() - 64);
            }
        }
    }

    xSemaphoreGiveRecursive(ctx.at_mutex);
    return ret;
}

esp_err_t idf_modem_send_at(uint8_t slot, const std::string& cmd, uint32_t timeout_ms,
                            std::string& response)
{
    ModemContext* ctx = context_for_slot(slot);
    return ctx ? send_at(*ctx, cmd, timeout_ms, response) : ESP_ERR_INVALID_ARG;
}

esp_err_t idf_modem_send_at_until(uint8_t slot, const std::string& cmd, const char* token,
                                  uint32_t timeout_ms, std::string& response)
{
    ModemContext* ctx = context_for_slot(slot);
    return ctx ? send_at_until(*ctx, cmd, token, timeout_ms, response) : ESP_ERR_INVALID_ARG;
}

esp_err_t idf_modem_send_pdu(uint8_t slot, const std::string& cmgs_cmd, const char* pdu,
                             uint32_t timeout_ms, std::string& response, void (*on_submit)(uint8_t))
{
    ModemContext* ctx = context_for_slot(slot);
    return ctx ? send_pdu(*ctx, cmgs_cmd, pdu, timeout_ms, response, on_submit) : ESP_ERR_INVALID_ARG;
}

static bool send_ok(ModemContext& ctx, const char* cmd, uint32_t timeout_ms = 1000,
                    std::string* out = nullptr)
{
    std::string resp;
    esp_err_t err = send_at(ctx, cmd, timeout_ms, resp);
    if (out) *out = resp;
    return err == ESP_OK;
}

static std::string query_imei_from_modem(ModemContext& ctx)
{
    // ML307R 的裸 AT+CGSN 可能返回模组序列号；只有带参数的标准 IMEI 查询才作为候选。
    static constexpr const char* kCommands[] = {"AT+CGSN=1", "AT+GSN=1"};
    for (const char* command : kCommands) {
        std::string response;
        if (!send_ok(ctx, command, 1000, &response)) continue;
        std::string imei = parse_imei_response(response);
        if (!imei.empty()) return imei;
    }
    return {};
}

static std::string parse_iccid_response(const std::string& raw)
{
    std::string line = first_payload_line(raw);
    size_t p = line.find(':');
    std::string value = idf_util_trim_copy(p == std::string::npos ? line : line.substr(p + 1));
    value.erase(std::remove(value.begin(), value.end(), '"'), value.end());
    for (char& ch : value) if (ch == 'f') ch = 'F';
    if (!value.empty() && value.back() == 'F') value.pop_back();
    if (is_iccid_text(value)) return value;
    // 部分固件会在 ICCID 前后附加槽位或状态字段，退回提取响应中的连续数字。
    return first_digit_run(raw, 15, 22);
}
static std::string parse_iccid_crsm_response(const std::string& raw)
{
    size_t marker = raw.find("+CRSM:");
    if (marker == std::string::npos) return {};
    long sw1 = 0;
    long sw2 = 0;
    if (sscanf(raw.c_str() + marker, "+CRSM: %ld,%ld", &sw1, &sw2) != 2 ||
        (sw1 != 144 && sw1 != 145) || sw2 != 0) return {};
    std::string encoded = first_quoted(raw, marker);
    if (encoded.size() != 20) return {};

    std::string value;
    value.reserve(encoded.size());
    for (size_t i = 0; i < encoded.size(); i += 2) {
        char low = static_cast<char>(toupper(static_cast<unsigned char>(encoded[i + 1])));
        char high = static_cast<char>(toupper(static_cast<unsigned char>(encoded[i])));
        if ((!isdigit(static_cast<unsigned char>(low)) && low != 'F') ||
            (!isdigit(static_cast<unsigned char>(high)) && high != 'F')) return {};
        value += low;
        value += high;
    }
    if (!value.empty() && value.back() == 'F') value.pop_back();
    for (char ch : value) if (!isdigit(static_cast<unsigned char>(ch))) return {};
    return is_iccid_text(value) ? value : std::string();
}

static std::string query_current_iccid(ModemContext& ctx)
{
    static constexpr const char* kCommands[] = {"AT+MCCID", "AT+ICCID", "AT+CCID"};
    std::string response;
    for (const char* command : kCommands) {
        esp_err_t err = send_at(ctx, command, IDF_MODEM_IDENTITY_TIMEOUT_MS, response);
        std::string iccid = err == ESP_OK ? parse_iccid_response(response) : std::string();
        MODEM_LOGF(ctx, "ICCID 查询 %s: %s", command,
                   err != ESP_OK ? esp_err_to_name(err) : (iccid.empty() ? "响应无有效卡号" : "已读取"));
        if (!iccid.empty()) return iccid;
    }
    // EF_ICCID 可在厂商命令不可用或部分 PIN 锁卡状态下通过标准受限访问读取。
    esp_err_t err = send_at(ctx, "AT+CRSM=176,12258,0,0,10", IDF_MODEM_IDENTITY_TIMEOUT_MS, response);
    std::string iccid = err == ESP_OK ? parse_iccid_crsm_response(response) : std::string();
    MODEM_LOGF(ctx, "ICCID 查询 EF_ICCID: %s",
               err != ESP_OK ? esp_err_to_name(err) : (iccid.empty() ? "响应无有效卡号" : "已读取"));
    return iccid;
}

static std::string query_sim_state(ModemContext& ctx)
{
    std::string resp;
    if (!send_ok(ctx, "AT+CPIN?", 1500, &resp)) {
        std::string compact = resp;
        compact.erase(std::remove_if(compact.begin(), compact.end(), [](unsigned char ch) {
            return isspace(ch);
        }), compact.end());
        if (compact.find("+CMEERROR:10") != std::string::npos) return "absent";
        return "unknown";
    }
    std::string compact = resp;
    compact.erase(std::remove_if(compact.begin(), compact.end(), [](unsigned char ch) {
        return isspace(ch);
    }), compact.end());
    if (compact.find("+CPIN:READY") != std::string::npos) return "ready";
    if (compact.find("+CPIN:SIMPIN2") != std::string::npos ||
        compact.find("+CPIN:SIMPUK2") != std::string::npos) return "other";
    if (compact.find("+CPIN:SIMPUK") != std::string::npos) return "puk";
    if (compact.find("+CPIN:SIMPIN") != std::string::npos) return "pin";
    return "other";
}

static void set_sim_status(ModemContext& ctx, const std::string& state, bool matched,
                           const std::string& message, const std::string& iccid = {})
{
    IdfModemStatus patch;
    patch.simState = state;
    patch.simCredentialMatched = matched;
    patch.simUnlockMessage = message;
    patch.iccid = iccid;
    if (state == "pin" || state == "puk" || state == "other") patch.phase = "sim_locked";
    update_status(ctx, patch, !iccid.empty(), false, true);
}

static constexpr bool sim_unlock_allowed(bool has_secret, uint8_t failed, uint8_t limit,
                                         bool puk, bool user_confirmed)
{
    return has_secret && failed < limit && (!puk || user_confirmed);
}
static_assert(sim_unlock_allowed(true, 0, 1, false, false), "PIN 应允许自动首次尝试");
static_assert(!sim_unlock_allowed(true, 1, 1, false, false), "达到上限后必须停止 PIN");
static_assert(!sim_unlock_allowed(true, 0, 1, true, false), "PUK 不得自动尝试");

static bool try_unlock_sim(ModemContext& ctx, bool allow_puk)
{
    send_ok(ctx, "ATE0", 1000);
    send_ok(ctx, "AT+CMEE=1", 1200);
    std::string state = query_sim_state(ctx);
    if (state == "ready") {
        // READY 后补读凭据主键，不能只依赖注册后概览采样。
        std::string iccid = get_status(ctx).iccid;
        if (iccid.empty()) iccid = query_current_iccid(ctx);
        bool matched = !iccid.empty() && idf_config_get_sim_unlock_view(iccid).found;
        set_sim_status(ctx, "ready", matched, {}, iccid);
        return true;
    }
    if (state != "pin" && state != "puk") {
        set_sim_status(ctx, state, false, state == "absent" ? "" : "无法自动处理该 SIM 状态");
        return false;
    }
    if (allow_puk && state != "puk") {
        set_sim_status(ctx, state, false, "当前 SIM 等待 PIN，未执行 PUK");
        return false;
    }

    std::string iccid = query_current_iccid(ctx);
    if (iccid.empty()) {
        set_sim_status(ctx, state, false, "锁卡状态下无法读取 ICCID，未尝试任何密码");
        return false;
    }
    IdfSimUnlockView view = idf_config_get_sim_unlock_view(iccid);
    if (!view.found) {
        set_sim_status(ctx, state, false, "当前 ICCID 没有匹配的凭据", iccid);
        return false;
    }
    const IdfSimCredential& item = view.credential;
    bool puk = state == "puk";
    const std::string& secret = puk ? item.puk : item.pin;
    uint8_t failed = puk ? item.pukFailedAttempts : item.pinFailedAttempts;
    uint8_t limit = puk ? item.pukMaxAttempts : item.pinMaxAttempts;
    bool has_secret = !secret.empty() && (!puk || !item.pin.empty());
    if (!sim_unlock_allowed(has_secret, failed, limit, puk, allow_puk) && !has_secret) {
        set_sim_status(ctx, state, true, puk ? "需要同时保存 PUK 和新 PIN" : "未保存 PIN", iccid);
        return false;
    }
    if (!sim_unlock_allowed(has_secret, failed, limit, puk, allow_puk) && failed >= limit) {
        set_sim_status(ctx, state, true, std::string(puk ? "PUK" : "PIN") + " 已达到本机失败次数上限", iccid);
        return false;
    }
    if (!sim_unlock_allowed(has_secret, failed, limit, puk, allow_puk)) {
        set_sim_status(ctx, state, true, "PUK 只允许在网页中手动确认执行", iccid);
        return false;
    }
    std::string attempt_key = iccid;
    if (!puk && ctx.last_pin_attempt_key == attempt_key) {
        set_sim_status(ctx, state, true, "本次运行已尝试该 PIN，等待修改凭据", iccid);
        return false;
    }
    if (!puk) ctx.last_pin_attempt_key = attempt_key;

    std::string cmd = puk ? "AT+CPIN=\"" + item.puk + "\",\"" + item.pin + "\""
                          : "AT+CPIN=\"" + item.pin + "\"";
    std::string resp;
    esp_err_t submit_err = send_at(ctx, cmd, 5000, resp);
    bool ready = false;
    for (int i = 0; i < 10 && !ready; ++i) {
        vTaskDelay(pdMS_TO_TICKS(500));
        ready = query_sim_state(ctx) == "ready";
    }
    if (ready) {
        idf_config_record_sim_unlock_result(iccid, puk, true);
        if (puk) idf_config_record_sim_unlock_result(iccid, false, true);
        ctx.last_pin_attempt_key.clear();
        set_sim_status(ctx, "ready", true, puk ? "PUK 解锁成功" : "PIN 自动解锁成功", iccid);
        MODEM_LOG_LINE(ctx, puk ? "SIM PUK 手动解锁成功" : "SIM PIN 自动解锁成功");
        return true;
    }
    if (submit_err == ESP_FAIL) idf_config_record_sim_unlock_result(iccid, puk, false);
    set_sim_status(ctx, state, true, std::string(puk ? "PUK" : "PIN") +
                   (submit_err == ESP_FAIL ? " 被模组拒绝，已停止继续尝试" : " 提交超时，未计入密码错误"), iccid);
    MODEM_LOG_LINE(ctx, puk ? "SIM PUK 解锁未成功，已停止继续尝试"
                            : "SIM PIN 解锁未成功，已停止继续尝试");
    return false;
}

static bool parse_csq(const std::string& resp, int& csq, int& ber)
{
    size_t p = resp.find("+CSQ:");
    if (p == std::string::npos) return false;
    std::string line = line_containing(resp, p);
    const char* token = strstr(line.c_str(), "+CSQ:");
    if (!token) return false;
    long values[2] = {};
    int count = 0;
    if (!parse_comma_longs(token + strlen("+CSQ:"), values, 2, count) || count < 2) return false;
    if (!((values[0] >= 0 && values[0] <= 31) || values[0] == 99)) return false;
    if (!((values[1] >= 0 && values[1] <= 7) || values[1] == 99)) return false;
    csq = static_cast<int>(values[0]);
    ber = static_cast<int>(values[1]);
    return true;
}

static bool parse_cereg(const std::string& resp, int& stat)
{
    size_t p = resp.find("+CEREG:");
    if (p == std::string::npos) return false;
    std::string line = line_containing(resp, p);
    const char* token = strstr(line.c_str(), "+CEREG:");
    if (!token) return false;
    std::string rest = token + strlen("+CEREG:");
    size_t comma = rest.find(',');
    std::string first = idf_util_trim_copy(rest.substr(0, comma));
    long first_value = -1;
    if (!parse_long_token(first, first_value)) return false;

    long status_value = first_value;
    if (comma != std::string::npos) {
        size_t second_end = rest.find(',', comma + 1);
        std::string second = idf_util_trim_copy(rest.substr(
            comma + 1, second_end == std::string::npos ? std::string::npos : second_end - comma - 1));
        long second_value = -1;
        // 查询响应是 +CEREG: <n>,<stat>；URC 是 +CEREG: <stat>[,<tac>,...]，
        // 后者第二字段通常是带引号的 TAC，不能把它当注册状态。
        if (parse_long_token(second, second_value)) status_value = second_value;
    }
    if (status_value < 0 || status_value > 5) return false;
    stat = static_cast<int>(status_value);
    return true;
}

// 注意：都要解析"包含 token 的那一行"。CEREG=2 的 URC 可能与查询响应混在同一段，
// 取"第一有效行"会把 +CEREG 行错当成 +COPS/+CGDCONT/+CNUM 的内容。
static std::string parse_cops(const std::string& resp)
{
    size_t p = resp.find("+COPS:");
    if (p == std::string::npos) return {};
    std::string line = line_containing(resp, p);
    // 自动模式下若未先选名称格式，AT+COPS? 只回 "+COPS: 0"(无引号运营商名)。
    // 此时返回空让上层改用 COPS=3,0 重试，而不是把模式位当运营商缓存下来。
    return first_quoted(line);
}

static std::string query_operator_from_modem(ModemContext& ctx)
{
    std::string response;
    // 只选择查询返回格式，不切换网络或注册模式；与网页手动查询使用相同等待时间。
    esp_err_t format_err = send_at(ctx, "AT+COPS=3,0", IDF_MODEM_IDENTITY_TIMEOUT_MS, response);
    if (format_err != ESP_OK) MODEM_LOGF(ctx, "运营商名称格式设置: %s", esp_err_to_name(format_err));
    esp_err_t err = send_at(ctx, "AT+COPS?", IDF_MODEM_IDENTITY_TIMEOUT_MS, response);
    std::string name = err == ESP_OK ? parse_cops(response) : std::string();
    MODEM_LOGF(ctx, "运营商查询 AT+COPS?: %s",
             err != ESP_OK ? esp_err_to_name(err) : (name.empty() ? "响应无运营商名称" : "已读取"));
    return name;
}

static std::string parse_apn(const std::string& resp)
{
    size_t pos = 0;
    while (true) {
        size_t p = resp.find("+CGDCONT:", pos);
        if (p == std::string::npos) return {};
        std::string line = line_containing(resp, p);
        size_t q1 = line.find('"');
        if (q1 != std::string::npos) {
            size_t q2 = line.find('"', q1 + 1);
            if (q2 != std::string::npos) {
                std::string apn = first_quoted(line, q2 + 1);
                if (!apn.empty()) return apn;
            }
        }
        pos = p + strlen("+CGDCONT:");
    }
}

// 部分模组/卡会把国际 TOA 字节 0x91 误解成 BCD 数字，导致 CNUM 号码出现
// "+19..." 前缀，例如 +1944756... 实际应为 +44756...。这里只在 "19" 后
// 紧跟常见国家/地区码时剥离，避免误伤真实 NANP 号码。
static std::string normalize_msisdn(std::string phone)
{
    size_t start = 0;
    while (start < phone.size() && isspace(static_cast<unsigned char>(phone[start]))) ++start;
    size_t end = phone.size();
    while (end > start && isspace(static_cast<unsigned char>(phone[end - 1]))) --end;
    if (start >= end) return {};
    phone = phone.substr(start, end - start);

    bool had_plus = !phone.empty() && phone[0] == '+';
    std::string digits;
    digits.reserve(phone.size());
    for (size_t i = had_plus ? 1 : 0; i < phone.size(); ++i) {
        unsigned char ch = static_cast<unsigned char>(phone[i]);
        if (isdigit(ch)) digits += static_cast<char>(ch);
        else if (!isspace(ch) && ch != '-' && ch != '(' && ch != ')') {
            return phone;
        }
    }
    if (digits.size() < 8) return phone;
    if (had_plus && digits.size() == 11 && digits[0] == '1') {
        return std::string("+") + digits;
    }

    static const char* kCountryCodes[] = {
        "44", "86", "33", "49", "81", "61", "91", "39", "34", "82", "65",
        "852", "886", "853", "855", "856", "60", "62", "63", "66", "84",
        "90", "971", "966", "974", "973", "968", "965", "962", "961",
        "20", "27", "234", "254", "255", "256", "233", "212",
        "7", "380", "48", "40", "36", "30", "31", "32",
        "41", "43", "45", "46", "47", "351", "352", "353", "354", "358",
        "420", "421", "370", "371", "372", "373", "374", "375", "376",
        "52", "55", "54", "56", "57", "51", "58",
        "1",
    };
    if (digits.rfind("19", 0) == 0) {
        std::string rest = digits.substr(2);
        for (const char* cc : kCountryCodes) {
            size_t n = strlen(cc);
            if (rest.size() < n + 6) continue;
            if (rest.compare(0, n, cc) != 0) continue;
            if (strcmp(cc, "1") == 0) continue;
            return std::string("+") + rest;
        }
    }
    if (had_plus) return std::string("+") + digits;
    return phone;
}

static std::string parse_cnum_phone(const std::string& resp)
{
    size_t p = resp.find("+CNUM:");
    if (p == std::string::npos) return {};
    std::string line = line_containing(resp, p);
    std::string alpha = first_quoted(line);
    size_t after_alpha = line.find('"');
    if (after_alpha == std::string::npos) return {};
    after_alpha = line.find('"', after_alpha + 1);
    if (after_alpha == std::string::npos) return {};
    std::string phone = first_quoted(line, after_alpha + 1);
    if (phone.empty()) phone = alpha;
    return normalize_msisdn(phone);
}

static bool starts_with(const std::string& text, const char* prefix)
{
    return text.rfind(prefix, 0) == 0;
}

static char hex_nibble(uint8_t value)
{
    value &= 0x0f;
    return value < 10 ? static_cast<char>('0' + value)
                      : static_cast<char>('A' + value - 10);
}

static std::string hex_encode_ascii(const std::string& text)
{
    std::string out;
    out.reserve(text.size() * 2);
    for (unsigned char ch : text) {
        out += hex_nibble(ch >> 4);
        out += hex_nibble(ch);
    }
    return out;
}

static bool parse_http_url(const std::string& raw_url, std::string& protocol,
                           std::string& host, std::string& path, std::string& error)
{
    std::string url = idf_util_trim_copy(raw_url);
    if (url.empty()) url = IDF_KEEPALIVE_DEFAULT_URL;
    if (url.size() > 2048) {
        error = "蜂窝HTTP URL过长";
        return false;
    }

    size_t proto_end = url.find("://");
    if (proto_end == std::string::npos || proto_end == 0) {
        error = "蜂窝HTTP URL格式无效，需要 http:// 或 https://";
        return false;
    }

    protocol = url.substr(0, proto_end);
    std::transform(protocol.begin(), protocol.end(), protocol.begin(),
                   [](unsigned char ch) { return static_cast<char>(tolower(ch)); });
    if (protocol != "http" && protocol != "https") {
        error = "蜂窝HTTP URL仅支持 http/https";
        return false;
    }

    size_t host_start = proto_end + 3;
    size_t path_start = url.find('/', host_start);
    if (path_start == std::string::npos) {
        host = url.substr(host_start);
        path = "/";
    } else {
        host = url.substr(host_start, path_start - host_start);
        path = url.substr(path_start);
    }
    size_t hash = path.find('#');
    if (hash != std::string::npos) path.resize(hash);
    host = idf_util_trim_copy(host);
    if (host.empty() || host.find('"') != std::string::npos ||
        host.find(' ') != std::string::npos || path.find('"') != std::string::npos ||
        path.find(' ') != std::string::npos) {
        error = "蜂窝HTTP URL包含非法字符";
        return false;
    }
    return true;
}

static void normalize_keepalive_payload_size(const std::string& host, std::string& path)
{
    if (host != "gg.incrafttime.top") return;
    if (!starts_with(path, "/api/payload?")) return;
    size_t pos = path.find("size=128684");
    if (pos != std::string::npos) path.replace(pos, strlen("size=128684"), "size=64342");
}

static void append_no_cache_query(std::string& path)
{
    path += (path.find('?') == std::string::npos) ? '?' : '&';
    char buf[48];
    snprintf(buf, sizeof(buf), "t=%llu&r=%08x",
             static_cast<unsigned long long>(esp_timer_get_time() / 1000ULL),
             static_cast<unsigned>(esp_random()));
    path += buf;
}

static esp_err_t send_at_locked(ModemContext& ctx, const std::string& cmd, uint32_t timeout_ms,
                                std::string& response, size_t max_capture = 1400,
                                uint32_t extra_read_ms = 50)
{
    capture_pending_uart_locked(ctx, 30);
    std::string wire = cmd;
    wire += "\r\n";
    uart_write_bytes(ctx.hardware.uart, wire.data(), wire.size());

    response.clear();
    response.reserve(std::min<size_t>(max_capture, 512));
    TickDeadline deadline(timeout_ms);
    uint8_t buf[128];
    esp_err_t ret = ESP_ERR_TIMEOUT;
    while (!deadline.expired()) {
        int got = uart_read_bytes(ctx.hardware.uart, buf, sizeof(buf), pdMS_TO_TICKS(80));
        if (got > 0) {
            preserve_uart_urcs(ctx, buf, static_cast<size_t>(got));
            size_t room = max_capture > response.size() ? max_capture - response.size() : 0;
            if (room > 0) response.append(reinterpret_cast<const char*>(buf), std::min<size_t>(room, got));
            int final_code = at_final_result(response);
            if (final_code != 0) {
                ret = final_code > 0 ? ESP_OK : ESP_FAIL;
                // 静默续期之上加总时长硬上限：URC 持续刷屏(如下载进度)时
                // 纯续期循环会无限占住 AT 通道锁，拖死所有 AT 使用方
                TickDeadline extra_hard(extra_read_ms * 4 + 200);
                TickDeadline extra_deadline(extra_read_ms);
                while (!extra_deadline.expired() && !extra_hard.expired()) {
                    int more = uart_read_bytes(ctx.hardware.uart, buf, sizeof(buf), pdMS_TO_TICKS(15));
                    if (more <= 0) continue;
                    preserve_uart_urcs(ctx, buf, static_cast<size_t>(more));
                    room = max_capture > response.size() ? max_capture - response.size() : 0;
                    if (room > 0) response.append(reinterpret_cast<const char*>(buf), std::min<size_t>(room, more));
                    extra_deadline.restart(extra_read_ms);
                }
                break;
            }
        }
    }
    return ret;
}

static bool send_at_data_locked(ModemContext& ctx, const std::string& cmd,
                                const std::string& data, std::string& response)
{
    if (send_at_locked(ctx, cmd, 3000, response) != ESP_OK) return false;
    if (uart_write_bytes(ctx.hardware.uart, data.data(), data.size()) != static_cast<int>(data.size())) return false;

    response.clear();
    TickDeadline deadline(5000);
    uint8_t buf[128];
    while (!deadline.expired()) {
        int got = uart_read_bytes(ctx.hardware.uart, buf, sizeof(buf), pdMS_TO_TICKS(80));
        if (got <= 0) continue;
        preserve_uart_urcs(ctx, buf, static_cast<size_t>(got));
        response.append(reinterpret_cast<const char*>(buf), static_cast<size_t>(got));
        int final_code = at_final_result(response);
        if (final_code != 0) return final_code > 0;
        if (response.size() > 512) response.erase(0, response.size() - 512);
    }
    return false;
}

static int parse_mhttp_create_id(const std::string& resp)
{
    size_t p = resp.find("+MHTTPCREATE:");
    if (p == std::string::npos) return -1;
    p += strlen("+MHTTPCREATE:");
    size_t end = resp.find_first_of("\r\n", p);
    std::string token = resp.substr(p, end == std::string::npos ? std::string::npos : end - p);
    size_t comma = token.find(',');
    if (comma != std::string::npos) token.resize(comma);
    long id = -1;
    if (!parse_long_token(token, id) || id < 0 || id > 255) return -1;
    return static_cast<int>(id);
}

static void parse_mhttp_head(ModemContext& ctx, const std::string& head, int http_id,
                             IdfCellularHttpResult& result,
                             bool& complete, bool& error)
{
    size_t comma = head.find(',');
    if (comma == std::string::npos) return;
    if (starts_with(head, "+MHTTPURC: \"header\"")) {
        long nums[4];
        int n = 0;
        if (parse_comma_longs(head.substr(comma + 1), nums, 4, n) && n >= 2 && nums[0] == http_id) {
            result.httpStatus = static_cast<int>(nums[1]);
            MODEM_LOGF(ctx, "蜂窝HTTP响应状态: %d", result.httpStatus);
        }
    } else if (starts_with(head, "+MHTTPURC: \"content\"")) {
        long nums[5];
        int n = 0;
        if (parse_comma_longs(head.substr(comma + 1), nums, 5, n) && n >= 4 && nums[0] == http_id) {
            result.expectedBytes = static_cast<uint32_t>(std::max<long>(0, nums[1]));
            result.bytesRead = static_cast<uint32_t>(std::max<long>(0, nums[2]));
            uint32_t current = static_cast<uint32_t>(std::max<long>(0, nums[3]));
            if ((result.expectedBytes > 0 && result.bytesRead >= result.expectedBytes) || current == 0) {
                complete = true;
            }
        }
    } else if (starts_with(head, "+MHTTPURC: \"err\"")) {
        long nums[3];
        int n = 0;
        if (parse_comma_longs(head.substr(comma + 1), nums, 3, n) && n >= 2 && nums[0] == http_id) {
            result.mhttpError = static_cast<int>(nums[1]);
            MODEM_LOGF(ctx, "蜂窝HTTP错误码: %d%s", result.mhttpError,
                       result.mhttpError == 4 ? "(SSL握手失败)" : "");
            error = true;
            complete = true;
        }
    }
}

static int comma_count(const std::string& text)
{
    int count = 0;
    for (char ch : text) {
        if (ch == ',') ++count;
    }
    return count;
}

static bool send_mhttp_header_locked(ModemContext& ctx, int http_id, bool more,
                                     const std::string& line)
{
    char head[64];
    snprintf(head, sizeof(head), "AT+MHTTPHEADER=%d,%d,%u,\"",
             http_id, more ? 1 : 0, static_cast<unsigned>(line.size()));
    std::string cmd = head;
    cmd += line;
    cmd += "\"";
    std::string resp;
    return send_at_locked(ctx, cmd, 3000, resp) == ESP_OK;
}

static void append_sms_urc_line(ModemContext& ctx, const std::string& line)
{
    std::string text = line;
    text += "\r\n";
    append_urc_text(ctx, text, esp_timer_get_time());
}

static bool wait_mhttp_download_locked(ModemContext& ctx, int http_id, uint32_t timeout_ms,
                                       uint32_t min_bytes,
                                       IdfCellularHttpResult& result)
{
    TickDeadline deadline(timeout_ms);
    std::string head;
    head.reserve(280);
    bool skipping_data = false;
    bool append_next_sms_payload = false;
    bool complete = false;
    bool error = false;
    uint8_t buf[128];

    while (!deadline.expired() && !complete) {
        int got = uart_read_bytes(ctx.hardware.uart, buf, sizeof(buf), pdMS_TO_TICKS(120));
        if (got <= 0) continue;
        for (int i = 0; i < got && !complete; ++i) {
            char ch = static_cast<char>(buf[i]);
            if (skipping_data) {
                if (ch == '\n') {
                    skipping_data = false;
                    head.clear();
                }
                continue;
            }
            if (ch == '\r' || ch == '\n') {
                std::string line = idf_util_trim_copy(head);
                if (!line.empty()) {
                    if (starts_with(line, "+MHTTPURC: \"err\"")) {
                        parse_mhttp_head(ctx, line, http_id, result, complete, error);
                    } else if (append_next_sms_payload || starts_with(line, "+CMT:") || starts_with(line, "+CMTI:")) {
                        append_sms_urc_line(ctx, line);
                        append_next_sms_payload = starts_with(line, "+CMT:");
                    } else if (line == "RING" || starts_with(line, "+CLIP:")) {
                        // 蜂窝请求最长可占用 UART ~90s，期间仍须保留短信与来电 URC。
                        append_sms_urc_line(ctx, line);
                    }
                }
                head.clear();
                continue;
            }
            if (head.size() < 620) head += ch;

            int need_commas = 0;
            if (starts_with(head, "+MHTTPURC: \"content\"")) need_commas = 5;
            else if (starts_with(head, "+MHTTPURC: \"header\"")) need_commas = 4;
            if (need_commas > 0 && comma_count(head) >= need_commas) {
                parse_mhttp_head(ctx, head, http_id, result, complete, error);
                skipping_data = true;
                head.clear();
            }
        }
    }

    if (!complete) MODEM_LOG_LINE(ctx, "蜂窝HTTP响应等待超时");
    return !error && complete && result.httpStatus >= 200 && result.httpStatus < 400 &&
           result.bytesRead >= min_bytes;
}

static bool valid_ip_address(const std::string& value)
{
    uint8_t address[16] = {};
    int family = value.find(':') == std::string::npos ? AF_INET : AF_INET6;
    if (inet_pton(family, value.c_str(), address) != 1) return false;
    size_t size = family == AF_INET ? 4 : 16;
    return std::any_of(address, address + size, [](uint8_t byte) { return byte != 0; });
}

static bool parse_cgpaddr_ip(const std::string& resp, std::string& ip)
{
    size_t p = resp.find("+CGPADDR:");
    if (p == std::string::npos) return false;
    size_t eol = resp.find('\n', p);
    if (eol == std::string::npos) eol = resp.size();
    p = resp.find(',', p);
    while (p != std::string::npos && p < eol) {
        size_t next = resp.find(',', p + 1);
        if (next == std::string::npos || next > eol) next = eol;
        std::string candidate = idf_util_trim_copy(resp.substr(p + 1, next - p - 1));
        candidate.erase(std::remove(candidate.begin(), candidate.end(), '"'), candidate.end());
        if (valid_ip_address(candidate)) {
            ip = candidate;
            return true;
        }
        p = next < eol ? next : std::string::npos;
    }
    return false;
}

static bool apn_valid_for_at(const std::string& apn)
{
    return apn.size() <= 96 && apn.find('"') == std::string::npos &&
           apn.find('\r') == std::string::npos && apn.find('\n') == std::string::npos;
}

static bool sample_cell_ip_once(ModemContext& ctx)
{
    std::string resp;
    std::string ip;
    if (send_ok(ctx, "AT+CGPADDR=1", 3000, &resp) && parse_cgpaddr_ip(resp, ip)) {
        set_status_cell_ip(ctx, ip);
        MODEM_LOGF(ctx, "蜂窝PDP IP: %s", ip.c_str());
        return true;
    }
    set_status_cell_ip(ctx, "");
    return false;
}

static bool wait_pdp_ready_locked(ModemContext& ctx, uint32_t timeout_ms, std::string& ip)
{
    TickDeadline deadline(timeout_ms);
    while (!deadline.expired()) {
        std::string resp;
        if (send_at_locked(ctx, "AT+CGPADDR=1", 3000, resp) == ESP_OK &&
            parse_cgpaddr_ip(resp, ip)) {
            IdfModemStatus patch;
            patch.cellIp = ip;
            update_status(ctx, patch);
            MODEM_LOGF(ctx, "蜂窝PDP已就绪，IP: %s", ip.c_str());
            return true;
        }
        vTaskDelay(pdMS_TO_TICKS(700));
    }
    MODEM_LOG_LINE(ctx, "蜂窝PDP等待超时：未取得有效IP");
    return false;
}

static bool parse_muestats_cell(const std::string& resp, IdfModemStatus& patch)
{
    size_t line_pos = resp.find("\"scell\"");
    if (line_pos == std::string::npos) return false;
    size_t line_end = resp.find('\n', line_pos);
    if (line_end == std::string::npos) line_end = resp.size();
    std::string line = resp.substr(line_pos, line_end - line_pos);

    std::string parts[12];
    int count = 0;
    size_t pos = 0;
    while (pos <= line.size() && count < 12) {
        size_t comma = line.find(',', pos);
        if (comma == std::string::npos) comma = line.size();
        parts[count++] = idf_util_trim_copy(line.substr(pos, comma - pos));
        if (comma == line.size()) break;
        pos = comma + 1;
    }
    if (count <= 10) return false;

    bool got = false;
    if (!parts[7].empty()) {
        long value = 0;
        if (parse_long_token(parts[7], value) && value > -32768) {
            patch.rsrp = static_cast<int>(value / 10);
            got = true;
        }
    }
    if (!parts[8].empty()) {
        long value = 0;
        if (parse_long_token(parts[8], value) && value > -32768) {
            patch.rsrq = static_cast<int>(value / 10);
            got = true;
        }
    }
    if (!parts[10].empty()) {
        long value = 0;
        if (parse_long_token(parts[10], value) && value > -32768) {
            patch.sinr = static_cast<int>(value / 10);
            got = true;
        }
    }
    return got;
}

static bool parse_cesq_signal(const std::string& resp, IdfModemStatus& patch)
{
    size_t p = resp.find("+CESQ:");
    if (p == std::string::npos) return false;
    long values[6] = {};
    int count = 0;
    size_t value_start = p + strlen("+CESQ:");
    size_t line_end = resp.find_first_of("\r\n", value_start);
    std::string line = resp.substr(
        value_start, line_end == std::string::npos ? std::string::npos : line_end - value_start);
    if (!parse_comma_longs(line, values, 6, count) || count < 6) return false;
    bool got = false;
    if (values[4] >= 0 && values[4] <= 34) {
        patch.rsrq = static_cast<int>(values[4] / 2 - 20);
        got = true;
    }
    if (values[5] >= 0 && values[5] <= 97) {
        patch.rsrp = static_cast<int>(values[5] - 141);
        got = true;
    }
    return got;
}

static void sample_signal_detail_once(ModemContext& ctx)
{
    ModemRuntimeStatus current = get_runtime_status(ctx);
    if (!current.modemReady) return;
    std::string resp;
    IdfModemStatus patch;
    bool got = false;
    if (send_ok(ctx, "AT+MUESTATS=\"cell\"", 2000, &resp)) {
        got = parse_muestats_cell(resp, patch);
    }
    if (!got && send_ok(ctx, "AT+CESQ", 2000, &resp)) {
        got = parse_cesq_signal(resp, patch);
    }
    if (!got) return;
    int next_rsrp = patch.rsrp != 999 ? patch.rsrp : current.rsrp;
    int next_rsrq = patch.rsrq != 999 ? patch.rsrq : current.rsrq;
    int next_sinr = patch.sinr != 999 ? patch.sinr : current.sinr;
    if (next_rsrp == 999 && next_rsrq == 999 && next_sinr == 999) return;

    update_status(ctx, patch, false, true);
}

static bool model_skips_cgact(ModemContext& ctx)
{
    IdfModemStatus status = get_status(ctx);
    if (status.model == "ML307Y") return true;
    std::string resp;
    if (!send_ok(ctx, "AT+CGMM", 1000, &resp)) return false;
    std::string model = first_payload_line(resp, "AT+CGMM");
    if (!model.empty()) {
        IdfModemStatus patch;
        patch.model = model;
        update_status(ctx, patch);
    }
    return model == "ML307Y";
}

static bool apply_configured_data_mode_once(ModemContext& ctx, const IdfSimNetworkView& cfg,
                                            uint32_t active_timeout_ms,
                                            uint32_t inactive_timeout_ms)
{
    std::string resp;
    std::string apn = idf_util_trim_copy(cfg.apn);
    // 数据漫游策略：未勾选"允许数据漫游"且当前处于漫游(CEREG=5)时不激活蜂窝数据。
    // 启动阶段注册状态未知(stat=-1)会乐观激活，注册完成后由 enforce_roaming_data_policy 兜底关闭。
    bool want_data = cfg.dataEnabled &&
                     (cfg.roamingEnabled || get_runtime_status(ctx).ceregStat != 5);
    if (want_data) {
        if (!apn.empty() && apn_valid_for_at(apn)) {
            std::string cmd = "AT+CGDCONT=1,\"IP\",\"";
            cmd += apn;
            cmd += "\"";
            send_ok(ctx, cmd.c_str(), 3000, &resp);
        } else if (!apn.empty()) {
            MODEM_LOG_LINE(ctx, "APN 包含非法字符，启动时未下发 CGDCONT");
        }
        send_ok(ctx, "AT+CGACT=1,1", active_timeout_ms, &resp);
        // 已激活的 PDP 在部分固件上会对重复 CGACT 返回 ERROR；实际拿到 IP 才算可用。
        return sample_cell_ip_once(ctx);
    }

    bool ok = send_ok(ctx, "AT+CGACT=0,1", inactive_timeout_ms, &resp);
    if (ok) set_status_cell_ip(ctx, "");
    return ok;
}

// 数据漫游策略兜底：未勾选"允许数据漫游"且当前漫游(stat=5)时确保蜂窝数据关闭。
// 启动阶段拿不到注册状态会先乐观激活，注册完成后在此关闭，避免漫游误跑流量。
// 此开关只控制数据 PDP；短信是否可用由 SIM、模组和运营商短信承载共同决定，
// 不能仅凭 CEREG/CREG 中的某一个状态判断。归属网络(stat=1)不干预，按常规激活。
static void enforce_roaming_data_policy(ModemContext& ctx, const IdfSimNetworkView& cfg, int stat)
{
    if (!cfg.dataEnabled || cfg.roamingEnabled) return;  // 未开数据或允许漫游数据：无需干预
    if (stat != 5) return;                                // 非漫游：归属网络按常规激活即可
    if (get_runtime_status(ctx).cellIpEmpty) return;      // 数据本就未激活，无需再关
    std::string resp;
    if (send_ok(ctx, "AT+CGACT=0,1", 3000, &resp)) {
        set_status_cell_ip(ctx, "");
        MODEM_LOG_LINE(ctx, "数据漫游已关闭：检测到漫游，已停用蜂窝数据(不跑流量)");
    }
}

static void schedule_data_mode_retry(ModemContext& ctx)
{
    ctx.data_mode_retry_pending = true;
    ctx.data_mode_retry_count = 0;
    ctx.next_data_mode_retry = xTaskGetTickCount() + pdMS_TO_TICKS(MODEM_DATA_MODE_RETRY_GAP_MS);
}

static void apply_startup_data_mode(ModemContext& ctx)
{
    if (model_skips_cgact(ctx)) {
        MODEM_LOG_LINE(ctx, "该型号跳过启动 CGACT 配置");
        return;
    }
    IdfSimNetworkView cfg = idf_config_get_sim_network_view(ctx.hardware.slot);
    bool ok = apply_configured_data_mode_once(ctx, cfg, 6000, 2500);
    if (ok) {
        MODEM_LOG_LINE(ctx, cfg.dataEnabled ? "已按配置启用蜂窝数据(AT+CGACT=1,1)"
                                            : "已禁用数据连接(AT+CGACT=0,1)，防止流量消耗");
    } else {
        MODEM_LOG_LINE(ctx, cfg.dataEnabled ? "启动时激活数据连接未成功，转入后台重试"
                                            : "启动时禁用数据连接未确认，转入后台重试");
        schedule_data_mode_retry(ctx);
    }
}

static bool plmn_valid(const std::string& plmn)
{
    if (plmn.empty()) return true;
    if (plmn.size() < 5 || plmn.size() > 6) return false;
    for (char ch : plmn) {
        if (!isdigit(static_cast<unsigned char>(ch))) return false;
    }
    return true;
}

static void apply_operator_if_configured(ModemContext& ctx, const IdfSimNetworkView& cfg)
{
    if (cfg.operatorPlmn.empty()) return;
    if (!plmn_valid(cfg.operatorPlmn)) {
        MODEM_LOG_LINE(ctx, "运营商 PLMN 非法，启动时未下发 COPS");
        return;
    }
    std::string cmd = "AT+COPS=1,2,\"";
    cmd += cfg.operatorPlmn;
    cmd += "\"";
    std::string resp;
    esp_err_t err = send_at(ctx, cmd, 30000, resp);
    MODEM_LOGF(ctx, "运营商: 锁定 PLMN %s %s", cfg.operatorPlmn.c_str(),
               err == ESP_OK ? "成功" : "失败(可能不可达)");
}

static bool process_data_mode_retry(ModemContext& ctx)
{
    if (!ctx.data_mode_retry_pending) return false;
    if (static_cast<int32_t>(xTaskGetTickCount() - ctx.next_data_mode_retry) < 0) return false;  // 回绕安全
    if (!at_channel_idle_now(ctx)) return false;

    ++ctx.data_mode_retry_count;
    IdfSimNetworkView cfg = idf_config_get_sim_network_view(ctx.hardware.slot);
    bool ok = apply_configured_data_mode_once(ctx, cfg, 8000, 3000);
    if (ok) {
        ctx.data_mode_retry_pending = false;
        MODEM_LOG_LINE(ctx, cfg.dataEnabled ? "后台重试：蜂窝数据已启用"
                                            : "后台重试：蜂窝数据已禁用");
    } else if (ctx.data_mode_retry_count >= MODEM_DATA_MODE_RETRY_MAX) {
        ctx.data_mode_retry_pending = false;
        MODEM_LOG_LINE(ctx, "后台重试 CGACT 仍失败，保留当前模组状态");
    } else {
        ctx.next_data_mode_retry = xTaskGetTickCount() + pdMS_TO_TICKS(MODEM_DATA_MODE_RETRY_GAP_MS);
        MODEM_LOG_LINE(ctx, "后台重试 CGACT 未成功，稍后再试");
    }
    return true;
}

static bool fetch_mhttp_once_locked(ModemContext& ctx, const std::string& protocol,
                                    const std::string& host,
                                    const std::string& path, const char* method,
                                    const char* content_type, const std::string& body,
                                    uint32_t min_response_bytes, IdfCellularHttpResult& result)
{
    for (int i = 0; i < 4; ++i) {
        std::string ignored;
        char cmd[24];
        snprintf(cmd, sizeof(cmd), "AT+MHTTPDEL=%d", i);
        send_at_locked(ctx, cmd, 1000, ignored, 256, 10);
    }

    std::string create_cmd = "AT+MHTTPCREATE=\"" + protocol + "://" + host + "\"";
    std::string resp;
    if (send_at_locked(ctx, create_cmd, 10000, resp, 1600, 1200) != ESP_OK) {
        result.message = "蜂窝HTTP创建失败";
        MODEM_LOGF(ctx, "蜂窝HTTP创建失败: %s", resp.c_str());
        return false;
    }

    int http_id = parse_mhttp_create_id(resp);
    if (http_id < 0) {
        result.message = "蜂窝HTTP创建失败：未返回连接ID";
        return false;
    }

    char cmd[128];
    if (protocol == "https") {
        snprintf(cmd, sizeof(cmd), "AT+MHTTPCFG=\"ssl\",%d,1,0", http_id);
        send_at_locked(ctx, cmd, 5000, resp);
    }
    snprintf(cmd, sizeof(cmd), "AT+MHTTPCFG=\"encoding\",%d,0,0", http_id);
    send_at_locked(ctx, cmd, 3000, resp);
    if (min_response_bytes > 0) {
        send_mhttp_header_locked(ctx, http_id, true,
                                 "Cache-Control: no-cache, no-store, must-revalidate");
        send_mhttp_header_locked(ctx, http_id, false, "Pragma: no-cache");
    } else if (content_type && *content_type) {
        send_mhttp_header_locked(ctx, http_id, false,
                                 std::string("Content-Type: ") + content_type);
    }

    int method_value = strcmp(method, "POST") == 0 ? 2 : 1;
    if (method_value == 2 && !body.empty()) {
        snprintf(cmd, sizeof(cmd), "AT+MHTTPCONTENT=%d,0,%u", http_id,
                 static_cast<unsigned>(body.size()));
        if (!send_at_data_locked(ctx, cmd, body, resp)) {
            result.message = "蜂窝HTTP请求正文发送失败";
            snprintf(cmd, sizeof(cmd), "AT+MHTTPDEL=%d", http_id);
            send_at_locked(ctx, cmd, 2000, resp, 256, 20);
            return false;
        }
    }

    snprintf(cmd, sizeof(cmd), "AT+MHTTPCFG=\"encoding\",%d,1,1", http_id);
    send_at_locked(ctx, cmd, 3000, resp);
    std::string request_cmd = "AT+MHTTPREQUEST=" + std::to_string(http_id) + "," +
                              std::to_string(method_value) + ",0," + hex_encode_ascii(path);
    if (send_at_locked(ctx, request_cmd, 10000, resp) != ESP_OK) {
        result.message = "蜂窝HTTP请求发送失败";
        snprintf(cmd, sizeof(cmd), "AT+MHTTPDEL=%d", http_id);
        send_at_locked(ctx, cmd, 2000, resp, 256, 20);
        return false;
    }

    bool ok = wait_mhttp_download_locked(ctx, http_id, CELLULAR_HTTP_TIMEOUT_MS,
                                         min_response_bytes, result);
    snprintf(cmd, sizeof(cmd), "AT+MHTTPDEL=%d", http_id);
    send_at_locked(ctx, cmd, 3000, resp, 256, 20);
    result.ok = ok;
    if (result.message.empty()) result.message = ok ? "蜂窝HTTP请求完成" : "蜂窝HTTP请求失败";
    return ok;
}

static esp_err_t cellular_http_request_impl(ModemContext& ctx, const std::string& url,
                                            const char* method,
                                            const char* content_type, const std::string& body,
                                            const IdfCellularHttpConfig& config,
                                            uint32_t min_response_bytes, bool keepalive,
                                            IdfCellularHttpResult& result)
{
    result = IdfCellularHttpResult();
    if (!ctx.started) {
        result.message = "模组尚未启动";
        return ESP_ERR_INVALID_STATE;
    }
    if (!method || (strcmp(method, "GET") != 0 && strcmp(method, "POST") != 0) ||
        body.size() > 4096 ||
        (content_type && (strchr(content_type, '\r') || strchr(content_type, '\n')))) {
        result.message = "蜂窝HTTP请求参数无效";
        return ESP_ERR_INVALID_ARG;
    }
    if (!keepalive && !config.dataEnabled) {
        result.message = "蜂窝数据未启用";
        return ESP_ERR_INVALID_STATE;
    }

    std::string protocol;
    std::string host;
    std::string path;
    if (!parse_http_url(url, protocol, host, path, result.message)) return ESP_ERR_INVALID_ARG;
    if (keepalive) {
        normalize_keepalive_payload_size(host, path);
        append_no_cache_query(path);
    }

    if (xSemaphoreTakeRecursive(ctx.at_mutex,
                                pdMS_TO_TICKS(CELLULAR_HTTP_TIMEOUT_MS + 45000UL)) != pdTRUE) {
        result.message = "模组串口忙，蜂窝HTTP未执行";
        return ESP_ERR_TIMEOUT;
    }

    MODEM_LOGF(ctx, "蜂窝HTTP请求: %s %s://%s", method, protocol.c_str(), host.c_str());
    std::string resp;
    std::string apn = idf_util_trim_copy(config.apn);
    if (!apn.empty() && apn_valid_for_at(apn)) {
        std::string cmd = "AT+CGDCONT=1,\"IP\",\"" + apn + "\"";
        send_at_locked(ctx, cmd, 3000, resp);
    }

    if (send_at_locked(ctx, "AT+CGACT=1,1", 10000, resp) != ESP_OK) {
        MODEM_LOG_LINE(ctx, "CGACT激活未返回OK，继续检查PDP地址");
    }
    std::string ip;
    if (!wait_pdp_ready_locked(ctx, CELLULAR_PDP_READY_TIMEOUT_MS, ip)) {
        set_status_cell_ip(ctx, "");
        if (!config.dataEnabled) send_at_locked(ctx, "AT+CGACT=0,1", 5000, resp);
        xSemaphoreGiveRecursive(ctx.at_mutex);
        result.message = "蜂窝PDP未取得有效IP，请查看日志";
        return ESP_FAIL;
    }
    result.cellIp = ip;

    bool ok = fetch_mhttp_once_locked(ctx, protocol, host, path, method, content_type, body,
                                      min_response_bytes, result);
    if (keepalive && !ok && protocol == "https" && result.mhttpError == 4) {
        MODEM_LOG_LINE(ctx, "保号HTTPS握手失败，改用HTTP重试一次");
        IdfCellularHttpResult retry;
        retry.cellIp = result.cellIp;
        ok = fetch_mhttp_once_locked(ctx, "http", host, path, method, content_type, body,
                                     min_response_bytes, retry);
        result = retry;
    }

    if (!config.dataEnabled) {
        send_at_locked(ctx, "AT+CGACT=0,1", 5000, resp);
        set_status_cell_ip(ctx, "");
    }
    result.ok = ok;
    xSemaphoreGiveRecursive(ctx.at_mutex);
    return ok ? ESP_OK : ESP_FAIL;
}

esp_err_t idf_modem_cellular_http_get(uint8_t slot, const std::string& url,
                                      const IdfCellularHttpConfig& config,
                                      IdfCellularHttpResult& result)
{
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx) {
        result = {};
        result.message = "非法模组 slot";
        return ESP_ERR_INVALID_ARG;
    }
    return cellular_http_request_impl(*ctx, url, "GET", nullptr, std::string(), config,
                                      CELLULAR_KEEPALIVE_MIN_BYTES, true, result);
}

esp_err_t idf_modem_cellular_http_request(uint8_t slot, const std::string& url, const char* method,
                                           const char* content_type, const std::string& body,
                                           const IdfCellularHttpConfig& config,
                                           IdfCellularHttpResult& result)
{
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx) {
        result = {};
        result.message = "非法模组 slot";
        return ESP_ERR_INVALID_ARG;
    }
    return cellular_http_request_impl(*ctx, url, method, content_type, body, config, 0, false,
                                      result);
}

static void sample_signal_once(ModemContext& ctx)
{
    std::string resp;
    if (!send_ok(ctx, "AT+CSQ", 1000, &resp)) return;
    int csq = -1;
    int ber = 99;
    if (!parse_csq(resp, csq, ber)) return;
    IdfModemStatus patch;
    patch.csq = csq;
    patch.ber = ber;
    update_status(ctx, patch, false, true);
}

static bool sample_identity_once(ModemContext& ctx, bool log_summary = false,
                                 bool include_network_fields = true)
{
    IdfModemStatus before = get_status(ctx);
    bool need_static = !ctx.identity_static_attempted ||
                       before.mfr.empty() ||
                       before.model.empty() ||
                       before.fwver.empty();
    std::string resp;
    IdfModemStatus patch;

    // 固件/厂家信息基本不变；启动采样未完整前允许补采，完整后不再反复查询。
    if (need_static && before.mfr.empty()) {
        if (send_ok(ctx, "AT+CGMI", 1000, &resp)) patch.mfr = first_payload_line(resp, "AT+CGMI");
        vTaskDelay(pdMS_TO_TICKS(150));
    }
    if (need_static && before.model.empty()) {
        if (send_ok(ctx, "AT+CGMM", 1000, &resp)) patch.model = first_payload_line(resp, "AT+CGMM");
        vTaskDelay(pdMS_TO_TICKS(150));
    }
    if (need_static && before.fwver.empty()) {
        if (send_ok(ctx, "AT+CGMR", 1000, &resp)) patch.fwver = first_payload_line(resp, "AT+CGMR");
        vTaskDelay(pdMS_TO_TICKS(150));
    }

    if (before.imei.size() < 14) {
        patch.imei = query_imei_from_modem(ctx);
        vTaskDelay(pdMS_TO_TICKS(150));
    }

    if (before.iccid.size() < 15) {
        patch.iccid = query_current_iccid(ctx);
        vTaskDelay(pdMS_TO_TICKS(150));
    }

    if (before.imsi.empty()) {
        if (send_ok(ctx, "AT+CIMI", 1000, &resp)) {
            patch.imsi = first_digits_line(resp, 14, 16);
            if (patch.imsi.empty()) patch.imsi = first_digit_run(resp, 14, 16);
        }
        vTaskDelay(pdMS_TO_TICKS(150));
    }

    // 运营商/APN 是启动展示字段，注册完成后至少要拿到运营商；本机号码很多 SIM 不返回，不阻塞启动。
    bool need_network = include_network_fields &&
                        (!ctx.identity_network_attempted || before.operatorName.empty());
    if (need_network) {
        if (before.operatorName.empty()) {
            patch.operatorName = query_operator_from_modem(ctx);
            vTaskDelay(pdMS_TO_TICKS(150));
        }
        if (before.apnSim.empty()) {
            if (send_ok(ctx, "AT+CGDCONT?", 1500, &resp)) patch.apnSim = parse_apn(resp);
            vTaskDelay(pdMS_TO_TICKS(150));
        }
        if (before.phone.empty()) {
            if (send_ok(ctx, "AT+CNUM", 1500, &resp)) patch.phone = parse_cnum_phone(resp);
        }
    }

    bool static_changed = (!patch.mfr.empty() && patch.mfr != before.mfr) ||
                          (!patch.model.empty() && patch.model != before.model) ||
                          (!patch.fwver.empty() && patch.fwver != before.fwver) ||
                          (!patch.imei.empty() && patch.imei != before.imei) ||
                          (!patch.iccid.empty() && patch.iccid != before.iccid) ||
                          (!patch.imsi.empty() && patch.imsi != before.imsi);
    bool network_changed = (!patch.operatorName.empty() && patch.operatorName != before.operatorName) ||
                           (!patch.apnSim.empty() && patch.apnSim != before.apnSim) ||
                           (!patch.phone.empty() && patch.phone != before.phone);
    bool material_static_change = (!patch.imei.empty() && !before.imei.empty() && patch.imei != before.imei) ||
                                  (!patch.iccid.empty() && !before.iccid.empty() && patch.iccid != before.iccid) ||
                                  (!patch.imsi.empty() && !before.imsi.empty() && patch.imsi != before.imsi);
    bool changed = static_changed || network_changed;
    update_status(ctx, patch, true, false);
    IdfModemStatus after = get_status(ctx);
    ctx.identity_static_attempted = !after.mfr.empty() && !after.model.empty() && !after.fwver.empty();
    if (include_network_fields) {
        ctx.identity_network_attempted = !after.operatorName.empty();
    }
    if (material_static_change) {
        ESP_LOGI(TAG, "slot=%u identity changed imei=%s iccid=%s imsi=%s", ctx.hardware.slot,
                 after.imei.empty() ? "-" : after.imei.c_str(),
                 after.iccid.empty() ? "-" : after.iccid.c_str(),
                 after.imsi.empty() ? "-" : after.imsi.c_str());
        MODEM_LOGF(ctx, "模组身份变化 IMEI=%s ICCID=%s IMSI=%s",
                   after.imei.empty() ? "-" : after.imei.c_str(),
                   after.iccid.empty() ? "-" : after.iccid.c_str(),
                   after.imsi.empty() ? "-" : after.imsi.c_str());
    }
    if (log_summary) {
        MODEM_LOGF(ctx, "模组信息采样完成：ICCID %s，运营商 %s；缺失字段将后台重试",
                   after.iccid.empty() ? "缺失" : "已读取",
                   after.operatorName.empty() ? "缺失" : "已读取");
    }
    save_identity_cache(ctx, patch.imei, patch.iccid);
    return changed;
}

static void modem_en_gpio_init(ModemContext& ctx)
{
    gpio_config_t io = {};
    io.pin_bit_mask = 1ULL << ctx.hardware.en;
    io.mode = GPIO_MODE_OUTPUT;
    io.pull_up_en = GPIO_PULLUP_DISABLE;
    io.pull_down_en = GPIO_PULLDOWN_DISABLE;
    io.intr_type = GPIO_INTR_DISABLE;
    gpio_config(&io);
}

// 只拉高 EN 保持/接通供电，不做断电周期(热启动探测用)。
// 先写输出寄存器再配方向：gpio_config 切到输出的瞬间即输出高，
// 避免默认输出 0 造成 EN 瞬时拉低、给在运行的模组一个断电毛刺
static void modem_power_hold_on(ModemContext& ctx)
{
    gpio_set_level(ctx.hardware.en, 1);
    modem_en_gpio_init(ctx);
    gpio_set_level(ctx.hardware.en, 1);
}

static void modem_power_cycle(ModemContext& ctx)
{
    modem_en_gpio_init(ctx);

    set_phase(ctx, "powering");
    gpio_set_level(ctx.hardware.en, 0);
    vTaskDelay(pdMS_TO_TICKS(MODEM_POWERDOWN_MS));
    gpio_set_level(ctx.hardware.en, 1);
    vTaskDelay(pdMS_TO_TICKS(MODEM_POWERUP_MIN_MS));
}

static bool wait_at_ready(ModemContext& ctx)
{
    TickDeadline deadline(MODEM_POWERUP_MAX_MS);
    while (!deadline.expired()) {
        if (send_ok(ctx, "AT", 700)) return true;
        vTaskDelay(pdMS_TO_TICKS(250));
    }
    return false;
}

static bool modem_hot_start_allowed(esp_reset_reason_t reason)
{
    switch (reason) {
        case ESP_RST_PANIC:
        case ESP_RST_INT_WDT:
        case ESP_RST_TASK_WDT:
        case ESP_RST_WDT:
        case ESP_RST_CPU_LOCKUP:
            return true;
        default:
            return false;
    }
}

static bool modem_quick_start_allowed(esp_reset_reason_t reason)
{
    switch (reason) {
        case ESP_RST_POWERON:
        case ESP_RST_SW:
        case ESP_RST_EXT:
            return true;
        default:
            return false;
    }
}

// 解析 AT+CPMS 设置命令的响应 "+CPMS: <used1>,<total1>,<used2>,<total2>,<used3>,<total3>"，
// 输出 <mem1> 总容量。查询响应(+CPMS: "SM",0,0,...)带引号存储名会解析失败，恰好只匹配设置响应。
static bool parse_cpms_total(const std::string& resp, long& total)
{
    size_t p = resp.find("+CPMS:");
    if (p == std::string::npos) return false;
    std::string line = line_containing(resp, p);
    const char* token = strstr(line.c_str(), "+CPMS:");
    if (!token) return false;
    long values[6] = {};
    int count = 0;
    if (!parse_comma_longs(token + strlen("+CPMS:"), values, 6, count) || count < 2) return false;
    total = values[1];
    return true;
}

static int sms_storage_code(const char* name)
{
    if (strcmp(name, "MT") == 0) return 0;
    if (strcmp(name, "ME") == 0) return 1;
    if (strcmp(name, "SM") == 0) return 2;
    return -1;
}

static void log_sms_storage_if_changed(ModemContext& ctx, const char* name)
{
    int current = sms_storage_code(name);
    int previous = ctx.logged_sms_storage_code.exchange(current, std::memory_order_relaxed);
    if (previous == current) return;
    // 首次 MT 是常规路径不提示；非 MT 或后续类型变化才记录，避免周期性 CPMS 重申刷屏。
    if (previous != -1 || current != 0) MODEM_LOGF(ctx, "短信存储使用 %s", name);
}

// 按优先级选择短信存储：MT → ME → SM。部分可写 eSIM 的 SM 存储返回 OK 但容量为
// 0,0(issue #3)，此时短信实际无处可存，必须视为不可用并继续尝试下一候选；
// 响应里解析不出容量的按可用处理(不同固件设置命令可能只回 OK)。
static bool select_sms_storage(ModemContext& ctx)
{
    static const struct { const char* cmd; const char* name; } kCandidates[] = {
        {"AT+CPMS=\"MT\",\"MT\",\"MT\"", "MT"},
        {"AT+CPMS=\"ME\",\"ME\",\"ME\"", "ME"},
        {"AT+CPMS=\"SM\",\"SM\",\"SM\"", "SM"},
    };
    for (const auto& c : kCandidates) {
        std::string resp;
        if (!send_ok(ctx, c.cmd, 1500, &resp)) continue;
        long total = -1;
        if (parse_cpms_total(resp, total) && total <= 0) {
            MODEM_LOGF(ctx, "短信存储 %s 容量为 0，尝试下一候选", c.name);
            continue;
        }
        log_sms_storage_if_changed(ctx, c.name);
        return true;
    }
    MODEM_LOG_LINE(ctx, "警告: 无可用短信存储(MT/ME/SM 均不可用)，短信接收可能失败");
    return false;
}

// 存储选择失败待补跑标志：冷启动时 CPMS 早于 SIM 就绪执行，SIM 初始化慢的开机
// 可能三个候选全失败(SIM busy)。注册成功意味着 SIM 必已就绪，届时补跑一次。
// 只在 modem_task 上下文读写，无需原子量。
static void retry_sms_storage_if_pending(ModemContext& ctx)
{
    if (!ctx.sms_storage_pending) return;
    MODEM_LOG_LINE(ctx, "SIM 已就绪，补跑短信存储选择");
    ctx.sms_storage_pending = !select_sms_storage(ctx);
}

void idf_modem_reassert_sms_storage(uint8_t slot)
{
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx) return;
    // 供短信任务周期性重申：模组自发复位(未被 ESP 侧察觉)后 CPMS 会回落到固件
    // 默认存储(常为 SM)；SM 容量为 0 的 eSIM 上短信从此无处可存，接收静默死亡，
    // 而 AT 探测/发送一切正常——"先能收后失效"的典型固件侧成因。
    // select_sms_storage 内部走加锁的 AT 通道，跨任务调用安全。
    select_sms_storage(*ctx);
}

static bool configure_sms_and_registration(ModemContext& ctx)
{
    send_ok(ctx, "ATE0", 1000);
    send_ok(ctx, "AT+CMEE=1", 1200);  // 明确返回 +CMS/+CME 数字错误，避免只有笼统 ERROR
    // 直推 +CMT 使用 Phase 2+ 确认流程；收到每个 PDU 后由短信任务发送 AT+CNMA=0。
    // ML307R 手册明确指出可靠的 TA-TE 短信传输需要 +CNMA，否则连续长短信可能只上报一段。
    bool phase2_ok = send_ok(ctx, "AT+CSMS=1", 1200);
    bool pdu_mode_ok = send_ok(ctx, "AT+CMGF=0", 1200);
    // 统一补收路径的短信存储位置：特殊类别/启动期落盘后，+CMTI 与 CMGL/CMGR
    // 必须查看同一存储，否则兜底索引会指向另一块存储。
    bool storage_ok = select_sms_storage(ctx);
    ctx.sms_storage_pending = !storage_ok;
    // 普通短信直接以 +CMT 送到 ESP32，绕过部分 eSIM/模组存储只留下末段的问题；
    // +CMTI 与 CMGL 轮询仍保留，补收启动期或特殊类别落盘的短信。
    bool cnmi_ok = send_ok(ctx, "AT+CNMI=2,2,0,0,0", 1200);
    send_ok(ctx, "AT+CEREG=2", 1200);
    // 开启主叫号码上报：来电时模组主动上报 RING + +CLIP: "号码",...，供来电通知使用。
    // 无语音能力的卡/模组下该指令可能 ERROR，忽略即可(收不到来电就不会有 URC)。
    send_ok(ctx, "AT+CLIP=1", 1200);
    // NET 指示灯开关(ML307R: AT+MLED=0,<0/1>)：每次初始化按保存的配置下发，
    // 覆盖模组记住的上次状态
    send_ok(ctx, idf_config_net_led_enabled(ctx.hardware.slot) ? "AT+MLED=0,1" : "AT+MLED=0,0", 1200);
    bool sms_ready = phase2_ok && pdu_mode_ok && storage_ok && cnmi_ok;
    if (!sms_ready) MODEM_LOG_LINE(ctx, "短信收发配置未完整生效，将在后续健康检查中重试");
    return sms_ready;
}

// SIM 就绪后的初始化顺序必须保持一致；启动、重启恢复和凭据解锁都走同一入口。
static bool initialize_sim_stack(ModemContext& ctx, bool allow_puk)
{
    if (!try_unlock_sim(ctx, allow_puk)) return false;
    configure_sms_and_registration(ctx);
    set_phase(ctx, "registering");
    apply_startup_data_mode(ctx);
    return true;
}

static bool response_has_compact(std::string response, const char* expected)
{
    response.erase(std::remove_if(response.begin(), response.end(), [](unsigned char ch) {
        return isspace(ch);
    }), response.end());
    return response.find(expected) != std::string::npos;
}

static void query_sms_receive_config(ModemContext& ctx, bool& phase2, bool& pdu, bool& cnmi)
{
    std::string resp;
    phase2 = send_ok(ctx, "AT+CSMS?", 1200, &resp) && response_has_compact(resp, "+CSMS:1,");
    pdu = send_ok(ctx, "AT+CMGF?", 1200, &resp) && response_has_compact(resp, "+CMGF:0");
    cnmi = send_ok(ctx, "AT+CNMI?", 1200, &resp) &&
           response_has_compact(resp, "+CNMI:2,2,0,0,0");
}

bool idf_modem_sms_health_check(uint8_t slot, std::string& summary)
{
    summary.clear();
    ModemContext* context = context_for_slot(slot);
    if (!context) {
        summary = "非法模组 slot";
        return false;
    }
    ModemContext& ctx = *context;
    if (!get_runtime_status(ctx).atReady) {
        idf_modem_request_reset(slot, true);
        summary = "模组 AT 未就绪，已请求模组硬重启";
        MODEM_LOGF(ctx, "每日短信体检异常: %s", summary.c_str());
        return false;
    }

    std::string resp;
    int stat = -1;
    bool registered = send_ok(ctx, "AT+CEREG?", 1200, &resp) && parse_cereg(resp, stat) &&
                      (stat == 1 || stat == 5);
    bool phase2 = false;
    bool pdu = false;
    bool cnmi = false;
    query_sms_receive_config(ctx, phase2, pdu, cnmi);
    bool storage = select_sms_storage(ctx);
    bool initially_ok = registered && phase2 && pdu && cnmi && storage;

    if (!phase2 || !pdu || !cnmi || !storage) {
        configure_sms_and_registration(ctx);
        stat = -1;
        registered = send_ok(ctx, "AT+CEREG?", 1200, &resp) && parse_cereg(resp, stat) &&
                     (stat == 1 || stat == 5);
        query_sms_receive_config(ctx, phase2, pdu, cnmi);
        storage = select_sms_storage(ctx);
    }

    bool final_ok = registered && phase2 && pdu && cnmi && storage;
    char state[192];
    snprintf(state, sizeof(state),
             "注册=%s，Phase2+=%s，PDU=%s，CNMI=%s，存储=%s（不含运营商端到端投递）",
             registered ? "正常" : "异常", phase2 ? "正常" : "异常", pdu ? "正常" : "异常",
             cnmi ? "正常" : "异常", storage ? "正常" : "异常");

    if (initially_ok) {
        summary = std::string("正常：") + state;
        MODEM_LOGF(ctx, "每日短信体检通过: %s", summary.c_str());
        return true;
    }
    if (final_ok) {
        summary = std::string("发现异常，已修复：") + state;
    } else {
        idf_modem_request_reset(slot, true);
        summary = std::string("异常未恢复，已请求模组硬重启：") + state;
    }
    MODEM_LOGF(ctx, "每日短信体检异常: %s", summary.c_str());
    return false;
}

// 重启后 AT 握手失败时置位：一旦后续任何探测发现 AT 恢复，立即补跑完整初始化
// (ATE0/CMGF/CNMI/CEREG/CGACT)。否则模组以默认配置运行——回显开着、URC 不上报、
// 数据连接按模组默认自动激活(产生流量费，恰是本项目要防止的)。
static bool handle_reset_request_if_any(ModemContext& ctx)
{
    // 逻辑通道尚未关闭时重启会把 eSIM 操作截断；切卡成功后 idf_esim 请求的软重启
    // 会等到 APDU 会话结束(深度归零)才执行。
    if (ctx.esim_operation_depth.load(std::memory_order_relaxed) != 0) return false;
    int request = ctx.reset_request.exchange(0, std::memory_order_relaxed);
    if (request == 0) return false;

    IdfModemStatus patch;
    patch.phase = "powering";
    patch.atReady = false;
    patch.modemReady = false;
    update_status(ctx, patch);
    reset_identity_sampling_state(ctx);
    if (request == 2) {
        MODEM_LOG_LINE(ctx, "执行模组硬重启");
        modem_power_cycle(ctx);
    } else {
        MODEM_LOG_LINE(ctx, "执行模组软重启");
        send_ok(ctx, "AT+CFUN=1,1", 15000);
        vTaskDelay(pdMS_TO_TICKS(5000));
    }

    if (!wait_at_ready(ctx)) {
        set_phase(ctx, "failed");
        MODEM_LOG_LINE(ctx, "模组重启后 AT 握手失败，等待恢复后补跑初始化");
        ctx.reinit_pending = true;
        return true;
    }
    patch = {};
    patch.started = true;
    patch.atReady = true;
    patch.modemReady = false;
    patch.phase = "at_ready";
    update_status(ctx, patch);
    initialize_sim_stack(ctx, false);
    ctx.reinit_pending = false;
    return true;
}

// AT 恢复后的补初始化（配合 ctx.reinit_pending）
static void run_pending_reinit_if_recovered(ModemContext& ctx)
{
    if (!ctx.reinit_pending) return;
    if (!at_channel_idle_now(ctx)) return;
    if (!send_ok(ctx, "AT", 700)) return;
    MODEM_LOG_LINE(ctx, "模组 AT 已恢复，补跑短信/注册/数据配置");
    IdfModemStatus patch;
    patch.started = true;
    patch.atReady = true;
    patch.modemReady = false;
    patch.phase = "at_ready";
    update_status(ctx, patch);
    initialize_sim_stack(ctx, false);
    ctx.reinit_pending = false;
}

static void modem_task(void* arg)
{
    ModemContext& ctx = *static_cast<ModemContext*>(arg);
    IdfModemStatus patch;
    patch.started = true;
    patch.phase = "powering";
    update_status(ctx, patch);
    reset_identity_sampling_state(ctx);

    bool at_ready = false;
    esp_reset_reason_t reset_reason = esp_reset_reason();
    // 热启动快路径只留给崩溃/看门狗等意外复位；正常上电/软件重启先快速拉高 EN 探测，
    // 能直接 AT 就省掉强制断电周期。USB/串口复位仍冷启动，避免烧录后沿用半初始化状态。
    if (modem_hot_start_allowed(reset_reason)) {
        modem_power_hold_on(ctx);
        if (wait_at_ready(ctx)) {
            at_ready = true;
            MODEM_LOG_LINE(ctx, "模组已在运行，跳过断电上电(热启动)");
        } else {
            MODEM_LOG_LINE(ctx, "意外复位后热启动探测失败，改为模组冷启动");
        }
    } else if (modem_quick_start_allowed(reset_reason)) {
        modem_power_hold_on(ctx);
        if (wait_at_ready(ctx)) {
            at_ready = true;
            MODEM_LOGF(ctx, "复位原因 %d，模组快速上电完成", static_cast<int>(reset_reason));
        } else {
            MODEM_LOGF(ctx, "复位原因 %d，模组快速上电超时，改为冷启动",
                       static_cast<int>(reset_reason));
        }
    } else {
        MODEM_LOGF(ctx, "复位原因 %d，模组执行冷启动", static_cast<int>(reset_reason));
    }

    if (!at_ready) {
        // 启动握手：失败绝不放弃(Arduino 版靠 modemHealthTick 无限恢复)。任务一旦退出，
        // 网页重启模组、URC 轮询、健康探测全部失效，设备只能整机断电才能恢复。
        modem_power_cycle(ctx);
        int round = 0;
        uint32_t retry_gap_ms = 5000;
        while (!wait_at_ready(ctx)) {
            set_phase(ctx, "failed");
            ++round;
            ESP_LOGE(TAG, "slot=%u AT 握手超时(第%d轮)", ctx.hardware.slot, round);
            MODEM_LOGF(ctx, "模组 AT 握手超时(第%d轮)，稍后重新上电重试", round);
            ctx.reset_request.store(0, std::memory_order_relaxed);  // 重启请求由本轮上电一并满足
            vTaskDelay(pdMS_TO_TICKS(retry_gap_ms));
            if (retry_gap_ms < 60000) retry_gap_ms *= 2;  // 5s→10s→…→60s 封顶，避免热循环
            modem_power_cycle(ctx);
        }
    }

    patch = {};
    patch.started = true;
    patch.atReady = true;
    patch.phase = "at_ready";
    update_status(ctx, patch);
    ESP_LOGI(TAG, "slot=%u AT 已就绪", ctx.hardware.slot);
    MODEM_LOG_LINE(ctx, "模组 AT 已就绪");

    bool sim_ready = initialize_sim_stack(ctx, false);
    TickType_t last_sim_unlock_check = xTaskGetTickCount();
    uint8_t sim_unlock_retry_level = 0;

    int check_count = 0;
    int stat = -1;
    while (sim_ready && check_count++ < 30) {
        std::string resp;
        if (send_ok(ctx, "AT+CEREG?", 1200, &resp) && parse_cereg(resp, stat)) {
            IdfModemStatus reg_patch;
            reg_patch.ceregStat = stat;
            reg_patch.phase = (stat == 1 || stat == 5) ? "sampling" : "registering";
            reg_patch.modemReady = (stat == 1 || stat == 5);
            update_status(ctx, reg_patch);
            if (reg_patch.modemReady) break;
        }
        vTaskDelay(pdMS_TO_TICKS(1500));
    }
    bool registered = (stat == 1 || stat == 5);
    bool post_register_done = false;
    TickType_t last_identity = 0;
    uint8_t identity_retry_level = 0;
    if (!sim_ready) {
        // 锁卡/无卡状态由 try_unlock_sim 写入，后台按退避间隔重查。
    } else if (!registered) {
        set_phase(ctx, "failed");
    } else {
        retry_sms_storage_if_pending(ctx);
        IdfSimNetworkView cfg = idf_config_get_sim_network_view(ctx.hardware.slot);
        apply_operator_if_configured(ctx, cfg);
        if (cfg.dataEnabled) sample_cell_ip_once(ctx);
        enforce_roaming_data_policy(ctx, cfg, stat);
        // 注册成功后立即做一轮首页基础信息采样；Web/WiFi 已先启动，不会阻塞页面打开。
        sample_signal_once(ctx);
        sample_signal_detail_once(ctx);
        sample_identity_once(ctx, false, true);
        last_identity = xTaskGetTickCount();
        post_register_done = startup_sampling_done(ctx);
        set_phase(ctx, post_register_done ? "ready" : "sampling");
    }

    TickType_t last_signal = 0;
    TickType_t last_detail = 0;
    TickType_t last_health = 0;
    TickType_t last_cell_ip = 0;
    int health_fail_count = 0;
    int dereg_count = 0;
    TickType_t last_sim_check = 0;
    int64_t sim_check_not_before_us = 0;  // 模组重启后给 SIM/CPIN 充分上电时间，避免误判二次拔插
    int sim_present = -1;  // -1=未知(仅记基线) 0=无卡 1=有卡
    bool sms_reconfigure_pending = false;  // 换卡/掉网恢复后在注册成功点再次重申短信栈
    while (true) {
        if (xSemaphoreTake(ctx.status_mutex, 0) == pdTRUE) {
            ctx.status.stackFreeBytes = uxTaskGetStackHighWaterMark(nullptr);
            xSemaphoreGive(ctx.status_mutex);
        }
        bool reset_handled = handle_reset_request_if_any(ctx);
        run_pending_reinit_if_recovered(ctx);
        if (!sim_ready && get_runtime_status(ctx).simReady) sim_ready = true;
        TickType_t now = xTaskGetTickCount();
        if (reset_handled) {
            sim_ready = get_runtime_status(ctx).simReady;
            // 运行中重启不能沿用重启前的局部注册状态；否则 status 已进入 registering，
            // 但本任务仍以 registered=true 按 60 秒慢周期探测，换卡后恢复会被无谓拖延。
            registered = false;
            post_register_done = false;
            health_fail_count = 0;
            dereg_count = 0;
            last_health = 0;
            last_cell_ip = 0;
            last_identity = 0;
            identity_retry_level = 0;
            last_sim_unlock_check = now;
            sim_unlock_retry_level = 0;
            last_sim_check = now;  // 给 SIM 上电初始化留出一个完整检测周期
            sim_check_not_before_us = esp_timer_get_time() + 30LL * 1000LL * 1000LL;
            sim_present = -1;
            sms_reconfigure_pending = true;
        }
        int unlock_request = ctx.sim_unlock_request.exchange(0, std::memory_order_relaxed);
        bool sim_unlock_checked = false;
        if (unlock_request != 0) {
            if (!at_channel_idle_now(ctx)) {
                ctx.sim_unlock_request.store(unlock_request, std::memory_order_relaxed);
            } else {
                if (unlock_request == 1) ctx.last_pin_attempt_key.clear();
                sim_ready = initialize_sim_stack(ctx, unlock_request == 2);
                sim_unlock_checked = true;
                last_sim_unlock_check = now;
                sim_unlock_retry_level = 0;
            }
        }
        bool sim_unlock_retry_due = !sim_ready &&
                                    now - last_sim_unlock_check >= pdMS_TO_TICKS(
                                        modem_retry_delay_ms(sim_unlock_retry_level));
        if (!sim_unlock_checked && sim_unlock_retry_due && at_channel_idle_now(ctx)) {
            // 所有恢复共用初始化顺序；后台重试永远不提交 PUK。
            sim_ready = initialize_sim_stack(ctx, false);
            sim_unlock_checked = true;
            last_sim_unlock_check = now;
            if (sim_ready) sim_unlock_retry_level = 0;
            else if (sim_unlock_retry_level + 1 < MODEM_RETRY_DELAY_COUNT) ++sim_unlock_retry_level;
        }
        if (sim_unlock_checked && sim_ready) {
            registered = false;
            post_register_done = false;
            last_identity = 0;
            identity_retry_level = 0;
            sms_reconfigure_pending = true;
            last_health = 0;
        }
        if (process_data_mode_retry(ctx)) {
            vTaskDelay(pdMS_TO_TICKS(200));
            continue;
        }
        // 用户手动刷新会绕过常规间隔并尽快跑一轮；若 AT 正忙，请求保留到下轮空闲时执行。
        bool force_sample = ctx.status_sampling.pending();
        bool web_active = force_sample ||
                          (esp_timer_get_time() -
                           ctx.last_web_poll_us.load(std::memory_order_relaxed)) < WEB_POLL_ACTIVE_WINDOW_US;
        bool startup_sampling = registered && !post_register_done;
        bool identity_retry_due = !startup_info_complete(ctx) &&
                                  (last_identity == 0 || now - last_identity >= pdMS_TO_TICKS(
                                      modem_retry_delay_ms(identity_retry_level)));
        // 未就绪 SIM 的手动刷新只采样，不触发密码提交；AT 忙时保留请求代次。
        bool can_sample = sim_ready || (force_sample && get_runtime_status(ctx).atReady);
        if (can_sample && (web_active || startup_sampling || identity_retry_due) && at_channel_idle_now(ctx)) {
            uint32_t sample_request = 0;
            if (force_sample) {
                sample_request = ctx.status_sampling.begin();
                MODEM_LOG_LINE(ctx, "模组信息刷新开始");
            }
            bool data_enabled = false;
            idf_config_get_cellular_settings(ctx.hardware.slot, data_enabled, nullptr);
            if (data_enabled && get_runtime_status(ctx).cellIpEmpty &&
                (last_cell_ip == 0 ||
                 now - last_cell_ip > pdMS_TO_TICKS(MODEM_DATA_MODE_RETRY_GAP_MS))) {
                sample_cell_ip_once(ctx);
                last_cell_ip = now;
            }
            if (startup_sampling || force_sample || last_signal == 0 ||
                now - last_signal > pdMS_TO_TICKS(SIGNAL_INTERVAL_WEB_MS)) {
                sample_signal_once(ctx);
                last_signal = now;
            }
            if (startup_sampling || force_sample || last_detail == 0 ||
                now - last_detail > pdMS_TO_TICKS(SIGNAL_DETAIL_INTERVAL_WEB_MS)) {
                sample_signal_detail_once(ctx);
                last_detail = now;
            }
            if (startup_sampling || force_sample || identity_retry_due) {
                bool identity_changed = sample_identity_once(ctx, force_sample, sim_ready);
                last_identity = now;
                if (startup_info_complete(ctx) || identity_changed) identity_retry_level = 0;
                else if (identity_retry_level + 1 < MODEM_RETRY_DELAY_COUNT) ++identity_retry_level;
            }
            if (startup_sampling && startup_sampling_done(ctx)) {
                set_phase(ctx, "ready");
                post_register_done = true;
            }
            if (force_sample) ctx.status_sampling.finish(sample_request);
        }
        // 正常态按 60s 健康探测；未注册但并非已确认无卡时缩短到 5s，
        // 让热插拔/自动重启后的注册恢复不必最多再等一分钟。
        uint32_t health_interval_ms = 60000UL;
        if (!registered && sim_present != 0) health_interval_ms = 5000UL;
        else if (sms_reconfigure_pending && sim_present != 0) health_interval_ms = 15000UL;
        if (sim_ready && (reset_handled || now - last_health > pdMS_TO_TICKS(health_interval_ms)) &&
            at_channel_idle_now(ctx)) {
            last_health = now;
            std::string resp;
            if (send_ok(ctx, "AT+CEREG?", 1200, &resp) && parse_cereg(resp, stat)) {
                health_fail_count = 0;
                bool now_ready = (stat == 1 || stat == 5);
                IdfModemStatus reg_patch;
                reg_patch.ceregStat = stat;
                reg_patch.modemReady = now_ready;
                reg_patch.phase = now_ready ? (post_register_done ? "ready" : "sampling") : "registering";
                update_status(ctx, reg_patch);
                if (now_ready) {
                    registered = true;
                    dereg_count = 0;
                    bool sms_reconfigured_now = false;
                    if (sms_reconfigure_pending) {
                        // CPIN READY 之后模组仍可能异步重建短信栈并回落默认设置；
                        // 必须在真正注册成功的稳定点再次写入 PDU/CNMI/CPMS。
                        MODEM_LOG_LINE(ctx, "网络重新注册成功，重申短信收发配置");
                        sms_reconfigure_pending = !configure_sms_and_registration(ctx);
                        sms_reconfigured_now = true;
                    }
                    if (!post_register_done) {
                        // 迟到/恢复的注册也要补跑必须的网络配置和首页基础信息。
                        // 掉网后恢复可能意味着模组自发复位过：存储选择无条件重跑，
                        // 不能只看 pending 标志(初次成功后它恒为 false)
                        if (!sms_reconfigured_now) {
                            ctx.sms_storage_pending = !select_sms_storage(ctx);
                        }
                        IdfSimNetworkView cfg = idf_config_get_sim_network_view(ctx.hardware.slot);
                        apply_operator_if_configured(ctx, cfg);
                        if (cfg.dataEnabled) sample_cell_ip_once(ctx);
                        enforce_roaming_data_policy(ctx, cfg, stat);
                        sample_signal_once(ctx);
                        sample_signal_detail_once(ctx);
                        sample_identity_once(ctx, false, true);
                        last_identity = now;
                        identity_retry_level = 0;
                        post_register_done = startup_sampling_done(ctx);
                        set_phase(ctx, post_register_done ? "ready" : "sampling");
                    }
                } else {
                    registered = false;
                    post_register_done = false;
                    sms_reconfigure_pending = true;
                    // 未注册态改为 5 秒快探测后，仍保持约 5 分钟再重启，避免普通的小区
                    // 重选/漫游注册过程被过早打断；已确认无卡时不做无意义的周期重启。
                    if (sim_present == 0) {
                        dereg_count = 0;
                    } else if (++dereg_count >= 60) {
                        dereg_count = 0;
                        MODEM_LOG_LINE(ctx, "模组长时间未注册网络，触发硬重启恢复");
                        ctx.reset_request.store(2, std::memory_order_relaxed);
                    }
                }
            } else if (++health_fail_count >= 3) {
                health_fail_count = 0;
                MODEM_LOG_LINE(ctx, "模组健康探测连续失败，触发硬重启恢复");
                ctx.reset_request.store(2, std::memory_order_relaxed);
            }
        }
        // SIM 热插拔检测：低频轮询 AT+CPIN?，识别运行中插卡/拔卡。
        // 插入(无卡→有卡)：自动硬重启模组 + 作废旧身份，让新卡从干净状态初始化；
        // 拔出(有卡→无卡)：标记未就绪并清空身份，避免概览沿用旧卡信息。
        int64_t sim_check_now_us = esp_timer_get_time();
        if (sim_check_now_us >= sim_check_not_before_us &&
            (last_sim_check == 0 || now - last_sim_check > pdMS_TO_TICKS(SIM_CHECK_INTERVAL_MS)) &&
            at_channel_idle_now(ctx)) {
            last_sim_check = now;
            std::string sim_state_now = query_sim_state(ctx);
            // 查询超时不等于拔卡：保留原基线，等待下一次 CPIN 返回确定状态。
            if (sim_state_now != "unknown") {
                int previous_present = sim_present;
                int present_now = sim_state_now == "absent" ? 0 : 1;
                sim_present = present_now;
                if (previous_present == 0 && present_now == 1) {
                    // 部分 ML307 固件换卡后会继续异步重建协议栈，必须硬重启才能稳定恢复短信配置。
                    MODEM_LOG_LINE(ctx, "检测到 SIM 卡插入，自动硬重启模组以完整初始化短信栈");
                    idf_modem_invalidate_sim_identity(ctx.hardware.slot);
                    registered = false;
                    post_register_done = false;
                    dereg_count = 0;
                    sms_reconfigure_pending = true;
                    idf_modem_request_reset(ctx.hardware.slot, true);
                } else if (present_now == 0 && previous_present != 0) {
                    if (previous_present == -1 && !sim_ready) {
                        set_sim_status(ctx, "absent", false, {});
                    } else {
                        // sim_ready 已证明此前 CPIN=READY；即使尚未建立在位基线，首次 absent 也是拔卡。
                        MODEM_LOG_LINE(ctx, "检测到 SIM 卡移除");
                        registered = false;
                        post_register_done = false;
                        sms_reconfigure_pending = true;
                        sim_ready = false;
                        idf_modem_invalidate_sim_identity(ctx.hardware.slot);
                        set_sim_status(ctx, "absent", false, {});
                        set_phase(ctx, "registering");
                    }
                } else if (!sim_ready && present_now == 1 &&
                           (previous_present == -1 || sim_state_now == "ready")) {
                    if (sim_state_now == "ready") {
                        MODEM_LOG_LINE(ctx, "检测到 SIM 已就绪，恢复短信和网络初始化");
                    }
                    sim_ready = initialize_sim_stack(ctx, false);
                    if (sim_ready) {
                        registered = false;
                        post_register_done = false;
                        sms_reconfigure_pending = true;
                        last_health = 0;
                    }
                }
            }
        }
        for (int i = 0; i < 10; ++i) {
            // 采样请求只有 AT 空闲时才会被外层消费；通道被长任务(保号下载/eSIM)
            // 占用期间若无条件 break，外层 while 会变成无延时热自旋，饿死 idle 任务
            if (ctx.status_sampling.pending() &&
                get_runtime_status(ctx).atReady && at_channel_idle_now(ctx)) break;
            // 事件驱动等待：UART 一有数据(URC/短信直推)立刻醒来抓取；
            // 无事件时 500ms 超时兜底轮询，节奏与原轮询一致
            uart_event_t evt;
            if (ctx.uart_evt_queue) {
                if (xQueueReceive(ctx.uart_evt_queue, &evt, pdMS_TO_TICKS(500)) == pdTRUE) {
                    do {
                        handle_uart_event_error(ctx, evt);
                    } while (xQueueReceive(ctx.uart_evt_queue, &evt, 0) == pdTRUE);
                }
            } else {
                vTaskDelay(pdMS_TO_TICKS(500));
            }
            // AT 通道被长任务(保号下载/大批量 CMGL)占用时抢不到锁：小睡再试，
            // 避免下载期间每个 RX 块事件都空转唤醒(数据由持锁方消费，URC 也由其转存)
            if (!poll_unsolicited_uart(ctx, 20)) vTaskDelay(pdMS_TO_TICKS(100));
            // 重启请求/AT恢复补初始化尽快响应，不等满 5s 轮询窗
            if (ctx.reset_request.load(std::memory_order_relaxed) != 0) break;
            if (ctx.status_sampling.pending() &&
                get_runtime_status(ctx).atReady && at_channel_idle_now(ctx)) break;
        }
    }
}

esp_err_t idf_modem_start(uint8_t slot)
{
    ModemContext* context = context_for_slot(slot);
    if (!context) return ESP_ERR_INVALID_ARG;
    ModemContext& ctx = *context;
    if (ctx.started) return ESP_OK;
    cleanup_start_resources(ctx);
    ctx.sim_unlock_request.store(0, std::memory_order_relaxed);
    ctx.last_pin_attempt_key.clear();
    // eSIM 需跨多条 CCHO/CGLA/CCHC 独占 AT 通道，同任务内的单条 AT 再递归取锁。
    ctx.at_mutex = xSemaphoreCreateRecursiveMutex();
    ctx.status_mutex = xSemaphoreCreateMutex();
    ctx.urc_mutex = xSemaphoreCreateMutex();
    if (!ctx.event_sem) ctx.event_sem = xSemaphoreCreateBinary();
    if (!ctx.at_mutex || !ctx.status_mutex || !ctx.urc_mutex || !ctx.event_sem) {
        cleanup_start_resources(ctx);
        return ESP_ERR_NO_MEM;
    }
    bool data_enabled = false;
    idf_config_get_cellular_settings(slot, data_enabled, nullptr);
    if (!data_enabled) set_status_cell_ip(ctx, "");

    uart_config_t uart_cfg = {};
    uart_cfg.baud_rate = MODEM_BAUD;
    uart_cfg.data_bits = UART_DATA_8_BITS;
    uart_cfg.parity = UART_PARITY_DISABLE;
    uart_cfg.stop_bits = UART_STOP_BITS_1;
    uart_cfg.flow_ctrl = UART_HW_FLOWCTRL_DISABLE;
    uart_cfg.source_clk = UART_SCLK_DEFAULT;

    // 带事件队列安装：RX 数据到达即产生事件，模组任务空闲期可被立刻唤醒
    esp_err_t err = uart_driver_install(ctx.hardware.uart, UART_RX_BUF, 0, 16, &ctx.uart_evt_queue, 0);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "slot=%u UART driver install failed: %s", slot, esp_err_to_name(err));
        MODEM_LOGF(ctx, "模组 UART 驱动安装失败: %s", esp_err_to_name(err));
        cleanup_start_resources(ctx);
        return err;
    }
    err = uart_param_config(ctx.hardware.uart, &uart_cfg);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "slot=%u UART param config failed: %s", slot, esp_err_to_name(err));
        MODEM_LOGF(ctx, "模组 UART 参数配置失败: %s", esp_err_to_name(err));
        uart_driver_delete(ctx.hardware.uart);
        ctx.uart_evt_queue = nullptr;
        cleanup_start_resources(ctx);
        return err;
    }
    err = uart_set_pin(ctx.hardware.uart, ctx.hardware.tx, ctx.hardware.rx, UART_PIN_NO_CHANGE, UART_PIN_NO_CHANGE);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "slot=%u UART pin config failed: %s", slot, esp_err_to_name(err));
        MODEM_LOGF(ctx, "模组 UART 引脚配置失败: %s", esp_err_to_name(err));
        uart_driver_delete(ctx.hardware.uart);
        ctx.uart_evt_queue = nullptr;
        cleanup_start_resources(ctx);
        return err;
    }
    uart_flush_input(ctx.hardware.uart);
    ctx.started = true;

    BaseType_t ok = xTaskCreate(modem_task, ctx.hardware.task_name, 8192, &ctx, 4,
                                &ctx.task_handle);
    if (ok != pdPASS) {
        ctx.started = false;
        uart_driver_delete(ctx.hardware.uart);
        ctx.uart_evt_queue = nullptr;
        ctx.task_handle = nullptr;
        cleanup_start_resources(ctx);
        return ESP_ERR_NO_MEM;
    }
    return ESP_OK;
}

IdfModemStatus idf_modem_get_status(uint8_t slot)
{
    ModemContext* ctx = context_for_slot(slot);
    if (ctx) return get_status(*ctx);
    IdfModemStatus invalid;
    invalid.phase = "invalid_slot";
    invalid.simUnlockMessage = "非法模组 slot";
    return invalid;
}

IdfModemReadiness idf_modem_get_readiness(uint8_t slot)
{
    IdfModemReadiness readiness;
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->status_mutex) return readiness;
    if (xSemaphoreTake(ctx->status_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        readiness.atReady = ctx->status.atReady;
        readiness.modemReady = ctx->status.modemReady;
        xSemaphoreGive(ctx->status_mutex);
    }
    return readiness;
}

std::string idf_modem_get_phone_number(uint8_t slot)
{
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->status_mutex) return {};
    std::string phone;
    if (xSemaphoreTake(ctx->status_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        phone = ctx->status.phone;
        xSemaphoreGive(ctx->status_mutex);
    }
    return phone;
}

esp_err_t idf_modem_get_imei(uint8_t slot, std::string& imei)
{
    imei.clear();
    ModemContext* context = context_for_slot(slot);
    if (!context) return ESP_ERR_INVALID_ARG;
    ModemContext& ctx = *context;
    IdfModemStatus status = get_status(ctx);
    if (is_imei_text(status.imei)) {
        imei = status.imei;
        return ESP_OK;
    }
    if (!ctx.started || !ctx.at_mutex) return ESP_ERR_INVALID_STATE;

    imei = query_imei_from_modem(ctx);
    if (!is_imei_text(imei)) {
        imei.clear();
        return ESP_ERR_NOT_FOUND;
    }

    IdfModemStatus patch;
    patch.imei = imei;
    update_status(ctx, patch, false, false);
    save_identity_cache(ctx, imei, std::string());
    return ESP_OK;
}

esp_err_t idf_modem_request_reset(uint8_t slot, bool hard_reset)
{
    ModemContext* context = context_for_slot(slot);
    if (!context) return ESP_ERR_INVALID_ARG;
    ModemContext& ctx = *context;
    if (!ctx.started) return ESP_ERR_INVALID_STATE;
    ctx.reset_request.store(hard_reset ? 2 : 1, std::memory_order_relaxed);
    set_phase(ctx, "powering");
    return ESP_OK;
}

esp_err_t idf_modem_request_sim_unlock(uint8_t slot, bool allow_puk)
{
    ModemContext* context = context_for_slot(slot);
    if (!context) return ESP_ERR_INVALID_ARG;
    ModemContext& ctx = *context;
    if (!ctx.started) return ESP_ERR_INVALID_STATE;
    ctx.sim_unlock_request.store(allow_puk ? 2 : 1, std::memory_order_relaxed);
    return ESP_OK;
}

bool idf_modem_at_idle(uint8_t slot)
{
    ModemContext* ctx = context_for_slot(slot);
    return ctx && at_channel_idle_now(*ctx);
}

void idf_modem_request_status_sample(uint8_t slot)
{
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx) return;
    ctx->last_web_poll_us.store(esp_timer_get_time(), std::memory_order_relaxed);
    bool already_pending = ctx->status_sampling.pending();
    ctx->status_sampling.request();
    if (!already_pending) MODEM_LOG_LINE(*ctx, "模组信息刷新已排队，等待 AT 通道就绪且空闲");
}

void idf_modem_begin_esim_operation(uint8_t slot)
{
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx) return;
    ctx->esim_operation_depth.fetch_add(1, std::memory_order_relaxed);
    if (ctx->at_mutex) xSemaphoreTakeRecursive(ctx->at_mutex, portMAX_DELAY);
}

void idf_modem_end_esim_operation(uint8_t slot)
{
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx) return;
    if (ctx->at_mutex) xSemaphoreGiveRecursive(ctx->at_mutex);
    ctx->esim_operation_depth.fetch_sub(1, std::memory_order_relaxed);
}

bool idf_modem_esim_operation_active(uint8_t slot)
{
    ModemContext* ctx = context_for_slot(slot);
    return ctx && ctx->esim_operation_depth.load(std::memory_order_relaxed) != 0;
}

void idf_modem_set_sim_identity_hook(void (*hook)(uint8_t slot))
{
    s_sim_identity_hook.store(hook, std::memory_order_relaxed);
}

void idf_modem_invalidate_sim_identity(uint8_t slot)
{
    ModemContext* context = context_for_slot(slot);
    if (!context) return;
    ModemContext& ctx = *context;
    // 清除随卡变化的身份字段：切/启/禁 eSIM Profile 后当前生效的卡已不同，
    // 但 sample_identity_once 对非空字段跳过重读，会一直沿用旧卡的号码/ICCID/运营商。
    if (ctx.status_mutex && xSemaphoreTake(ctx.status_mutex, pdMS_TO_TICKS(500)) == pdTRUE) {
        ctx.status.iccid.clear();
        ctx.status.imsi.clear();
        ctx.status.phone.clear();
        ctx.status.operatorName.clear();
        ctx.status.apnSim.clear();
        ctx.status.identityFresh = false;
        xSemaphoreGive(ctx.status_mutex);
    }
    // 复位采样"已尝试"标志，让网络字段(运营商/号码)重新查询；静态字段(型号/IMEI)非空仍跳过
    reset_identity_sampling_state(ctx);
    idf_modem_request_status_sample(slot);
    // 通知上层(idf_esim)：热插拔换卡后 EID 等缓存也要失效
    void (*hook)(uint8_t) = s_sim_identity_hook.load(std::memory_order_relaxed);
    if (hook) hook(slot);
}

void idf_modem_power_off_all_for_restart(void)
{
    for (ModemContext& ctx : s_modems) {
        if (!ctx.started) continue;
        // 先写输出寄存器再配方向，切到输出的瞬间即输出低
        gpio_set_level(ctx.hardware.en, 0);
        modem_en_gpio_init(ctx);
        gpio_set_level(ctx.hardware.en, 0);
    }
    // 保证断电时间足够(与 modem_power_cycle 一致)，ESP 重启后是干净的模组冷启动
    vTaskDelay(pdMS_TO_TICKS(MODEM_POWERDOWN_MS));
}

bool idf_modem_wait_event(uint8_t slot, uint32_t timeout_ms)
{
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx) return false;
    if (!ctx->event_sem) {
        vTaskDelay(pdMS_TO_TICKS(timeout_ms));
        return false;
    }
    return xSemaphoreTake(ctx->event_sem, pdMS_TO_TICKS(timeout_ms)) == pdTRUE;
}

void idf_modem_signal_event(uint8_t slot)
{
    ModemContext* ctx = context_for_slot(slot);
    if (ctx) signal_event(*ctx);
}

bool idf_modem_take_urc(uint8_t slot, std::string& out)
{
    out.clear();
    ModemContext* ctx = context_for_slot(slot);
    if (!ctx || !ctx->urc_mutex) return false;
    if (xSemaphoreTake(ctx->urc_mutex, pdMS_TO_TICKS(100)) != pdTRUE) return false;
    if (!ctx->urc_buffer.empty()) {
        out.swap(ctx->urc_buffer);
    }
    xSemaphoreGive(ctx->urc_mutex);
    return !out.empty();
}
