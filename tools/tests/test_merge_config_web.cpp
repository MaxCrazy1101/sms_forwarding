#include <cassert>
#include <climits>
#include <cstring>
#include <cstdio>
#include <cerrno>
#include <regex.h>
#include <stdexcept>
#include <mutex>
#include <thread>
#include "freertos/FreeRTOS.h"
#include "idf_config.h"
#include "idf_push.h"
#include "idf_modem.h"
#include "idf_sms.h"
#include "idf_util.h"
#include "idf_log.h"
#include "idf_heavy_operation_pause.h"
#include "nvs.h"

// 外部 HTTP/模组/NVS 是明确的主机替身；被验证的配置、解析、规则和等待函数来自生产源码。
struct httpd_req_t {
    std::string body, response;
    int status = 200;
    bool authenticated = true, csrf = true;
};
constexpr int HTTPD_400_BAD_REQUEST = 400;
static bool check_auth(httpd_req_t* req) { return req->authenticated; }
static bool check_csrf(httpd_req_t* req) { return req->csrf; }
static void set_json_no_cache(httpd_req_t*) {}
static void set_no_cache_headers(httpd_req_t*) {}
static int read_body(httpd_req_t* req, std::string& out, size_t max) {
    if (req->body.size() > max) { req->status = 413; return ESP_FAIL; }
    out = req->body; return ESP_OK;
}
static int httpd_resp_send_err(httpd_req_t* req, int status, const char* text) {
    req->status = status; req->response = text; return ESP_OK;
}
static int httpd_resp_set_status(httpd_req_t* req, const char*) { req->status = 400; return ESP_OK; }
static int httpd_resp_sendstr(httpd_req_t* req, const char* text) { req->response = text; return ESP_OK; }
static int httpd_resp_send(httpd_req_t* req, const char* text, size_t size) {
    req->response.assign(text, size); return ESP_OK;
}

static std::mutex gate;
static thread_local bool gate_held = false;
static int locks = 0, unlocks = 0, concurrent_pushes = 0;
void idf_push_heavy_operation_lock() { gate.lock(); gate_held = true; ++locks; }
void idf_push_heavy_operation_unlock() { assert(gate_held); gate_held = false; ++unlocks; gate.unlock(); }
static int reply_result = 1;
static bool send_failed = false, health_failed = false, throw_poll = false;
static int began_slot = -1, canceled_slot = -1, sent_slot = -1, health_slot = -1;
static int pushed_count = 0;
static bool email_queued = false;
bool idf_modem_sms_health_check(uint8_t slot, std::string& summary) {
    assert(gate_held); health_slot = slot; summary = "host health result"; return !health_failed;
}
bool idf_sms_reply_check_begin(uint8_t slot, const std::string&, const std::string&) {
    assert(gate_held); began_slot = slot; return true;
}
void idf_sms_reply_check_arm(uint8_t slot, int timeout) {
    assert(gate_held && slot == began_slot && timeout == 5);
}
int idf_sms_reply_check_poll(uint8_t slot) {
    assert(!gate_held && slot == began_slot);
    // 另一执行者获得同一 gate，模拟 push/SMTP 网络事务；没有释放时本测试会超时失败。
    std::thread push([] { std::lock_guard<std::mutex> lock(gate); ++concurrent_pushes; });
    push.join();
    if (throw_poll) throw std::runtime_error("injected polling error");
    return reply_result;
}
void idf_sms_reply_check_cancel(uint8_t slot) { assert(gate_held && slot == began_slot); canceled_slot = slot; }
esp_err_t idf_sms_send_text(uint8_t slot, const std::string&, const std::string&, std::string& text, bool track) {
    assert(gate_held); assert(!track || slot == began_slot); sent_slot = slot; text = "host send result";
    return send_failed ? ESP_FAIL : ESP_OK;
}
int idf_push_enqueue_notify(uint8_t, const char*, const char*, const char*) { return pushed_count; }
bool idf_push_enqueue_email(const char*, const char*) { return email_queued; }
static IdfCellularHttpConfig cellular_http_config(bool enabled, const std::string& apn) { return {enabled, apn}; }
static std::string cellular_http_message(const IdfCellularHttpResult& result) { return result.message; }
esp_err_t idf_modem_cellular_http_get(uint8_t, const std::string&, const IdfCellularHttpConfig&, IdfCellularHttpResult&) { return ESP_FAIL; }
static std::string format_epoch_local(uint32_t epoch, int tz) { return idf_util_format_epoch_local(epoch, tz); }
static bool valid_ussd_code(const std::string&) { return false; }
static bool run_ussd(uint8_t, const std::string&, std::string&) { return false; }
static void sched_set_job_message(const std::string&) {}
static void vTaskDelay(int) {}

IdfModemStatus idf_modem_get_status(uint8_t slot) {
    IdfModemStatus modem;
    modem.phase = "registering";
    modem.ceregStat = INT_MIN;
    modem.stackFreeBytes = UINT32_MAX;
    modem.sampleRequested = UINT32_MAX - slot;
    modem.sampleCompleted = UINT32_MAX - 2;
    modem.sampleRunning = slot == 0;
    modem.imsi = "46000\"escaped";
    return modem;
}
IdfSmsStatus idf_sms_get_status(uint8_t) { return {UINT32_MAX, UINT32_MAX, UINT32_MAX, true}; }
int idf_sms_outgoing_queue_depth(uint8_t) { return INT_MAX; }

#include "merge_functions.inc"

static void config_roundtrip()
{
    assert(idf_config_load() == ESP_OK);
    IdfSchedTask tasks[IDF_MAX_SCHED_TASKS];
    for (int i = 0; i < IDF_MAX_SCHED_TASKS; ++i) {
        auto& task = tasks[i];
        task.slot = i % 2; task.enabled = true; task.action = 2;
        task.name = "双槽任务"; task.target = "10086"; task.payload = "余额查询";
        task.intervalDays = i + 1; task.startMinute = 480 + i;
        task.checkSystem = true; task.checkReply = true;
        task.replySender = "^10086$"; task.replyBody = "余额为.*元"; task.replyTimeoutSec = 5 + i;
    }
    assert(idf_config_save_sched_tasks(tasks) == ESP_OK);
    auto exported = idf_config_export_text(true);
    assert(idf_config_load() == ESP_OK); // 真正从替身 NVS 重载生产配置。
    auto loaded = idf_config_get();
    auto core = idf_config_get_scheduler_core_view();
    for (int i = 0; i < IDF_MAX_SCHED_TASKS; ++i) {
        const auto& task = loaded.schedTasks[i];
        assert(task.slot == i % 2 && task.startMinute == 480 + i && task.intervalDays == i + 1);
        assert(task.checkSystem && task.checkReply && task.replySender == "^10086$");
        assert(task.replyBody == "余额为.*元" && task.replyTimeoutSec == 5 + i);
        assert(core.schedTasks[i].startMinute == task.startMinute && core.schedTasks[i].lastRun == task.lastRun);
    }
    assert(idf_config_import_text(exported, nullptr) == ESP_OK);
    assert(idf_config_import_text("st0Reply=0\n", nullptr) == ESP_ERR_INVALID_ARG);
    assert(idf_config_import_text("st0Slot=2\nst0Reply=0\n", nullptr) == ESP_ERR_INVALID_ARG);
    assert(idf_config_import_text("st0Slot=-1\nst0Reply=0\n", nullptr) == ESP_ERR_INVALID_ARG);
    assert(idf_config_import_text("st0Slot=1\nst0Reply=0\n", nullptr) == ESP_OK);
    assert(idf_config_get_sched_run_view(0).task.slot == 1);
    assert(idf_config_import_text("kaEnabled=1\n", nullptr) == ESP_ERR_INVALID_ARG);
    assert(idf_config_import_text("kaSlot=2\nkaEnabled=1\n", nullptr) == ESP_ERR_INVALID_ARG);
    assert(idf_config_import_text("kaSlot=1\nkaEnabled=1\n", nullptr) == ESP_OK);
    tasks[0].slot = 2;
    assert(idf_config_save_sched_tasks(tasks) == ESP_ERR_INVALID_ARG);
    tasks[0].slot = 0; tasks[0].replyBody = "[";
    assert(idf_config_save_sched_tasks(tasks) == ESP_ERR_INVALID_ARG);
    tasks[0].replyBody = ""; tasks[0].replyTimeoutSec = 4;
    assert(idf_config_save_sched_tasks(tasks) == ESP_ERR_INVALID_ARG);
}

static void fields_and_rules()
{
    IdfSchedTask tasks[IDF_MAX_SCHED_TASKS];
    IdfFormFields fields;
    for (int i = 0; i < IDF_MAX_SCHED_TASKS; ++i) fields.emplace_back("st" + std::to_string(i) + "Slot", std::to_string(i % 2));
    fields.emplace_back("st0Time", "08:30"); fields.emplace_back("st0Reply", "on");
    fields.emplace_back("st0Act", "2"); fields.emplace_back("st0Sender", "^10086$");
    fields.emplace_back("st0Timeout", "300");
    assert(parse_sched_tasks_form(fields, tasks));
    assert(tasks[0].startMinute == 510 && tasks[0].checkReply && tasks[0].replyTimeoutSec == 300);
    assert(tasks[1].slot == 1 && tasks[1].startMinute == -1);
    fields.erase(fields.begin()); assert(!parse_sched_tasks_form(fields, tasks));
    fields.emplace_back("st0Slot", "2"); assert(!parse_sched_tasks_form(fields, tasks));
    fields.back().second = "0"; fields.emplace_back("st0Slot", "0"); assert(!parse_sched_tasks_form(fields, tasks));

    const std::string rules = "slot\t1\tdrop\t1\nfrom\t^10086$\t2,email\t1\n";
    auto slot0 = idf_push_eval_forward_rules(0, rules, "10086", "");
    auto slot1 = idf_push_eval_forward_rules(1, rules, "10086", "");
    assert(slot0.matched && !slot0.drop && slot0.ruleIndex == 2 && slot0.chMask == 2 && slot0.email);
    assert(slot1.matched && slot1.drop && slot1.ruleIndex == 1);
    auto perl = idf_push_eval_forward_rules(0, "re\t^\\d{2}$\t1\t1\n", "", "42");
    assert(perl.matched && perl.chMask == 1);
    auto disabled = idf_push_eval_forward_rules(0, "slot\t0\tdrop\t0\nkw\t余额\t3\t1\n", "", "余额为10元");
    assert(disabled.matched && disabled.ruleIndex == 2 && disabled.chMask == 4);

    for (const auto& body : {"rules=", "slot=2", "slot=-1", "slot=00", "slot=1&slot=0", "slot=x"}) {
        httpd_req_t req; req.body = body;
        assert(handle_test_rule(&req) == ESP_OK && req.status == 400);
    }
    httpd_req_t invalid; invalid.body = "slot=0&rules=re%09%5B%09drop%091";
    assert(handle_test_rule(&invalid) == ESP_OK && invalid.status == 400);
    for (int slot = 0; slot < 2; ++slot) {
        httpd_req_t req;
        req.body = "slot=" + std::to_string(slot) + "&rules=slot%091%09drop%091%0Afrom%09%5E10086%24%092%2Cemail%091&sender=10086";
        assert(handle_test_rule(&req) == ESP_OK && req.status == 200);
        std::puts(req.response.c_str());
    }
    httpd_req_t denied; denied.body = "slot=1"; denied.authenticated = false;
    assert(handle_test_rule(&denied) == ESP_OK && denied.response.empty());
    denied.authenticated = true; denied.csrf = false;
    assert(handle_test_rule(&denied) == ESP_OK && denied.response.empty());
}

static void actual_reply_wait()
{
    IdfSchedRunView view; view.task.slot = 1; view.task.action = 2;
    view.task.target = "10086"; view.task.checkReply = true; view.task.checkSystem = true;
    view.task.replySender = "^10086$"; view.task.replyTimeoutSec = 5;
    for (int result : {1, -1}) {
        reply_result = result; canceled_slot = -1;
        idf_push_heavy_operation_lock();
        std::string message;
        assert(sched_run_action(view, view.task, "test", message) == (result > 0));
        assert(gate_held && canceled_slot == 1 && sent_slot == 1 && health_slot == 1);
        idf_push_heavy_operation_unlock();
    }
    int before = concurrent_pushes;
    send_failed = true; canceled_slot = -1;
    idf_push_heavy_operation_lock();
    std::string message;
    assert(!sched_run_action(view, view.task, "test", message));
    assert(gate_held && canceled_slot == 1 && concurrent_pushes == before);
    idf_push_heavy_operation_unlock(); send_failed = false;

    // 异常离开纯等待作用域也恢复同一 gate 所有权；异常显式传播，不能假报成功。
    throw_poll = true; idf_push_heavy_operation_lock();
    try { sched_run_action(view, view.task, "test", message); assert(false); }
    catch (const std::runtime_error&) { assert(gate_held); }
    idf_push_heavy_operation_unlock(); throw_poll = false;
    assert(locks == unlocks && concurrent_pushes == 3);

    view.task.action = 0; view.task.checkReply = false; view.task.checkSystem = false;
    view.emailEnabled = view.emailConfigured = true;
    assert(!sched_run_action(view, view.task, "test", message)); // 邮件配置存在但真实入队失败。
    email_queued = true; assert(sched_run_action(view, view.task, "test", message));
}

int main()
{
    config_roundtrip();
    for (uint8_t slot = 0; slot < 2; ++slot) {
        std::string json;
        append_modem_status_json(json, slot, IdfModemSlotConfig());
        std::puts(json.c_str());
    }
    fields_and_rules();
    actual_reply_wait();
}
