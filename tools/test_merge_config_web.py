#!/usr/bin/env python3
"""编译真实配置组件和 Web/规则函数，验证双槽合并契约；硬件替身仅用于主机测试。"""
from pathlib import Path
import json
import os
import shlex
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def function(source: str, signature: str) -> str:
    start = source.index(signature)
    return source[start:source.index("\n}", start) + 2] + "\n"


def main() -> None:
    web = (ROOT / "components/idf_web/idf_web.cpp").read_text()
    push = (ROOT / "components/idf_push/idf_push.cpp").read_text()
    extracts = []
    for signature in [
        "static int hex_value(", "static std::string url_decode(",
        "static IdfFormFields parse_urlencoded(", "static const std::string* find_field(",
        "static bool has_field(", "static std::string field_text(",
        "static size_t field_count(", "static bool parse_int_strict(",
        "static int field_int(", "static uint8_t field_u8(",
        "static bool parse_slot_text(", "static bool field_slot(",
        "static esp_err_t reject_invalid_slot(httpd_req_t* req)\n",
        "static bool parse_sched_tasks_form(", "static void json_prop(",
    ]:
        extracts.append(function(web, signature))
    for signature in ["static bool parse_push_channel_token(",
                      "static bool regex_search_case_insensitive(",
                      "IdfForwardRuleDecision idf_push_eval_forward_rules("]:
        extracts.append(function(push, signature))
    for signature in ["static esp_err_t handle_test_rule(",
                      "static void append_modem_status_json(",
                      "static bool sched_run_action("]:
        extracts.append(function(web, signature))

    with tempfile.TemporaryDirectory(prefix="sms-merge-web-") as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "freertos").mkdir()
        headers = {
            "esp_err.h": '''#pragma once
using esp_err_t = int;
constexpr int ESP_OK=0, ESP_ERR_INVALID_ARG=1, ESP_ERR_TIMEOUT=2,
  ESP_ERR_NO_MEM=3, ESP_ERR_NOT_FOUND=4, ESP_ERR_NVS_NOT_FOUND=5, ESP_FAIL=6;
inline const char* esp_err_to_name(int) { return "injected error"; }
''',
            "esp_log.h": '''#pragma once
#define ESP_LOGI(tag, ...) ((void)(tag))
#define ESP_LOGW(tag, ...) ((void)(tag))
#define ESP_LOGE(tag, ...) ((void)(tag))
''',
            "idf_log.h": '''#pragma once
inline void idf_logf(const char*, ...) {}
inline void idf_log_line(const char*) {}
''',
            "freertos/FreeRTOS.h": '''#pragma once
constexpr int portMAX_DELAY=0, pdTRUE=1;
#define pdMS_TO_TICKS(ms) (ms)
''',
            "freertos/semphr.h": '''#pragma once
using SemaphoreHandle_t = void*;
inline void* xSemaphoreCreateMutex() { return reinterpret_cast<void*>(1); }
inline int xSemaphoreTake(void*, int) { return 1; }
inline void xSemaphoreGive(void*) {}
''',
            "nvs_flash.h": "#pragma once\n",
            "nvs.h": '''#pragma once
#include <map>
#include <string>
#include <cstring>
#include <cstdint>
#include "esp_err.h"
using nvs_handle_t = int;
constexpr int NVS_READONLY=0, NVS_READWRITE=1;
inline std::map<std::string, std::string> host_nvs;
inline int nvs_open(const char*, int, int* out) { *out=1; return ESP_OK; }
inline void nvs_close(int) {}
inline int nvs_commit(int) { return ESP_OK; }
inline int nvs_erase_all(int) { host_nvs.clear(); return ESP_OK; }
inline int nvs_set_str(int, const char* key, const char* value) { host_nvs[key]=value; return ESP_OK; }
inline int nvs_get_str(int, const char* key, char* out, size_t* len) {
 auto it=host_nvs.find(key); if (it==host_nvs.end()) return ESP_ERR_NVS_NOT_FOUND;
 if (out) { if (*len < it->second.size()+1) return ESP_FAIL; std::memcpy(out, it->second.c_str(), it->second.size()+1); }
 *len=it->second.size()+1; return ESP_OK;
}
template<class T> int host_nvs_get(const char* key, T* out) {
 auto it=host_nvs.find(key); if (it==host_nvs.end()) return ESP_ERR_NVS_NOT_FOUND;
 *out=static_cast<T>(std::stoll(it->second)); return ESP_OK;
}
inline int nvs_get_u8(int, const char* key, uint8_t* out) { return host_nvs_get(key,out); }
inline int nvs_get_u32(int, const char* key, uint32_t* out) { return host_nvs_get(key,out); }
inline int nvs_get_i32(int, const char* key, int32_t* out) { return host_nvs_get(key,out); }
inline int nvs_set_u8(int, const char* key, uint8_t v) { host_nvs[key]=std::to_string(v); return ESP_OK; }
inline int nvs_set_u32(int, const char* key, uint32_t v) { host_nvs[key]=std::to_string(v); return ESP_OK; }
inline int nvs_set_i32(int, const char* key, int32_t v) { host_nvs[key]=std::to_string(v); return ESP_OK; }
''',
        }
        for name, content in headers.items():
            (tmp_path / name).write_text(content)
        (tmp_path / "merge_functions.inc").write_text("\n".join(extracts))
        binary = tmp_path / "test"
        include_dirs = [tmp_path, ROOT / "components/idf_config/include",
                        ROOT / "components/idf_push/include", ROOT / "components/idf_web/include",
                        ROOT / "components/idf_modem/include", ROOT / "components/idf_sms/include",
                        ROOT / "components/idf_logbuf/include"]
        subprocess.run(shlex.split(os.environ.get("CXX", "c++")) +
                       ["-std=c++17", "-Wall", "-Wextra", "-Werror", "-pthread"] +
                       ["-I" + str(directory) for directory in include_dirs] +
                       [str(ROOT / "tools/tests/test_merge_config_web.cpp"),
                        str(ROOT / "components/idf_config/idf_config.cpp"),
                        str(ROOT / "components/idf_logbuf/idf_util.cpp"),
                        "-o", str(binary)], check=True)
        result = subprocess.run([str(binary)], check=True, text=True, capture_output=True)
        documents = [json.loads(line) for line in result.stdout.splitlines()]
        assert len(documents) == 4
        for slot, document in enumerate(documents[:2]):
            assert document["slot"] == slot
            assert document["smsTotal"] == 4294967295
            assert document["modemStackFree"] == 4294967295
            assert document["modemSampleRequested"] == 4294967295 - slot
            assert document["modemSampleRunning"] == (slot == 0)
            assert document["imsi"] == '46000"escaped'
        assert documents[2]["ruleIndex"] == 2 and documents[2]["matched"]
        assert documents[3]["ruleIndex"] == 1 and documents[3]["drop"]
        print("双槽配置/NVS/规则API/完整状态JSON/实际回复等待gate：主机回归通过")


if __name__ == "__main__":
    main()
