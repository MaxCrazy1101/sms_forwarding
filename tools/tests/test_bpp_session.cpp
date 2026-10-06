#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <vector>
#include "idf_esim_lpa.h"
constexpr esp_err_t ESP_OK = 0, ESP_FAIL = 1, ESP_ERR_INVALID_ARG = 2,
                   ESP_ERR_INVALID_STATE = 3, ESP_ERR_NO_MEM = 4,
                   ESP_ERR_INVALID_SIZE = 5, ESP_ERR_INVALID_RESPONSE = 6;
constexpr size_t kMaxBppSegmentBytes = 1024 * 1024, kMaxBppDecodedBytes = 1024 * 1024, kMaxBppElements = 4096;
constexpr bool idf_modem_slot_valid(uint8_t slot) { return slot < 2; }
static int guards[2] = {}, opens[2] = {}, closes[2] = {};
static bool fail_open = false, fail_write = false;
struct Block { uint8_t slot, number; bool last; std::vector<uint8_t> bytes; };
static std::vector<Block> blocks;
static void clear_sensitive_bytes(std::vector<uint8_t>& bytes) {
    volatile uint8_t* data = bytes.data();
    for (size_t i = 0; i < bytes.size(); ++i) data[i] = 0;
    bytes.clear();
}
class SensitiveBytes {
public:
    std::vector<uint8_t> value;
    ~SensitiveBytes() { clear_sensitive_bytes(value); }
};
// 只有 APDU/RTOS 边界是主机替身；会话 owner、writer 和 DER 分段完全使用生产代码。
class EsimOperationGuard {
public:
    explicit EsimOperationGuard(uint8_t slot) : slot_(slot) { ++guards[slot_]; }
    ~EsimOperationGuard() { --guards[slot_]; }
private:
    uint8_t slot_;
};
class EsimApduSession {
public:
    EsimApduSession(uint8_t slot, bool) : slot_(slot) {}
    ~EsimApduSession() { close(); }
    esp_err_t open(std::string&) { if (fail_open) return ESP_FAIL; ++opens[slot_]; opened_ = true; return ESP_OK; }
    void close() { if (opened_) { ++closes[slot_]; opened_ = false; } }
    esp_err_t exchange_store_data_block(const uint8_t* data, size_t length, bool last, uint8_t number,
                                        std::vector<uint8_t>& response, std::string&) {
        assert(opened_ && guards[slot_] == 1);
        if (fail_write) return ESP_FAIL;
        blocks.push_back({slot_, number, last, std::vector<uint8_t>(data, data + length)});
        response.clear();
        if (last && length == 3 && data[0] == 0x86 && data[2] == 0x66) response = {0xBF, 0x37, 0};
        return ESP_OK;
    }
private:
    uint8_t slot_;
    bool opened_ = false;
};
// @生产会话@
// @生产流式解析@

static void test_session_slot(uint8_t slot) {
    int before_open = opens[slot], before_close = closes[slot];
    std::string message;
    IdfEsimLpaBppSession session;
    assert(session.begin_segment(2, message) == ESP_ERR_INVALID_ARG);
    assert(session.begin_segment(slot, message) == ESP_OK);
    assert(session.begin_segment(slot, message) == ESP_OK);
    assert(opens[slot] == before_open + 1 && closes[slot] == before_close && guards[slot] == 1);
    assert(session.begin_segment(1 - slot, message) == ESP_ERR_INVALID_ARG);
    assert(guards[slot] == 1 && guards[1 - slot] == 0);
    session.close(); session.close();
    assert(closes[slot] == before_close + 1 && guards[slot] == 0);
    fail_open = true;
    assert(session.begin_segment(slot, message) == ESP_FAIL);
    assert(guards[slot] == 0 && guards[1 - slot] == 0);
    fail_open = false;
}
static void test_writer_segments(uint8_t slot) {
    std::string message;
    std::vector<uint8_t> response;
    int before_open = opens[slot], before_close = closes[slot];
    const size_t initial_blocks = blocks.size();
    {
        BppSegmentWriter writer(slot);
        std::array<uint8_t, 121> first = {};
        assert(writer.begin(message) == ESP_OK);
        assert(writer.write(first.data(), first.size(), message) == ESP_OK);
        assert(writer.finish(response, message) == ESP_OK);
        assert(blocks.size() == initial_blocks + 2);
        assert(blocks[initial_blocks].number == 0 && !blocks[initial_blocks].last);
        assert(blocks[initial_blocks + 1].number == 1 && blocks[initial_blocks + 1].last);
        assert(guards[slot] == 1 && closes[slot] == before_close);
        assert(writer.begin(message) == ESP_OK);
        const uint8_t second[] = {1, 2, 3};
        assert(writer.write(second, sizeof(second), message) == ESP_OK);
        assert(writer.finish(response, message) == ESP_OK);
        assert(blocks.back().slot == slot && blocks.back().number == 0 && blocks.back().last);
        assert(opens[slot] == before_open + 1 && closes[slot] == before_close);
    }
    assert(closes[slot] == before_close + 1 && guards[slot] == 0);
    BppSegmentWriter failed(slot);
    assert(failed.begin(message) == ESP_OK);
    const uint8_t byte = 0;
    assert(failed.write(&byte, 1, message) == ESP_OK);
    fail_write = true;
    assert(failed.finish(response, message) == ESP_FAIL);
    assert(guards[slot] == 0);
    fail_write = false;
    assert(failed.begin(message) == ESP_OK);
    assert(failed.write(nullptr, 1, message) == ESP_ERR_INVALID_ARG);
    assert(guards[slot] == 0);
}
static const std::vector<uint8_t> bpp = {
    0xBF, 0x36, 30,
    0xBF, 0x23, 1, 0,
    0xA0, 3, 0x87, 1, 0x11,
    0xA1, 6, 0x88, 1, 0x22, 0x88, 1, 0x33,
    0xA2, 3, 0x87, 1, 0x44,
    0xA3, 6, 0x86, 1, 0x55, 0x86, 1, 0x66,
};
static void test_streaming_download(uint8_t slot) {
    std::string message;
    const int before_open = opens[slot], before_close = closes[slot];
    const size_t initial_blocks = blocks.size();
    BppStreamParser parser(slot);
    for (uint8_t byte : bpp) assert(parser.feed(&byte, 1, message) == ESP_OK);
    assert(parser.segment_count() == 9 && guards[slot] == 1);
    assert(opens[slot] == before_open + 1 && closes[slot] == before_close);
    assert(parser.finish(message) == ESP_OK);
    assert(closes[slot] == before_close + 1 && guards[slot] == 0);
    assert(parser.installation_result() == std::vector<uint8_t>({0xBF, 0x37, 0}));
    std::vector<uint8_t> written;
    for (size_t i = initial_blocks; i < blocks.size(); ++i) {
        const Block& block = blocks[i];
        assert(block.slot == slot && block.number == 0 && block.last);
        written.insert(written.end(), block.bytes.begin(), block.bytes.end());
    }
    assert(written == bpp); // 分段重置没有丢失或重复 DER 字节。
    BppStreamParser aborted(slot);
    assert(aborted.feed(bpp.data(), 7, message) == ESP_OK && guards[slot] == 1);
    aborted.abort();
    assert(guards[slot] == 0);
    BppStreamParser malformed(slot);
    assert(malformed.feed(bpp.data(), 7, message) == ESP_OK);
    const uint8_t invalid[] = {0xA9, 1, 0};
    assert(malformed.feed(invalid, sizeof(invalid), message) == ESP_ERR_INVALID_RESPONSE);
    assert(guards[slot] == 0);
}
int main() {
    for (uint8_t slot = 0; slot < 2; ++slot) {
        test_session_slot(slot); test_writer_segments(slot); test_streaming_download(slot);
        assert(guards[0] == 0 && guards[1] == 0);
    }
    std::puts("dual-slot BPP session/120-byte writer/9-segment streaming/cleanup regressions passed");
}
