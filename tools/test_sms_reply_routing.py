#!/usr/bin/env python3
"""编译实际双槽 SMS owner/接收/发件函数，验证回复检测不消费正常收信。"""
from pathlib import Path
import os
import re
import shlex
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
source = (ROOT / 'components/idf_sms/idf_sms.cpp').read_text()


def function(name):
    pattern = re.compile(r'^(?:static )?(?:esp_err_t|std::string|std::vector<std::string>|void|bool|int|uint32_t|ConcatSlot&) ' + re.escape(name) + r'\(', re.M)
    for match in pattern.finditer(source):
        brace = source.index('\n{', match.start())
        if ';' in source[match.start():brace]:
            continue
        end = source.index('\n}', brace) + 2
        return source[match.start():end]
    raise ValueError('未找到生产函数: ' + name)


names = [
    'parse_sms_index_token', 'starts_with', 'fingerprint_add', 'sms_fingerprint',
    'concat_part_fingerprint', 'seen_recently', 'update_status', 'enqueue_index',
    'pop_index', 'parse_cmti_index', 'clear_concat_slot', 'assemble_concat',
    'concat_recently_done', 'record_concat_done', 'find_concat_slot',
    'process_sms_content', 'handle_decoded_pdu', 'process_urc_line', 'process_urc_text',
    'expire_concat_slots', 'reply_check_submitted', 'idf_sms_reply_check_begin',
    'idf_sms_reply_check_arm', 'idf_sms_reply_check_poll', 'idf_sms_reply_check_cancel',
    'sms_text_basic_gsm7', 'sms_split_parts', 'sms_response_error_line',
    'sms_submit_failure_detail', 'idf_sms_send_text',
]
fixture = (ROOT / 'tools/tests/test_sms_reply_routing.cpp').read_text()
structs = source[source.index('struct ConcatPart {'):source.index('static SemaphoreHandle_t s_pdu_mutex')]
fixture = fixture.replace('// @生产运行态@', structs)
fixture = fixture.replace('// @生产函数@', '\n\n'.join(function(name) for name in names))
with tempfile.TemporaryDirectory(prefix='sms-reply-routing-') as temp:
    cpp = Path(temp) / 'test.cpp'
    binary = Path(temp) / 'test'
    cpp.write_text(fixture)
    subprocess.run([*shlex.split(os.environ.get('CXX', 'c++')), '-std=c++17', '-Wall',
                    '-Wextra', '-Werror', '-pedantic',
                    '-I' + str(ROOT / 'components/idf_sms/include'), str(cpp), '-o', str(binary)], check=True)
    subprocess.run([str(binary)], check=True)
