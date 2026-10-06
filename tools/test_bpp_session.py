#!/usr/bin/env python3
"""编译真实 BPP 会话、120-byte writer 和 DER parser，验证同槽通道复用/清理。"""
from pathlib import Path
import os
import shlex
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
esim = (ROOT / 'components/idf_esim/idf_esim.cpp').read_text()
lpa = (ROOT / 'components/idf_lpa/idf_lpa.cpp').read_text()
impl_start = esim.index('struct IdfEsimLpaBppSessionImpl {')
impl_end = esim.index('\n};', impl_start) + 3
session_start = esim.index('IdfEsimLpaBppSession::IdfEsimLpaBppSession()')
session_end = esim.index('\nesp_err_t idf_esim_list_profiles', session_start)
stream_start = lpa.index('class BppSegmentWriter {')
stream_end = lpa.index('\nclass BppBase64Decoder', stream_start)
fixture = (ROOT / 'tools/tests/test_bpp_session.cpp').read_text()
fixture = fixture.replace('// @生产会话@', esim[impl_start:impl_end] + '\n' + esim[session_start:session_end])
fixture = fixture.replace('// @生产流式解析@', lpa[stream_start:stream_end])
with tempfile.TemporaryDirectory(prefix='bpp-session-') as temp:
    cpp = Path(temp) / 'test.cpp'
    binary = Path(temp) / 'test'
    cpp.write_text(fixture)
    # 仅用主机错误常量替代 SDK 头，其余公共接口使用生产头文件。
    (Path(temp) / 'esp_err.h').write_text('#pragma once\nusing esp_err_t = int;\n')
    subprocess.run([*shlex.split(os.environ.get('CXX', 'c++')), '-std=c++17', '-Wall',
                    '-Wextra', '-Werror', '-pedantic', '-I' + temp,
                    '-I' + str(ROOT / 'components/idf_esim/include'), str(cpp), '-o', str(binary)], check=True)
    subprocess.run([str(binary)], check=True)
