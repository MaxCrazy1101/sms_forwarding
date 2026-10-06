#pragma once

#include "idf_push.h"

// 仅用于 heavy worker 已持有 gate 的纯回复等待区；离开作用域先恢复所有权。
class IdfHeavyOperationPause {
public:
    IdfHeavyOperationPause() { idf_push_heavy_operation_unlock(); }
    ~IdfHeavyOperationPause() { idf_push_heavy_operation_lock(); }
    IdfHeavyOperationPause(const IdfHeavyOperationPause&) = delete;
    IdfHeavyOperationPause& operator=(const IdfHeavyOperationPause&) = delete;
};
