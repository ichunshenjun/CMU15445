//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {
//exec_ctx提供上下文信息，plan提供执行查询操作所需的所有信息
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan){
    this->table_info_=this->exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void SeqScanExecutor::Init() { 
    this->table_iter_=table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(table_iter_==table_info_->table_->End()){
        return false;
    }
    *tuple=*table_iter_;
    *rid=table_iter_->GetRid();
    ++table_iter_;
    return true;
}
}  // namespace bustub
