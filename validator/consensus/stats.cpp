/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "ton/ton-tl.hpp"

#include "stats.h"

namespace ton::validator::consensus::stats {

std::unique_ptr<Id> Id::create(ShardIdFull shard, td::uint32 cc_seqno, size_t idx, size_t total_validators,
                               ValidatorWeight weight, ValidatorWeight total_weight,
                               td::uint32 slots_per_leader_window) {
  return std::unique_ptr<Id>(new Id(shard.workchain, shard.shard, cc_seqno, idx, total_validators, weight, total_weight,
                                    slots_per_leader_window));
}

tl::EventRef Id::to_tl() const {
  return create_tl_object<tl::id>(workchain_, shard_, cc_seqno_, idx_, total_validators_, weight_, total_weight_,
                                  slots_per_leader_window_);
}

std::string Id::to_string() const {
  return PSTRING() << "Id{workchain=" << workchain_ << ", shard=" << shard_ << ", cc_seqno=" << cc_seqno_
                   << ", idx=" << idx_ << ", total_validators=" << total_validators_ << ", weight=" << weight_
                   << ", total_weight=" << total_weight_ << ", slots_per_leader_window=" << slots_per_leader_window_
                   << "}";
}

Id::Id(WorkchainId workchain, ShardId shard, td::uint32 cc_seqno, size_t idx, size_t total_validators,
       ValidatorWeight weight, ValidatorWeight total_weight, td::uint32 slots_per_leader_window)
    : workchain_(workchain)
    , shard_(shard)
    , cc_seqno_(cc_seqno)
    , idx_(idx)
    , total_validators_(total_validators)
    , weight_(weight)
    , total_weight_(total_weight)
    , slots_per_leader_window_(slots_per_leader_window) {
}

std::unique_ptr<CollateStarted> CollateStarted::create(td::uint32 slot) {
  return std::unique_ptr<CollateStarted>(new CollateStarted(slot));
}

tl::EventRef CollateStarted::to_tl() const {
  return create_tl_object<tl::collateStarted>(target_slot_);
}

std::string CollateStarted::to_string() const {
  return PSTRING() << "CollateStarted{slot=" << target_slot_ << "}";
}

CollateStarted::CollateStarted(td::uint32 target_slot) : target_slot_(target_slot) {
}

std::unique_ptr<CollateFinished> CollateFinished::create(td::uint32 slot, RawCandidateId id, BlockIdExt block) {
  return std::unique_ptr<CollateFinished>(new CollateFinished(slot, id, block));
}

tl::EventRef CollateFinished::to_tl() const {
  return create_tl_object<tl::collateFinished>(target_slot_, id_.to_tl(), create_tl_block_id(block_));
}

std::string CollateFinished::to_string() const {
  return PSTRING() << "CollateFinished{slot=" << target_slot_ << ", id=" << id_ << ", block=" << block_.to_str() << "}";
}

CollateFinished::CollateFinished(td::uint32 target_slot, RawCandidateId id, BlockIdExt block)
    : target_slot_(target_slot), id_(id), block_(block) {
}

std::unique_ptr<CollatedEmpty> CollatedEmpty::create(RawCandidateId id) {
  return std::unique_ptr<CollatedEmpty>(new CollatedEmpty(id));
}

tl::EventRef CollatedEmpty::to_tl() const {
  return create_tl_object<tl::collatedEmpty>(id_.to_tl());
}

std::string CollatedEmpty::to_string() const {
  return PSTRING() << "CollatedEmpty{id=" << id_ << "}";
}

CollatedEmpty::CollatedEmpty(RawCandidateId id) : id_(id) {
}

std::unique_ptr<CandidateReceived> CandidateReceived::create(RawCandidateId id) {
  return std::unique_ptr<CandidateReceived>(new CandidateReceived(id));
}

tl::EventRef CandidateReceived::to_tl() const {
  return create_tl_object<tl::candidateReceived>(id_.to_tl());
}

std::string CandidateReceived::to_string() const {
  return PSTRING() << "CandidateReceived{id=" << id_ << "}";
}

CandidateReceived::CandidateReceived(RawCandidateId id) : id_(id) {
}

std::unique_ptr<ValidationStarted> ValidationStarted::create(RawCandidateId id) {
  return std::unique_ptr<ValidationStarted>(new ValidationStarted(id));
}

tl::EventRef ValidationStarted::to_tl() const {
  return create_tl_object<tl::validationStarted>(id_.to_tl());
}

std::string ValidationStarted::to_string() const {
  return PSTRING() << "ValidationStarted{id=" << id_ << "}";
}

ValidationStarted::ValidationStarted(RawCandidateId id) : id_(id) {
}

std::unique_ptr<ValidationFinished> ValidationFinished::create(RawCandidateId id) {
  return std::unique_ptr<ValidationFinished>(new ValidationFinished(id));
}

tl::EventRef ValidationFinished::to_tl() const {
  return create_tl_object<tl::validationFinished>(id_.to_tl());
}

std::string ValidationFinished::to_string() const {
  return PSTRING() << "ValidationFinished{id=" << id_ << "}";
}

ValidationFinished::ValidationFinished(RawCandidateId id) : id_(id) {
}

std::unique_ptr<BlockAccepted> BlockAccepted::create(RawCandidateId id) {
  return std::unique_ptr<BlockAccepted>(new BlockAccepted(id));
}

tl::EventRef BlockAccepted::to_tl() const {
  return create_tl_object<tl::blockAccepted>(id_.to_tl());
}

std::string BlockAccepted::to_string() const {
  return PSTRING() << "BlockAccepted{id=" << id_ << "}";
}

BlockAccepted::BlockAccepted(RawCandidateId id) : id_(id) {
}

}  // namespace ton::validator::consensus::stats
