/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "types.h"

namespace ton::validator::consensus::stats {

namespace tl {

using id = ton_api::consensus_stats_id;
using collateStarted = ton_api::consensus_stats_collateStarted;
using collateFinished = ton_api::consensus_stats_collateFinished;
using collatedEmpty = ton_api::consensus_stats_collatedEmpty;
using candidateReceived = ton_api::consensus_stats_candidateReceived;
using validationStarted = ton_api::consensus_stats_validationStarted;
using validationFinished = ton_api::consensus_stats_validationFinished;
using blockAccepted = ton_api::consensus_stats_blockAccepted;

using timestampedEvent = ton_api::consensus_stats_timestampedEvent;
using TimestampedEventRef = tl_object_ptr<timestampedEvent>;

using events = ton_api::consensus_stats_events;
using EventsRef = tl_object_ptr<events>;

}  // namespace tl

class Id : public Event {
 public:
  static std::unique_ptr<Id> create(ShardIdFull shard, td::uint32 cc_seqno, size_t idx, size_t total_validators,
                                    ValidatorWeight weight, ValidatorWeight total_weight,
                                    td::uint32 slots_per_leader_window);

  tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  Id(WorkchainId workchain, ShardId shard, td::uint32 cc_seqno, size_t idx, size_t total_validators,
     ValidatorWeight weight, ValidatorWeight total_weight, td::uint32 slots_per_leader_window);

  WorkchainId workchain_;
  ShardId shard_;
  td::uint32 cc_seqno_;
  size_t idx_;
  size_t total_validators_;
  ValidatorWeight weight_;
  ValidatorWeight total_weight_;
  td::uint32 slots_per_leader_window_;
};

class CollateStarted : public Event {
 public:
  static std::unique_ptr<CollateStarted> create(td::uint32 slot);

  tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  CollateStarted(td::uint32 target_slot);

  td::uint32 target_slot_;
};

class CollateFinished : public Event {
 public:
  static std::unique_ptr<CollateFinished> create(td::uint32 slot, RawCandidateId id, BlockIdExt block);

  tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  CollateFinished(td::uint32 target_slot, RawCandidateId id, BlockIdExt block);

  td::uint32 target_slot_;
  RawCandidateId id_;
  BlockIdExt block_;
};

class CollatedEmpty : public Event {
 public:
  static std::unique_ptr<CollatedEmpty> create(RawCandidateId id);

  tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  CollatedEmpty(RawCandidateId id);

  RawCandidateId id_;
};

class CandidateReceived : public Event {
 public:
  static std::unique_ptr<CandidateReceived> create(RawCandidateId id);

  tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  CandidateReceived(RawCandidateId id);

  RawCandidateId id_;
};

class ValidationStarted : public Event {
 public:
  static std::unique_ptr<ValidationStarted> create(RawCandidateId id);

  tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  ValidationStarted(RawCandidateId id);

  RawCandidateId id_;
};

class ValidationFinished : public Event {
 public:
  static std::unique_ptr<ValidationFinished> create(RawCandidateId id);

  tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  ValidationFinished(RawCandidateId id);

  RawCandidateId id_;
};

class BlockAccepted : public Event {
 public:
  static std::unique_ptr<BlockAccepted> create(RawCandidateId id);

  tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  BlockAccepted(RawCandidateId id);

  RawCandidateId id_;
};

}  // namespace ton::validator::consensus::stats
