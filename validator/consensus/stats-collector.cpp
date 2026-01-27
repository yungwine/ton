/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include <map>

#include "common/stats.h"
#include "validator-session/validator-session-types.h"

#include "bus.h"
#include "stats.h"

namespace ton::validator::consensus {

namespace {

class ConsensusStats : public ton::stats::Tag {
 public:
  std::string_view name() const override {
    return "consensus";
  }
};

ConsensusStats consensus_stats;

class StatsCollectorImpl : public runtime::SpawnsWith<Bus>, public runtime::ConnectsTo<Bus> {
 public:
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const TraceEvent> event) {
    add_event(event->event);
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const CandidateReceived> event) {
    if (event->candidate->leader != owning_bus()->local_id.idx) {
      add_event(stats::CandidateReceived::create(event->candidate->id));
    }
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const FinalizeBlock> event) {
    add_event(stats::BlockAccepted::create(event->candidate->id));
  }

  void start_up() override {
    recorder = ton::stats::recorder_for(consensus_stats);
    id = owning_bus()->session_id;
  }

  void tear_down() override {
    flush();
  }

  void alarm() override {
    flush();
    alarm_timestamp().relax(td::Timestamp::in(5));
  }

 private:
  void add_event(const std::unique_ptr<const stats::Event>& event) {
    events.push_back(create_tl_object<stats::tl::timestampedEvent>(td::Clocks::system(), event->to_tl()));
    alarm_timestamp().relax(td::Timestamp::in(5));
  }

  void flush() {
    if (events.empty()) {
      return;
    }
    auto output = create_tl_object<stats::tl::events>(id, std::move(events));
    recorder->add(std::move(output));
  }

  ValidatorSessionId id;
  std::unique_ptr<ton::stats::Recorder> recorder;
  std::vector<stats::tl::TimestampedEventRef> events;
};

}  // namespace

void StatsCollector::register_in(runtime::Runtime& runtime) {
  runtime.register_actor<StatsCollectorImpl>("StatsCollector");
}

}  // namespace ton::validator::consensus
