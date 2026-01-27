/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "consensus/stats.h"

#include "votes.h"

namespace ton::validator::consensus::simplex::stats {

namespace tl {

using voted = ton_api::consensus_simplex_stats_voted;
using certObserved = ton_api::consensus_simplex_stats_certObserved;

}  // namespace tl

class Voted : public consensus::stats::Event {
 public:
  static std::unique_ptr<Voted> create(Vote vote);

  consensus::stats::tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  Voted(Vote vote);

  Vote vote_;
};

class CertObserved : public consensus::stats::Event {
 public:
  static std::unique_ptr<CertObserved> create(Vote vote);

  consensus::stats::tl::EventRef to_tl() const override;
  std::string to_string() const override;

 private:
  CertObserved(Vote vote);

  Vote vote_;
};

}  // namespace ton::validator::consensus::simplex::stats
