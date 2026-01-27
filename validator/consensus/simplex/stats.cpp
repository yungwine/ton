/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "ton/ton-tl.hpp"

#include "stats.h"

namespace ton::validator::consensus::simplex::stats {

std::unique_ptr<Voted> Voted::create(Vote vote) {
  return std::unique_ptr<Voted>(new Voted(std::move(vote)));
}

consensus::stats::tl::EventRef Voted::to_tl() const {
  return create_tl_object<tl::voted>(vote_.to_tl());
}

std::string Voted::to_string() const {
  return PSTRING() << "Voted{vote=" << vote_ << "}";
}

Voted::Voted(Vote vote) : vote_(std::move(vote)) {
}

std::unique_ptr<CertObserved> CertObserved::create(Vote vote) {
  return std::unique_ptr<CertObserved>(new CertObserved(std::move(vote)));
}

consensus::stats::tl::EventRef CertObserved::to_tl() const {
  return create_tl_object<tl::certObserved>(vote_.to_tl());
}

std::string CertObserved::to_string() const {
  return PSTRING() << "CertObserved{vote=" << vote_ << "}";
}

CertObserved::CertObserved(Vote vote) : vote_(std::move(vote)) {
}

}  // namespace ton::validator::consensus::simplex::stats
