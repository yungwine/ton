#include "FFIPublicOverlayClient.h"

namespace tonlib {

namespace {

class ClientWrapper : public PublicOverlayClient {
 public:
  ClientWrapper(PublicOverlayClientConfig config, td::unique_ptr<td::Guard> actor_counter)
      : PublicOverlayClient(std::move(config)), actor_counter_(std::move(actor_counter)) {
  }

 private:
  td::unique_ptr<td::Guard> actor_counter_;
};

}  // namespace

FFIPublicOverlayClient::FFIPublicOverlayClient(FFIEventLoop& loop, PublicOverlayClientConfig config) : loop_(loop) {
  loop.run_in_context([&] {
    client_ = td::actor::create_actor<ClientWrapper>("PublicOverlayClient", std::move(config), loop.new_actor());
  });
}

FFIAwaitable<td::Unit>* FFIPublicOverlayClient::init() {
  auto bridge = FFIAwaitable<td::Unit>::create_bridge<td::Unit>(loop_, std::identity{});
  loop_.run_in_context(
      [&] { td::actor::send_closure(client_, &PublicOverlayClient::init, std::move(bridge.promise)); });
  return bridge.awaitable;
}

}  // namespace tonlib
