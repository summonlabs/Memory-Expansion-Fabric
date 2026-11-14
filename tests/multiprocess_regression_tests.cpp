// Multiprocess regression tests: every request is answered, only spawned
// worker incarnations register, and a worker id runs one process at a time.
#include "test_framework.hpp"
#include "proto_client.hpp"
#include "process_util.hpp"
#ifdef _WIN32
#  define NOMINMAX
#  define WIN32_LEAN_AND_MEAN
#  include <winsock2.h>
#  include <windows.h>
#  undef ERROR
#endif
#include "memory_expansion_fabric/protocol.hpp"
#include <string>
#include <vector>

namespace mef = memory_expansion_fabric;

static std::vector<std::uint8_t> putU64(std::uint64_t v) {
    std::vector<std::uint8_t> b(8);
    for (int i = 0; i < 8; ++i) b[i] = (std::uint8_t)((v >> (8*i)) & 0xFFu);
    return b;
}

static std::vector<std::uint8_t> join(std::vector<std::uint8_t> a, const std::vector<std::uint8_t>& b) {
    a.insert(a.end(), b.begin(), b.end());
    return a;
}

static std::vector<std::uint8_t> spawnPayload(std::uint64_t worker, std::uint64_t boot) {
    return join(putU64(worker), putU64(boot));
}

static std::vector<std::uint8_t> helloPayload(std::uint64_t worker, std::uint64_t boot,
                                              std::uint64_t epoch, std::uint64_t pid) {
    return join(join(putU64(worker), putU64(boot)), join(putU64(epoch), putU64(pid)));
}

static long long fieldInt(const std::string& text, const std::string& key) {
    auto pos = text.find(key);
    if (pos == std::string::npos) return -1;
    pos += key.size();
    long long v = 0; bool any = false;
    while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9') { v = v*10 + (text[pos]-'0'); any = true; ++pos; }
    return any ? v : -1;
}

// The INSPECT line starting with `prefix` (e.g. "worker 3 "), or "" if absent.
static std::string lineOf(const std::string& text, const std::string& prefix) {
    std::size_t pos = 0;
    while (pos < text.size()) {
        std::size_t end = text.find('\n', pos);
        if (end == std::string::npos) end = text.size();
        if (text.compare(pos, prefix.size(), prefix) == 0) return text.substr(pos, end - pos);
        pos = end + 1;
    }
    return "";
}

static bool inspect(MefClient& cli, std::string& out) {
    std::vector<std::uint8_t> resp;
    if (!cli.request(mef::FrameType::INSPECT, {}, mef::FrameType::INSPECT_RESP, resp)) return false;
    out.assign(resp.begin(), resp.end());
    return true;
}

// Condition-based: returns when the predicate holds or the coordinator
// connection breaks. Never converted to a result by a time limit.
template <typename Pred>
static bool waitUntil(MefClient& cli, Pred p) {
    for (;;) {
        std::string t;
        if (!inspect(cli, t)) return false;
        if (p(t)) return true;
        ::Sleep(2);
    }
}

// Sends `type` and then INSPECT on the same connection. The coordinator handles
// one connection's frames in order, so the INSPECT reply is the barrier: the
// request was answered only if `respType` arrived before it.
static bool answeredBeforeInspect(MefClient& cli, mef::FrameType type, const std::vector<std::uint8_t>& payload,
                                  mef::FrameType respType, std::vector<std::uint8_t>& respOut) {
    if (!cli.sendFrame(type, payload)) return false;
    if (!cli.sendFrame(mef::FrameType::INSPECT, {})) return false;
    bool answered = false;
    for (;;) {
        mef::Frame f;
        if (!cli.recvFrame(f)) return false;
        if (f.type == mef::FrameType::INSPECT_RESP) return answered;
        if (f.type == respType) { answered = true; respOut = f.payload; }
    }
}

static HANDLE spawnCoord(std::uint16_t port) {
    std::string exe = procutil::exeDir() + "\\mef_coordinator.exe";
    return procutil::spawnProcess("\"" + exe + "\" --port " + std::to_string(port) + " --seed");
}

static void stopCoord(HANDLE coord) {
    procutil::killProcess(coord);
    procutil::waitExit(coord);
    procutil::closeHandle(coord);
}

static bool spawnWorker(MefClient& cli, std::uint64_t worker, std::uint64_t boot, std::uint8_t& status) {
    std::vector<std::uint8_t> resp;
    if (!cli.request(mef::FrameType::SPAWN_WORKER, spawnPayload(worker, boot),
                     mef::FrameType::SPAWN_WORKER_RESP, resp)) return false;
    status = resp.empty() ? 0xFF : resp[0];
    return true;
}

static bool terminateWorker(MefClient& cli, std::uint64_t worker, std::uint8_t& status) {
    std::vector<std::uint8_t> resp;
    if (!cli.request(mef::FrameType::TERMINATE_WORKER, putU64(worker),
                     mef::FrameType::TERMINATE_WORKER_RESP, resp)) return false;
    status = resp.empty() ? 0xFF : resp[0];
    return true;
}

TEST(mp_every_request_is_answered) {
    std::uint16_t port = procutil::pickFreePort();
    HANDLE coord = spawnCoord(port);
    CHECK_MSG(coord != nullptr, "coordinator spawn failed");
    if (!coord) return;
    MefClient cli;
    CHECK(cli.connectAny("127.0.0.1", port));

    mef::ConsumerRequirements zero;
    zero.consumer = mef::ConsumerId(10); zero.consumerGeneration = mef::ConsumerGeneration(1);
    zero.requiredBytes = 0;
    const std::vector<std::uint8_t> zeroReq = mef::encodeRequirements(zero).value();
    const std::vector<std::uint8_t> garbage = {1, 2, 3};

    struct Case { const char* name; mef::FrameType type; std::vector<std::uint8_t> payload; mef::FrameType resp; };
    const std::vector<Case> cases = {
        {"commit of an unknown reservation", mef::FrameType::COMMIT, putU64(999999), mef::FrameType::COMMIT_RESP},
        {"commit with a short payload", mef::FrameType::COMMIT, garbage, mef::FrameType::COMMIT_RESP},
        {"select of zero bytes", mef::FrameType::SELECT, zeroReq, mef::FrameType::SELECT_RESP},
        {"select with a malformed payload", mef::FrameType::SELECT, garbage, mef::FrameType::SELECT_RESP},
        {"reserve with a malformed payload", mef::FrameType::RESERVE, garbage, mef::FrameType::RESERVE_RESP},
        {"release with a short payload", mef::FrameType::RELEASE, garbage, mef::FrameType::RELEASE_RESP},
        {"spawn with a short payload", mef::FrameType::SPAWN_WORKER, garbage, mef::FrameType::SPAWN_WORKER_RESP},
        {"terminate with a short payload", mef::FrameType::TERMINATE_WORKER, garbage, mef::FrameType::TERMINATE_WORKER_RESP},
        {"hello with a short payload", mef::FrameType::HELLO, garbage, mef::FrameType::HELLO_ACK},
    };
    for (const auto& c : cases) {
        std::vector<std::uint8_t> resp;
        bool answered = answeredBeforeInspect(cli, c.type, c.payload, c.resp, resp);
        CHECK_MSG(answered, "%s got no reply", c.name);
        if (!answered) continue;
        if (c.resp == mef::FrameType::COMMIT_RESP)
            CHECK_MSG(!mef::decodeReservation(resp).ok(), "%s: reply is not a failure", c.name);
        else if (c.resp == mef::FrameType::SELECT_RESP)
            CHECK_MSG(!mef::decodeSelection(resp).ok(), "%s: reply is not a failure", c.name);
        else
            CHECK_MSG(resp.size() == 1 && resp[0] == 0, "%s: reply is not a failure", c.name);
    }

    // The connection still serves well-formed requests.
    mef::ConsumerRequirements ok = zero; ok.requiredBytes = 4096;
    std::vector<std::uint8_t> selResp;
    CHECK(cli.request(mef::FrameType::SELECT, mef::encodeRequirements(ok).value(),
                      mef::FrameType::SELECT_RESP, selResp));
    CHECK(mef::decodeSelection(selResp).ok());
    stopCoord(coord);
}

TEST(mp_hello_requires_spawned_incarnation) {
    std::uint16_t port = procutil::pickFreePort();
    HANDLE coord = spawnCoord(port);
    CHECK_MSG(coord != nullptr, "coordinator spawn failed");
    if (!coord) return;
    MefClient cli;
    CHECK(cli.connectAny("127.0.0.1", port));

    std::uint8_t st = 0;
    CHECK(spawnWorker(cli, 3, 10, st));
    CHECK_EQ(st, 1);
    CHECK(waitUntil(cli, [](const std::string& t) {
        std::string w = lineOf(t, "worker 3 ");
        return fieldInt(w, "boot=") == 10 && fieldInt(w, "alive=") == 1 && fieldInt(t, "ev=") == 1;
    }));

    MefClient other;
    CHECK(other.connectAny("127.0.0.1", port));
    std::vector<std::uint8_t> ack;
    // A worker id the coordinator never spawned: no process handle could ever prove its death.
    CHECK(other.request(mef::FrameType::HELLO, helloPayload(9, 90, 1, 4242), mef::FrameType::HELLO_ACK, ack));
    CHECK_MSG(ack.size() == 1 && ack[0] == 0, "HELLO for an unspawned worker was admitted");
    // Another boot for the spawned worker id would displace the live incarnation.
    CHECK(other.request(mef::FrameType::HELLO, helloPayload(3, 99, 1, 4243), mef::FrameType::HELLO_ACK, ack));
    CHECK_MSG(ack.size() == 1 && ack[0] == 0, "HELLO for an unspawned boot was admitted");

    std::string t;
    CHECK(inspect(cli, t));
    CHECK_MSG(lineOf(t, "worker 9 ").empty(), "unspawned worker registered:\n%s", t.c_str());
    std::string w3 = lineOf(t, "worker 3 ");
    CHECK_MSG(fieldInt(w3, "boot=") == 10 && fieldInt(w3, "alive=") == 1,
              "spawned incarnation displaced:\n%s", t.c_str());

    // The spawned incarnation's death is still proven through its handle.
    CHECK(terminateWorker(cli, 3, st));
    CHECK_EQ(st, 1);
    CHECK(waitUntil(cli, [](const std::string& text) {
        return fieldInt(lineOf(text, "worker 3 "), "alive=") == 0;
    }));
    stopCoord(coord);
}

TEST(mp_duplicate_spawn_refused_and_incarnations_tracked) {
    std::uint16_t port = procutil::pickFreePort();
    HANDLE coord = spawnCoord(port);
    CHECK_MSG(coord != nullptr, "coordinator spawn failed");
    if (!coord) return;
    MefClient cli;
    CHECK(cli.connectAny("127.0.0.1", port));

    std::uint8_t st = 0;
    CHECK(spawnWorker(cli, 3, 10, st));
    CHECK_EQ(st, 1);
    CHECK(waitUntil(cli, [](const std::string& t) {
        std::string w = lineOf(t, "worker 3 ");
        return fieldInt(w, "boot=") == 10 && fieldInt(w, "alive=") == 1 && fieldInt(t, "ev=") == 1;
    }));

    // A second process for a worker id whose process is still running.
    CHECK(spawnWorker(cli, 3, 20, st));
    CHECK_MSG(st == 0, "second SPAWN for a running worker id was accepted");
    if (st != 0) {
        // Let the duplicate register first, so no worker is left in its
        // connect loop once the coordinator is gone.
        waitUntil(cli, [](const std::string& t) { return fieldInt(lineOf(t, "worker 3 "), "boot=") == 20; });
        stopCoord(coord);
        return;
    }

    CHECK(terminateWorker(cli, 3, st));
    CHECK_EQ(st, 1);
    CHECK(waitUntil(cli, [](const std::string& t) {
        return fieldInt(lineOf(t, "worker 3 "), "alive=") == 0 && fieldInt(t, "ev=") != 1;
    }));

    // Once the process is gone a fresh incarnation may be spawned, and it stays
    // terminable through its own handle.
    CHECK(spawnWorker(cli, 3, 20, st));
    CHECK_EQ(st, 1);
    CHECK(waitUntil(cli, [](const std::string& t) {
        std::string w = lineOf(t, "worker 3 ");
        return fieldInt(w, "boot=") == 20 && fieldInt(w, "alive=") == 1 && fieldInt(t, "ev=") == 1;
    }));
    CHECK(terminateWorker(cli, 3, st));
    CHECK_EQ(st, 1);
    CHECK(waitUntil(cli, [](const std::string& t) {
        std::string w = lineOf(t, "worker 3 ");
        return fieldInt(w, "boot=") == 20 && fieldInt(w, "alive=") == 0;
    }));

    std::vector<std::uint8_t> audResp;
    CHECK(cli.request(mef::FrameType::AUDIT, {}, mef::FrameType::AUDIT_RESP, audResp));
    std::string aud(audResp.begin(), audResp.end());
    CHECK_MSG(aud.find("FAIL") == std::string::npos, "audit: %s", aud.c_str());
    stopCoord(coord);
}

MEF_MAIN()
