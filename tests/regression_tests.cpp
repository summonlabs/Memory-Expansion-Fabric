// Regression tests for worker-incarnation fencing, reservation-release authority
// and pool membership.
#include "test_framework.hpp"
#include "scenario.hpp"
#include <string>
#include <vector>

using namespace memory_expansion_fabric;
using namespace mef_scenario;

namespace {

void setupRegion(Fabric& f, const Authority& a, RegionId region, ProviderId provider,
                 std::uint64_t total) {
    // The domain is shared; a second registration reports ALREADY_EXISTS.
    (void)f.registerDomain(ExpansionDomainDescriptor{ExpansionDomainId(1), ExpansionDomainGeneration(1),
                                                     "d", OriginClass::SYNTHETIC, FailureDomainId()}, a);
    CHECK(f.registerProvider(makeProvider(provider, ProviderGeneration(1), ProviderKind::SYNTHETIC, total), a).ok());
    CHECK(f.registerRegion(makeRegion(region, RegionGeneration(1), provider, ExpansionDomainId(1), total), a).ok());
}

RegionEvidence evidence(RegionId region, RegionGeneration rgen, ProviderId provider, std::uint64_t online,
                        WorkerId w, WorkerBootId b, std::uint64_t seq) {
    return makeEvidence(region, rgen, provider, ProviderGeneration(1), online, HealthState::HEALTHY, true,
                        Locality::LOCAL, 100, 1000000000ull, w, b, CoordinatorEpoch(1),
                        EvidenceId(seq), EvidenceGeneration(seq), seq);
}

ConsumerRequirements request(std::uint64_t bytes, PoolId pool = PoolId()) {
    ConsumerRequirements r;
    r.consumer = ConsumerId(10);
    r.consumerGeneration = ConsumerGeneration(1);
    r.requiredBytes = bytes;
    r.preferredPool = pool;
    return r;
}

bool auditClean(Fabric& f) {
    auto aud = f.audit();
    return aud.ok() && aud.value().find("FAIL") == std::string::npos;
}

const RegionSnapshot* regionOf(const FabricSnapshot& s, RegionId id) {
    for (const auto& r : s.regions) if (r.id == id) return &r;
    return nullptr;
}

const ReservationSnapshot* reservationOf(const FabricSnapshot& s, ReservationId id) {
    for (const auto& r : s.reservations) if (r.id == id) return &r;
    return nullptr;
}

} // namespace

// A boot id belongs to one incarnation: once superseded it is never authority again.
TEST(superseded_worker_boot_never_readmitted) {
    Authority a = operatorAuth();
    Fabric f = makeFabric();
    WorkerId w(7);
    CHECK(f.registerWorker(w, WorkerBootId(1), CoordinatorEpoch(1), 100, a).ok());
    CHECK(f.registerWorker(w, WorkerBootId(2), CoordinatorEpoch(1), 200, a).ok());
    CHECK(!f.registerWorker(w, WorkerBootId(1), CoordinatorEpoch(1), 100, a).ok());
    CHECK(f.workerAlive(w, WorkerBootId(2)));
    CHECK(!f.workerAlive(w, WorkerBootId(1)));
    // Nor after the current incarnation dies.
    CHECK(f.markWorkerDead(w, a).ok());
    CHECK(!f.registerWorker(w, WorkerBootId(1), CoordinatorEpoch(1), 100, a).ok());
    CHECK(!f.registerWorker(w, WorkerBootId(2), CoordinatorEpoch(1), 200, a).ok());
    CHECK(f.registerWorker(w, WorkerBootId(3), CoordinatorEpoch(1), 300, a).ok());
    CHECK(f.workerAlive(w, WorkerBootId(3)));
}

// A new boot of a worker supersedes the old incarnation: the old incarnation's
// evidence and reservations lose their authority at once, even if the new
// incarnation dies before publishing anything.
TEST(worker_boot_takeover_fences_superseded_incarnation) {
    Authority a = operatorAuth();
    Fabric f = makeFabric();
    setupRegion(f, a, RegionId(1), ProviderId(1), 4096);
    CHECK(f.registerConsumer(ConsumerId(10), ConsumerGeneration(1), a).ok());
    WorkerId w(7);
    CHECK(f.registerWorker(w, WorkerBootId(1), CoordinatorEpoch(1), 100, a).ok());
    Authority wa1(CoordinatorEpoch(1), w, WorkerBootId(1));
    CHECK(f.publishRegionEvidence(evidence(RegionId(1), RegionGeneration(1), ProviderId(1), 4096,
                                           w, WorkerBootId(1), 1), wa1).ok());
    auto held = f.requestReservation(request(1024), wa1);
    CHECK(held.ok());

    CHECK(f.registerWorker(w, WorkerBootId(2), CoordinatorEpoch(1), 200, a).ok());
    auto snap = f.snapshot();
    const RegionSnapshot* rg = regionOf(snap, RegionId(1));
    CHECK(rg != nullptr);
    if (rg) {
        CHECK_MSG(rg->evidenceStatus != EvidenceStatus::CURRENT,
                  "superseded incarnation's evidence still CURRENT");
        CHECK_EQ(rg->reservedBytes, 0u);
    }
    if (held.ok()) {
        const ReservationSnapshot* rs = reservationOf(snap, held.value());
        CHECK(rs != nullptr && rs->state != ReservationState::RESERVED);
    }
    auto sel = f.select(request(512), a);
    CHECK(sel.ok() && sel.value().ranked.empty());

    // The new incarnation dies without publishing: nothing may be admitted on
    // the superseded incarnation's evidence.
    CHECK(f.markWorkerDead(w, a).ok());
    CHECK(!f.requestReservation(request(512), a).ok());
    CHECK(auditClean(f));

    // Fresh evidence from a fresh incarnation restores eligibility.
    CHECK(f.registerWorker(w, WorkerBootId(3), CoordinatorEpoch(1), 300, a).ok());
    Authority wa3(CoordinatorEpoch(1), w, WorkerBootId(3));
    CHECK(f.publishRegionEvidence(evidence(RegionId(1), RegionGeneration(1), ProviderId(1), 4096,
                                           w, WorkerBootId(3), 2), wa3).ok());
    auto sel2 = f.select(request(512), a);
    CHECK(sel2.ok() && sel2.value().ranked.size() == 1u);
    CHECK(auditClean(f));
}

// Only the holder may release a reservation (the same rule commit applies).
TEST(release_requires_the_reservation_holder) {
    Authority a = operatorAuth();
    Fabric f = makeFabric();
    setupRegion(f, a, RegionId(1), ProviderId(1), 4096);
    CHECK(f.registerConsumer(ConsumerId(10), ConsumerGeneration(1), a).ok());
    CHECK(f.publishRegionEvidence(evidence(RegionId(1), RegionGeneration(1), ProviderId(1), 4096,
                                           WorkerId(), WorkerBootId(), 1), a).ok());
    WorkerId w1(7), w2(8);
    CHECK(f.registerWorker(w1, WorkerBootId(1), CoordinatorEpoch(1), 100, a).ok());
    CHECK(f.registerWorker(w2, WorkerBootId(1), CoordinatorEpoch(1), 200, a).ok());
    Authority holder(CoordinatorEpoch(1), w1, WorkerBootId(1));
    Authority other(CoordinatorEpoch(1), w2, WorkerBootId(1));

    auto rid = f.requestReservation(request(1024), holder);
    CHECK(rid.ok());
    if (!rid.ok()) return;
    CHECK(f.commitReservation(rid.value(), holder).ok());
    CHECK(!f.releaseReservation(rid.value(), other).ok());
    CHECK(!f.releaseReservation(rid.value(), a).ok());
    CHECK_EQ(f.regionFree(RegionId(1)), 3072u);
    // The committed bytes cannot be handed out a second time.
    CHECK(!f.requestReservation(request(4096), a).ok());
    CHECK(f.releaseReservation(rid.value(), holder).ok());
    CHECK_EQ(f.regionFree(RegionId(1)), 4096u);

    auto op = f.requestReservation(request(1024), a);
    CHECK(op.ok());
    if (op.ok()) {
        CHECK(!f.releaseReservation(op.value(), holder).ok());
        CHECK(f.releaseReservation(op.value(), a).ok());
    }
    CHECK_EQ(f.regionFree(RegionId(1)), 4096u);
    CHECK(auditClean(f));
}

// Re-adding a replaced region to a pool updates its member entry in place.
TEST(pool_member_generation_replaced_not_duplicated) {
    Authority a = operatorAuth();
    Fabric f = makeFabric();
    setupRegion(f, a, RegionId(1), ProviderId(1), 8192);
    CHECK(f.registerConsumer(ConsumerId(10), ConsumerGeneration(1), a).ok());
    CHECK(f.registerPool(PoolDescriptor{PoolId(1), PoolGeneration(1), "pool", OriginClass::SYNTHETIC, {}}, a).ok());
    CHECK(f.addPoolMember(PoolId(1), PoolMember{RegionId(1), RegionGeneration(1)}, a).ok());
    CHECK(f.registerRegion(makeRegion(RegionId(1), RegionGeneration(2), ProviderId(1), ExpansionDomainId(1), 8192), a).ok());
    CHECK(f.addPoolMember(PoolId(1), PoolMember{RegionId(1), RegionGeneration(2)}, a).ok());
    CHECK(f.publishRegionEvidence(evidence(RegionId(1), RegionGeneration(2), ProviderId(1), 8192,
                                           WorkerId(), WorkerBootId(), 2), a).ok());

    auto snap = f.snapshot();
    CHECK_EQ(snap.pools.size(), 1u);
    if (snap.pools.size() == 1u) {
        CHECK_EQ(snap.pools[0].members.size(), 1u);
        CHECK(!snap.pools[0].members.empty() &&
              snap.pools[0].members[0].generation == RegionGeneration(2));
        // The pool view never exceeds the backing region's capacity.
        CHECK_EQ(snap.pools[0].totalBytes, 8192u);
        CHECK_EQ(snap.pools[0].freeBytes, 8192u);
    }
    CHECK(auditClean(f));

    auto sel = f.select(request(512, PoolId(1)), a);
    CHECK(sel.ok() && sel.value().ranked.size() == 1u);
    auto rid = f.requestReservation(request(512, PoolId(1)), a);
    CHECK(rid.ok());
    if (rid.ok()) CHECK(f.commitReservation(rid.value(), a).ok());
    CHECK(auditClean(f));
}

// Selection through a pool follows the pool's current generation and members;
// malformed member sets are rejected without changing the pool.
TEST(pool_generation_replacement_governs_selection) {
    Authority a = operatorAuth();
    Fabric f = makeFabric();
    setupRegion(f, a, RegionId(1), ProviderId(1), 8192);
    setupRegion(f, a, RegionId(2), ProviderId(2), 8192);
    CHECK(f.publishRegionEvidence(evidence(RegionId(1), RegionGeneration(1), ProviderId(1), 8192,
                                           WorkerId(), WorkerBootId(), 1), a).ok());
    CHECK(f.publishRegionEvidence(evidence(RegionId(2), RegionGeneration(1), ProviderId(2), 8192,
                                           WorkerId(), WorkerBootId(), 2), a).ok());
    CHECK(f.registerConsumer(ConsumerId(10), ConsumerGeneration(1), a).ok());
    CHECK(f.registerPool(PoolDescriptor{PoolId(1), PoolGeneration(1), "pool", OriginClass::SYNTHETIC, {}}, a).ok());
    CHECK(f.addPoolMember(PoolId(1), PoolMember{RegionId(1), RegionGeneration(1)}, a).ok());

    // Generation 2 of the pool contains region 2 only.
    CHECK(f.replacePoolGeneration(PoolDescriptor{PoolId(1), PoolGeneration(2), "pool", OriginClass::SYNTHETIC,
                                                 {PoolMember{RegionId(2), RegionGeneration(1)}}}, a).ok());
    auto sel = f.select(request(512, PoolId(1)), a);
    CHECK(sel.ok() && sel.value().ranked.size() == 1u);
    CHECK(sel.ok() && !sel.value().ranked.empty() && sel.value().ranked[0].region == RegionId(2));
    auto rid = f.requestReservation(request(512, PoolId(1)), a);
    CHECK(rid.ok());
    CHECK_EQ(f.regionFree(RegionId(1)), 8192u);
    CHECK_EQ(f.regionFree(RegionId(2)), 8192u - 512u);

    CHECK(!f.replacePoolGeneration(PoolDescriptor{PoolId(1), PoolGeneration(3), "pool", OriginClass::SYNTHETIC,
                                                  {PoolMember{RegionId(2), RegionGeneration(1)},
                                                   PoolMember{RegionId(2), RegionGeneration(1)}}}, a).ok());
    CHECK(!f.replacePoolGeneration(PoolDescriptor{PoolId(1), PoolGeneration(3), "pool", OriginClass::SYNTHETIC,
                                                  {PoolMember{RegionId(2), RegionGeneration(7)}}}, a).ok());
    CHECK(!f.replacePoolGeneration(PoolDescriptor{PoolId(1), PoolGeneration(3), "pool", OriginClass::SYNTHETIC,
                                                  {PoolMember{RegionId(99), RegionGeneration(1)}}}, a).ok());
    auto snap = f.snapshot();
    CHECK(snap.pools.size() == 1u && snap.pools[0].generation == PoolGeneration(2));
    CHECK(snap.pools.size() == 1u && snap.pools[0].totalBytes == 8192u);
    if (rid.ok()) CHECK(f.commitReservation(rid.value(), a).ok());
    CHECK(auditClean(f));
}

MEF_MAIN()
