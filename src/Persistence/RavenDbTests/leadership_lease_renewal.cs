using System.Reflection;
using JasperFx.Core.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Raven.Client.Documents;
using Shouldly;
using Wolverine;
using Wolverine.Persistence.Durability;
using Wolverine.RavenDb;
using Wolverine.RavenDb.Internals;

namespace RavenDbTests;

[Collection("raven")]
public class leadership_lease_renewal : IAsyncLifetime
{
    private readonly DatabaseFixture _fixture;
    private IDocumentStore _store = null!;
    private IHost _host = null!;

    public leadership_lease_renewal(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _store = _fixture.StartRavenStore();
        _host = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Services.AddSingleton(_store);
                opts.Durability.Mode = DurabilityMode.Solo;
                opts.ServiceName = "lease-renewal";
                opts.UseRavenDbPersistence();
            }).StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public async Task leader_keeps_lock_when_in_memory_lease_lapses_without_a_contender()
    {
        // Reproduces the "false stepdown every 5 minutes" bug: pre-fix,
        // HasLeadershipLock checked only DistributedLock.ExpirationTime, so once
        // the 5-min lease window passed the leader thought the lock was gone —
        // even though no peer had touched the CompareExchange entry. The
        // heartbeat then fired a spurious LostLeadership/AssumedLeadership pair.
        var store = _host.Services.GetService<IMessageStore>()!.As<RavenDbMessageStore>();

        (await store.Nodes.TryAttainLeadershipLockAsync(CancellationToken.None)).ShouldBeTrue();
        store.Nodes.HasLeadershipLock().ShouldBeTrue();

        ExpireInMemoryLeaderLock(store);

        store.Nodes.HasLeadershipLock().ShouldBeTrue();
    }

    [Fact]
    public async Task background_renewal_keeps_lock_alive_across_multiple_cycles()
    {
        // End-to-end check that the renewal task actually fires. Compress the
        // intervals so the test takes ~1s instead of minutes.
        var store = _host.Services.GetService<IMessageStore>()!.As<RavenDbMessageStore>();
        store.LeaseDuration = TimeSpan.FromSeconds(2);
        store.LeaseRenewalInterval = TimeSpan.FromMilliseconds(150);
        store.LeaseStalenessGrace = TimeSpan.FromMilliseconds(150);

        (await store.Nodes.TryAttainLeadershipLockAsync(CancellationToken.None)).ShouldBeTrue();

        var initialIndex = ReadLastLockIndex(store);

        // Wait long enough for several renewal cycles to fire.
        await Task.Delay(TimeSpan.FromSeconds(1));

        store.Nodes.HasLeadershipLock().ShouldBeTrue();
        ReadLastLockIndex(store).ShouldBeGreaterThan(initialIndex);

        await store.Nodes.ReleaseLeadershipLockAsync();
    }

    [Fact]
    public async Task release_stops_renewal_loop_and_drops_in_memory_state()
    {
        var store = _host.Services.GetService<IMessageStore>()!.As<RavenDbMessageStore>();
        store.LeaseRenewalInterval = TimeSpan.FromMilliseconds(100);

        (await store.Nodes.TryAttainLeadershipLockAsync(CancellationToken.None)).ShouldBeTrue();
        await store.Nodes.ReleaseLeadershipLockAsync();

        store.Nodes.HasLeadershipLock().ShouldBeFalse();
        ReadLeaderLock(store).ShouldBeNull();

        // Give any leftover loop iteration a chance to misbehave; HasLeadershipLock
        // must stay false.
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        store.Nodes.HasLeadershipLock().ShouldBeFalse();
    }

    private static readonly FieldInfo LeaderLockField = typeof(RavenDbMessageStore).GetField(
        "_leaderLock", BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly FieldInfo LastLockIndexField = typeof(RavenDbMessageStore).GetField(
        "_lastLockIndex", BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static void ExpireInMemoryLeaderLock(RavenDbMessageStore store)
    {
        var distLock = (DistributedLock)LeaderLockField.GetValue(store)!;
        distLock.ExpirationTime = DateTimeOffset.UtcNow.AddMinutes(-1);
    }

    private static long ReadLastLockIndex(RavenDbMessageStore store)
        => (long)LastLockIndexField.GetValue(store)!;

    private static DistributedLock? ReadLeaderLock(RavenDbMessageStore store)
        => (DistributedLock?)LeaderLockField.GetValue(store);
}
