using Microsoft.Extensions.Logging;
using Raven.Client.Documents.Operations.CompareExchange;
using Wolverine.Runtime;

namespace Wolverine.RavenDb.Internals;

// TODO -- harden all locking methods
public partial class RavenDbMessageStore
{
    private string _leaderLockId = null!;
    private string _scheduledLockId = null!;
    private long _lastScheduledLockIndex = 0;
    private DistributedLock? _scheduledLock;
    private IWolverineRuntime _runtime = null!;

    private DistributedLock? _leaderLock;
    private long _lastLockIndex = 0;

    // Postgres holds its advisory lock for the lifetime of an open NpgsqlConnection;
    // there's no expiration to renew because the lock dies with the session. RavenDB
    // CompareExchange entries have no session lifetime — they sit there forever
    // unless deleted. This renewal task is the RavenDB analog of "as long as the
    // process is alive, the lock is held": while we own the lease, refresh it
    // periodically via CAS so HasLeadershipLock stays truthful indefinitely.
    internal TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(5);
    internal TimeSpan LeaseRenewalInterval { get; set; } = TimeSpan.FromMinutes(1);
    internal TimeSpan LeaseStalenessGrace { get; set; } = TimeSpan.FromSeconds(30);
    private CancellationTokenSource? _leaderRenewalCts;
    private Task? _leaderRenewalTask;
    private readonly object _leaderRenewalGate = new();
    private DateTimeOffset _leaderLockLastSuccessAt;


    public bool HasLeadershipLock()
    {
        if (_leaderLock == null) return false;

        // Postgres' equivalent (`select 1` ping on the advisory-lock connection) has
        // no direct analog here, so we use the renewal task's last-success timestamp
        // as the liveness signal. While the loop is healthy this stays fresh; if it
        // can't reach RavenDB or another node has stolen the lock, the timestamp
        // ages out and we treat the lock as lost. The lease's nominal ExpirationTime
        // is a server-side crash-recovery hint only — never the in-memory truth.
        var maxStaleness = LeaseRenewalInterval + LeaseStalenessGrace;
        return DateTimeOffset.UtcNow - _leaderLockLastSuccessAt <= maxStaleness;
    }

    // Error handling is outside of this
    public async Task<bool> TryAttainLeadershipLockAsync(CancellationToken token)
    {
        var newLock = new DistributedLock
        {
            NodeId = _options.UniqueNodeId,
            ExpirationTime = DateTimeOffset.UtcNow.Add(LeaseDuration),
        };

        if (_leaderLock == null)
        {
            var result = await _store.Operations.SendAsync(new PutCompareExchangeValueOperation<DistributedLock>(_leaderLockId, newLock, 0), token: token);
            if (result.Successful)
            {
                _leaderLock = newLock;
                _lastLockIndex = result.Index;
                _leaderLockLastSuccessAt = DateTimeOffset.UtcNow;
                startLeaderRenewalLoop();
                return true;
            }

            var tookOver = await tryTakeOverIfExpiredAsync(_leaderLockId, newLock,
                lockSet: l => { _leaderLock = l; _leaderLockLastSuccessAt = DateTimeOffset.UtcNow; },
                indexSet: i => _lastLockIndex = i, token);
            if (tookOver) startLeaderRenewalLoop();
            return tookOver;
        }

        var result2 = await _store.Operations.SendAsync(new PutCompareExchangeValueOperation<DistributedLock>(_leaderLockId, newLock, _lastLockIndex), token: token);
        if (result2.Successful)
        {
            _leaderLock = newLock;
            _lastLockIndex = result2.Index;
            _leaderLockLastSuccessAt = DateTimeOffset.UtcNow;
            startLeaderRenewalLoop();
            return true;
        }

        return false;
    }

    // Error handling is outside of this
    public async Task ReleaseLeadershipLockAsync()
    {
        await stopLeaderRenewalLoopAsync();
        if (_leaderLock == null) return;
        await _store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<DistributedLock>(_leaderLockId, _lastLockIndex));
        _leaderLock = null;
    }

    private void startLeaderRenewalLoop()
    {
        lock (_leaderRenewalGate)
        {
            if (_leaderRenewalTask is not null) return;
            _leaderRenewalCts = new CancellationTokenSource();
            _leaderRenewalTask = Task.Run(() => leaderRenewalLoopAsync(_leaderRenewalCts.Token));
        }
    }

    private async Task stopLeaderRenewalLoopAsync()
    {
        CancellationTokenSource? cts;
        Task? task;
        lock (_leaderRenewalGate)
        {
            cts = _leaderRenewalCts;
            task = _leaderRenewalTask;
            _leaderRenewalCts = null;
            _leaderRenewalTask = null;
        }

        if (cts is null) return;
        cts.Cancel();
        if (task is not null)
        {
            try { await task.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
        cts.Dispose();
    }

    private async Task leaderRenewalLoopAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(LeaseRenewalInterval, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (_leaderLock is null) return;

            var renewed = new DistributedLock
            {
                NodeId = _options.UniqueNodeId,
                ExpirationTime = DateTimeOffset.UtcNow.Add(LeaseDuration),
            };

            try
            {
                var result = await _store.Operations.SendAsync(
                    new PutCompareExchangeValueOperation<DistributedLock>(_leaderLockId, renewed, _lastLockIndex),
                    token: token).ConfigureAwait(false);

                if (result.Successful)
                {
                    _leaderLock = renewed;
                    _lastLockIndex = result.Index;
                    _leaderLockLastSuccessAt = DateTimeOffset.UtcNow;
                }
                else
                {
                    // CAS failed — the CE entry was changed by someone else (or vanished).
                    // Drop in-memory state so HasLeadershipLock returns false on the next
                    // heartbeat tick and the controller falls into the normal election path.
                    _runtime?.Logger.LogWarning(
                        "RavenDb leadership lease renewal lost — CompareExchange index moved. Dropping in-memory lock so the next heartbeat steps down cleanly.");
                    _leaderLock = null;
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception e)
            {
                // Transient failure — keep trying on the next tick. The lease has plenty
                // of headroom (5 min TTL, 1 min renewal interval) so a few failed attempts
                // don't lose us the lock.
                _runtime?.Logger.LogWarning(e, "RavenDb leadership lease renewal failed; will retry");
            }
        }
    }

    // Error handling is outside of this
    public async Task<bool> TryAttainScheduledJobLockAsync(CancellationToken token)
    {
        var newLock = new DistributedLock
        {
            NodeId = _options.UniqueNodeId,
            ExpirationTime = DateTimeOffset.UtcNow.AddMinutes(5),
        };
        
        if (_scheduledLock == null)
        {
            var result = await _store.Operations.SendAsync(new PutCompareExchangeValueOperation<DistributedLock>(_scheduledLockId, newLock, 0), token: token);
            if (result.Successful)
            {
                _scheduledLock = newLock;
                _lastScheduledLockIndex = result.Index;
                return true;
            }

            return await tryTakeOverIfExpiredAsync(_scheduledLockId, newLock, lockSet: l => _scheduledLock = l, indexSet: i => _lastScheduledLockIndex = i, token);
        }
        
        var result2 = await _store.Operations.SendAsync(new PutCompareExchangeValueOperation<DistributedLock>(_scheduledLockId, newLock, _lastScheduledLockIndex), token: token);
        if (result2.Successful)
        {
            _scheduledLock = newLock;
            _lastScheduledLockIndex = result2.Index;
            return true;
        }

        return false;
    }

    public async Task ReleaseScheduledJobLockAsync()
    {
        if (_scheduledLock == null) return;
        await _store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<DistributedLock>(_scheduledLockId, _lastScheduledLockIndex));
        _scheduledLock = null;
    }

    // A predecessor process can crash without releasing its lock, leaving the CE value
    // behind indefinitely. The DistributedLock.ExpirationTime field is the recovery
    // hook: if the existing lock is past its expiration, CAS-replace it using the
    // current CE index. Mirrors the equivalent path in CosmosDbMessageStore.Locking.
    private async Task<bool> tryTakeOverIfExpiredAsync(string lockId, DistributedLock newLock, Action<DistributedLock> lockSet, Action<long> indexSet, CancellationToken token)
    {
        var existing = await _store.Operations.SendAsync(new GetCompareExchangeValueOperation<DistributedLock>(lockId), token: token);
        if (existing?.Value == null) return false;
        if (existing.Value.ExpirationTime > DateTimeOffset.UtcNow) return false;

        var takeover = await _store.Operations.SendAsync(new PutCompareExchangeValueOperation<DistributedLock>(lockId, newLock, existing.Index), token: token);
        if (!takeover.Successful) return false;

        lockSet(newLock);
        indexSet(takeover.Index);
        return true;
    }
}

public class DistributedLock
{
    public Guid NodeId { get; set; }
    public DateTimeOffset ExpirationTime { get; set; } 
}