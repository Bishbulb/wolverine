using System.Diagnostics;
using JasperFx;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Wolverine.Persistence;
using Wolverine.Runtime;
using Wolverine.Runtime.Agents;
using Wolverine.Runtime.Handlers;
using Wolverine.Runtime.WorkerQueues;
using Wolverine.Transports;

namespace Wolverine.RavenDb.Internals.Durability;

public partial class RavenDbDurabilityAgent : IAgent
{
    private readonly IDocumentStore _store;
    private readonly IWolverineRuntime _runtime;
    private readonly RavenDbMessageStore _parent;
    private readonly ILocalQueue _localQueue;
    private readonly DurabilitySettings _settings;
    private readonly ILogger<RavenDbDurabilityAgent> _logger;

    private Task? _recoveryTask;
    private Task? _scheduledJob;
    private IDisposable? _changesSubscription;

    private readonly CancellationTokenSource _cancellation = new();
    private readonly CancellationTokenSource _combined;
    private PersistenceMetrics _metrics = null!;

    // [PRENTICE-DBG] Unique instance id to detect duplicate agent creation/startup.
    private readonly string _instanceId = Guid.NewGuid().ToString("N").Substring(0, 8);

    public RavenDbDurabilityAgent(IDocumentStore store, IWolverineRuntime runtime, RavenDbMessageStore parent)
    {
        _store = store;
        _runtime = runtime;
        _parent = parent;
        _localQueue = (ILocalQueue)runtime.Endpoints.AgentForLocalQueue(TransportConstants.Scheduled);
        _settings = runtime.DurabilitySettings;

        Uri = new Uri($"{PersistenceConstants.AgentScheme}://ravendb/durability");

        _logger = runtime.LoggerFactory.CreateLogger<RavenDbDurabilityAgent>();

        _combined = CancellationTokenSource.CreateLinkedTokenSource(runtime.Cancellation, _cancellation.Token);

        _logger.LogInformation(
            "[PRENTICE-DBG] RavenDbDurabilityAgent ctor instance={Instance} thread={Thread} parentStoreUri={ParentUri}",
            _instanceId, Environment.CurrentManagedThreadId, parent.Uri);

        // [PRENTICE-DBG] Hook every IncomingMessage write through ANY session in this process.
        // Captures the doc state at write time + a stack trace naming the caller.
        _store.OnBeforeStore += (_, args) =>
        {
            if (args.Entity is IncomingMessage im)
            {
                var stack = new StackTrace(1, false).ToString();
                var firstLines = string.Join(" | ", stack
                    .Split('\n')
                    .Select(l => l.Trim())
                    .Where(l => l.StartsWith("at "))
                    .Take(10));
                _logger.LogInformation(
                    "[PRENTICE-DBG] OnBeforeStore IncomingMessage docId={DocId} envId={Env} status={Status} ownerId={Owner} execTime={Exec} stack={Stack}",
                    im.Id, im.EnvelopeId, im.Status, im.OwnerId, im.ExecutionTime, firstLines);
            }
        };

        // [PRENTICE-DBG] Server-side push of EVERY change to IncomingMessages.
        // Catches writes from any process/session, not just ours.
        try
        {
            var changes = _store.Changes();
            var observer = new PrenticeChangeObserver(_logger);
            _changesSubscription = changes
                .ForDocumentsInCollection("IncomingMessages")
                .Subscribe(observer);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "[PRENTICE-DBG] Failed to subscribe to Raven IncomingMessages changes");
        }
    }

    private sealed class PrenticeChangeObserver : IObserver<Raven.Client.Documents.Changes.DocumentChange>
    {
        private readonly ILogger _logger;
        public PrenticeChangeObserver(ILogger logger) => _logger = logger;
        public void OnCompleted() { }
        public void OnError(Exception error) =>
            _logger.LogError(error, "[PRENTICE-DBG] RAVEN-CHANGE subscription error");
        public void OnNext(Raven.Client.Documents.Changes.DocumentChange change) =>
            _logger.LogInformation(
                "[PRENTICE-DBG] RAVEN-CHANGE docId={DocId} type={Type} cv={ChangeVector} collection={Collection}",
                change.Id, change.Type, change.ChangeVector, change.CollectionName);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        StartTimers();

        return Task.CompletedTask;
    }

    internal void StartTimers()
    {
        _logger.LogInformation(
            "[PRENTICE-DBG] StartTimers called instance={Instance} thread={Thread} alreadyHasRecoveryTask={HasRec} alreadyHasScheduledJob={HasSched}",
            _instanceId, Environment.CurrentManagedThreadId, _recoveryTask != null, _scheduledJob != null);

        _metrics = new PersistenceMetrics(_runtime, _settings, null);

        if (_settings.DurabilityMetricsEnabled)
        {
            _metrics.StartPolling(_runtime.LoggerFactory.CreateLogger<PersistenceMetrics>(), _parent);
        }

        var recoveryStart = _settings.ScheduledJobFirstExecution.Add(new Random().Next(0, 1000).Milliseconds());

        _recoveryTask = Task.Run(async () =>
        {
            await Task.Delay(recoveryStart, _combined.Token);
            using var timer = new PeriodicTimer(_settings.ScheduledJobPollingTime);

            while (!_combined.IsCancellationRequested)
            {
                var lastExpiredTime = DateTimeOffset.UtcNow;

                await tryRecoverIncomingMessages();
                await tryRecoverOutgoingMessagesAsync();

                if (_settings.DeadLetterQueueExpirationEnabled)
                {
                    // Crudely just doing this every hour
                    var now = DateTimeOffset.UtcNow;
                    if (now > lastExpiredTime.AddHours(1))
                    {
                        await tryDeleteExpiredDeadLetters();
                    }
                }

                await timer.WaitForNextTickAsync(_combined.Token);
            }
        }, _combined.Token);

        _scheduledJob = Task.Run(async () =>
        {
            await Task.Delay(recoveryStart, _combined.Token);
            using var timer = new PeriodicTimer(_settings.ScheduledJobPollingTime);

            while (!_combined.IsCancellationRequested)
            {
                await runScheduledJobs();
                await timer.WaitForNextTickAsync(_combined.Token);
            }
        }, _combined.Token);

    }

    // [PRENTICE-DBG] Expose instance id to partial-class siblings.
    internal string DebugInstanceId => _instanceId;

    private async Task tryDeleteExpiredDeadLetters()
    {
        var now = DateTimeOffset.UtcNow;
        using var session = _store.OpenAsyncSession();
        var op =
            await _store.Operations.SendAsync(
                new DeleteByQueryOperation<DeadLetterMessage>("DeadLetterMessages", x => x.ExpirationTime < now));
 
        await op.WaitForCompletionAsync();
    }


    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cancellation.Cancel();

        if (_metrics != null)
        {
            _metrics.SafeDispose();
        }
        
        if (_recoveryTask != null)
        {
            _recoveryTask.SafeDispose();
        }

        if (_scheduledJob != null)
        {
            _scheduledJob.SafeDispose();
        }

        _changesSubscription?.SafeDispose();

        return Task.CompletedTask;
    }

    public Uri Uri { get; set; }
    public AgentStatus Status { get; set; }
}