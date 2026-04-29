using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace Wolverine.RavenDb.Internals.Durability;

public partial class RavenDbDurabilityAgent
{
    private async Task runScheduledJobs()
    {
        var pollId = Guid.NewGuid().ToString("N").Substring(0, 6);
        var thread = Environment.CurrentManagedThreadId;
        _logger.LogInformation(
            "[PRENTICE-DBG] runScheduledJobs ENTER instance={Instance} pollId={PollId} thread={Thread}",
            DebugInstanceId, pollId, thread);

        try
        {
            if (!(await _parent.TryAttainScheduledJobLockAsync(_combined.Token)))
            {
                _logger.LogInformation(
                    "[PRENTICE-DBG] runScheduledJobs LOCK-DENIED instance={Instance} pollId={PollId}",
                    DebugInstanceId, pollId);
                return;
            }

            _logger.LogInformation(
                "[PRENTICE-DBG] runScheduledJobs LOCK-ACQUIRED instance={Instance} pollId={PollId}",
                DebugInstanceId, pollId);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error trying to attain the scheduled job lock");
            return;
        }

        try
        {
            using var session = _store.OpenAsyncSession();
            var incoming = await session.Query<IncomingMessage>()
                .Where(x => x.Status == EnvelopeStatus.Scheduled && x.ExecutionTime <= DateTimeOffset.UtcNow)
                .OrderBy(x => x.ExecutionTime)
                .Take(_settings.RecoveryBatchSize)
                .ToListAsync(_combined.Token);

            _logger.LogInformation(
                "[PRENTICE-DBG] runScheduledJobs FETCHED instance={Instance} pollId={PollId} count={Count} now={Now}",
                DebugInstanceId, pollId, incoming.Count, DateTimeOffset.UtcNow.ToString("O"));

            foreach (var msg in incoming)
            {
                var cv = session.Advanced.GetChangeVectorFor(msg);
                _logger.LogInformation(
                    "[PRENTICE-DBG] runScheduledJobs FETCHED-ROW pollId={PollId} docId={DocId} envId={Env} status={Status} ownerId={Owner} execTime={Exec} cv={Cv}",
                    pollId, msg.Id, msg.EnvelopeId, msg.Status, msg.OwnerId, msg.ExecutionTime, cv);
            }

            if (!incoming.Any())
            {
                return;
            }

            await locallyPublishScheduledMessages(incoming, session, pollId);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error while trying to process ");
        }
        finally
        {
            try
            {
                await _parent.ReleaseScheduledJobLockAsync();
                _logger.LogInformation(
                    "[PRENTICE-DBG] runScheduledJobs LOCK-RELEASED instance={Instance} pollId={PollId}",
                    DebugInstanceId, pollId);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error trying to release the scheduled job lock");
            }
        }
    }

    private async Task locallyPublishScheduledMessages(List<IncomingMessage> incoming, IAsyncDocumentSession session, string pollId)
    {
        var envelopes = incoming.Select(x => x.Read()).ToList();

        foreach (var message in incoming)
        {
            message.Status = EnvelopeStatus.Incoming;
            message.OwnerId = _settings.AssignedNodeNumber;
        }

        var requestsBefore = session.Advanced.NumberOfRequests;
        var hasChangesBefore = session.Advanced.HasChanges;
        var dirtyBefore = incoming
            .Where(m => session.Advanced.HasChanged(m))
            .Select(m => m.Id)
            .ToList();
        _logger.LogInformation(
            "[PRENTICE-DBG] locallyPublishScheduledMessages PRE-SAVE pollId={PollId} hasChanges={HasChanges} requestsBefore={Reqs} dirtyDocIds=[{Dirty}]",
            pollId, hasChangesBefore, requestsBefore, string.Join(",", dirtyBefore));

        await session.SaveChangesAsync();

        var requestsAfter = session.Advanced.NumberOfRequests;
        _logger.LogInformation(
            "[PRENTICE-DBG] locallyPublishScheduledMessages SAVED instance={Instance} pollId={PollId} count={Count} requestsAfter={Reqs} delta={Delta}",
            DebugInstanceId, pollId, envelopes.Count, requestsAfter, requestsAfter - requestsBefore);

        // [PRENTICE-DBG] POST-SAVE VERIFY: bypass the index, load each doc by ID
        // in a fresh session, log its persisted Status. Definitively answers
        // "did the save flip the row?"
        try
        {
            using var verify = _store.OpenAsyncSession();
            foreach (var msg in incoming)
            {
                var reloaded = await verify.LoadAsync<IncomingMessage>(msg.Id);
                var cv = reloaded != null ? verify.Advanced.GetChangeVectorFor(reloaded) : "(null)";
                _logger.LogInformation(
                    "[PRENTICE-DBG] POST-SAVE-VERIFY pollId={PollId} docId={DocId} envId={Env} persistedStatus={Status} persistedOwnerId={Owner} cv={Cv}",
                    pollId, msg.Id, msg.EnvelopeId, reloaded?.Status, reloaded?.OwnerId, cv);
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "[PRENTICE-DBG] POST-SAVE-VERIFY failed pollId={PollId}", pollId);
        }

        // This is very low risk
        foreach (var envelope in envelopes)
        {
            _logger.LogInformation(
                "[PRENTICE-DBG] LocalQueue.Enqueue instance={Instance} pollId={PollId} envelopeId={EnvId} messageType={Type} destination={Dest} status={Status} thread={Thread}",
                DebugInstanceId, pollId, envelope.Id, envelope.MessageType, envelope.Destination, envelope.Status, Environment.CurrentManagedThreadId);
            await _localQueue.EnqueueAsync(envelope);
        }
    }
}