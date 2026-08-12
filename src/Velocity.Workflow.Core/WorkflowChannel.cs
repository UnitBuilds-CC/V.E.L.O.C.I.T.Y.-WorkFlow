using System;
using System.Collections.Concurrent;

namespace Velocity.Workflow.Core;

public class WorkflowChannel<TMessage>
{
    private readonly ConcurrentQueue<TMessage> _queue = new();
    public int Count => _queue.Count;

    public void SendSignal(TMessage payload)
    {
        _queue.Enqueue(payload);
    }

    public bool TryReceiveSignal(out TMessage message)
    {
        return _queue.TryDequeue(out message!);
    }

    public TMessage PeekQueryState()
    {
        if (_queue.TryPeek(out var message))
        {
            return message;
        }
        return default!;
    }
}
