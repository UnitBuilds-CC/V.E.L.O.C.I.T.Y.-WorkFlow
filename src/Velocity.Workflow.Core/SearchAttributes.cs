using System;
using System.Collections.Generic;

namespace Velocity.Workflow.Core;

public class SearchAttributes
{
    private readonly Dictionary<string, object> _attributes = new();

    public void Set(string key, object value)
    {
        _attributes[key] = value;
    }

    public bool TryGet<T>(string key, out T value)
    {
        if (_attributes.TryGetValue(key, out var raw) && raw is T typed)
        {
            value = typed;
            return true;
        }
        value = default!;
        return false;
    }
}
