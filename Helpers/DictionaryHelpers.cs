namespace WindowsService.Helpers
{
    public static class DictionaryHelpers
    {
        public static void AddOrUpdate<T, U>(this IDictionary<T, U> d, T key, U val)
        {
            if (d.ContainsKey(key))
                d[key] = val;
            else
                d.Add(key, val);
        }
    }
}