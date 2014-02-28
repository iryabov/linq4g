package com.linq4g

import groovy.transform.CompileStatic

class Linq4g {
    Map<Class, List> sources = new HashMap<>()
    List<Map<Class, Integer>> index
    Closure<List> storageProcessor;

    Linq4g(Closure<List> storageProcessor)
    {
        this.storageProcessor = storageProcessor
    }

    Linq4g()
    {
    }

    private <T> List<T> getAll(Class<T> alias)
    {
        storageProcessor.call(alias) as List<T>
    }

    private <T> List<T> getAll(Closure from, Class<T> alias)
    {
        List<T> list = new LinkedList<>()
        if (from.parameterTypes.length != 1)
            throw new IllegalArgumentException("must be one argument")
        Class parentAlias = from.parameterTypes[0]
        if (sources.containsKey(parentAlias))
        {
            def sourceSet = new HashSet()
            for (Map<Class, Integer> currentRecord : index)
            {
                if (sourceSet.contains(currentRecord.get(parentAlias))) continue
                sourceSet.add(currentRecord.get(parentAlias))
                def result = null
                def args = extractArgs(from.parameterTypes, currentRecord)
                if (args[0] != null)
                {
                    result = call(from, args)
                }
                aggregate(list, result, alias)
            }
        }
        else
        {
            for (Object item : getAll(parentAlias))
            {
                Object result = from.call(item)
                aggregate(list, result, alias)
            }
        }
        return list
    }

    private <T> Map<Integer, List<T>> getAllAndJoin(Closure from, Class<T> alias)
    {
        if (from.parameterTypes.length != 1)
            throw new IllegalArgumentException("must be one argument")
        Class parentAlias = from.parameterTypes[0]
        if (!sources.containsKey(parentAlias))
            throw new UnsupportedOperationException(
                    "alias ${alias} had to first get in 'from' or 'join' or use 'join' with 'on' closure")
        Map<Integer, List<T>> map = new HashMap<>()
        def sourceSet = new HashSet()
        for (int currentIndex = 0; currentIndex < index.size(); currentIndex++)
        {
            def currentRecord = index[currentIndex]
            if (sourceSet.contains(currentRecord.get(parentAlias))) continue
            sourceSet.add(currentRecord.get(parentAlias))
            def args = extractArgs(from.parameterTypes, currentRecord)
            def result = null
            if (args[0] != null)
            {
                result = call(from, args)
            }
            List<T> list = new LinkedList<>()
            aggregate(list, result, alias)
            map.put(currentIndex, list)
        }
        return map
    }

    private <T> void aggregate(List<T> list, Object result, Class<T> alias)
    {
        if (result == null) return
        if (alias.isAssignableFrom(result.class))
        {
            list.add(result as T)
        }
        else if (result instanceof T[])
        {
            for (T t : result)
            {
                list.add(t)
            }
        }
        else if (result instanceof Collection)
        {
            for (Object it : result)
            {
                aggregate(list, it, alias)
            }
        }
        else
            throw new IllegalArgumentException("${result?.toString()} must be Collection<${alias}>")
    }

    private static <E> E call(Closure<E> closure, Object[] args)
    {
        switch (args.size())
        {
            case 0: return closure.call()
            case 1: return closure.call(args[0])
            case 2: return closure.call(args[0], args[1])
            case 3: return closure.call(args[0], args[1], args[2])
            case 4: return closure.call(args[0], args[1], args[2], args[3])
            case 5: return closure.call(args[0], args[1], args[2], args[3], args[4])
            case 6: return closure.call(args[0], args[1], args[2], args[3], args[4], args[5])
            case 7: return closure.call(args[0], args[1], args[2], args[3], args[4], args[5], args[6])
        }
        throw new IllegalArgumentException(args + " more 7 arguments for closure");
    }

    private Object[] extractArgs(Class[] parameterTypes, Map<Class, Integer> record)
    {
        List args = new ArrayList()
        for (Class type : parameterTypes)
        {
            def index = record.get(type)
            if (index == null)
            {
                args.add(null)
            }
            else
            {
                args.add(sources.get(type)?.get(index))
            }
        }
        return args
    }

    private Object[] extractArgs(Class[] parameterTypes, Map<Class, Integer> record, Object item)
    {
        List args = new ArrayList()
        for (Class type : parameterTypes)
        {
            if (type.isAssignableFrom(item.class))
            {
                args.add(item)
                continue
            }
            args.add(sources.get(type)?.get(record.get(type)))
        }
        return args
    }

    public <E> List<E> select(Closure<E> callable)
    {
        List<E> result = new ArrayList<>(index.size())
        for (Map<Class, Integer> it : index)
        {
            result.add(call(callable, extractArgs(callable.parameterTypes, it)))
        }
        return result
    }

    private <T> Linq4g innerFrom(List<T> list, Class<T> alias)
    {
        sources.put(alias, list)
        reindex(list, alias)
        return this
    }

    public <T> Linq4g from(List<T> list, Class<T> alias)
    {
        innerFrom(list, alias)
    }

    public <T> Linq4g from(Class<T> alias)
    {
        return innerFrom(getAll(alias), alias)
    }

    public <T> Linq4g from(Closure from, Class<T> alias)
    {
        List<T> list = getAll(from, alias)
        return innerFrom(list, alias)
    }

    public <T> Linq4g join(List<T> list, Class<T> alias, Closure<Boolean> on)
    {
        innerJoin(list, alias, on, false)
        return this
    }

    public <T> Linq4g join(Class<T> alias, Closure<Boolean> on)
    {
        return join(getAll(alias), alias, on)
    }

    public <T> Linq4g join(Closure from, Class<T> alias, Closure<Boolean> on)
    {
        return join(getAll(from, alias), alias, on)
    }

    public <T> Linq4g join(Closure from, Class<T> alias)
    {
        return innerPreparedJoin(getAllAndJoin(from, alias), alias, false)
    }

    public <T> Linq4g leftJoin(List<T> list, Class<T> alias, Closure<Boolean> on)
    {
        return innerJoin(list, alias, on, true)
    }

    public <T> Linq4g leftJoin(Class<T> alias, Closure<Boolean> on)
    {
        return leftJoin(getAll(alias), alias, on)
    }

    public <T> Linq4g leftJoin(Closure from, Class<T> alias, Closure<Boolean> on)
    {
        return leftJoin(getAll(from, alias), alias, on)
    }

    public <T> Linq4g leftJoin(Closure from, Class<T> alias)
    {
        return innerPreparedJoin(getAllAndJoin(from, alias), alias, true)
    }


    public Linq4g where(Closure<Boolean> expr)
    {
        List<Map<Class, Integer>> newBagIndex = new ArrayList<>()
        for (Map<Class, Integer> it : index)
        {
            if (call(expr, extractArgs(expr.parameterTypes, it)))
            {
                newBagIndex.add(it)
            }
        }
        index.clear()
        index = newBagIndex
        return this
    }


    public Linq4g order(Direction direction, Closure<? extends Comparable> compare)
    {
        index.sort(new Comparator<Map<Class, Integer>>() {
            @Override
            int compare(Map<Class, Integer> o1, Map<Class, Integer> o2)
            {
                Comparable c1 = call(compare, extractArgs(compare.parameterTypes, o1));
                Comparable c2 = call(compare, extractArgs(compare.parameterTypes, o2));
                if (direction == null) return 0;
                switch (direction)
                {
                    case Direction.DESC: return c2 <=> c1
                    default: return c1 <=> c2
                }
            }
        })
        return this;
    }

    private <T> Linq4g innerJoin(List<T> source, Class<T> alias, Closure<Boolean> on, boolean left)
    {
        sources.put(alias, source);
        List<Map<Class, Integer>> newBagIndex = new ArrayList<>();
        for (Map<Class, Integer> currentRecord : index)
        {
            boolean joinMiss = false
            boolean joinHit = false
            for (int sourceIndex = 0; sourceIndex < source.size(); sourceIndex++)
            {
                if (call(on, extractArgs(on.parameterTypes, currentRecord, source[sourceIndex])))
                {
                    if (joinMiss)
                    {
                        currentRecord.put(alias, sourceIndex)
                    }
                    else
                    {
                        joinHit = true
                        joinToIndex(newBagIndex, currentRecord, alias, sourceIndex)
                    }
                }
                else
                {
                    if ((left) && (!joinMiss) && (!joinHit))
                    {
                        joinMiss = true
                        joinToIndex(newBagIndex, currentRecord, alias, null)
                    }
                }
            }
            if ((left) && (!joinMiss) && (!joinHit))
            {
                joinToIndex(newBagIndex, currentRecord, alias, null)
            }
        }
        index.clear()
        index = newBagIndex
        return this
    }

    @CompileStatic
    private <T> Linq4g innerPreparedJoin(Map<Integer, List<T>> sourceMap, Class<T> alias, boolean left)
    {
        List<Map<Class, Integer>> newBagIndex = new ArrayList<>()
        Integer sourceIndex = -1
        List<T> source = new LinkedList<>()
        sources.put(alias, source)
        for (Map.Entry<Integer, List<T>> entry : sourceMap.entrySet())
        {
            def currentRecord = index[entry.getKey()]
            boolean joinMiss = false
            boolean joinHit = false
            for (int localIndex = 0; localIndex < entry.getValue().size(); localIndex++)
            {
                def value = entry.getValue()[localIndex]
                if (value != null)
                {
                    source.add(value)
                    sourceIndex++
                    if (joinMiss)
                    {
                        currentRecord.put(alias, sourceIndex)
                    }
                    else
                    {
                        joinHit = true
                        joinToIndex(newBagIndex, currentRecord, alias, sourceIndex)
                    }
                }
                else
                {
                    if ((left) && (!joinMiss) && (!joinHit))
                    {
                        joinMiss = true
                        joinToIndex(newBagIndex, currentRecord, alias, null)
                    }
                }
            }
            if ((left) && (!joinMiss) && (!joinHit))
            {
                joinToIndex(newBagIndex, currentRecord, alias, null)
            }
        }
        index.clear()
        index = newBagIndex
        return this
    }

    private void reindex(List list, Class alias)
    {
        index = new ArrayList<>(list.size());
        for (int k = 0; k < list.size(); k++)
        {
            Map<Class, Integer> bag = new HashMap<>();
            bag.put(alias, k);
            index.add(bag);
        }
    }

    private static void incIndex(List<Map<Class, Integer>> added, Map<Class, Integer> currentRecord, Class alias,
                                 Integer sourceIndex)
    {
        Map<Class, Integer> newRecord = new HashMap<>();
        newRecord.put(alias, sourceIndex);
        for (Map.Entry<Class, Integer> e : currentRecord.entrySet())
        {
            if (e.getKey().equals(alias)) continue;
            newRecord.put(e.getKey(), e.getValue());
        }
        added.add(newRecord);
    }

    private static <T> void joinToIndex(List<Map<Class, Integer>> newBagIndex, Map<Class, Integer> currentRecord,
                                        Class<T> alias, Integer sourceIndex)
    {
        if ((currentRecord.containsKey(alias)))
        {
            if (currentRecord.get(alias) != sourceIndex)
            {
                incIndex(newBagIndex, currentRecord, alias, sourceIndex)
            }
        }
        else
        {
            currentRecord.put(alias, sourceIndex)
            newBagIndex.add(currentRecord)
        }
    }

}

