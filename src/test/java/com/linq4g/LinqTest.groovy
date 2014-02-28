package com.linq4g

import org.junit.Test

class LinqTest
{
    @Test
    void testFrom()
    {
        Linq4g query = new Linq4g()
        Object[] result = query.from([new Foo(1, 1), new Foo(2, 1), new Foo(3, 2)], Foo.class)
                .select { Foo f ->
            return [f.id, f.barId]
        }
        assert 3 == result.length
        assert [1, 1] == result[0]
        assert [2, 1] == result[1]
        assert [3, 2] == result[2]

        query = new Linq4g({ Class alias ->
            switch (alias)
            {
                case Foo.class: return [new Foo(1, 1), new Foo(2, 1), new Foo(3, 2)]
            }
            throw new IllegalStateException();
        } as Closure<List>)
        result = query.from(Foo.class)
                .select { Foo f ->
            return [f.id, f.barId]
        }
        assert 3 == result.length
        assert [1, 1] == result[0]
        assert [2, 1] == result[1]
        assert [3, 2] == result[2]

        query = new Linq4g({ Class alias ->
            switch (alias)
            {
                case Bar.class: return [new Bar(1, new Inner(new Foo(1, 1), new Foo(2, 1))),
                        new Bar(2, new Inner(new Foo(3, 2)))]
            }
            throw new IllegalStateException();
        } as Closure<List>)
        result = query.from({ Bar bar -> bar.inners*.foos }, Foo.class)
                .select { Foo f ->
            return [f.id, f.barId]
        }
        assert 3 == result.length
        assert [1, 1] == result[0]
        assert [2, 1] == result[1]
        assert [3, 2] == result[2]
    }

    @Test
    void testJoin()
    {
        Linq4g query = new Linq4g()
        query.from([new Foo(1, 1), new Foo(2, 1), new Foo(3, 2)], Foo.class)
                .join([new Bar(1), new Bar(2)], Bar.class) { Bar b, Foo f -> f.barId == b.id }
        Object[] result = query.select { Foo f, Bar b -> [f.id, f.barId, b.id] }
        assert 3 == result.length
        assert [1, 1, 1] == result[0]
        assert [2, 1, 1] == result[1]
        assert [3, 2, 2] == result[2]

        query = new Linq4g()
        query.from([new Bar(1, new Foo(1, 1), new Foo(2, 1)), new Bar(2, new Foo(3, 2))], Bar.class)
                .join({ Bar bar -> bar.foos }, Foo.class) { Bar b, Foo f -> f.barId == b.id }
        result = query.select { Foo f, Bar b ->
            return [f.id, f.barId, b.id]
        }
        assert 3 == result.length
        assert [1, 1, 1] == result[0]
        assert [2, 1, 1] == result[1]
        assert [3, 2, 2] == result[2]

        query = new Linq4g()
        query.from([new Bar(1, new Foo(1, 1), new Foo(2, 1)), new Bar(2)], Bar.class)
                .join({ Bar bar -> bar.foos }, Foo.class) { Bar b, Foo f -> f.barId == b.id }
        result = query.select { Foo f, Bar b -> [f?.id, f?.barId, b.id] }
        assert 2 == result.length
        assert [1, 1, 1] == result[0]
        assert [2, 1, 1] == result[1]

        query = new Linq4g()
        query.from([new Bar(1, new Inner(new Foo(1, 1), new Foo(2, 1))),
                new Bar(2, new Inner(new Foo(3, 2)))], Bar.class)
                .join({ Bar bar -> bar.inners*.foos }, Foo.class) { Bar b, Foo f -> f.barId == b.id }
        result = query.select { Foo f, Bar b -> [f.id, f.barId, b.id] }
        assert 3 == result.length
        assert [1, 1, 1] == result[0]
        assert [2, 1, 1] == result[1]
        assert [3, 2, 2] == result[2]

        query = new Linq4g()
        query.from([new Bar(1, new Inner(new Foo(1, 1), new Foo(2, 1))),
                new Bar(2, new Inner(new Foo(3, 2)))], Bar.class)
                .join({ Bar bar -> bar.inners*.foos }, Foo.class) { Bar b, Foo f -> f.barId == b.id }
        result = query.select { Foo f, Bar b -> [f.id, f.barId, b.id] }
        assert 3 == result.length
        assert [1, 1, 1] == result[0]
        assert [2, 1, 1] == result[1]
        assert [3, 2, 2] == result[2]
    }

    @Test
    void testLeftJoin()
    {
        Linq4g query = new Linq4g()
        Object[] result
        query.from([new Foo(1, 1), new Foo(2, 1), new Foo(3, 2)], Foo.class)
                .leftJoin([new Bar(1)], Bar.class) { Bar b, Foo f -> f.barId == b.id }
        result = query.select { Foo f, Bar b -> [f.id, f.barId, b?.id] }
        assert 3 == result.length
        assert [1, 1, 1] == result[0]
        assert [2, 1, 1] == result[1]
        assert [3, 2, null] == result[2]

        query = new Linq4g()
        query.from([new Bar(1, new Foo(1, 1), new Foo(2, 1)), new Bar(2)], Bar.class)
                .leftJoin({ Bar bar -> bar.foos }, Foo.class) { Bar b, Foo f -> f.barId == b.id }
        result = query.select { Foo f, Bar b -> [f?.id, f?.barId, b.id] }
        assert 3 == result.length
        assert [1, 1, 1] == result[0]
        assert [2, 1, 1] == result[1]
        assert [null, null, 2] == result[2]

        query = new Linq4g()
        query.from([new Foo(1, 1), new Foo(2, 1), new Foo(3, 2)], Foo.class)
                .leftJoin([new Bar(1), new Bar(2)], Bar.class) { Bar b, Foo f -> f.barId == b.id }
        result = query.select { Foo f, Bar b -> [f.id, f.barId, b?.id] }
        assert 3 == result.length
        assert [1, 1, 1] == result[0]
        assert [2, 1, 1] == result[1]
        assert [3, 2, 2] == result[2]

        query = new Linq4g()
        query.from([new Foo(1, 1), new Foo(2, 1), new Foo(3, 2)], Foo.class)
                .leftJoin([], Bar.class) { Bar b, Foo f -> f.barId == b.id }
        result = query.select { Foo f, Bar b -> [f.id, f.barId, b?.id] }
        assert 3 == result.length
        assert [1, 1, null] == result[0]
        assert [2, 1, null] == result[1]
        assert [3, 2, null] == result[2]
    }

    @Test
    void testPreparedJoin()
    {
        Linq4g query = new Linq4g()
        query.from([new Bar(1, new Foo(1, 1), new Foo(2, 1)), new Bar(2, new Foo(3, 2))], Bar.class)
        query.join({ Bar bar -> bar.foos }, Foo)
        def result = query.select { Foo f, Bar b -> [f?.id, f?.barId, b.id] }
        assert 3 == result.size()
        assert [1, 1, 1] == result[0]
        assert [2, 1, 1] == result[1]
        assert [3, 2, 2] == result[2]

        query = new Linq4g()
        query.from([new Bar(1, new Foo(1, 1), null), new Bar(2)], Bar.class)
        query.join({ Bar bar -> bar.foos }, Foo)
        result = query.select { Foo f, Bar b -> [f?.id, f?.barId, b.id] }
        assert 1 == result.size()
        assert [1, 1, 1] == result[0]

        query = new Linq4g()
        query.from([new Bar(1, new Foo(1, 1), null), new Bar(2)], Bar.class)
        query.leftJoin({ Bar bar -> bar.foos }, Foo)
        result = query.select { Foo f, Bar b -> [f?.id, f?.barId, b.id] }
        assert 2 == result.size()
        assert [1, 1, 1] == result[0]
        assert [null, null, 2] == result[1]
    }

    @Test
    void testWhere()
    {
        Linq4g query = new Linq4g()
        query.from([new Foo(1, 1), new Foo(2, 1), new Foo(3, 2)], Foo.class)
                .where { Foo foo -> foo.barId == 1 }
        Object[] result = query.select { Foo f ->
            return [f.id, f.barId]
        }
        assert 2 == result.length
        assert [1, 1] == result[0]
        assert [2, 1] == result[1]
    }

    @Test
    void testOrder()
    {
        Linq4g query = new Linq4g()
        query.from([new Foo(2, 1), new Foo(3, 2), new Foo(1, 1)], Foo.class)
                .order(Direction.ASC) { Foo foo -> foo.id }
        Object[] result = query.select { Foo f ->
            return [f.id]
        }
        assert 3 == result.length
        assert [1] == result[0]
        assert [2] == result[1]
        assert [3] == result[2]
    }

    class Foo
    {
        int id
        int barId

        Foo(int id, int barId)
        {
            this.id = id
            this.barId = barId
        }
    }

    class Bar
    {
        int id
        Foo[] foos;
        Inner[] inners;

        Bar(int id)
        {
            this.id = id
        }

        Bar(int id, Foo... foos)
        {
            this.id = id
            this.foos = foos
        }

        Bar(int id, Inner... inners)
        {
            this.id = id
            this.inners = inners
        }
    }

    class Inner
    {
        Foo[] foos;

        Inner(Foo... foos)
        {
            this.foos = foos
        }
    }

}

