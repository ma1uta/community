/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.helpers.collection;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Specification;

/**
 * TODO
 */
public final class Iterables
{
    private static Iterable EMPTY = new Iterable()
    {
        Iterator iterator = new Iterator()
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }

            @Override
            public Object next()
            {
                throw new NoSuchElementException(  );
            }

            @Override
            public void remove()
            {
            }
        };

        @Override
        public Iterator iterator()
        {
            return iterator;
        }
    };

    public static <T> Iterable<T> empty()
    {
        return EMPTY;
    }

    public static <T> Iterable<T> constant( final T item )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new Iterator<T>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return true;
                    }

                    @Override
                    public T next()
                    {
                        return item;
                    }

                    @Override
                    public void remove()
                    {
                    }
                };
            }
        };
    }

    public static <T> Iterable<T> limit( final int limitItems, final Iterable<T> iterable )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                final Iterator<T> iterator = iterable.iterator();

                return new Iterator<T>()
                {
                    int count;

                    @Override
                    public boolean hasNext()
                    {
                        return count < limitItems && iterator.hasNext();
                    }

                    @Override
                    public T next()
                    {
                        count++;
                        return iterator.next();
                    }

                    @Override
                    public void remove()
                    {
                        iterator.remove();
                    }
                };
            }
        };
    }

    public static <T> Function<Iterable<T>, Iterable<T>> limit( final int limitItems)
    {
        return new Function<Iterable<T>, Iterable<T>>()
        {
            @Override
            public Iterable<T> map(Iterable<T> ts)
            {
                return limit(limitItems, ts);
            }
        };
    }

    public static <T> Iterable<T> unique( final Iterable<T> iterable)
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                final Iterator<T> iterator = iterable.iterator();

                return new Iterator<T>()
                {
                    Set<T> items = new HashSet<T>();
                    T nextItem;

                    @Override
                    public boolean hasNext()
                    {
                        while(iterator.hasNext())
                        {
                            nextItem = iterator.next();
                            if (items.add( nextItem ))
                                return true;
                        }

                        return false;
                    }

                    @Override
                    public T next()
                    {
                        if (nextItem == null && !hasNext())
                            throw new NoSuchElementException(  );

                        return nextItem;
                    }

                    @Override
                    public void remove()
                    {
                    }
                };
            }
        };
    }

    public static <T, C extends Collection<T>> C addAll( C collection, Iterable<? extends T> iterable )
    {
        for( T item : iterable )
        {
            collection.add( item );
        }

        return collection;
    }

    public static long count( Iterable<?> iterable )
    {
        long c = 0;
        Iterator<?> iterator = iterable.iterator();
        while( iterator.hasNext() )
        {
            iterator.next();
            c++;
        }
        return c;
    }

    public static <X> Iterable<X> filter( Specification<? super X> specification, Iterable<X> i )
    {
        return new FilterIterable<X>( i, specification );
    }

    public static <X> X first( Iterable<? extends X> i )
    {
        Iterator<? extends X> iter = i.iterator();
        if( iter.hasNext() )
        {
            return iter.next();
        } else
        {
            return null;
        }
    }

    public static <X> X single( Iterable<? extends X> i )
    {
        Iterator<? extends X> iter = i.iterator();
        if( iter.hasNext() )
        {
            X result = iter.next();

            if (iter.hasNext())
                throw new IllegalArgumentException( "More than one element in iterable" );

            return result;
        } else
        {
            throw new IllegalArgumentException( "No elements in iterable" );
        }
    }

    public static <X> Iterable<X> skip( final int skip, final Iterable<X> iterable)
    {
        return new Iterable<X>()
        {
            @Override
            public Iterator<X> iterator()
            {
                Iterator<X> iterator = iterable.iterator();

                for (int i = 0; i < skip; i++)
                {
                    if (iterator.hasNext())
                        iterator.next();
                    else
                        return Iterables.<X>empty().iterator();
                }

                return iterator;
            }
        };
    }

    public static <X> X last( Iterable<? extends X> i )
    {
        Iterator<? extends X> iter = i.iterator();
        X item = null;
        while( iter.hasNext() )
            item = iter.next();

        return item;
    }

    public static <X> Iterable<X> reverse( Iterable<X> iterable )
    {
        List<X> list = toList( iterable );
        Collections.reverse( list );
        return list;
    }

    public static <T> boolean matchesAny( Specification<? super T> specification, Iterable<T> iterable )
    {
        boolean result = false;

        for( T item : iterable )
        {
            if( specification.satisfiedBy( item ) )
            {
                result = true;
                break;
            }
        }

        return result;
    }

    public static <T> boolean matchesAll( Specification<? super T> specification, Iterable<T> iterable )
    {
        boolean result = true;
        for( T item : iterable )
        {
            if( !specification.satisfiedBy( item ) )
            {
                result = false;
            }
        }

        return result;
    }

    public static <X, I extends Iterable<? extends X>> Iterable<X> flatten( I... multiIterator )
    {
        return new FlattenIterable<X, I>( Arrays.asList( multiIterator ) );
    }

    public static <X, I extends Iterable<? extends X>> Iterable<X> flattenIterables( Iterable<I> multiIterator )
    {
        return new FlattenIterable<X, I>( multiIterator );
    }

    public static <T> Iterable<T> mix( final Iterable<T>... iterables )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                final Iterable<Iterator<T>> iterators = toList(map( new Function<Iterable<T>, Iterator<T>>()
                        {
                            @Override
                            public Iterator<T> map( Iterable<T> iterable )
                            {
                                return iterable.iterator();
                            }
                        }, Iterables.iterable( iterables) ));

                return new Iterator<T>()
                {
                    Iterator<Iterator<T>> iterator;

                    Iterator<T> iter;

                    @Override
                    public boolean hasNext()
                    {
                        for( Iterator<T> iterator : iterators )
                        {
                            if (iterator.hasNext())
                            {
                                return true;
                            }
                        }

                        return false;
                    }

                    @Override
                    public T next()
                    {
                        if (iterator == null)
                        {
                            iterator = iterators.iterator();
                        }

                        while (iterator.hasNext())
                        {
                            iter = iterator.next();

                            if (iter.hasNext())
                                return iter.next();
                        }

                        iterator = null;

                        return next();
                    }

                    @Override
                    public void remove()
                    {
                        if (iter != null)
                            iter.remove();
                    }
                };
            }
        };
    }

    public static <FROM, TO> Iterable<TO> map( Function<? super FROM, ? extends TO> function, Iterable<FROM> from )
    {
        return new MapIterable<FROM, TO>( from, function );
    }

    public static <T> Iterable<T> iterable( Enumeration<T> enumeration )
    {
        List<T> list = new ArrayList<T>();
        while( enumeration.hasMoreElements() )
        {
            T item = enumeration.nextElement();
            list.add( item );
        }

        return list;
    }

    public static <T> Iterable<T> iterable( T... items )
    {
        return Arrays.asList( items );
    }

    public static <T, C> Iterable<T> cast( Iterable<C> iterable )
    {
        Iterable iter = iterable;
        return iter;
    }

    public static <FROM, TO> Function<FROM, TO> cast()
    {
        return new Function<FROM, TO>()
        {
            @Override
            public TO map( FROM from )
            {
                return (TO)from;
            }
        };
    }

    public static <FROM, TO> TO fold( Function<? super FROM, TO> function, Iterable<? extends FROM> i )
    {
        return last( map( function, i ) );
    }

    public static <T, C extends T> Iterable<T> prepend( final C item, final Iterable<T> iterable )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new Iterator<T>()
                {
                    T first = item;
                    Iterator<T> iterator;

                    @Override
                    public boolean hasNext()
                    {
                        if( first != null )
                            return true;
                        else
                        {
                            if( iterator == null )
                            {
                                iterator = iterable.iterator();
                            }
                        }

                        return iterator.hasNext();
                    }

                    @Override
                    public T next()
                    {
                        if( first != null )
                        {
                            try
                            {
                                return first;
                            } finally
                            {
                                first = null;
                            }
                        } else
                            return iterator.next();
                    }

                    @Override
                    public void remove()
                    {
                    }
                };
            }
        };
    }

    public static <T, C extends T> Iterable<T> append( final C item, final Iterable<T> iterable )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                final Iterator<T> iterator = iterable.iterator();

                return new Iterator<T>()
                {
                    T last = item;

                    @Override
                    public boolean hasNext()
                    {
                        if(iterator.hasNext())
                        {
                            return true;
                        } else
                        {
                            return last != null;
                        }
                    }

                    @Override
                    public T next()
                    {
                        if (iterator.hasNext() )
                            return iterator.next();
                        else
                            try
                            {
                                return last;
                            } finally
                            {
                                last  = null;
                            }
                    }

                    @Override
                    public void remove()
                    {
                    }
                };
            }
        };
    }

    public static <T> Iterable<T> cache(Iterable<T> iterable)
    {
        return new CacheIterable<T>(iterable);
    }

    public static <T> List<T> toList( Iterable<T> iterable )
    {
        return addAll( new ArrayList<T>(), iterable );
    }

    public static Object[] toArray(Iterable<Object> iterable)
    {
        return toArray( Object.class, iterable );
    }

    public static <T> T[] toArray(Class<T> componentType, Iterable<T> iterable)
    {
        if (iterable == null)
            return null;

        List<T> list = toList( iterable );
        return list.toArray( (T[]) Array.newInstance( componentType, list.size() ) );
    }

    private static class MapIterable<FROM, TO>
            implements Iterable<TO>
    {
        private final Iterable<FROM> from;
        private final Function<? super FROM, ? extends TO> function;

        public MapIterable( Iterable<FROM> from, Function<? super FROM, ? extends TO> function )
        {
            this.from = from;
            this.function = function;
        }

        public Iterator<TO> iterator()
        {
            return new MapIterator<FROM, TO>( from.iterator(), function );
        }

        static class MapIterator<FROM, TO>
                implements Iterator<TO>
        {
            private final Iterator<FROM> fromIterator;
            private final Function<? super FROM, ? extends TO> function;

            public MapIterator( Iterator<FROM> fromIterator, Function<? super FROM, ? extends TO> function )
            {
                this.fromIterator = fromIterator;
                this.function = function;
            }

            public boolean hasNext()
            {
                return fromIterator.hasNext();
            }

            public TO next()
            {
                FROM from = fromIterator.next();

                return function.map( from );
            }

            public void remove()
            {
                fromIterator.remove();
            }
        }
    }

    private static class FilterIterable<T>
            implements Iterable<T>
    {
        private Iterable<T> iterable;

        private Specification<? super T> specification;

        public FilterIterable( Iterable<T> iterable, Specification<? super T> specification )
        {
            this.iterable = iterable;
            this.specification = specification;
        }

        public Iterator<T> iterator()
        {
            return new FilterIterator<T>( iterable.iterator(), specification );
        }

        static class FilterIterator<T>
                implements Iterator<T>
        {
            private Iterator<T> iterator;

            private Specification<? super T> specification;

            private T currentValue;
            boolean finished = false;
            boolean nextConsumed = true;

            public FilterIterator( Iterator<T> iterator, Specification<? super T> specification )
            {
                this.specification = specification;
                this.iterator = iterator;
            }

            public boolean moveToNextValid()
            {
                boolean found = false;
                while( !found && iterator.hasNext() )
                {
                    T currentValue = iterator.next();
                    boolean satisfies = specification.satisfiedBy( currentValue );

                    if( satisfies )
                    {
                        found = true;
                        this.currentValue = currentValue;
                        nextConsumed = false;
                    }
                }
                if( !found )
                {
                    finished = true;
                }
                return found;
            }

            public T next()
            {
                if( !nextConsumed )
                {
                    nextConsumed = true;
                    return currentValue;
                } else
                {
                    if( !finished )
                    {
                        if( moveToNextValid() )
                        {
                            nextConsumed = true;
                            return currentValue;
                        }
                    }
                }
                return null;
            }

            public boolean hasNext()
            {
                return !finished &&
                        (!nextConsumed || moveToNextValid());
            }

            public void remove()
            {
            }
        }
    }

    private static class FlattenIterable<T, I extends Iterable<? extends T>>
            implements Iterable<T>
    {
        private Iterable<I> iterable;

        public FlattenIterable( Iterable<I> iterable )
        {
            this.iterable = iterable;
        }

        public Iterator<T> iterator()
        {
            return new FlattenIterator<T, I>( iterable.iterator() );
        }

        static class FlattenIterator<T, I extends Iterable<? extends T>>
                implements Iterator<T>
        {
            private Iterator<I> iterator;
            private Iterator<? extends T> currentIterator;

            public FlattenIterator( Iterator<I> iterator )
            {
                this.iterator = iterator;
                currentIterator = null;
            }

            public boolean hasNext()
            {
                if( currentIterator == null )
                {
                    if( iterator.hasNext() )
                    {
                        I next = iterator.next();
                        currentIterator = next.iterator();
                    } else
                    {
                        return false;
                    }
                }

                while( !currentIterator.hasNext() &&
                        iterator.hasNext() )
                {
                    currentIterator = iterator.next().iterator();
                }

                return currentIterator.hasNext();
            }

            public T next()
            {
                return currentIterator.next();
            }

            public void remove()
            {
                if( currentIterator == null )
                {
                    throw new IllegalStateException();
                }

                currentIterator.remove();
            }
        }
    }

    private static class CacheIterable<T>
        implements Iterable<T>
    {
        private Iterable<T> iterable;
        private Iterable<T> cache;

        private CacheIterable( Iterable<T> iterable )
        {
            this.iterable = iterable;
        }

        @Override
        public Iterator<T> iterator()
        {
            if (cache != null)
                return cache.iterator();

            final Iterator<T> source = iterable.iterator();

            return new Iterator<T>()
            {
                List<T> iteratorCache = new ArrayList<T>();

                @Override
                public boolean hasNext()
                {
                    boolean hasNext = source.hasNext();
                    if (!hasNext)
                    {
                        cache = iteratorCache;
                    }
                    return hasNext;
                }

                @Override
                public T next()
                {
                    T next = source.next();
                    iteratorCache.add( next );
                    return next;
                }

                @Override
                public void remove()
                {

                }
            };
        }
    }
}
