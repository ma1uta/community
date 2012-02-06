/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.base;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.index.IndexHits;

public class SingleIndexHit<T> implements IndexHits<T>
{
    private T item;

    public SingleIndexHit( T item )
    {
        assert item != null;
        this.item = item;
    }

    @Override
    public boolean hasNext()
    {
        return item != null;
    }

    @Override
    public T next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        try
        {
            return item;
        }
        finally
        {
            item = null;
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator()
    {
        return this;
    }

    @Override
    public int size()
    {
        return 1;
    }

    @Override
    public void close()
    {
    }

    @Override
    public T getSingle()
    {
        return item;
    }

    @Override
    public float currentScore()
    {
        return Float.NaN;
    }
}
