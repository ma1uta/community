/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.base.keyvalue;

import java.util.Collection;
import java.util.Collections;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.base.AbstractIndex;
import org.neo4j.index.base.AbstractIndexImplementation;
import org.neo4j.index.base.EntityId;
import org.neo4j.index.base.IndexBaseXaConnection;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.IndexTransaction;
import org.neo4j.index.base.NoIndexHits;
import org.neo4j.index.base.SingleIndexHit;

public abstract class OneToOneIndex<T extends PropertyContainer> extends AbstractIndex<T>
{
    protected OneToOneIndex( AbstractIndexImplementation provider, IndexIdentifier identifier )
    {
        super( provider, identifier );
    }

    @Override
    public IndexHits<T> get( String key, Object value )
    {
        IndexBaseXaConnection connection = getReadOnlyConnection();
        IndexTransaction tx = connection != null ? connection.getTx() : null;
        Collection<EntityId> removed = tx != null ? tx.getRemovedIds( this, key, value ) :
                Collections.<EntityId>emptyList();
        EntityId id = tx != null ? tx.getSingleAddedId( this, key, value ) : null;
        if ( id == null )
        {
            getProvider().dataSource().getReadLock();
            try
            {
                EntityId idFromDb = getFromDb( key, value );
                if ( removed == null || !removed.contains( idFromDb ) )
                {
                    id = idFromDb;
                }
            }
            finally
            {
                getProvider().dataSource().releaseReadLock();
            }
        }
        
        T entity = null;
        if ( id != null )
        {
            try
            {
                entity = idToEntity( id );
            }
            catch ( NotFoundException e )
            {
            }
        }

        return entity != null ? new SingleIndexHit<T>( entity ) : NoIndexHits.<T>instance();
    }
    
    public void remove(T entity)
    {
        throw new UnsupportedOperationException();
    }
    
    public void remove(T entity, String key)
    {
        throw new UnsupportedOperationException();
    }

    protected abstract EntityId getFromDb( String key, Object value );

    @Override
    public IndexHits<T> query( String key, Object queryOrQueryObject )
    {
        throw new UnsupportedOperationException( "Unsupported for one-to-one index" );
    }

    @Override
    public IndexHits<T> query( Object queryOrQueryObject )
    {
        throw new UnsupportedOperationException( "Unsupported for one-to-one index" );
    }
}
