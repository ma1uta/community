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
package org.neo4j.index.base.keyvalue;

import java.util.Map;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.index.base.AbstractIndex;
import org.neo4j.index.base.EntityType;
import org.neo4j.index.base.IndexCommand;
import org.neo4j.index.base.IndexDataSource;
import org.neo4j.index.base.IndexIdentifier;
import org.neo4j.index.base.IndexTransaction;
import org.neo4j.index.base.TxData;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

public abstract class KeyValueTransaction extends IndexTransaction
{
    public KeyValueTransaction( int identifier, XaLogicalLog xaLog, IndexDataSource dataSource )
    {
        super( identifier, xaLog, dataSource );
    }

    @Override
    public <T extends PropertyContainer> void add( AbstractIndex<T> index, T entity, String key,
            Object value )
    {
        TxDataBoth data = getTxData( index, true );
        insert( index, entity, key, value, data.added( true ), data.removed( false ) );

        IndexCommand command = getDefinitions( true ).add( index.getName(), index.getEntityTypeEnum(),
                    getEntityId( entity ), key, valueAsString( value ) );
        queueCommand( index.getIdentifier(), command );
    }

    @Override
    public <T extends PropertyContainer> void remove( AbstractIndex<T> index, T entity,
            String key, Object value )
    {
        TxDataBoth data = getTxData( index, true );
        insert( index, entity, key, value, data.removed( true ), data.added( false ) );
        IndexCommand command = getDefinitions( true ).remove( index.getName(),
                index.getEntityTypeEnum(), getEntityId( entity ), key, valueAsString( value ) );
        queueCommand( index.getIdentifier(), command );
    }

    private String valueAsString( Object value )
    {
        return value != null ? value.toString() : null;
    }

    @Override
    public void createIndex( EntityType entityType, String indexName,
            Map<String, String> config )
    {
        queueCommand( new IndexIdentifier( entityType, indexName ),
                getDefinitions( true ).create( indexName, entityType, config ) );
    }
    
    @Override
    protected <T extends PropertyContainer> void deleteIndex( AbstractIndex<T> index )
    {
        super.deleteIndex( index );
        queueCommand( index.getIdentifier(),
                getDefinitions( true ).delete( index.getName(), index.getEntityTypeEnum() ) );
    }

    @Override
    protected TxData newTxData( AbstractIndex index, TxDataType txDataType )
    {
        return new KeyValueTxData();
    }
}
