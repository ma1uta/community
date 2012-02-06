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
package org.neo4j.index.impl.lucene;

import static org.neo4j.helpers.collection.IteratorUtil.singleOrNull;

import java.util.Collection;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.neo4j.index.base.EntityId;
import org.neo4j.index.base.TxData;
import org.neo4j.index.lucene.QueryContext;

abstract class LuceneTxData implements TxData
{
    final LuceneIndex index;
    
    LuceneTxData( LuceneIndex index )
    {
        this.index = index;
    }

    abstract Collection<EntityId> query( Query query, QueryContext contextOrNull );

    abstract IndexSearcher asSearcher( QueryContext context );

    @Override
    public EntityId getSingle( String key, Object value )
    {
        return singleOrNull( get( key, value ) );
    }
}
