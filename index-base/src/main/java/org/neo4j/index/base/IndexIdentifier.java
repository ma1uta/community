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

public class IndexIdentifier
{
    private final EntityType entityType;
    private final String indexName;
    
    public IndexIdentifier( EntityType entityType, String indexName )
    {
        this.entityType = entityType;
        this.indexName = indexName;
    }
    
    public EntityType getEntityType()
    {
        return entityType;
    }
    
    public String getIndexName()
    {
        return indexName;
    }
    
    @Override
    public boolean equals( Object o )
    {
        if ( o == null || !( o instanceof IndexIdentifier) ) return false;
        IndexIdentifier i = (IndexIdentifier) o;
        return entityType.equals( i.entityType ) && indexName.equals( i.indexName );
    }
    
    @Override
    public int hashCode()
    {
        int code = 17;
        code += 7*entityType.hashCode();
        code += 12*indexName.hashCode();
        return code;
    }
    
    @Override
    public String toString()
    {
        return "IndexIdentifier[" + entityType + ", " + indexName + "]";
    }
}
