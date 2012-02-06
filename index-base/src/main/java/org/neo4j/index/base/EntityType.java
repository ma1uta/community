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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

public enum EntityType
{
    NODE( (byte)0, Node.class ),
    RELATIONSHIP( (byte)1, Relationship.class );
    
    private final byte id;
    private final Class<?> cls;

    private EntityType( byte id, Class<?> cls )
    {
        this.id = id;
        this.cls = cls;
    }
    
    public byte byteValue()
    {
        return this.id;
    }
    
    public Class<? extends PropertyContainer> getType()
    {
        return cls.asSubclass( PropertyContainer.class );
    }
    
    public static EntityType entityType( byte id )
    {
        return EntityType.values()[id];
    }

    public static EntityType entityType( Class<? extends PropertyContainer> type )
    {
        if ( type.equals( Node.class ) ) return NODE;
        if ( type.equals( Relationship.class ) ) return RELATIONSHIP;
        throw new IllegalArgumentException( type.getName() );
    }
}
