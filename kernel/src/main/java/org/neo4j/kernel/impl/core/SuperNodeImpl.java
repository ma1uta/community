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
package org.neo4j.kernel.impl.core;

import static org.neo4j.kernel.impl.util.DirectionWrapper.wrapDirection;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArray;

public class SuperNodeImpl extends NodeImpl
{
    SuperNodeImpl( long id, long firstRel, long firstProp, boolean newNode )
    {
        super( id, firstRel, firstProp, newNode );
    }

    SuperNodeImpl( long id, long firstRel, long firstProp )
    {
        super( id, firstRel, firstProp );
    }

    protected Pair<ArrayMap<String,RelIdArray>,Map<Long,RelationshipImpl>> getInitialRelationships(
            NodeManager nodeManager, ArrayMap<String,RelIdArray> tmpRelMap )
    {
        return Pair.<ArrayMap<String,RelIdArray>,Map<Long,RelationshipImpl>>of( new ArrayMap<String, RelIdArray>(), new HashMap<Long, RelationshipImpl>() );
    }
    
    @Override
    public int getDegree( NodeManager nm, RelationshipType type )
    {
        return hasMoreRelationshipsToLoad() ?
                nm.getRelationshipCount( this, type, DirectionWrapper.BOTH ) :
                super.getDegree( nm, type );
    }
    
    @Override
    public int getDegree( NodeManager nm, Direction direction )
    {
        return hasMoreRelationshipsToLoad() ?
                nm.getRelationshipCount( this, null, wrapDirection( direction ) ) :
                super.getDegree( nm, direction );
    }
    
    @Override
    public int getDegree( NodeManager nm, RelationshipType type, Direction direction )
    {
        return hasMoreRelationshipsToLoad() ?
                nm.getRelationshipCount( this, type, wrapDirection( direction ) ) :
                super.getDegree( nm, type, direction );
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes( NodeManager nm )
    {
        return hasMoreRelationshipsToLoad() ? nm.getRelationshipTypes( this ) : super.getRelationshipTypes( nm );
    }
}
