/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArray;

public class SuperNodeImpl extends NodeImpl
{
    SuperNodeImpl( long id, long firstRel, long firstProp, boolean newNode )
    {
        super( id, firstRel, firstProp, newNode );
        System.out.println( "super:" + id );
    }

    SuperNodeImpl( long id, long firstRel, long firstProp )
    {
        super( id, firstRel, firstProp );
        System.out.println( "super:" + id );
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
    
    @Override
    protected RelationshipLoadingPosition initializeRelChainPosition( NodeManager nm,
            RelationshipLoadingPosition cachedPosition )
    {
        long firstRel = cachedPosition.position( DirectionWrapper.BOTH, NO_RELATIONSHIP_TYPES );
        if ( firstRel == Record.NO_NEXT_RELATIONSHIP.intValue() ) return cachedPosition;
        Pair<RelationshipType[], Map<String, RelationshipGroupRecord>> groups = nm.loadRelationshipGroups( getId(), firstRel );
        return new SuperNodeChainPosition( groups.first(), groups.other() );
    }
}
