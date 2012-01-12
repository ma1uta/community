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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.util.DirectionWrapper;

public class SuperNodeChainPosition implements RelationshipLoadingPosition
{
    public static class Definition implements RelationshipLoadingPosition.Definition
    {
        private final Map<Integer, RelationshipGroupRecord> groups;

        public Definition( Map<Integer, RelationshipGroupRecord> groups )
        {
            this.groups = groups;
        }
        
        @Override
        public RelationshipLoadingPosition build( NodeManager nm )
        {
            Pair<RelationshipType[], Map<String, RelationshipGroupRecord>> translation = nm.translateRelationshipGroups( groups );
            return new SuperNodeChainPosition( translation.first(), translation.other() );
        }
    }
    
    private final Map<String, RelationshipLoadingPosition> positions = new HashMap<String, RelationshipLoadingPosition>();
    private final Map<String, RelationshipGroupRecord> groups;
    private final RelationshipType[] types;
    private RelationshipLoadingPosition currentPosition;
    
    public SuperNodeChainPosition( RelationshipType[] types, Map<String, RelationshipGroupRecord> groups )
    {
        this.types = types;
        this.groups = groups;
    }
    
    @Override
    public void updateFirst( long first )
    {
        // TODO here we need relationship groups for any new
    }
    
    @Override
    public long position( DirectionWrapper direction, RelationshipType[] types )
    {
        if ( types.length == 0 ) types = this.types;
        for ( RelationshipType type : types )
        {
            RelationshipLoadingPosition position = getTypePosition( type );
            if ( position.hasMore( direction, types ) )
            {
                currentPosition = position;
                return position.position( direction, types );
            }
        }
        return Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private RelationshipLoadingPosition getTypePosition( RelationshipType type )
    {
        RelationshipLoadingPosition position = positions.get( type.name() );
        if ( position == null )
        {
            RelationshipGroupRecord record = groups.get( type.name() );
            position = record != null ? new TypePosition( record ) : RelationshipLoadingPosition.EMPTY;
            positions.put( type.name(), position );
        }
        return position;
    }

    @Override
    public long nextPosition( long nextPosition, DirectionWrapper direction, RelationshipType[] types )
    {
        currentPosition.nextPosition( nextPosition, direction, types );
        if ( nextPosition != Record.NO_NEXT_RELATIONSHIP.intValue() ) return nextPosition;
        return position( direction, types );
    }
    
    @Override
    public boolean hasMore( DirectionWrapper direction, RelationshipType[] types )
    {
        if ( types.length == 0 ) types = this.types;
        for ( RelationshipType type : types )
        {
            RelationshipLoadingPosition position = positions.get( type.name() );
            if ( position == null || position.hasMore( direction, types ) ) return true;
        }
        return false;
    }
    
    private static class TypePosition implements RelationshipLoadingPosition
    {
        private final EnumMap<DirectionWrapper, RelationshipLoadingPosition> directions =
                new EnumMap<DirectionWrapper,RelationshipLoadingPosition>( DirectionWrapper.class );
        private RelationshipLoadingPosition currentPosition;
        
        TypePosition( RelationshipGroupRecord record )
        {
            for ( DirectionWrapper dir : DirectionWrapper.values() )
            {
                directions.put( dir, new SingleChainPosition( dir.getNextRel( record ) ) );
            }
        }
        
        @Override
        public void updateFirst( long first )
        {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public long position( DirectionWrapper direction, RelationshipType[] types )
        {
            if ( direction == DirectionWrapper.BOTH )
            {
                for ( RelationshipLoadingPosition position : directions.values() )
                {
                    if ( position.hasMore( direction, types ) )
                    {
                        currentPosition = position;
                        return position.position( direction, types );
                    }
                }
            }
            else
            {
                for ( DirectionWrapper dir : new DirectionWrapper[] {direction, DirectionWrapper.BOTH} )
                {
                    RelationshipLoadingPosition position = directions.get( dir );
                    if ( position.hasMore( dir, types ) )
                    {
                        currentPosition = position;
                        return position.position( dir, types );
                    }
                }
            }
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        }

        @Override
        public long nextPosition( long position, DirectionWrapper direction, RelationshipType[] types )
        {
            currentPosition.nextPosition( position, direction, types );
            if ( position != Record.NO_NEXT_RELATIONSHIP.intValue() ) return position;
            return position( direction, types );
        }
        
        @Override
        public boolean hasMore( DirectionWrapper direction, RelationshipType[] types )
        {
            if ( direction == DirectionWrapper.BOTH )
            {
                return directions.get( DirectionWrapper.OUTGOING ).hasMore( direction, types ) ||
                        directions.get( DirectionWrapper.INCOMING ).hasMore( direction, types ) ||
                        directions.get( DirectionWrapper.BOTH ).hasMore( direction, types );
            }
            else
            {
                return directions.get( direction ).hasMore( direction, types ) ||
                        directions.get( DirectionWrapper.BOTH ).hasMore( direction, types );
            }
        }
    }
}
