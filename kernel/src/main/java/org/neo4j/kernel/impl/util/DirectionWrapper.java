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
package org.neo4j.kernel.impl.util;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.util.RelIdArray.IdBlock;
import org.neo4j.kernel.impl.util.RelIdArray.RelIdIteratorImpl;

public enum DirectionWrapper
{
    OUTGOING( Direction.OUTGOING )
    {
        @Override
        public RelIdIterator iterator( RelIdArray ids )
        {
            return new RelIdIteratorImpl( ids, RelIdArray.DIRECTIONS_FOR_OUTGOING );
        }

        @Override
        public IdBlock getLastBlock( RelIdArray ids )
        {
            return ids.lastOutBlock;
        }

        @Override
        public void setLastBlock( RelIdArray ids, IdBlock block )
        {
            ids.lastOutBlock = block;
        }
        
        @Override
        public long getNextRel( RelationshipGroupRecord group )
        {
            return group.getNextOut();
        }
        
        @Override
        public void setNextRel( RelationshipGroupRecord group, long id )
        {
            group.setNextOut( id );
        }
    },
    INCOMING( Direction.INCOMING )
    {
        @Override
        public RelIdIterator iterator( RelIdArray ids )
        {
            return new RelIdIteratorImpl( ids, RelIdArray.DIRECTIONS_FOR_INCOMING );
        }

        @Override
        public IdBlock getLastBlock( RelIdArray ids )
        {
            return ids.lastInBlock;
        }

        @Override
        public void setLastBlock( RelIdArray ids, IdBlock block )
        {
            ids.lastInBlock = block;
        }
        
        @Override
        public long getNextRel( RelationshipGroupRecord group )
        {
            return group.getNextIn();
        }
        
        @Override
        public void setNextRel( RelationshipGroupRecord group, long id )
        {
            group.setNextIn( id );
        }
    },
    BOTH( Direction.BOTH )
    {
        @Override
        public RelIdIterator iterator( RelIdArray ids )
        {
            return new RelIdIteratorImpl( ids, RelIdArray.DIRECTIONS_FOR_BOTH );
        }

        @Override
        public IdBlock getLastBlock( RelIdArray ids )
        {
            return ids.getLastLoopBlock();
        }

        @Override
        public void setLastBlock( RelIdArray ids, IdBlock block )
        {
            ids.setLastLoopBlock( block );
        }
        
        @Override
        public long getNextRel( RelationshipGroupRecord group )
        {
            return group.getNextLoop();
        }
        
        @Override
        public void setNextRel( RelationshipGroupRecord group, long id )
        {
            group.setNextLoop( id );
        }
    };
    
    private final Direction direction;

    private DirectionWrapper( Direction direction )
    {
        this.direction = direction;
    }
    
    public abstract RelIdIterator iterator( RelIdArray ids );
    
    /*
     * Only used during add
     */
    public abstract IdBlock getLastBlock( RelIdArray ids );
    
    /*
     * Only used during add
     */
    public abstract void setLastBlock( RelIdArray ids, IdBlock block );
    
    public abstract long getNextRel( RelationshipGroupRecord group );
    
    public abstract void setNextRel( RelationshipGroupRecord group, long id );
    
    public Direction direction()
    {
        return this.direction;
    }
    
    public static DirectionWrapper wrapDirection( Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING: return DirectionWrapper.OUTGOING;
        case INCOMING: return DirectionWrapper.INCOMING;
        case BOTH: return DirectionWrapper.BOTH;
        default: throw new IllegalArgumentException( "" + direction );
        }
    }
}