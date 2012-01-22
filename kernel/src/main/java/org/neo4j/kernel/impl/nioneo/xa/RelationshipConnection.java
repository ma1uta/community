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
package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

enum RelationshipConnection
{
    START_PREV
    {
        @Override
        long get( RelationshipRecord rel )
        {
            return rel.isFirstInStartNodeChain() ? Record.NO_NEXT_RELATIONSHIP.intValue() : rel.getStartNodePrevRel();
        }

        @Override
        void set( RelationshipRecord rel, long id, boolean isFirst )
        {
            rel.setStartNodePrevRel( id );
            rel.setFirstInStartNodeChain( isFirst );
        }

        @Override
        RelationshipConnection otherSide()
        {
            return START_NEXT;
        }

        @Override
        long compareNode( RelationshipRecord rel )
        {
            return rel.getStartNode();
        }

        @Override
        RelationshipConnection start()
        {
            return this;
        }

        @Override
        RelationshipConnection end()
        {
            return END_PREV;
        }

        @Override
        boolean isFirstInChain( RelationshipRecord rel )
        {
            return rel.isFirstInStartNodeChain();
        }
    },
    START_NEXT
    {
        @Override
        long get( RelationshipRecord rel )
        {
            return rel.getStartNodeNextRel();
        }

        @Override
        void set( RelationshipRecord rel, long id, boolean isFirst )
        {
            rel.setStartNodeNextRel( id );
        }

        @Override
        RelationshipConnection otherSide()
        {
            return START_PREV;
        }

        @Override
        long compareNode( RelationshipRecord rel )
        {
            return rel.getStartNode();
        }

        @Override
        RelationshipConnection start()
        {
            return this;
        }

        @Override
        RelationshipConnection end()
        {
            return END_NEXT;
        }

        @Override
        boolean isFirstInChain( RelationshipRecord rel )
        {
            return rel.isFirstInStartNodeChain();
        }
    },
    END_PREV
    {
        @Override
        long get( RelationshipRecord rel )
        {
            return rel.isFirstInEndNodeChain() ? Record.NO_NEXT_RELATIONSHIP.intValue() : rel.getEndNodePrevRel();
        }

        @Override
        void set( RelationshipRecord rel, long id, boolean isFirst )
        {
            rel.setEndNodePrevRel( id );
            rel.setFirstInEndNodeChain( isFirst );
        }

        @Override
        RelationshipConnection otherSide()
        {
            return END_NEXT;
        }

        @Override
        long compareNode( RelationshipRecord rel )
        {
            return rel.getEndNode();
        }

        @Override
        RelationshipConnection start()
        {
            return START_PREV;
        }

        @Override
        RelationshipConnection end()
        {
            return this;
        }

        @Override
        boolean isFirstInChain( RelationshipRecord rel )
        {
            return rel.isFirstInEndNodeChain();
        }
    },
    END_NEXT
    {
        @Override
        long get( RelationshipRecord rel )
        {
            return rel.getEndNodeNextRel();
        }

        @Override
        void set( RelationshipRecord rel, long id, boolean isFirst )
        {
            rel.setEndNodeNextRel( id );
        }

        @Override
        RelationshipConnection otherSide()
        {
            return END_PREV;
        }

        @Override
        long compareNode( RelationshipRecord rel )
        {
            return rel.getEndNode();
        }

        @Override
        RelationshipConnection start()
        {
            return START_NEXT;
        }

        @Override
        RelationshipConnection end()
        {
            return this;
        }

        @Override
        boolean isFirstInChain( RelationshipRecord rel )
        {
            return rel.isFirstInEndNodeChain();
        }
    };
    
    abstract long get( RelationshipRecord rel );
    
    abstract boolean isFirstInChain( RelationshipRecord rel );

    abstract void set( RelationshipRecord rel, long id, boolean isFirt );
    
    abstract long compareNode( RelationshipRecord rel );
    
    abstract RelationshipConnection otherSide();
    
    abstract RelationshipConnection start();
    
    abstract RelationshipConnection end();
}