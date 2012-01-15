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
package org.neo4j.kernel.impl.nioneo.store;

public class RelationshipRecord extends PrimitiveRecord
{
    private final long startNode;
    private final long endNode;
    private final int type;
    private long startNodePrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
    private long startNodeNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    private long endNodePrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
    private long endNodeNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    private boolean firstInStartNodeChain;
    private boolean firstInEndNodeChain;

    public RelationshipRecord( long id, long startNode, long endNode, int type )
    {
        // TODO take firstProp in here
        super( id, Record.NO_NEXT_PROPERTY.intValue() );
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;
    }

    public long getStartNode()
    {
        return startNode;
    }

    public long getEndNode()
    {
        return endNode;
    }

    public int getType()
    {
        return type;
    }
    
    public boolean isFirstInStartNodeChain()
    {
        return firstInStartNodeChain;
    }
    
    public void setFirstInStartNodeChain( boolean first )
    {
        this.firstInStartNodeChain = first;
    }
    
    public boolean isFirstInEndNodeChain()
    {
        return firstInEndNodeChain;
    }
    
    public void setFirstInEndNodeChain( boolean first )
    {
        this.firstInEndNodeChain = first;
    }

    public long getStartNodePrevRel()
    {
        return startNodePrevRel;
    }

    public void setStartNodePrevRel( long rel )
    {
        this.startNodePrevRel = rel;
    }

    public long getStartNodeNextRel()
    {
        return startNodeNextRel;
    }

    public void setStartNodeNextRel( long rel )
    {
        this.startNodeNextRel = rel;
    }

    public long getEndNodePrevRel()
    {
        return endNodePrevRel;
    }

    public void setEndNodePrevRel( long rel )
    {
        this.endNodePrevRel = rel;
    }

    public long getEndNodeNextRel()
    {
        return endNodeNextRel;
    }

    public void setEndNodeNextRel( long rel )
    {
        this.endNodeNextRel = rel;
    }

    @Override
    public String toString()
    {
        return new StringBuilder( "Relationship[" ).append( getId() ).append( ",used=" ).append( inUse() ).append(
                ",start=" ).append( startNode ).append( ",end=" ).append( endNode ).append( ",type=" ).append(
                type ).append( ",sPrev=" ).append( startNodePrevRel ).append( ",sNext=" ).append( startNodeNextRel ).append(
                ",ePrev=" ).append( endNodePrevRel ).append( ",eNext=" ).append( endNodeNextRel ).append( ",prop=" ).append(
                getFirstProp() ).append( (firstInStartNodeChain?",sFirst":"") ).append( (firstInEndNodeChain?",eFirst":"") ).append( "]" ).toString();
    }
}