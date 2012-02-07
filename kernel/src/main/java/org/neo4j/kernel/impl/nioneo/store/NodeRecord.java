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
package org.neo4j.kernel.impl.nioneo.store;

/**
 * A record in the {@link NodeStore} with accessors for the different parts of record.
 */
public class NodeRecord extends PrimitiveRecord
{
    private long committedFirstRel;
    private long firstRel;
    private boolean superNode;
    private boolean committedSuperNode;
    
    /**
     * @param id record id.
     * @param superNode if this record is a super node or not.
     * @param firstRel the first relationship record in the chain for this node.
     * If this is a super node nextRel refers to the first relationship group id. 
     * @param firstProp the first property record in the property chain.
     */
    public NodeRecord( long id, boolean superNode, long firstRel, long firstProp )
    {
        super( id, firstProp );
        this.committedSuperNode = this.superNode = superNode;
        this.committedFirstRel = this.firstRel = firstRel;
    }

    /**
     * @return the first relationship record for this node.
     */
    public long getFirstRel()
    {
        return firstRel;
    }

    /**
     * Sets the first relationship record for this node.
     * @param firstRel the first relationship in this chain.
     */
    public void setFirstRel( long firstRel )
    {
        this.firstRel = firstRel;
    }
    
    /**
     * @return the first relationship for this node as seen in the committed store file,
     * so even if {@link #setFirstRel(long)} is called the value returned here isn't affected.
     * If this node record is marked as {@link #isCreated() created} it will return -1.
     */
    public long getCommittedFirstRel()
    {
        return isCreated() ? Record.NO_NEXT_RELATIONSHIP.intValue() : committedFirstRel;
    }

    /**
     * @return whether or not this node is a super node.
     */
    public boolean isSuperNode()
    {
        return superNode;
    }
    
    /**
     * Sets whether or not this node is a super node.
     * @param superNode whether ot not this node is a super node.
     */
    public void setSuperNode( boolean superNode )
    {
        this.superNode = superNode;
    }
    
    /**
     * @return whether or not this node is a super node, as seen in the committed store file.
     * so even if {@link #setSuperNode(boolean)} is called the value returned here isn't affected.
     * If this node record is marked as {@link #isCreated() created} it will return false.
     */
    public boolean isCommittedSuperNode()
    {
        return isCreated() ? false : committedSuperNode;
    }
    
    @Override
    public String toString()
    {
        return new StringBuilder( "Node[" ).append( getId() ).append( ",used=" ).append( inUse() ).append( ",rel=" ).append(
                firstRel ).append( ",prop=" ).append( getFirstProp() ).append( superNode?",superNode":"" ).append( "]" ).toString();
    }

    @Override
    void setIdTo( PropertyRecord property )
    {
        property.setNodeId( getId() );
    }
}
