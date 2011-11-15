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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;

public class TestRelationshipCount extends AbstractNeo4jTestCase
{
    @Test
    public void convertToSuperNode() throws Exception
    {
        Node node = getGraphDb().createNode();
        EnumMap<MyRelTypes, Set<Relationship>> rels = new EnumMap<MyRelTypes, Set<Relationship>>( MyRelTypes.class );
        for ( MyRelTypes type : MyRelTypes.values() ) rels.put( type, new HashSet<Relationship>() );
        int expectedRelCount = 0;
        for ( int i = 0; i < 6; i++, expectedRelCount++ )
        {
            MyRelTypes type = MyRelTypes.values()[i%MyRelTypes.values().length];
            Relationship rel = node.createRelationshipTo( getGraphDb().createNode(), type );
            rels.get( type ).add( rel );
        }
        newTransaction();
        for ( int i = 0; i < 10000; i++, expectedRelCount++ )
        {
            node.createRelationshipTo( getGraphDb().createNode(), MyRelTypes.TEST );
        }
        clearCache();
        assertEquals( expectedRelCount, node.getDegree() );
        assertEquals( expectedRelCount, node.getDegree( Direction.BOTH ) );
        assertEquals( expectedRelCount, node.getDegree( Direction.OUTGOING ) );
        assertEquals( 0, node.getDegree( Direction.INCOMING ) );
        assertEquals( rels.get( MyRelTypes.TEST2 ),
                asSet( node.getRelationships( MyRelTypes.TEST2 ) ) );
        assertEquals( join( rels.get( MyRelTypes.TEST_TRAVERSAL ), rels.get( MyRelTypes.TEST2 ) ),
                asSet( node.getRelationships( MyRelTypes.TEST_TRAVERSAL, MyRelTypes.TEST2 ) ) );
    }

    @Test
    public void withoutLoops() throws Exception
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        assertEquals( 0, node1.getDegree() );
        assertEquals( 0, node2.getDegree() );
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        assertEquals( 1, node1.getDegree() );
        assertEquals( 1, node2.getDegree() );
        node1.createRelationshipTo( getGraphDb().createNode(), MyRelTypes.TEST2 );
        assertEquals( 2, node1.getDegree() );
        assertEquals( 1, node2.getDegree() );
        newTransaction();
        assertEquals( 2, node1.getDegree() );
        assertEquals( 1, node2.getDegree() );

        for ( int i = 0; i < 1000; i++ ) 
        {
            if ( i%2 == 0 ) node1.createRelationshipTo( node2, MyRelTypes.TEST );
            else node2.createRelationshipTo( node1, MyRelTypes.TEST );
            assertEquals( i+2+1, node1.getDegree() );
            assertEquals( i+1+1, node2.getDegree() );
            if ( i%10 == 0 )
            {
                newTransaction();
                clearCache();
            }
        }
        
        for ( int i = 0; i < 2; i++ )
        {
            assertEquals( 1002, node1.getDegree() );
            assertEquals( 1002, node1.getDegree( Direction.BOTH ) );
            assertEquals( 502, node1.getDegree( Direction.OUTGOING ) );
            assertEquals( 500, node1.getDegree( Direction.INCOMING ) );
            assertEquals( 1, node1.getDegree( MyRelTypes.TEST2 ) );
            assertEquals( 1001, node1.getDegree( MyRelTypes.TEST ) );

            assertEquals( 1001, node1.getDegree( MyRelTypes.TEST, Direction.BOTH ) );
            assertEquals( 501, node1.getDegree( MyRelTypes.TEST, Direction.OUTGOING ) );
            assertEquals( 500, node1.getDegree( MyRelTypes.TEST, Direction.INCOMING ) );
            assertEquals( 1, node1.getDegree( MyRelTypes.TEST2, Direction.OUTGOING ) );
            assertEquals( 0, node1.getDegree( MyRelTypes.TEST2, Direction.INCOMING ) );
            newTransaction();
        }
        
        for ( Relationship rel : node1.getRelationships() ) rel.delete();
        node1.delete();
        for ( Relationship rel : node2.getRelationships() ) rel.delete();
        node2.delete();
        newTransaction();
    }
    
    @Test
    public void withLoops() throws Exception
    {
        // Just to make sure it doesn't work by accident what with ids aligning with count
        for ( int i = 0; i < 10; i++ ) getGraphDb().createNode().createRelationshipTo( getGraphDb().createNode(), MyRelTypes.TEST );
        
        Node node = getGraphDb().createNode();
        assertEquals( 0, node.getDegree() );
        Relationship rel1 = node.createRelationshipTo( node, MyRelTypes.TEST );
        assertEquals( 1, node.getDegree() );
        Node otherNode = getGraphDb().createNode();
        Relationship rel2 = node.createRelationshipTo( otherNode, MyRelTypes.TEST2 );
        assertEquals( 2, node.getDegree() );
        assertEquals( 1, otherNode.getDegree() );
        newTransaction();
        assertEquals( 2, node.getDegree() );
        Relationship rel3 = node.createRelationshipTo( node, MyRelTypes.TEST_TRAVERSAL );
        assertEquals( 3, node.getDegree() );
        assertEquals( 1, otherNode.getDegree() );
        rel2.delete();
        assertEquals( 2, node.getDegree() );
        assertEquals( 0, otherNode.getDegree() );
        rel3.delete();
        assertEquals( 1, node.getDegree() );
    }
    
    @Test
    public void ensureRightDegreeOnDiscreteNodes() throws Exception
    {
        ensureRightDegree( getGraphDb().createNode(), 0 );
    }
    
    @Test
    public void ensureRightDegreeOnSuperNodes() throws Exception
    {
        Node node = getGraphDb().createNode();
        int initialSize = 1000;
        for ( int i = 0; i < initialSize; i++ )
        {
            node.createRelationshipTo( getGraphDb().createNode(), MyRelTypes.TEST_TRAVERSAL );
        }
        newTransaction();
        ensureRightDegree( node, initialSize );
    }
    
    private void ensureRightDegree( Node me, int initialSize )
    {
        assertEquals( initialSize, me.getDegree() );
        assertEquals( 0, me.getDegree( MyRelTypes.TEST ) );
        assertEquals( initialSize, me.getDegree( Direction.OUTGOING ) );
        assertEquals( initialSize, me.getDegree( Direction.BOTH ) );
        assertEquals( 0, me.getDegree( MyRelTypes.TEST, Direction.OUTGOING ) );
        assertEquals( 0, me.getDegree( MyRelTypes.TEST, Direction.BOTH ) );
        
        /*
         * me-[:TEST]->   x5
         *  <-[:TEST]-    x2
         * me-[:TEST2]->  x6
         *  <-[:TEST2]-   x7
         * me-[:TEST2]-me x3
         *              = 23 + initialSize
         */
        for ( int i = 0; i < 5; i++ )
        {
            Node otherNode = getGraphDb().createNode();
            me.createRelationshipTo( otherNode, MyRelTypes.TEST );
            assertEquals( 1, otherNode.getDegree() );
        }
        for ( int i = 0; i < 2; i++ )
        {
            Node otherNode = getGraphDb().createNode();
            otherNode.createRelationshipTo( me, MyRelTypes.TEST );
            assertEquals( 1, otherNode.getDegree() );
        }
        for ( int i = 0; i < 6; i++ )
        {
            Node otherNode = getGraphDb().createNode();
            me.createRelationshipTo( otherNode, MyRelTypes.TEST2 );
            assertEquals( 1, otherNode.getDegree() );
        }
        for ( int i = 0; i < 3; i++ ) me.createRelationshipTo( me, MyRelTypes.TEST2 );
        for ( int i = 0; i < 7; i++ )
        {
            Node otherNode = getGraphDb().createNode();
            otherNode.createRelationshipTo( me, MyRelTypes.TEST2 );
            assertEquals( 1, otherNode.getDegree() );
        }
        
        for ( int i = 0; i < 2; i++ )
        {
            assertEquals( 23+initialSize, me.getDegree() );
            assertEquals( 14+initialSize, me.getDegree( Direction.OUTGOING ) );
            assertEquals( 12, me.getDegree( Direction.INCOMING ) );
            assertEquals( 7, me.getDegree( MyRelTypes.TEST ) );
            assertEquals( 16, me.getDegree( MyRelTypes.TEST2 ) );
            assertEquals( 7, me.getDegree( MyRelTypes.TEST, Direction.BOTH ) );
            assertEquals( 5, me.getDegree( MyRelTypes.TEST, Direction.OUTGOING ) );
            assertEquals( 2, me.getDegree( MyRelTypes.TEST, Direction.INCOMING ) );
            assertEquals( 16, me.getDegree( MyRelTypes.TEST2, Direction.BOTH ) );
            assertEquals( 9, me.getDegree( MyRelTypes.TEST2, Direction.OUTGOING ) );
            assertEquals( 10, me.getDegree( MyRelTypes.TEST2, Direction.INCOMING ) );
            newTransaction();
        }
        
        // Delete one of each type/direction combination
        deleteOneRelationship( me, MyRelTypes.TEST, Direction.OUTGOING, false );
        deleteOneRelationship( me, MyRelTypes.TEST, Direction.INCOMING, false );
        deleteOneRelationship( me, MyRelTypes.TEST2, Direction.OUTGOING, false );
        deleteOneRelationship( me, MyRelTypes.TEST2, Direction.INCOMING, false );
        deleteOneRelationship( me, MyRelTypes.TEST2, Direction.OUTGOING, true );
        
        // Count with the deleted rels
        for ( int i = 0; i < 2; i++ )
        {
            assertEquals( 23+initialSize-5, me.getDegree() );
            assertEquals( 14+initialSize-3, me.getDegree( Direction.OUTGOING ) );
            assertEquals( 12-3, me.getDegree( Direction.INCOMING ) );
            assertEquals( 7-2, me.getDegree( MyRelTypes.TEST ) );
            assertEquals( 16-3, me.getDegree( MyRelTypes.TEST2 ) );
            assertEquals( 7-2, me.getDegree( MyRelTypes.TEST, Direction.BOTH ) );
            assertEquals( 5-1, me.getDegree( MyRelTypes.TEST, Direction.OUTGOING ) );
            assertEquals( 2-1, me.getDegree( MyRelTypes.TEST, Direction.INCOMING ) );
            assertEquals( 16-3, me.getDegree( MyRelTypes.TEST2, Direction.BOTH ) );
            assertEquals( 9-2, me.getDegree( MyRelTypes.TEST2, Direction.OUTGOING ) );
            assertEquals( 10-2, me.getDegree( MyRelTypes.TEST2, Direction.INCOMING ) );
            newTransaction();
        }
        
        // Clean up
        for ( Relationship rel : me.getRelationships() )
        {
            Node otherNode = rel.getOtherNode( me );
            if ( !otherNode.equals( me ) ) otherNode.delete();
            rel.delete();
        }
        me.delete();
    }

    private void deleteOneRelationship( Node node, MyRelTypes type, Direction direction, boolean deleteALoopRelationship )
    {
        for ( Relationship rel : node.getRelationships( type, direction ) )
        {
            if ( isLoop( rel ) == deleteALoopRelationship )
            {
                rel.delete();
                return;
            }
        }
        fail( "Couldn't find " + (deleteALoopRelationship ? "loop" : "non-loop") + " relationship " +
                type.name() + " " + direction + " to delete" );
    }

    private boolean isLoop( Relationship r )
    {
        return r.getStartNode().equals( r.getEndNode() );
    }
}
