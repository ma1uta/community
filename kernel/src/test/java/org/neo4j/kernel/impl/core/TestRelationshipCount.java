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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;

public class TestRelationshipCount extends AbstractNeo4jTestCase
{
    private static enum RelType implements RelationshipType
    {
        INITIAL( false ),
        TYPE1( true ),
        TYPE2( true );
        
        boolean measure;
        
        private RelType( boolean measure )
        {
            this.measure = measure;
        }
    }
    
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
        node.createRelationshipTo( node, MyRelTypes.TEST );
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
    public void ensureRightDegree() throws Exception
    {
        for ( int initialSize : new int[] { 0, 95, 200 } )
        {
            ensureRightDegree( initialSize,
                    asList(
                    create( RelType.TYPE1, Direction.OUTGOING, 5 ),
                    create( RelType.TYPE1, Direction.INCOMING, 2 ),
                    create( RelType.TYPE2, Direction.OUTGOING, 6 ),
                    create( RelType.TYPE2, Direction.INCOMING, 7 ),
                    create( RelType.TYPE2, Direction.BOTH, 3 ) ),
                    
                    asList(
                    delete( RelType.TYPE1, Direction.OUTGOING, 0 ),
                    delete( RelType.TYPE1, Direction.INCOMING, 1 ),
                    delete( RelType.TYPE2, Direction.OUTGOING, Integer.MAX_VALUE ),
                    delete( RelType.TYPE2, Direction.INCOMING, 1 ),
                    delete( RelType.TYPE2, Direction.BOTH, Integer.MAX_VALUE ) )/*null*/ );
    
            ensureRightDegree( initialSize,
                    asList(
                    create( RelType.TYPE1, Direction.BOTH, 1 ),
                    create( RelType.TYPE1, Direction.OUTGOING, 5 ),
                    create( RelType.TYPE2, Direction.OUTGOING, 6 ),
                    create( RelType.TYPE1, Direction.INCOMING, 2 ),
                    create( RelType.TYPE2, Direction.BOTH, 3 ),
                    create( RelType.TYPE2, Direction.INCOMING, 7 ),
                    create( RelType.TYPE2, Direction.BOTH, 3 ) ), null );

            ensureRightDegree( initialSize,
                    asList(
                    create( RelType.TYPE1, Direction.BOTH, 2 ),
                    create( RelType.TYPE2, Direction.BOTH, 1 ),
                    create( RelType.TYPE1, Direction.OUTGOING, 1 ),
                    create( RelType.TYPE2, Direction.OUTGOING, 10 ),
                    create( RelType.TYPE1, Direction.INCOMING, 2 ),
                    create( RelType.TYPE2, Direction.BOTH, 2 ),
                    create( RelType.TYPE2, Direction.INCOMING, 7 ) ), null );
        }
    }
    
    private void ensureRightDegree( int initialSize, Collection<RelationshipCreationSpec> cspecs,
            Collection<RelationshipDeletionSpec> dspecs )
    {
        Map<RelType, int[]> expectedCounts = new EnumMap<RelType, int[]>( RelType.class );
        for ( RelType type : RelType.values() ) expectedCounts.put( type, new int[3] );
        Node me = getGraphDb().createNode();
        for ( int i = 0; i < initialSize; i++ )
        {
            me.createRelationshipTo( getGraphDb().createNode(), RelType.INITIAL );
        }
        newTransaction();
        expectedCounts.get( RelType.INITIAL )[0] = initialSize;
        
        assertCounts( me, expectedCounts );
        int counter = 0;
        for ( RelationshipCreationSpec spec : cspecs )
        {
            for ( int i = 0; i < spec.count; i++ )
            {
                Node otherNode = null;
                if ( spec.dir == Direction.OUTGOING ) me.createRelationshipTo( (otherNode = getGraphDb().createNode()), spec.type );
                else if ( spec.dir == Direction.INCOMING ) (otherNode = getGraphDb().createNode()).createRelationshipTo( me, spec.type );
                else me.createRelationshipTo( me, spec.type );
                expectedCounts.get( spec.type )[spec.dir.ordinal()]++;
                
                if ( otherNode != null ) assertEquals( 1, otherNode.getDegree() );
                assertCounts( me, expectedCounts );
                if ( counter%3 == 0 && counter > 0 )
                {
                    newTransaction();
                    assertCounts( me, expectedCounts );
                }
            }
        }
        
        assertCounts( me, expectedCounts );
        newTransaction();
        assertCounts( me, expectedCounts );
        
        // Delete one of each type/direction combination
        counter = 0;
        if ( dspecs == null )
        {
            for ( RelType type : RelType.values() )
            {
                if ( !type.measure ) continue;
                for ( Direction direction : Direction.values() )
                {
                    int[] counts = expectedCounts.get( type );
                    if ( counts[direction.ordinal()] > 0 )
                    {
                        deleteOneRelationship( me, type, direction, 0 );
                        counts[direction.ordinal()]--;
                        assertCounts( me, expectedCounts );
                        if ( counter%3 == 0 && counter > 0 )
                        {
                            newTransaction();
                            assertCounts( me, expectedCounts );
                        }
                    }
                }
            }
        }
        else
        {
            for ( RelationshipDeletionSpec spec : dspecs )
            {
                deleteOneRelationship( me, spec.type, spec.dir, spec.which );
                expectedCounts.get( spec.type )[spec.dir.ordinal()]--;
                assertCounts( me, expectedCounts );
                if ( counter%3 == 0 && counter > 0 )
                {
                    newTransaction();
                    assertCounts( me, expectedCounts );
                }
            }
        }
        
        assertCounts( me, expectedCounts );
        newTransaction();
        assertCounts( me, expectedCounts );
        
        // Clean up
        for ( Relationship rel : me.getRelationships() )
        {
            Node otherNode = rel.getOtherNode( me );
            if ( !otherNode.equals( me ) ) otherNode.delete();
            rel.delete();
        }
        me.delete();
    }

    private void assertCounts( Node me, Map<RelType, int[]> expectedCounts )
    {
        assertEquals( totalCount( expectedCounts, Direction.BOTH ), me.getDegree() );
        assertEquals( totalCount( expectedCounts, Direction.BOTH ), me.getDegree( Direction.BOTH ) );
        assertEquals( totalCount( expectedCounts, Direction.OUTGOING ), me.getDegree( Direction.OUTGOING ) );
        assertEquals( totalCount( expectedCounts, Direction.INCOMING ), me.getDegree( Direction.INCOMING ) );
        for ( Map.Entry<RelType, int[]> entry : expectedCounts.entrySet() )
        {
            RelType type = entry.getKey();
            assertEquals( totalCount( entry.getValue(), Direction.BOTH ), me.getDegree( type ) );
            assertEquals( totalCount( entry.getValue(), Direction.OUTGOING ), me.getDegree( type, Direction.OUTGOING ) );
            assertEquals( totalCount( entry.getValue(), Direction.INCOMING ), me.getDegree( type, Direction.INCOMING ) );
            assertEquals( totalCount( entry.getValue(), Direction.BOTH ), me.getDegree( type, Direction.BOTH ) );
        }
    }

    private int totalCount( Map<RelType, int[]> expectedCounts, Direction direction )
    {
        int result = 0;
        for ( Map.Entry<RelType, int[]> entry : expectedCounts.entrySet() )
        {
            result += totalCount( entry.getValue(), direction );
        }
        return result;
    }
    
    private int totalCount( int[] expectedCounts, Direction direction )
    {
        int result = 0;
        if ( direction == Direction.OUTGOING || direction == Direction.BOTH ) result += expectedCounts[0];
        if ( direction == Direction.INCOMING || direction == Direction.BOTH ) result += expectedCounts[1];
        result += expectedCounts[2];
        return result;
    }

    private void deleteOneRelationship( Node node, RelType type, Direction direction, int which )
    {
        Relationship last = null;
        int counter = 0;
        for ( Relationship rel : node.getRelationships( type, direction ) )
        {
            if ( isLoop( rel ) == (direction == Direction.BOTH) )
            {
                last = rel;
                if ( counter++ == which )
                {
                    rel.delete();
                    return;
                }
            }
        }
        
        if ( which == Integer.MAX_VALUE && last != null )
        {
            last.delete();
            return;
        }
        
        fail( "Couldn't find " + (direction == Direction.BOTH ? "loop" : "non-loop") + " relationship " +
                type.name() + " " + direction + " to delete" );
    }

    private boolean isLoop( Relationship r )
    {
        return r.getStartNode().equals( r.getEndNode() );
    }
    
    private static class RelationshipCreationSpec
    {
        private final RelType type;
        private final Direction dir;
        private final int count;

        RelationshipCreationSpec( RelType type, Direction dir, int count )
        {
            this.type = type;
            this.dir = dir;
            this.count = count;
        }
    }
    
    private static class RelationshipDeletionSpec
    {
        private final RelType type;
        private final Direction dir;
        private final int which;

        RelationshipDeletionSpec( RelType type, Direction dir, int which /*Integer.MAX_VALUE==last*/ )
        {
            this.type = type;
            this.dir = dir;
            this.which = which;
        }
    }
    
    static RelationshipCreationSpec create( RelType type, Direction dir, int count )
    {
        return new RelationshipCreationSpec( type, dir, count );
    }

    static RelationshipDeletionSpec delete( RelType type, Direction dir, int which )
    {
        return new RelationshipDeletionSpec( type, dir, which );
    }
}
