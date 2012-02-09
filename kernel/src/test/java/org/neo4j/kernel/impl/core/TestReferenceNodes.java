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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.BatchTransaction;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TargetDirectory;

public class TestReferenceNodes
{
    private static enum Types implements RelationshipType
    {
        PRODUCT,
        BOUGHT;
    }
    
    private GraphDatabaseService db;
    
    protected GraphDatabaseService newDb()
    {
        return newDb( new ImpermanentGraphDatabase() );
    }
    
    protected GraphDatabaseService newDb( GraphDatabaseService db )
    {
        this.db = db;
        return db;
    }
    
    @After
    public void doAfter()
    {
        if ( db != null ) db.shutdown();
    }
    
    @Test
    public void createReferenceNodeOutsideTx()
    {
        GraphDatabaseService db = newDb();
        String name = "something different";
        assertNull( db.getReferenceNodeIfExists( name ) ); 
        Node node = db.getReferenceNode( name );
        assertEquals( node, db.getReferenceNode( name ) );
    }
    
    @Test
    public void multipleReferenceNodes() throws Exception
    {
        String path = TargetDirectory.forTest( getClass() ).directory( "refs", true ).getAbsolutePath();
        GraphDatabaseService db = newDb( new EmbeddedGraphDatabase( path ) );
        
        Transaction tx = db.beginTx();
        assertNull( db.getReferenceNodeIfExists( "users" ) );
        Node users = db.getReferenceNode( "users" );
        assertNull( db.getReferenceNodeIfExists( "products" ) );
        Node products = db.getReferenceNode( "products" );
        assertEquals( users, db.getReferenceNode( "users" ) );
        assertEquals( products, db.getReferenceNode( "products" ) );
        assertFalse( users.equals( products ) );
        tx.success();
        tx.finish();
        
        tx = db.beginTx();
        Node product = db.createNode();
        products.createRelationshipTo( product, Types.PRODUCT );
        Node user = db.createNode();
        user.createRelationshipTo( product, Types.BOUGHT );
        tx.success();
        tx.finish();
        db.shutdown();
        
        db = newDb( new EmbeddedGraphDatabase( path ) );
        assertEquals( users.getId(), db.getReferenceNode( "users" ).getId() );
        assertEquals( products.getId(), db.getReferenceNode( "products" ).getId() );
        assertEquals( product.getId(), db.getReferenceNode( "products" ).getSingleRelationship( Types.PRODUCT, Direction.OUTGOING ).getEndNode().getId() );
    }
    
    @Test
    public void deleteThenGetRefNodeShouldReturnSameInThatTx()
    {
        GraphDatabaseService db = newDb();
        Transaction tx = db.beginTx();
        String name = "yeah";
        Node refNode = db.getReferenceNode( name );
        tx.success(); tx.finish();
        
        tx = db.beginTx();
        assertEquals( refNode, db.getReferenceNodeIfExists( name ) );
        refNode.delete();
        assertEquals( refNode, db.getReferenceNodeIfExists( name ) );
        assertEquals( refNode, db.getReferenceNode( name ) );
        tx.success();
        tx.finish();
        
        assertNull( db.getReferenceNodeIfExists( name ) );
        Node newRefNode = db.getReferenceNode( name );
        assertFalse( newRefNode.equals( refNode ) );
    }
    
    @Test
    public void createMany()
    {
        GraphDatabaseService db = newDb();
        BatchTransaction tx = BatchTransaction.beginBatchTx( db, 5 );
        Node[] nodes = new Node[100];
        for ( int i = 0; i < nodes.length; i++ )
        {
            nodes[i] = db.getReferenceNode( "ref node " + i );
            tx.increment();
        }
        tx.finish();
        for ( int i = 0; i < nodes.length; i++ ) assertEquals( nodes[i], db.getReferenceNode( "ref node " + i ) );
    }
    
    @Test
    public void multipleThreadsCreatingSameRefNode() throws Exception
    {
        newDb();
        int numThreads = Runtime.getRuntime().availableProcessors();
        for ( int i = 0; i < 100; i++ )
        {
            List<ReferenceNodeGetter> threads = new ArrayList<ReferenceNodeGetter>();
            CountDownLatch readyLatch = new CountDownLatch( numThreads );
            CountDownLatch runLatch = new CountDownLatch( 1 );
            CountDownLatch doneLatch = new CountDownLatch( numThreads );
            for ( int j = 0; j < numThreads; j++ ) threads.add( new ReferenceNodeGetter( readyLatch, runLatch, doneLatch, "nr" + i ) );
            readyLatch.await();
            runLatch.countDown();
            doneLatch.await();
            
            // Assert that they all got the same reference node for that name
            Node comparisonRefNode = null;
            for ( ReferenceNodeGetter referenceNodeGetter : threads )
            {
                assertNotNull( referenceNodeGetter.refNode );
                if ( comparisonRefNode == null ) comparisonRefNode = referenceNodeGetter.refNode;
                else assertEquals( comparisonRefNode, referenceNodeGetter.refNode );
            }
        }
    }
    
    private class ReferenceNodeGetter extends Thread
    {
        private final CountDownLatch readyLatch;
        private final CountDownLatch runLatch;
        private final CountDownLatch doneLatch;
        private final String refName;
        private volatile Node refNode;

        ReferenceNodeGetter( CountDownLatch readyLatch, CountDownLatch runLatch, CountDownLatch doneLatch, String refName )
        {
            this.readyLatch = readyLatch;
            this.runLatch = runLatch;
            this.doneLatch = doneLatch;
            this.refName = refName;
            start();
        }
        
        @Override
        public void run()
        {
            try
            {
                readyLatch.countDown();
                try
                {
                    runLatch.await();
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
                refNode = db.getReferenceNode( refName );
            }
            finally
            {
                doneLatch.countDown();
            }
        }
    }
}
