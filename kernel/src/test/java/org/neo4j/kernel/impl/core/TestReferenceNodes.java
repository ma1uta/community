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
import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TargetDirectory;

public class TestReferenceNodes
{
    private static enum Types implements RelationshipType
    {
        PRODUCT,
        BOUGHT;
    }
    
    @Test
    public void multipleReferenceNodes() throws Exception
    {
        String path = TargetDirectory.forTest( getClass() ).directory( "refs", true ).getAbsolutePath();
        GraphDatabaseService db = new EmbeddedGraphDatabase( path );
        
        Node users = db.getReferenceNode( "users" );
        Node products = db.getReferenceNode( "products" );
        assertEquals( users, db.getReferenceNode( "users" ) );
        assertEquals( products, db.getReferenceNode( "products" ) );
        assertFalse( users.equals( products ) );
        
        Transaction tx = db.beginTx();
        Node product = db.createNode();
        products.createRelationshipTo( product, Types.PRODUCT );
        Node user = db.createNode();
        user.createRelationshipTo( product, Types.BOUGHT );
        tx.success();
        tx.finish();
        db.shutdown();
        
        db = new EmbeddedGraphDatabase( path );
        assertEquals( users.getId(), db.getReferenceNode( "users" ).getId() );
        assertEquals( products.getId(), db.getReferenceNode( "products" ).getId() );
        assertEquals( product.getId(), db.getReferenceNode( "products" ).getSingleRelationship( Types.PRODUCT, Direction.OUTGOING ).getEndNode().getId() );
        
        tx = db.beginTx();
        users = db.getReferenceNode( "users" );
        users.delete();
        tx.success();
        tx.finish();
        assertFalse( users.getId() == db.getReferenceNode( "users" ).getId() );
        db.shutdown();
    }
    
    @Test
    public void deleteIssue()
    {
        GraphDatabaseService db = new ImpermanentGraphDatabase();
        Node refNode = db.getReferenceNode( "yeah" );
        Transaction tx = db.beginTx();
        refNode.delete();
        Node newRefNode = db.getReferenceNode( "yeah" );
        assertFalse( refNode.equals( newRefNode ) );
        tx.success();
        tx.finish();
        db.shutdown();
    }
}
