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

import java.io.IOException;

import java.io.File;

import org.junit.Ignore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.BatchTransaction;

@Ignore
public class MakeSuperNode
{
    private static final String PATH = "target/mydb";

    private static enum T implements RelationshipType
    {
        YEAH,
        YO,
        WII,
        BLAA;
    }
    
    public static void main( String[] args ) throws IOException
    {
        FileUtils.deleteRecursively( new File( PATH ) );
        GraphDatabaseService db = new EmbeddedGraphDatabase( PATH );
        BatchTransaction tx = BatchTransaction.beginBatchTx( db );
        tx.printProgress( true );
        
        Node node = db.createNode();
        db.getReferenceNode().createRelationshipTo( node, T.YEAH );
        
        // (me) <-- x 10
        // (me) --> x 1000000-10
        
        for ( int i = 0; i < 10000000; i++ )
        {
            if ( i % 100000 == 0 ) db.createNode().createRelationshipTo( node, T.YO );
            else if ( i == 700001 ) node.createRelationshipTo( db.createNode(), T.WII );
            else if ( i == 1234567 ) db.createNode().createRelationshipTo( node, T.BLAA );
            else node.createRelationshipTo( db.createNode(), T.YO );
            tx.increment();
        }
        
        tx.finish();
        db.shutdown();
    }
}
