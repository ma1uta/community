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
package deleteme;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.test.BatchTransaction;

public class CreateSuperNode
{
    private static enum Types implements RelationshipType
    {
        SUPER_DUPER,
        CRUFT,
        TYPE_ONE,
        TYPE_TWO;
    }
    
    public static void main( String[] args ) throws Exception
    {
        File path = new File( "super-db" );
        FileUtils.deleteRecursively( path );
        GraphDatabaseService db = new EmbeddedGraphDatabase( path.getAbsolutePath() );
        try
        {
            createSuperNode( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private static Node createSuperNode( GraphDatabaseService db )
    {
        BatchTransaction tx = BatchTransaction.beginBatchTx( db, 100000 );
        tx.printProgress( true );
        Node superNode = db.createNode();
        db.getReferenceNode().createRelationshipTo( superNode, Types.SUPER_DUPER );
        for ( int i = 0; i < 5000000; i++ )
        {
            Node otherNode = db.createNode();
            if ( i%500000 == 0 )
            {
                if ( i%1000000 == 0 ) superNode.createRelationshipTo( otherNode, Types.TYPE_ONE );
                else otherNode.createRelationshipTo( superNode, Types.TYPE_ONE );
            }
            else if ( i%750000 == 0 )
            {
                if ( i%1500000 == 0 ) superNode.createRelationshipTo( otherNode, Types.TYPE_TWO );
                else superNode.createRelationshipTo( otherNode, Types.TYPE_TWO );
            }
            else
            {
                superNode.createRelationshipTo( otherNode, Types.CRUFT );
            }
            tx.increment();
        }
        tx.finish();
        return superNode;
    }
}
