/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package deleteme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

public class GraphOperations
{
    private static final RelationshipType REL_TYPE_TREE = DynamicRelationshipType.withName( "TREE" );
    private static final RelationshipType REL_TYPE_EXTRA = DynamicRelationshipType.withName( "EXTRA" );
    
    private final GraphDatabaseService db;
    private final int maxDepth;
    private final Random random = new Random( (long)(System.currentTimeMillis()*Math.random()) );
    private final boolean alsoIndexing;

    public GraphOperations( GraphDatabaseService db, int maxDepth, boolean indexingAlso )
    {
        this.db = db;
        this.maxDepth = maxDepth;
        this.alsoIndexing = indexingAlso;
    }
    
    public void doBatchOfOperations( int count, Operation... chooseBetweenOpsOrNoneForRandom )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node refNode = db.getReferenceNode();
            for ( int i = 0; i < count; i++ )
            {
                Node node = findRandomNode( refNode );
                node = node != null ? node : refNode;
                Operation operation = selectRandomOperation( refNode, node, chooseBetweenOpsOrNoneForRandom );
                if ( operation != null )
                {
                    operation.perform( node, alsoIndexing );
                }
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
    
    protected Node getReferenceNode( GraphDatabaseService db )
    {
        return db.getReferenceNode();
    }

    public void doBatchOfCreationOperations( int count )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node refNode = db.getReferenceNode();
            for ( int i = 0; i < count; i++ )
            {
                Node node = findRandomNode( refNode );
                node = node != null ? node : refNode;
                selectRandomCreationOperation().perform( node, alsoIndexing );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
    
    private Operation selectRandomCreationOperation()
    {
        while ( true )
        {
            Operation op = randomOperation();
            if ( !op.isDelete() && !op.isDebug() ) return op;
        }
    }
    
    private Operation randomOperation()
    {
        while ( true )
        {
            Operation op = Operation.values()[(int) ( random.nextInt( Operation.values().length ) )];
            if ( !op.isDebug() ) return op;
        }
    }

    private Operation selectRandomOperation( Node refNode, Node node, Operation... chooseBetweenOpsOrNoneForRandom )
    {
        Operation operation = null;
        while ( operation == null )
        {
            Operation op = chooseBetweenOpsOrNoneForRandom.length > 0 ?
                    randomOperation( chooseBetweenOpsOrNoneForRandom ) : randomOperation();
            if ( op.isDelete() )
            {
                // There's only 10% chance (of the initial chance that the selected
                // operation is a delete operation of some kind) that a delete operations
                // will be performed. For the other 90% just select a new randomly.
                // This is to be certain that the db grows all the time.
                if ( random.nextFloat() > 0.1 )
                {
                    continue;
                }
            }
            if ( node.equals( refNode ) && op == Operation.deleteNode )
            {
                if ( hasOtherNodes() )
                {
                    continue;
                }
                else
                {
                    break;
                }
            }
            operation = op;
        }
        return operation;
    }
    
    private Operation randomOperation( Operation[] possibleOperations )
    {
        return possibleOperations[random.nextInt( possibleOperations.length )];
    }

    private boolean hasOtherNodes()
    {
        Node refNode = db.getReferenceNode();
        for ( Node node : db.getAllNodes() )
        {
            if ( !node.equals( refNode ) )
            {
                return true;
            }
        }
        return false;
    }

    protected Node findRandomNode( Node startNode )
    {
        return findRandomNode( startNode, false );
    }
    
    private Node findRandomNode( Node startNode, boolean onlyLeaves )
    {
        int randomMaxDepth = random.nextInt( maxDepth-2 ) + 2;  // maxDepth - random.nextInt( random.nextInt( maxDepth ) );
        Node node = startNode;
        int atLevel = 0;
        for ( int i = 0; onlyLeaves || i < randomMaxDepth; i++, atLevel++ )
        {
            List<Relationship> rels = new ArrayList<Relationship>();
            for ( Relationship rel : node.getRelationships( REL_TYPE_TREE, Direction.OUTGOING ) )
            {
                rels.add( rel );
            }
            if ( rels.isEmpty() )
            {
                break;
            }
            Relationship rel = rels.get( random.nextInt( rels.size() ) );
            node = rel.getEndNode();
        }
        return node;
    }
    
    private static Relationship findRandomRelationship( Node node )
    {
        List<Relationship> rels = new ArrayList<Relationship>();
        for ( Relationship rel : node.getRelationships( REL_TYPE_EXTRA ) )
        {
            rels.add( rel );
        }
        return rels.isEmpty() ? null : rels.get( (int)(Math.random()*rels.size()) );
    }

    private static Map<GraphDatabaseService, Index<Node>> indexMap = new HashMap<GraphDatabaseService, Index<Node>>();

    public static enum Operation
    {
        createNode
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                Node child = node.getGraphDatabase().createNode();
                Relationship rel = node.createRelationshipTo( child, REL_TYPE_TREE );
                debug( "Created node:" + child + " and rel:" + rel );
            }
        },
        storeIndex( false, true )
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                final GraphDatabaseService graphDb = node.getGraphDatabase();
                final Index<Node> index = graphDb.index().forNodes( "nodes" );
                index.add( node, "key", "value" );
                indexMap.put( graphDb, index );
            }
        },
        useIndex( false, true )
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                indexMap.get( node.getGraphDatabase() ).get( "key", "value" );
            }
        },
        createRelationship
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                Relationship aRel = findRandomRelationship( node );
                if ( aRel == null )
                {
                    return;
                }
                Node otherNode = aRel.getOtherNode( node );
                for ( Relationship rel : node.getRelationships( REL_TYPE_EXTRA ) )
                {
                    otherNode = rel.getOtherNode( node );
                    if ( Math.random() > 0.7 )
                    {
                        break;
                    }
                }
                Relationship rel = node.createRelationshipTo( otherNode, REL_TYPE_EXTRA );
                debug( "Created rel:" + rel );
            }
        },
        setNodeProperty
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                String key = randomKey();
                Object value = randomValue();
                boolean hasExisting = node.hasProperty( key );
                node.setProperty( key, value );
                if ( alsoIndexing )
                {
                    Index<Node> index = node.getGraphDatabase().index().forNodes( "index" );
                    if ( hasExisting ) index.remove( node, key );
                    index.add( node, key, value );
                }
                debug( "Set property " + key + ", " + value + " on " + node );
            }
        },
        removeNodeProperty
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                String key = randomKey();
                node.removeProperty( key );
                if ( alsoIndexing ) node.getGraphDatabase().index().forNodes( "index" ).remove( node, key );
                debug( "Removed property " + key + " from " + node );
            }
        },
        setRelationshipProperty
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                Relationship rel = findRandomRelationship( node );
                if ( rel != null )
                {
                    String key = randomKey();
                    Object value = randomValue();
                    boolean hasExisting = rel.hasProperty( key );
                    rel.setProperty( key, value );
                    if ( alsoIndexing )
                    {
                        Index<Relationship> index = node.getGraphDatabase().index().forRelationships( "index" );
                        if ( hasExisting ) index.remove( rel, key );
                        index.add( rel, key, value );
                    }
                    debug( "Set property " + key + ", " + value + " on " + rel );
                }
            }
        },
        removeRelationshipProperty
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                Relationship rel = findRandomRelationship( node );
                if ( rel != null )
                {
                    String key = randomKey();
                    rel.removeProperty( key );
                    if ( alsoIndexing )
                    {
                        node.getGraphDatabase().index().forRelationships( "index" ).remove( rel, key );
                    }
                    debug( "Removed property " + key + " from " + rel );
                }
            }
        },
        deleteNode
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                if ( node.equals( node.getGraphDatabase().getReferenceNode() ) ) return;
                for ( Relationship rel : node.getRelationships() ) rel.delete();
                node.delete();
                System.out.println( "deleted " + node );
            }
        },
        deleteRelationship
        {
            @Override
            void perform( Node node, boolean alsoIndexing )
            {
                Relationship rel = findRandomRelationship( node );
                if ( rel != null )
                {
                    rel.delete();
                    if ( alsoIndexing )
                    {
                        node.getGraphDatabase().index().forRelationships( "index" ).remove( rel );
                    }
                    debug( "Deleted " + rel );
                }
            }
        };
        
        private final boolean isDelete;
        private boolean isDebug;

        Operation()
        {
            this( false, false );
        }
        
        Operation( boolean isDelete, boolean isDebug )
        {
            this.isDelete = isDelete;
            this.isDebug = isDebug;
        }
        
        abstract void perform( Node node, boolean alsoIndexing );
        
        boolean isDelete()
        {
            return isDelete;
        }
        
        boolean isDebug()
        {
            return isDebug;
        }
    }

    private static void debug( String string )
    {
//        System.out.println( string );
    }
    
    private static Object randomValue()
    {
        int value = (int) (Math.random()*16);
        Object result = null;
        if ( value == 0 ) result = (byte) 10;
        else if ( value == 1 ) result = (short) 100;
        else if ( value == 2 ) result = (char) 'b';
        else if ( value == 3 ) result = (int) 1000;
        else if ( value == 4 ) result = (long) 10000;
        else if ( value == 5 ) result = (float) 10.5f;
        else if ( value == 6 ) result = (double) 20.232d;
        else if ( value == 7 ) result = "dfjdkfjk";
        else if ( value == 8 ) result = new byte[] { 11, 20, 30 };
        else if ( value == 9 ) result = new short[] { 1001, 2001, 3001 };
        else if ( value == 10 ) result = new char[] { 'd', 'e', 'f' };
        else if ( value == 11 ) result = new int[] { 10001, 100002, 100003 };
        else if ( value == 12 ) result = new long[] { 20001, 20002, 30003 };
        else if ( value == 13 ) result = new float[] { 123.3f, 345.23f, 232.55f };
        else if ( value == 14 ) result = new double[] { 544.2, 384, 9384.45 };
        else if ( value == 15 ) result = new String[] { "string1", "string 2", "another completely different string" };
        return result;
    }

    private static String randomKey()
    {
        return "" + (int)(Math.random()*100);
    }
}
