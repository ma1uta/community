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

import javax.transaction.TransactionManager;

import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.ArrayMap;

public class ReferenceNodeHolder
{
    private ArrayMap<String,NameData<Long>> nameToId = new ArrayMap<String,NameData<Long>>( 5, true, true );
    private ArrayMap<Integer, String> idToName = new ArrayMap<Integer, String>( 5, true, true );
    private ArrayMap<Long, NameData<Long>> nodeIdToName = new ArrayMap<Long, NameData<Long>>( 5, true, true );

    private final TransactionManager transactionManager;
    private final PersistenceManager persistenceManager;
    private final EntityIdGenerator idGenerator;

    ReferenceNodeHolder( TransactionManager transactionManager,
        PersistenceManager persistenceManager, EntityIdGenerator idGenerator )
    {
        this.transactionManager = transactionManager;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
    }

    void addRaw( NameData<Long>... types )
    {
        for ( NameData<Long> type : types ) put( type );
    }

    private void put( NameData<Long> type )
    {
        nameToId.put( type.getName(), type );
        idToName.put( type.getId(), type.getName() );
        nodeIdToName.put( type.getPayload(), type );
    }

//    public Long add( String name, boolean create )
//    {
//        NameData<Long> id = nameToId.get( name );
//        if ( id == null )
//        {
//            if ( !create ) return null;
//            id = create( name );
//            put( id );
//            return id.getPayload();
//        }
//        return id.getPayload();
//    }
    
    public NameData<Long> get( long nodeId )
    {
        return nodeIdToName.get( nodeId );
    }
    
    public NameData<Long> get( String name )
    {
        return nameToId.get( name );
    }

//    public NameData<Long> getOrCreate( String name )
//    {
//        NameData<Long> id = nameToId.get( name );
//        if ( id != null ) return id;
//        id = create( name );
//        put( id );
//        return id;
//    }

//    private synchronized NameData<Long> create( final String name )
//    {
//        NameData<Long> result = nameToId.get( name );
//        if ( result != null ) return result;
//        
//        ExecutorService executor = Executors.newSingleThreadExecutor();
//        Future<NameData<Long>> future = executor.submit( new Callable<NameData<Long>>()
//        {
//            @Override
//            public NameData<Long> call()
//            {
//                boolean success = false;
//                try
//                {
//                    transactionManager.begin();
//                    long nodeId = idGenerator.nextId( Node.class );
//                    int nameId = (int) idGenerator.nextId( ReferenceNodeStore.class );
//                    persistenceManager.createReferenceNode( name, nameId, nodeId );
//                    transactionManager.commit();
//                    success = true;
//                    return new NameData<Long>( nameId, name, nodeId );
//                }
//                catch ( Throwable t )
//                {
//                    t.printStackTrace();
//                    throw Exceptions.launderedException( t );
//                }
//                finally
//                {
//                    if ( !success )
//                    {
//                        try
//                        {
//                            transactionManager.rollback();
//                        }
//                        catch ( Throwable tt )
//                        {
//                            tt.printStackTrace();
//                        }
//                    }
//                }
//            }
//        } );
//        try
//        {
//            result = future.get();
//            put( result );
//            return result;
//        }
//        catch ( Exception e )
//        {
//            throw Exceptions.launderedException( e );
//        }
//        finally
//        {
//            executor.shutdown();
//        }
//    }
    
    public Iterable<String> getNames()
    {
        return nameToId.keySet();
    }

    public void remove( int id )
    {
        String name = idToName.remove( id );
        if ( name != null ) nameToId.remove( name );
    }
}
