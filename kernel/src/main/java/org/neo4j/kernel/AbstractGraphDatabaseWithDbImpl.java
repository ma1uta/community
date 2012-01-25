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
package org.neo4j.kernel;

import java.util.Collection;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;

public class AbstractGraphDatabaseWithDbImpl extends AbstractGraphDatabase
{
    private EmbeddedGraphDbImpl graphDbImpl;

    protected AbstractGraphDatabaseWithDbImpl( String storeDir )
    {
        super( storeDir );
    }
    
    protected void setGraphDbImplAtConstruction( EmbeddedGraphDbImpl db )
    {
        assert this.graphDbImpl == null;
        this.graphDbImpl = db;
    }

    @Override
    public Node createNode()
    {
        return graphDbImpl.createNode();
    }

    @Override
    public Node getNodeById( long id )
    {
        return graphDbImpl.getNodeById( id );
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return graphDbImpl.getRelationshipById( id );
    }

    @Override
    public Node getReferenceNode()
    {
        return graphDbImpl.getReferenceNode();
    }

    @Override
    public Node getReferenceNode( String name )
    {
        return graphDbImpl.getReferenceNode( name );
    }
    
    @Override
    public Node getReferenceNodeIfExists( String name )
    {
        return graphDbImpl.getReferenceNodeIfExists( name );
    }
    
    @Override
    public TransactionBuilder tx()
    {
        return graphDbImpl.tx();
    }

    @Override
    public Transaction beginTx()
    {
        return tx().begin();
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return graphDbImpl.registerTransactionEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return graphDbImpl.unregisterTransactionEventHandler( handler );
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return graphDbImpl.registerKernelEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return graphDbImpl.unregisterKernelEventHandler( handler );
    }

    @Override
    public IndexManager index()
    {
        return graphDbImpl.index();
    }

    @Override
    protected void close()
    {
        graphDbImpl.shutdown();
    }

    @Override
    public Config getConfig()
    {
        return graphDbImpl.getConfig();
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return graphDbImpl.getManagementBeans( type );
    }

    @Override
    public KernelData getKernelData()
    {
        return graphDbImpl.getKernelData();
    }
}
