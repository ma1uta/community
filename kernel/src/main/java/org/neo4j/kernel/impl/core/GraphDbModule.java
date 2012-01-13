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

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.cache.AdaptiveCacheManager;
import org.neo4j.kernel.impl.core.NodeManager.CacheType;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.LockManager;

public class GraphDbModule
{
    private boolean startIsOk = true;

    private static final int INDEX_COUNT = 2500;

    private PersistenceManager persistenceManager;
    private NodeManager nodeManager;
    
    public GraphDbModule(
            PersistenceManager persistenceManager,
            NodeManager nodeManager)
    {
        this.persistenceManager = persistenceManager;
        this.nodeManager = nodeManager;
    }
    
    public void init()
    {
    }

    public void start()
    {
        if ( !startIsOk )
        {
            return;
        }
        
        // load and verify from PS
        NameData[] relTypes = null;
        NameData[] propertyIndexes = null;
        // beginTx();
        relTypes = persistenceManager.loadAllRelationshipTypes();
        propertyIndexes = persistenceManager.loadPropertyIndexes( INDEX_COUNT );
        // commitTx();
        nodeManager.addRawRelationshipTypes( relTypes );
        nodeManager.addPropertyIndexes( propertyIndexes );
        if ( propertyIndexes.length < INDEX_COUNT )
        {
            nodeManager.setHasAllpropertyIndexes( true );
        }
        nodeManager.start( );
        startIsOk = false;
    }
    
    public void stop()
    {
        nodeManager.clearPropertyIndexes();
        nodeManager.clearCache();
        nodeManager.stop();
    }

    public void destroy()
    {
    }

    public NodeManager getNodeManager()
    {
        return this.nodeManager;
    }
}
