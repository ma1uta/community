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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.core.GraphDbModule;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.persistence.IdGeneratorModule;
import org.neo4j.kernel.impl.persistence.PersistenceModule;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

class GraphDbInstance
{
    interface Configuration
    {
        boolean read_only(boolean def);
        boolean use_memory_mapped_buffers(boolean def);

        boolean dump_configuration(boolean def);
        boolean backup_slave(boolean def);
    }
    
    private boolean started = false;
    private final boolean create;
    private String storeDir;
    private final Configuration conf;
    private Config config;
    private AbstractGraphDatabase graphDb;
    private KernelExtensionLoader kernelExtensionLoader;

    private NioNeoDbPersistenceSource persistenceSource = null;
    private TransactionManager txManager;
    private GraphDbModule graphDbModule;
    private IdGeneratorModule idGeneratorModule;
    private PersistenceModule persistenceModule;
    private TxModule txModule;


    GraphDbInstance( String storeDir, boolean create, Config config, AbstractGraphDatabase graphDb,
                     KernelExtensionLoader kernelExtensionLoader, NioNeoDbPersistenceSource persistenceSource,
                     TransactionManager txManager,
                     GraphDbModule graphDbModule,
                     IdGeneratorModule idGeneratorModule,
                     PersistenceModule persistenceModule,
                     TxModule txModule)
    {
        this.storeDir = storeDir;
        this.create = create;
        this.config = config;
        this.graphDb = graphDb;
        this.kernelExtensionLoader = kernelExtensionLoader;
        this.persistenceSource = persistenceSource;
        this.txManager = txManager;
        this.graphDbModule = graphDbModule;
        this.idGeneratorModule = idGeneratorModule;
        this.persistenceModule = persistenceModule;
        this.txModule = txModule;
        this.conf = ConfigProxy.config(config.getParams(), Configuration.class);
    }

    /**
     * Starts Neo4j with default configuration
     */
    public synchronized void start()
    {
        if ( started )
        {
            throw new IllegalStateException( "Neo4j instance already started" );
        }
        boolean dumpToConsole = conf.dump_configuration(false);
        storeDir = FileUtils.fixSeparatorsInPath( storeDir );
        StringLogger logger = graphDb.getMessageLog();

/* TODO Moved to EGD
        String separator = System.getProperty( "file.separator" );
        String store = storeDir + separator + NeoStore.DEFAULT_NAME;
        params.put( "store_dir", storeDir );
        params.put( "neo_store", store );
        params.put( "create", String.valueOf( create ) );
        String logicalLog = storeDir + separator + NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
        params.put( "logical_log", logicalLog );
*/

        kernelExtensionLoader.configureKernelExtensions();

        txModule.init();
        persistenceModule.init();
        persistenceSource.init();
        idGeneratorModule.init();
        graphDbModule.init();

        kernelExtensionLoader.initializeIndexProviders();

        txModule.start();
        persistenceModule.start();
        persistenceSource.start();
        idGeneratorModule.start();
        graphDbModule.start();

        started = true;
    }

    /**
     * Returns true if Neo4j is started.
     *
     * @return True if Neo4j started
     */
    public boolean started()
    {
        return started;
    }

    /**
     * Shut down Neo4j.
     */
    public synchronized void shutdown()
    {
        if ( started )
        {
            graphDbModule.stop();
            idGeneratorModule.stop();
            persistenceSource.stop();
            persistenceModule.stop();
            txModule.stop();

            graphDbModule.destroy();
            idGeneratorModule.destroy();
            persistenceSource.destroy();
            persistenceModule.destroy();
            txModule.destroy();
        }
        started = false;
    }

    public boolean transactionRunning()
    {
        try
        {
            return txManager.getTransaction() != null;
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                    "Unable to get transaction.", e );
        }
    }
}
