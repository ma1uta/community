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

import static org.neo4j.helpers.Exceptions.launderedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.cache.AdaptiveCacheManager;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.GraphDbModule;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.core.ReadOnlyNodeManager;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.persistence.IdGenerator;
import org.neo4j.kernel.impl.persistence.IdGeneratorModule;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.persistence.PersistenceModule;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.impl.transaction.ReadOnlyTxManager;
import org.neo4j.kernel.impl.transaction.TransactionManagerProvider;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.tooling.GlobalGraphOperations;


/**
 * Exposes the methods {@link #getManagementBeans(Class)}() a.s.o.
 */
public abstract class AbstractGraphDatabase
        implements GraphDatabaseService, GraphDatabaseSPI
{

    interface Configuration
    {
        boolean read_only(boolean def);

        NodeManager.CacheType cache_type(NodeManager.CacheType def);

        boolean load_kernel_extensions( boolean def );
    }

    private static final NodeManager.CacheType DEFAULT_CACHE_TYPE = NodeManager.CacheType.soft;

    protected String storeDir;
    protected Map<String, String> params;
    private StoreId storeId;

    protected final List<KernelEventHandler> kernelEventHandlers = new CopyOnWriteArrayList<KernelEventHandler>();
    protected final Collection<TransactionEventHandler<?>> transactionEventHandlers = new CopyOnWriteArraySet<TransactionEventHandler<?>>();

    protected StringLogger msgLog;
    protected volatile EmbeddedGraphDbImpl graphDbImpl;
    protected RelationshipTypeHolder relationshipTypeHolder;
    protected NodeManager nodeManager;
    protected IndexManagerImpl indexManager;
    protected Config config;
    protected KernelPanicEventGenerator kernelPanicEventGenerator;
    protected TxHook txHook;
    protected FileSystemAbstraction fileSystem;
    protected XaDataSourceManager xaDataSourceManager;
    protected TxModule txModule;
    protected RagManager ragManager;
    protected LockManager lockManager;
    protected AdaptiveCacheManager cacheManager;
    protected IdGeneratorFactory idGeneratorFactory;
    protected RelationshipTypeCreator relationshipTypeCreator;
    protected LastCommittedTxIdSetter lastCommittedTxIdSetter;
    protected NioNeoDbPersistenceSource persistenceSource;
    protected IdGenerator idGenerator;
    protected IdGeneratorModule idGeneratorModule;
    protected TxEventSyncHookFactory syncHook;
    protected PersistenceManager persistenceManager;
    protected PropertyIndexManager propertyIndexManager;
    protected LockReleaser lockReleaser;
    protected PersistenceModule persistenceModule;
    protected GraphDbModule graphDbModule;
    protected IndexStore indexStore;
    protected LogBufferFactory logBufferFactory;
    protected KernelExtensionLoader extensionLoader;
    protected GraphDbInstance graphDbInstance;
    protected AbstractTransactionManager txManager;
    protected TxIdGenerator txIdGenerator;
    protected StoreFactory storeFactory;
    protected XaFactory xaFactory;
    protected DiagnosticsManager diagnosticsManager;
    protected NeoStoreXaDataSource neoDataSource;

    protected NodeAutoIndexerImpl nodeAutoIndexer;
    protected RelationshipAutoIndexerImpl relAutoIndexer;
    protected KernelData extensions;

    protected AbstractGraphDatabase(String storeDir, Map<String, String> params)
    {
        this.params = params;
        this.storeDir = canonicalize( storeDir );
    }

    protected void init()
    {
        Configuration conf = ConfigProxy.config(params, Configuration.class);

        boolean readOnly = conf.read_only(false);

        NodeManager.CacheType cacheType;
        try
        {
            cacheType = conf.cache_type(DEFAULT_CACHE_TYPE);
        }
        catch ( IllegalArgumentException e )
        {
            throw new IllegalArgumentException( "Invalid cache type, please use one of: " +
                    Arrays.asList(NodeManager.CacheType.values()) + " or keep empty for default (" +
                    DEFAULT_CACHE_TYPE + ")", e.getCause() );
        }

        // Instantiate all services - some are overridable by subclasses
        this.msgLog = createStringLogger();

        diagnosticsManager = new DiagnosticsManager( msgLog );

        kernelPanicEventGenerator = new KernelPanicEventGenerator( kernelEventHandlers );

        txHook = createTxHook();

        fileSystem = createFileSystemAbstraction();

        xaDataSourceManager = new XaDataSourceManager(new DependencyResolverImpl());

        if (readOnly)
        {
            txManager = new ReadOnlyTxManager();

        } else
        {
            String serviceName = params.get( Config.TXMANAGER_IMPLEMENTATION );
            if ( serviceName == null )
            {
                txManager = new TxManager( this.storeDir, kernelPanicEventGenerator, txHook, msgLog, fileSystem);
            }
            else {
                TransactionManagerProvider provider;
                provider = Service.load(TransactionManagerProvider.class, serviceName);
                if ( provider == null )
                {
                    throw new IllegalStateException( "Unknown transaction manager implementation: "
                            + serviceName );
                }
                txManager = provider.loadTransactionManager( this.storeDir, kernelPanicEventGenerator, txHook, msgLog, fileSystem);
            }
        }

        txIdGenerator = createTxIdGenerator();

        txModule = new TxModule(kernelPanicEventGenerator, xaDataSourceManager, txManager);

        ragManager = new RagManager(txManager);
        lockManager = createLockManager();

        cacheManager = new AdaptiveCacheManager(ConfigProxy.config(params, AdaptiveCacheManager.Configuration.class));

        idGeneratorFactory = createIdGeneratorFactory();

        relationshipTypeCreator = new DefaultRelationshipTypeCreator();

        lastCommittedTxIdSetter = createLastCommittedTxIdSetter();

        persistenceSource = new NioNeoDbPersistenceSource(xaDataSourceManager);

        idGenerator = new IdGenerator(persistenceSource);

        idGeneratorModule = new IdGeneratorModule(idGenerator);

        syncHook = new DefaultTxEventSyncHookFactory(transactionEventHandlers, txManager);

        // TODO Cyclic dependency! lockReleaser is null here
        persistenceManager = new PersistenceManager(txManager,
                persistenceSource, syncHook, lockReleaser );

        propertyIndexManager = new PropertyIndexManager(
                txManager, persistenceManager, idGenerator);

        lockReleaser = new LockReleaser(lockManager, txManager, nodeManager, propertyIndexManager);
        persistenceManager.setLockReleaser(lockReleaser); // TODO This cyclic dep needs to be refactored

        persistenceModule = new PersistenceModule(persistenceManager);

        relationshipTypeHolder = new RelationshipTypeHolder( txManager,
            persistenceManager, idGenerator, relationshipTypeCreator );

        nodeManager = !readOnly ? new NodeManager( ConfigProxy.config(params, NodeManager.Configuration.class), this, cacheManager,
                lockManager, lockReleaser, txManager,
                persistenceManager, idGenerator, relationshipTypeHolder, cacheType, propertyIndexManager,

                createNodeLookup(), createRelationshipLookups() ):
                new ReadOnlyNodeManager( ConfigProxy.config(params, NodeManager.Configuration.class), this,
                        cacheManager, lockManager, lockReleaser,
                        txManager, persistenceManager, idGenerator,
                        relationshipTypeHolder, cacheType, propertyIndexManager,
                        createNodeLookup(), createRelationshipLookups());

        lockReleaser.setNodeManager(nodeManager); // TODO Another cyclic dep that needs to be refactored

        graphDbModule = new GraphDbModule( persistenceManager, nodeManager);

        indexStore = new IndexStore( this.storeDir, fileSystem);

        // Default settings that need to be available
        // TODO THIS IS A SMELL - SHOULD BE AVAILABLE THROUGH OTHER MEANS!
        String separator = System.getProperty( "file.separator" );
        String store = this.storeDir + separator + NeoStore.DEFAULT_NAME;
        params.put( "store_dir", this.storeDir );
        params.put( "neo_store", store );
        String logicalLog = this.storeDir + separator + NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
        params.put( "logical_log", logicalLog );
        // END SMELL

        config = new Config( this.isEphemeral(), this.storeDir,  params );

        /*
         *  LogBufferFactory needs access to the parameters so it has to be added after the default and
         *  user supplied configurations are consolidated
         */

        logBufferFactory = new DefaultLogBufferFactory();

        extensions = new DefaultKernelData(config);

        if ( conf.load_kernel_extensions(true))
        {
            extensionLoader = new DefaultKernelExtensionLoader( extensions );
        }
        else
        {
            extensionLoader = KernelExtensionLoader.DONT_LOAD;
        }

        graphDbInstance = new GraphDbInstance( this.storeDir, true, config, this, extensionLoader, persistenceSource, txManager, graphDbModule, idGeneratorModule, persistenceModule, txModule);

        this.graphDbImpl = new EmbeddedGraphDbImpl( this.getStoreDir(), this,
                lockManager,
                txManager,
                lockReleaser,
                kernelEventHandlers,
                transactionEventHandlers,
                kernelPanicEventGenerator,
                extensions, graphDbInstance,
                nodeManager);

        indexManager = new IndexManagerImpl(config, indexStore, xaDataSourceManager, txManager, graphDbImpl);
        nodeAutoIndexer = new NodeAutoIndexerImpl( ConfigProxy.config( params, NodeAutoIndexerImpl.Configuration.class ), indexManager, nodeManager);
        relAutoIndexer = new RelationshipAutoIndexerImpl( ConfigProxy.config( params, RelationshipAutoIndexerImpl.Configuration.class ), indexManager, nodeManager);

        // TODO This cyclic dependency should be resolved
        indexManager.setNodeAutoIndexer( nodeAutoIndexer );
        indexManager.setRelAutoIndexer( relAutoIndexer );

        // Factories for things that needs to be created later
        storeFactory = new StoreFactory(config.getParams(), idGeneratorFactory, fileSystem, lastCommittedTxIdSetter, msgLog, txHook);
        xaFactory = new XaFactory(config.getParams(), txIdGenerator, txManager, logBufferFactory, fileSystem, msgLog );

        // Create DataSource
        List<Pair<TransactionInterceptorProvider, Object>> providers = new ArrayList<Pair<TransactionInterceptorProvider, Object>>( 2 );
        for ( TransactionInterceptorProvider provider : Service.load( TransactionInterceptorProvider.class ) )
        {
            Object prov = params.get( TransactionInterceptorProvider.class.getSimpleName() + "." + provider.name() );
            if ( prov != null )
            {
                providers.add( Pair.of( provider, prov ) );
            }
        }

        try
        {
            neoDataSource = new NeoStoreXaDataSource( ConfigProxy.config( params, NeoStoreXaDataSource.Configuration.class ),
                    storeFactory, lockManager, lockReleaser, msgLog, xaFactory, providers, new DependencyResolverImpl());
            xaDataSourceManager.registerDataSource( neoDataSource );

            storeId = neoDataSource.getStoreId();

            KernelDiagnostics.register( diagnosticsManager, this,
                                        neoDataSource );
        } catch (IOException e)
        {
            throw new IllegalStateException("Could not create Neo XA datasource", e);
        }
    }

    protected void start()
    {
        boolean started = false;
        try
        {
            graphDbInstance.start();

            diagnosticsManager.startup();

            nodeAutoIndexer.start();
            relAutoIndexer.start();
            extensionLoader.load();

            started = true; // must be last
        }
        catch ( Error cause )
        {
            msgLog.logMessage( "Startup failed", cause );
            throw cause;
        }
        catch ( RuntimeException cause )
        {
            cause.printStackTrace();
            msgLog.logMessage( "Startup failed", cause );
            throw cause;
        }
        finally
        {
            // If startup failed, cleanup the extensions - or they will leak
            if ( !started ) extensions.shutdown( msgLog );
        }
    }

    protected LastCommittedTxIdSetter createLastCommittedTxIdSetter()
    {
        return new DefaultLastCommittedTxIdSetter();
    }

    protected TxIdGenerator createTxIdGenerator()
    {
        return TxIdGenerator.DEFAULT;
    }

    protected RelationshipProxy.RelationshipLookups createRelationshipLookups()
    {
        return new RelationshipProxy.RelationshipLookups()
        {
            @Override
            public Node lookupNode( long nodeId )
            {
                return nodeManager.getNodeById( nodeId );
            }

            @Override
            public RelationshipImpl lookupRelationship( long relationshipId )
            {
                return nodeManager.getRelationshipForProxy( relationshipId );
            }

            @Override
            public GraphDatabaseService getGraphDatabaseService()
            {
                return AbstractGraphDatabase.this;
            }

            @Override
            public NodeManager getNodeManager()
            {
                return nodeManager;
            }
        };
    }

    protected NodeProxy.NodeLookup createNodeLookup()
    {
        return new NodeProxy.NodeLookup()
        {
            @Override
            public NodeImpl lookup( long nodeId )
            {
                return nodeManager.getNodeForProxy( nodeId );
            }

            @Override
            public GraphDatabaseService getGraphDatabase()
            {
                return AbstractGraphDatabase.this;
            }

            @Override
            public NodeManager getNodeManager()
            {
                return nodeManager;
            }
        };
    }

    protected TxHook createTxHook()
    {
        return new DefaultTxHook();
    }

    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new CommonFactories.DefaultIdGeneratorFactory();
    }

    protected LockManager createLockManager()
    {
        return new LockManager(ragManager);
    }

    protected StringLogger createStringLogger()
    {
        return StringLogger.logger( this.storeDir );
    }
    
    @Override
    public final void shutdown()
    {
        close();
        msgLog.close();
    }

    public final String getStoreDir()
    {
        return storeDir;
    }

    public StoreId getStoreId()
    {
        return storeId;
    }
    
    @Override
    public Transaction beginTx()
    {
        return tx().begin();
    }

    /**
     * Get a single management bean. Delegates to {@link #getSingleManagementBean(Class)}.
     *
     * @deprecated since Neo4j may now have multiple beans implementing the same bean interface, this method has been
     *             deprecated in favor of {@link #getSingleManagementBean(Class)} and {@link #getManagementBeans(Class)}
     *             . Version 1.5 of Neo4j will be the last version to contain this method.
     */
    @Deprecated
    public final <T> T getManagementBean( Class<T> type )
    {
        return getSingleManagementBean( type );
    }

    public final <T> T getSingleManagementBean( Class<T> type )
    {
        Iterator<T> beans = getManagementBeans( type ).iterator();
        if ( beans.hasNext() )
        {
            T bean = beans.next();
            if ( beans.hasNext() )
                throw new NotFoundException( "More than one management bean for " + type.getName() );
            return bean;
        }
        return null;
    }

    protected boolean isEphemeral()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [" + getStoreDir() + "]";
    }
    
    @Override
    public Iterable<Node> getAllNodes()
    {
        return GlobalGraphOperations.at( this ).getAllNodes();
    }
    
    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return GlobalGraphOperations.at( this ).getAllRelationshipTypes();
    }

    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        return this.graphDbImpl.registerKernelEventHandler( handler );
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return this.graphDbImpl.registerTransactionEventHandler( handler );
    }

    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        return this.graphDbImpl.unregisterKernelEventHandler( handler );
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return this.graphDbImpl.unregisterTransactionEventHandler( handler );
    }

    /**
     * A non-standard convenience method that loads a standard property file and
     * converts it into a generic <Code>Map<String,String></CODE>. Will most
     * likely be removed in future releases.
     *
     * @param file the property file to load
     * @return a map containing the properties from the file
     * @throws IllegalArgumentException if file does not exist
     */
    public static Map<String,String> loadConfigurations( String file )
    {
        return EmbeddedGraphDbImpl.loadConfigurations( file );
    }

    public Node createNode()
    {
        return nodeManager.createNode();
    }

    public Node getNodeById( long id )
    {
        return graphDbImpl.getNodeById( id );
    }

    public Relationship getRelationshipById( long id )
    {
        return graphDbImpl.getRelationshipById( id );
    }

    public Node getReferenceNode()
    {
        return graphDbImpl.getReferenceNode();
    }

    protected void close()
    {
        graphDbImpl.shutdown();
    }

    public TransactionBuilder tx()
    {
        return graphDbImpl.tx();
    }

    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return graphDbImpl.getManagementBeans( type );
    }

    public KernelData getKernelData()
    {
        return graphDbImpl.getKernelData();
    }

    public IndexManager index()
    {
        return indexManager;
    }

    // GraphDatabaseSPI implementation - THESE SHOULD EVENTUALLY BE REMOVED! DON'T ADD deps on these!
    public Config getConfig()
    {
        return config;
    }

    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    public LockReleaser getLockReleaser()
    {
        return lockReleaser;
    }

    public LockManager getLockManager()
    {
        return lockManager;
    }

    public XaDataSourceManager getXaDataSourceManager()
    {
        return xaDataSourceManager;
    }

    public TransactionManager getTxManager()
    {
        return txManager;
    }

    public TxModule getTxModule()
    {
        return txModule;
    }

    @Override
    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return relationshipTypeHolder;
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return idGeneratorFactory;
    }

    @Override
    public DiagnosticsManager getDiagnosticsManager()
    {
        return diagnosticsManager;
    }

    public final StringLogger getMessageLog()
    {
        return msgLog;
    }

    private String canonicalize( String path )
    {
        try
        {
            return new File( path ).getCanonicalFile().getAbsolutePath();
        }
        catch ( IOException e )
        {
            return new File( path ).getAbsolutePath();
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if( this == o )
        {
            return true;
        }
        if( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        AbstractGraphDatabase that = (AbstractGraphDatabase) o;

        if( getStoreId() != null ? !getStoreId().equals( that.getStoreId() ) : that.getStoreId() != null )
        {
            return false;
        }
        if( !storeDir.equals( that.storeDir ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return storeDir.hashCode();
    }

    private static class DefaultTxHook implements TxHook
    {
        @Override
        public void initializeTransaction( int eventIdentifier )
        {
            // Do nothing from the ordinary here
        }

        public boolean hasAnyLocks( javax.transaction.Transaction tx )
        {
            return false;
        }

        public void finishTransaction( int eventIdentifier, boolean success )
        {
            // Do nothing from the ordinary here
        }

        @Override
        public boolean freeIdsDuringRollback()
        {
            return true;
        }
    }

    private class DefaultKernelData extends KernelData
    {
        private final Config config;

        public DefaultKernelData(Config config)
        {
            this.config = config;
        }

        @Override
        public Version version()
        {
            return Version.getKernel();
        }

        @Override
        public Config getConfig()
        {
            return config;
        }

        @Override
        public Map<String, String> getConfigParams()
        {
            return config.getParams();
        }

        @Override
        public GraphDatabaseSPI graphDatabase()
        {
            return AbstractGraphDatabase.this;
        }
    }

    private class DefaultKernelExtensionLoader implements KernelExtensionLoader
    {
        private final KernelData extensions;

        private Collection<KernelExtension<?>> loaded;

        public DefaultKernelExtensionLoader(KernelData extensions)
        {
            this.extensions = extensions;
        }

        @Override
        public void configureKernelExtensions()
        {
            loaded = extensions.loadExtensionConfigurations( msgLog );
        }

        @Override
        public void initializeIndexProviders()
        {
            loadIndexImplementations(indexManager, msgLog);
        }

        @Override
        public void load()
        {
            extensions.loadExtensions( loaded, msgLog );
        }

        void loadIndexImplementations( IndexManagerImpl indexes, StringLogger msgLog )
            {
                for ( IndexProvider index : Service.load( IndexProvider.class ) )
                {
                    try
                    {
                        indexes.addProvider( index.identifier(), index.load( new DependencyResolverImpl() ) );
                    }
                    catch ( Throwable cause )
                    {
                        msgLog.logMessage( "Failed to load index provider " + index.identifier(), cause );
                        if ( isAnUpgradeProblem( cause ) ) throw launderedException( cause );
                        else cause.printStackTrace();
                    }
                }
            }


        private boolean isAnUpgradeProblem( Throwable cause )
        {
            while ( cause != null )
            {
                if ( cause instanceof Throwable ) return true;
                cause = cause.getCause();
            }
            return false;
        }

    }

    private class DefaultTxEventSyncHookFactory implements TxEventSyncHookFactory
    {
        private final Collection<TransactionEventHandler<?>> transactionEventHandlers;
        private AbstractTransactionManager txManager;

        public DefaultTxEventSyncHookFactory(Collection<TransactionEventHandler<?>> transactionEventHandlers, AbstractTransactionManager txManager)
        {
            this.transactionEventHandlers = transactionEventHandlers;
            this.txManager = txManager;
        }

        @Override
        public TransactionEventsSyncHook create()
        {
            return transactionEventHandlers.isEmpty() ? null :
                    new TransactionEventsSyncHook(
                            nodeManager, transactionEventHandlers, txManager);
        }
    }

    class DependencyResolverImpl
            implements DependencyResolver
    {
        @Override
        public <T> T resolveDependency(Class<T> type)
        {
            if (type.equals(Map.class))
                return (T) config.getParams();
            else if (GraphDatabaseService.class.isAssignableFrom(type))
                return (T) AbstractGraphDatabase.this;
            else if (TransactionManager.class.isAssignableFrom(type))
                return (T) txManager;
            else if (LockManager.class.isAssignableFrom(type))
                return (T) lockManager;
            else if (LockReleaser.class.isAssignableFrom(type))
                return (T) lockReleaser;
            else if (StoreFactory.class.isAssignableFrom(type))
                return (T) storeFactory;
            else if (StringLogger.class.isAssignableFrom(type))
                return (T) msgLog;
            else if (IndexStore.class.isAssignableFrom(type))
                return (T) indexStore;
            else if (XaFactory.class.isAssignableFrom(type))
                return (T) xaFactory;
            else if (XaDataSourceManager.class.isAssignableFrom(type))
                return (T) xaDataSourceManager;
            else if (FileSystemAbstraction.class.isAssignableFrom(type))
                return (T) fileSystem;
            else
                throw new IllegalArgumentException("Could not resolve dependency of type:"+type.getName());
        }
    }
}
