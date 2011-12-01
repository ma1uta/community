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
package org.neo4j.index.base;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.LogBackedXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransactionFactory;

/**
 * An base class for a {@link XaDataSource} for an {@link Index},
 * or {@link IndexProvider} rather.
 * This class is public because the XA framework requires it.
 */
public abstract class IndexDataSource extends LogBackedXaDataSource
{
    private final XaContainer xaContainer;
    private final String storeDir;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(); 
    final IndexStore indexStore;
    final IndexProviderStore store;
    private boolean closed;

    /**
     * Constructs this data source.
     * 
     * @param params XA parameters.
     * @throws InstantiationException if the data source couldn't be
     * instantiated
     */
    public IndexDataSource( Map<Object,Object> params ) 
        throws InstantiationException
    {
        super( params );
        try
        {
            this.storeDir = (String) params.get( "index_store_dir" );
            this.indexStore = (IndexStore) params.get( IndexStore.class );
            ensureDirectoryCreated( this.storeDir );
            FileSystemAbstraction fileSystem = (FileSystemAbstraction) params.get( FileSystemAbstraction.class );
            this.store = new IndexProviderStore( new File( (String) params.get( "index_store_db" ) ), fileSystem );
            boolean isReadOnly = params.containsKey( "read_only" ) ?
                    (Boolean) params.get( "read_only" ) : false;
            initializeBeforeLogicalLog( params );
                    
            if ( !isReadOnly )
            {
                XaCommandFactory cf = new IndexCommandFactory();
                XaTransactionFactory tf = new IndexTransactionFactory();
                xaContainer = XaContainer.create( this, this.storeDir + "/logical.log", cf, tf, null, params );
                try
                {
                    xaContainer.openLogicalLog();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "Unable to open logical log in " + this.storeDir, e );
                }
                
                setKeepLogicalLogsIfSpecified( (String) params.get( Config.KEEP_LOGICAL_LOGS ), getName() );
                setLogicalLogAtCreationTime( xaContainer.getLogicalLog() );
            }
            else
            {
                xaContainer = null;
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }
    
    protected void initializeBeforeLogicalLog( Map<?, ?> params )
    {
    }

    protected String getStoreDir()
    {
        return storeDir;
    }
    
    public IndexStore getIndexStore()
    {
        return indexStore;
    }
    
    public IndexProviderStore getIndexProviderStore()
    {
        return store;
    }
    
    private void ensureDirectoryCreated( String path )
    {
        File dir = new File( path );
        if ( !dir.exists() )
        {
            if ( !dir.mkdirs() )
            {
                throw new RuntimeException( "Unable to create directory path["
                    + dir.getAbsolutePath() + "] for Neo4j store." );
            }
        }
    }

    protected static String getStoreDir( Map<?, ?> params )
    {
        String dbStoreDir = (String) params.get( "store_dir" );
        String dataSourceName = (String) params.get( "data_source_name" );
        File dir = new File( new File( dbStoreDir, "index" ), dataSourceName );
        if ( !dir.exists() )
        {
            if ( !dir.mkdirs() )
            {
                throw new RuntimeException( "Unable to create directory path["
                    + dir.getAbsolutePath() + "] for Neo4j store." );
            }
        }
        return dir.getAbsolutePath();
    }
    
    @Override
    public final void close()
    {
        if ( closed )
        {
            return;
        }
        synchronized ( this )
        {
            xaContainer.close();
            store.close();
            actualClose();
            closed = true;
        }
    }
    
    public boolean isClosed()
    {
        return closed;
    }

    protected abstract void actualClose();

    @Override
    public XaConnection getXaConnection()
    {
        return new IndexBaseXaConnection( storeDir, xaContainer.getResourceManager(), getBranchId() );
    }
    
    protected XaCommand readCommand( ReadableByteChannel channel, ByteBuffer buffer ) throws IOException
    {
        return IndexCommand.readCommand( channel, buffer );
    }
    
    protected abstract void flushAll();
    
    private class IndexCommandFactory extends XaCommandFactory
    {
        IndexCommandFactory()
        {
            super();
        }

        @Override
        public XaCommand readCommand( ReadableByteChannel channel, 
            ByteBuffer buffer ) throws IOException
        {
            return IndexDataSource.this.readCommand( channel, buffer );
        }
    }
    
    private class IndexTransactionFactory extends XaTransactionFactory
    {
        @Override
        public XaTransaction create( int identifier )
        {
            return createTransaction( identifier, this.getLogicalLog() );
        }

        @Override
        public void flushAll()
        {
            IndexDataSource.this.flushAll();
        }

        @Override
        public long getCurrentVersion()
        {
            return store.getVersion();
        }
        
        @Override
        public long getAndSetNewVersion()
        {
            return store.incrementVersion();
        }

        @Override
        public long getLastCommittedTx()
        {
            return store.getLastCommittedTx();
        }
    }
    
    public void getReadLock()
    {
        lock.readLock().lock();
    }
    
    public void releaseReadLock()
    {
        lock.readLock().unlock();
    }
    
    public void getWriteLock()
    {
        lock.writeLock().lock();
    }
    
    public void releaseWriteLock()
    {
        lock.writeLock().unlock();
    }
    
    protected abstract XaTransaction createTransaction( int identifier,
        XaLogicalLog logicalLog );

    @Override
    public long getCreationTime()
    {
        return store.getCreationTime();
    }
    
    @Override
    public long getRandomIdentifier()
    {
        return store.getRandomNumber();
    }
    
    @Override
    public long getCurrentLogVersion()
    {
        return store.getVersion();
    }
    
    public long getLastCommittedTxId()
    {
        return this.store.getLastCommittedTx();
    }
    
    @Override
    public void setLastCommittedTxId( long txId )
    {
        this.store.setLastCommittedTx( txId );
    }
    
    @Override
    public XaContainer getXaContainer()
    {
        return xaContainer;
    }
}
