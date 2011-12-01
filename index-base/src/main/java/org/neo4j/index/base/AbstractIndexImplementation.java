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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.kernel.Config;

public abstract class AbstractIndexImplementation extends IndexImplementation
{
    private GraphDatabaseService graphDb;
    private IndexConnectionBroker<IndexBaseXaConnection> broker;
    private IndexDataSource dataSource;
    
    protected AbstractIndexImplementation( GraphDatabaseService db, Config config )
    {
        this.graphDb = db;
        boolean isReadOnly = config.isReadOnly();
        Map<Object, Object> params = new HashMap<Object, Object>( config.getParams() );
        params.put( "read_only", isReadOnly );
        params.put( "ephemeral", config.isEphemeral() );
        String dbStoreDir = (String) params.get( "store_dir" );
        params.put( "index_store_dir", getIndexStoreDir( dbStoreDir, getDataSourceName() ) );
        params.put( "index_store_db", getIndexStoreDb( dbStoreDir, getDataSourceName() ) );
        this.dataSource = (IndexDataSource) config.getTxModule().registerDataSource(
                getDataSourceName(), getDataSourceClass().getName(),
                getDataSourceBranchId(), params, true );
        this.broker = isReadOnly ?
                new ReadOnlyConnectionBroker<IndexBaseXaConnection>( config.getTxModule().getTxManager() ) :
                new ConnectionBroker( config.getTxModule().getTxManager(), dataSource );
    }
    
    public IndexConnectionBroker<IndexBaseXaConnection> broker()
    {
        return this.broker;
    }
    
    public GraphDatabaseService graphDb()
    {
        return this.graphDb;
    }
    
    public IndexDataSource dataSource()
    {
        return this.dataSource;
    }
    
    protected abstract Class<? extends IndexDataSource> getDataSourceClass();
    
    public static String getIndexBaseDir( String dbStoreDir, String dataSourceName )
    {
        return new File( new File( dbStoreDir, "index" ), dataSourceName ).getAbsolutePath();
    }

    public static String getIndexStoreDir( String dbStoreDir, String dataSourceName )
    {
        return getIndexBaseDir( dbStoreDir, dataSourceName );
    }

    public static String getIndexStoreDb( String dbStoreDir, String dataSourceName )
    {
        return new File( getIndexStoreDir( dbStoreDir, dataSourceName ), "store.db" ).getAbsolutePath();
    }
    
    protected abstract byte[] getDataSourceBranchId();
}
