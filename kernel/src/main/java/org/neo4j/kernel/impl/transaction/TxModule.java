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
package org.neo4j.kernel.impl.transaction;

import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.DependencyResolver;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Can reads a XA data source configuration file and registers all the data
 * sources defined there or be used to manually add XA data sources.
 * <p>
 * This module will create a instance of each {@link XaDataSource} once started
 * and will close them once stopped.
 *
 * @see XaDataSourceManager
 */
public class TxModule
{
    private static final String MODULE_NAME = "TxModule";

    private boolean startIsOk = true;

    private final AbstractTransactionManager txManager;
    private final XaDataSourceManager xaDsManager;
    private final KernelPanicEventGenerator kpe;

    public TxModule( KernelPanicEventGenerator kpe, XaDataSourceManager xaDsManager, AbstractTransactionManager txManager)
    {
        this.kpe = kpe;
        this.xaDsManager = xaDsManager;
        this.txManager = txManager;
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
        txManager.init(xaDsManager);
        startIsOk = false;
    }

    public void stop()
    {
        xaDsManager.unregisterAllDataSources();
        txManager.stop();
    }

    public void destroy()
    {
    }
}