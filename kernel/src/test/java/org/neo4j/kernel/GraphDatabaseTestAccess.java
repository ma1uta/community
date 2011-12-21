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
package org.neo4j.kernel;

import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

/**
 * Used in testing and makes some internals configurable, f.ex {@link FileSystemAbstraction}
 * and {@link IdGeneratorFactory}. Otherwise its functionality is equivalent to
 * {@link EmbeddedGraphDatabase}.
 */
public class GraphDatabaseTestAccess extends AbstractGraphDatabaseWithDbImpl
{
    protected final FileSystemAbstraction fileSystem;

    public GraphDatabaseTestAccess( String storeDir, Map<String, String> config,
            IdGeneratorFactory idGenerators, FileSystemAbstraction fileSystem )
    {
        super( storeDir );
        config = config != null ? config : MapUtil.stringMap();
        setGraphDbImplAtConstruction( new EmbeddedGraphDbImpl( getStoreDir(), null, config, this,
                CommonFactories.defaultLockManagerFactory(),
                idGenerators, CommonFactories.defaultRelationshipTypeCreator(),
                CommonFactories.defaultReferenceNodeCreator(),
                CommonFactories.defaultTxIdGeneratorFactory(),
                CommonFactories.defaultTxHook(),
                CommonFactories.defaultLastCommittedTxIdSetter(), fileSystem ) );
        this.fileSystem = fileSystem;
    }
}
