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
package org.neo4j.kernel.impl.nioneo.store;

import java.util.Map;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;

public class ReferenceNodeStore extends AbstractNameStore<ReferenceNodeRecord, Long>
{
    public static final String TYPE_DESCRIPTOR = "ReferenceNodeStore";
    private static final int RECORD_SIZE = 1/*inUse*/ + 8/*nodeId*/ + 4/*nameId*/;

    public ReferenceNodeStore( String fileName, Map<?, ?> config )
    {
        super( fileName, config, IdType.REFERENCE_NODE );
    }
    
    public static void createStore( String fileName, Map<?, ?> config )
    {
        IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get( IdGeneratorFactory.class );
        FileSystemAbstraction fileSystem = (FileSystemAbstraction) config.get( FileSystemAbstraction.class );
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ), idGeneratorFactory, fileSystem );
        DynamicStringStore.createStore( fileName + ".names",
            NAME_STORE_BLOCK_SIZE, idGeneratorFactory, fileSystem, IdType.REFERENCE_NODE_BLOCK );
        ReferenceNodeStore store = new ReferenceNodeStore( fileName, config );
        store.close();
    }

    @Override
    public void accept( Processor processor, ReferenceNodeRecord record )
    {
        // TODO Auto-generated method stub
    }

    @Override
    protected IdType getNameIdType()
    {
        return IdType.REFERENCE_NODE_BLOCK;
    }

    @Override
    protected String getNameStorePostfix()
    {
        return ".names";
    }

    @Override
    protected ReferenceNodeRecord newRecord( int id )
    {
        return new ReferenceNodeRecord( id );
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }
    
    @Override
    protected void readRecord( ReferenceNodeRecord record, Buffer buffer )
    {
        record.setNodeId( buffer.getLong() );
        record.setNameId( buffer.getInt() );
    }
    
    @Override
    protected void writeRecord( ReferenceNodeRecord record, Buffer buffer )
    {
        buffer.putLong( record.getNodeId() );
        buffer.putInt( record.getNameId() );
    }
    
    @Override
    protected NameData<Long> newNameData( String name, ReferenceNodeRecord record )
    {
        return new NameData<Long>( record.getId(), name, record.getNodeId() );
    }
}
