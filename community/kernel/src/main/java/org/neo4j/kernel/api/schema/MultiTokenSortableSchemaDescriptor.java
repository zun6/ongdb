package org.neo4j.kernel.api.schema;

import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.internal.kernel.api.schema.SchemaComputer;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.storageengine.api.EntityType;

import java.util.Map;

public class MultiTokenSortableSchemaDescriptor extends MultiTokenSchemaDescriptor
{
    private int[] sortIds;

    private Map<String,String> sortTypes;

    public MultiTokenSortableSchemaDescriptor( int[] entityTokens, EntityType entityType, int[] propertyIds )
    {
        super( entityTokens, entityType, propertyIds );
    }

    public MultiTokenSortableSchemaDescriptor( int[] entityTokens, EntityType entityType, int[] propertyIds, int[] sortIds )
    {
        super( entityTokens, entityType, propertyIds );
        this.sortIds = sortIds;
    }

    public MultiTokenSortableSchemaDescriptor( int[] entityTokens, EntityType entityType, int[] propertyIds, int[] sortIds, Map<String,String> sortTypes )
    {
        super( entityTokens, entityType, propertyIds );
        this.sortIds = sortIds;
        this.sortTypes = sortTypes;
    }

    public int[] getSortIds()
    {
        return sortIds;
    }

    public Map<String,String> getSortTypes()
    {
        return sortTypes;
    }

    // I think this is used to determine which properties to pick up for indexing!
    @Override
    public int[] getPropertyIds()
    {
        int[] propertyIds = super.getPropertyIds();
        return ArrayUtils.addAll(propertyIds, sortIds);
    }

    @Override
    public boolean isAffected( long[] entityTokenIds )
    {
        boolean affected = super.isAffected( entityTokenIds );
        return affected;
//        for ( int id : sortIds )
//        {
//            if ( ArrayUtils.contains( entityTokenIds, id ) )
//            {
//                return true;
//            }
//        }
//        return false;
    }

    @Override
    public <R> R computeWith( SchemaComputer<R> computer )
    {
        return computer.computeSpecific( this );
    }

    @Override
    public void processWith( SchemaProcessor processor )
    {
        processor.processSpecific( this );
    }
}
