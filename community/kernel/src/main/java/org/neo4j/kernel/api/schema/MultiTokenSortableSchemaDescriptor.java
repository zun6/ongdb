package org.neo4j.kernel.api.schema;

import org.neo4j.storageengine.api.EntityType;

public class MultiTokenSortableSchemaDescriptor extends MultiTokenSchemaDescriptor
{
    private int[] sortIds;

    public MultiTokenSortableSchemaDescriptor( int[] entityTokens, EntityType entityType, int[] propertyIds )
    {
        super( entityTokens, entityType, propertyIds );
    }
}
