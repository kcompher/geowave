package mil.nga.giat.geowave.core.store.index;

import java.util.List;

import mil.nga.giat.geowave.core.index.IndexStrategy;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;

/**
 * Interface which defines an index strategy.
 * 
 */
public interface FieldIndexStrategy extends
		IndexStrategy<FilterableConstraints, List<FieldInfo<?>>>
{

}
