package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.QueryConstraints;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;

public interface FilterableConstraints extends
		QueryConstraints
{
	public QueryFilter getFilter();
}
