package mil.nga.giat.geowave.core.store.index.numeric;

import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

public abstract class NumericQueryConstraint implements
		FilterableConstraints
{

	@Override
	public int getDimensionCount() {
		return 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public QueryFilter getFilter() {
		// TODO Auto-generated method stub
		return null;
	}

}
