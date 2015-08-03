package mil.nga.giat.geowave.core.index.text;

import mil.nga.giat.geowave.core.index.QueryConstraints;

public abstract class NumericQueryConstraint implements
		QueryConstraints
{

	@Override
	public int getDimensionCount() {
		return 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

}
