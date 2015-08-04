package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

public class NumericIndexDataStore extends
		AbstractIndexDataStore
{
	public static final String TABLE_SUFFIX = "NUMERIC";

	public NumericIndexDataStore(
			final AccumuloOperations accumuloOperations )
			throws InstantiationException {
		super(
				accumuloOperations);
	}

	@Override
	public String getTableSuffix() {
		return TABLE_SUFFIX;
	}

}
