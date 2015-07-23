package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.QueryConstraints;
import mil.nga.giat.geowave.core.store.CloseableIterator;

public interface IndexStore<QueryRangeType extends QueryConstraints, EntryRangeType>
{
	public void addIndex(
			Index<QueryRangeType, EntryRangeType> index );

	public Index<QueryRangeType, EntryRangeType> getIndex(
			ByteArrayId indexId );

	public boolean indexExists(
			ByteArrayId indexId );

	public CloseableIterator<Index<QueryRangeType, EntryRangeType>> getIndices();
}
