package mil.nga.giat.geowave.core.store.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;

/**
 * This is a simple HashMap based in-memory implementation of the IndexStore and
 * can be useful if it is undesirable to persist and query objects within
 * another storage mechanism such as an accumulo table.
 */
public class MemoryIndexStore implements
		PrimaryIndexStore
{
	private final Map<ByteArrayId, Index<MultiDimensionalNumericData, MultiDimensionalNumericData>> indexMap = new HashMap<ByteArrayId, Index<MultiDimensionalNumericData, MultiDimensionalNumericData>>();

	public MemoryIndexStore(
			final PrimaryIndex[] initialIndices ) {
		for (final PrimaryIndex index : initialIndices) {
			addIndex(index);
		}
	}

	@Override
	public void addIndex(
			final Index<MultiDimensionalNumericData, MultiDimensionalNumericData> index ) {
		indexMap.put(
				index.getId(),
				index);
	}

	@Override
	public Index<MultiDimensionalNumericData, MultiDimensionalNumericData> getIndex(
			final ByteArrayId indexId ) {
		return indexMap.get(indexId);
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId ) {
		return indexMap.containsKey(indexId);
	}

	@Override
	public CloseableIterator<Index<MultiDimensionalNumericData, MultiDimensionalNumericData>> getIndices() {
		return new CloseableIterator.Wrapper<Index<MultiDimensionalNumericData, MultiDimensionalNumericData>>(
				new ArrayList<Index<MultiDimensionalNumericData, MultiDimensionalNumericData>>(
						indexMap.values()).iterator());
	}

}
