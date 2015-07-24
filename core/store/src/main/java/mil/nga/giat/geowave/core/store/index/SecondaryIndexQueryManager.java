package mil.nga.giat.geowave.core.store.index;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.query.Query;

/**
 * Manages query the secondary indices given a query. Eventually is replaced by
 * a CBO!
 * 
 * 
 * @param <T>
 *            The type of entity being indexed
 */
public class SecondaryIndexQueryManager
{
	final IndexStore secondaryIndexStore;
	final IndexDataStore secondaryIndexDataStore;

	public SecondaryIndexQueryManager(
			final IndexStore secondaryIndexStore,
			final IndexDataStore secondaryIndexDataStore ) {
		this.secondaryIndexStore = secondaryIndexStore;
		this.secondaryIndexDataStore = secondaryIndexDataStore;
	}

	/**
	 * Query across primary indices
	 * 
	 * @param query
	 * @param visibility
	 * @return association between primary index ID and the ranges associated
	 *         with that index
	 * @throws IOException
	 */
	public Map<ByteArrayId, List<ByteArrayRange>> query(
			final Query query,
			final String... visibility )
			throws IOException {
		try (CloseableIterator<Index<?, ?>> indicesIt = secondaryIndexStore.getIndices()) {
			while (indicesIt.hasNext()) {
				SecondaryIndex index = (SecondaryIndex) indicesIt.next();
				if (query.isSupported(index)) {
					return secondaryIndexDataStore.query(
							index.getId(),
							query.getSecondaryIndexConstraints(index),
							query.getSecondaryQueryFilter(index),
							visibility);
				}
			}
		}

		return Collections.emptyMap();
	}

}
