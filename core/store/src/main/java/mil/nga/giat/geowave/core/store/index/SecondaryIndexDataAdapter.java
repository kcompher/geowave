package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

/**
 * This interface allows for a data adapter to define a set of secondary indices
 * 
 * @param <T>
 *            The type for the data element that is being adapted
 * 
 */
public interface SecondaryIndexDataAdapter<T> extends
		DataAdapter<T>
{
	public ByteArrayId[] getSupportedIndexIds();

	public SecondaryIndex createIndex(
			ByteArrayId indexId );

	public EntryVisibilityHandler<T> getVisibilityHandler(
			ByteArrayId indexId );
}
