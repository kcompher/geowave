package mil.nga.giat.geowave.core.store.index;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.DeleteCallback;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.ScanCallback;

/**
 * One manager associated with each primary index.
 * 
 * 
 * @param <T>
 *            The type of entity being indexed
 */
public class SecondaryIndexDataManager<T> implements
		IngestCallback<T>,
		DeleteCallback<T>,
		ScanCallback<T>
{
	private final SecondaryIndexDataAdapter<T> adapter;
	final IndexDataStore secondaryIndexStore;
	final ByteArrayId primaryIndexId;
	private final Map<ByteArrayId, SecondaryIndex> indexMap = new HashMap<ByteArrayId, SecondaryIndex>();

	public SecondaryIndexDataManager(
			final IndexDataStore secondaryIndexStore,
			final SecondaryIndexDataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		this.adapter = adapter;
		this.secondaryIndexStore = secondaryIndexStore;
		this.primaryIndexId = primaryIndexId;

	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {

		for (ByteArrayId indexID : adapter.getSupportedIndexIds()) {
			SecondaryIndex index = indexMap.get(indexID);
			if (index == null) {
				index = adapter.createIndex(indexID);
				indexMap.put(
						indexID,
						index);
			}
			final List<FieldInfo<?>> infos = new LinkedList<FieldInfo<?>>();
			for (ByteArrayId fieldID : index.getFieldIDs()) {
				infos.add(getFieldInfo(
						entryInfo,
						fieldID));
			}
			final List<ByteArrayId> ranges = index.getIndexStrategy().getInsertionIds(
					infos);
			final EntryVisibilityHandler<T> visibilityHandler = adapter.getVisibilityHandler(indexID);
			final ByteArrayId visibility = new ByteArrayId(
					visibilityHandler.getVisibility(
							entryInfo,
							entry));
			secondaryIndexStore.store(
					indexID,
					adapter.getDataId(entry),
					ranges,
					visibility,
					primaryIndexId,
					entryInfo.getRowIds(),
					infos);
		}

	}

	private FieldInfo<?> getFieldInfo(
			final DataStoreEntryInfo entryInfo,
			final ByteArrayId fieldID ) {
		for (FieldInfo<?> info : entryInfo.getFieldInfo()) {
			if (info.getDataValue().getId().equals(
					fieldID)) return info;
		}
		return null;
	}

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (ByteArrayId indexID : adapter.getSupportedIndexIds()) {
			SecondaryIndex index = indexMap.get(indexID);
			if (index == null) {
				index = adapter.createIndex(indexID);
				indexMap.put(
						indexID,
						index);
			}
			final List<FieldInfo<?>> infos = new LinkedList<FieldInfo<?>>();
			for (ByteArrayId fieldID : index.getFieldIDs()) {
				infos.add(getFieldInfo(
						entryInfo,
						fieldID));
			}
			final List<ByteArrayId> ranges = index.getIndexStrategy().getInsertionIds(
					infos);
			final EntryVisibilityHandler<T> visibilityHandler = adapter.getVisibilityHandler(indexID);
			final ByteArrayId visibility = new ByteArrayId(
					visibilityHandler.getVisibility(
							entryInfo,
							entry));
			secondaryIndexStore.remove(
					indexID,
					adapter.getDataId(entry),
					ranges,
					visibility,
					primaryIndexId,
					entryInfo.getRowIds(),
					infos);
		}
	}

	@Override
	public void entryScanned(
			DataStoreEntryInfo entryInfo,
			T entry ) {
		// to do

	}
}
