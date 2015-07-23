package mil.nga.giat.geowave.core.store.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.DeleteCallback;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.query.Query;

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
			List<FieldInfo<?>> infos = null;
			for (ByteArrayId fieldID : index.getFieldIDs()) {
				infos.add(getFieldInfo(
						entryInfo,
						fieldID));
			}
			final List<ByteArrayId> ranges = index.indexStrategy.getInsertionIds(infos);
			final EntryVisibilityHandler<T> visibilityHandler = adapter.getVisibilityHandler(indexID);
			final ByteArrayId visibility = new ByteArrayId(
					visibilityHandler.getVisibility(
							entryInfo,
							entry));
			secondaryIndexStore.store(
					indexID,
					ranges,
					visibility,
					primaryIndexId,
					entryInfo.getRowIds());
		}

	}

	private FieldInfo getFieldInfo(
			final DataStoreEntryInfo entryInfo,
			final ByteArrayId fieldID ) {
		for (FieldInfo info : entryInfo.getFieldInfo()) {
			if (info.getDataValue().getId().equals(
					fieldID)) return info;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
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
			List<FieldInfo<?>> infos = null;
			for (ByteArrayId fieldID : index.getFieldIDs()) {
				infos.add(getFieldInfo(
						entryInfo,
						fieldID));
			}
			final List<ByteArrayId> ranges = index.indexStrategy.getInsertionIds(infos);
			final EntryVisibilityHandler<T> visibilityHandler = adapter.getVisibilityHandler(indexID);
			final ByteArrayId visibility = new ByteArrayId(
					visibilityHandler.getVisibility(
							entryInfo,
							entry));
			secondaryIndexStore.remove(
					indexID,
					ranges,
					visibility,
					primaryIndexId,
					entryInfo.getRowIds());
		}
	}

	@Override
	public void entryScanned(
			DataStoreEntryInfo entryInfo,
			T entry ) {
		// to do

	}
}
