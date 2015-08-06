package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.IndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.log4j.Logger;

public class AccumuloSecondaryIndexDataStore implements
		IndexDataStore,
		AutoCloseable
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloSecondaryIndexDataStore.class);
	private static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";
	private final AccumuloOperations accumuloOperations;
	private final Map<String, Writer> writerCache = new HashMap<>();

	public AccumuloSecondaryIndexDataStore(
			final AccumuloOperations accumuloOperations ) {
		super();
		this.accumuloOperations = accumuloOperations;
	}

	private Writer getWriter(
			final String secondaryIndexName ) {
		if (writerCache.containsKey(secondaryIndexName)) {
			return writerCache.get(secondaryIndexName);
		}
		Writer writer = null;
		try {
			writer = accumuloOperations.createWriter(
					TABLE_PREFIX + secondaryIndexName,
					true,
					false);
			writerCache.put(
					secondaryIndexName,
					writer);
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Error creating writer",
					e);
		}
		return writer;
	}

	@Override
	public void store(
			final String secondaryIndexName,
			final ByteArrayId indexID,
			final ByteArrayId dataId, // FIXME what is the purpose of dataId ???
			final List<ByteArrayId> ranges,
			final ByteArrayId visibility,
			final ByteArrayId dataLocationID,
			final List<ByteArrayId> dataRowIds,
			final List<FieldInfo<?>> attributeInfos ) {
		final Writer writer = getWriter(secondaryIndexName);
		if (writer != null) {
			// TODO visibility
			// final ColumnVisibility columnVisibility = new
			// ColumnVisibility(visibility.getBytes());
			for (final ByteArrayId range : ranges) {
				final Mutation m = new Mutation(
						range.getBytes());
				for (final ByteArrayId dataRowId : dataRowIds) {
					m.put(
							indexID.getBytes(),
							dataLocationID.getBytes(),
							// columnVisibility,
							dataRowId.getBytes());
				}
				for (final FieldInfo<?> fieldInfo : attributeInfos) {
					m.put(
							indexID.getBytes(),
							fieldInfo.getDataValue().getId().getBytes(),
							// columnVisibility,
							fieldInfo.getWrittenValue());
				}
				writer.write(m);
			}
		}
	}

	@Override
	public void remove(
			final ByteArrayId indexID,
			final ByteArrayId dataId,
			final List<ByteArrayId> ranges,
			final ByteArrayId visibility,
			final ByteArrayId dataLocationID,
			final List<ByteArrayId> dataRowIds,
			final List<FieldInfo<?>> attributeInfos ) {
		// TODO Auto-generated method stub
	}

	@Override
	public CloseableIterator<Map<ByteArrayId, List<ByteArrayRange>>> query(
			final ByteArrayId indexID,
			final List<ByteArrayRange> ranges,
			final List<QueryFilter> constraints,
			final String... visibility ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close()
			throws Exception {
		for (final Writer writer : writerCache.values()) {
			writer.close();
		}
	}
}
