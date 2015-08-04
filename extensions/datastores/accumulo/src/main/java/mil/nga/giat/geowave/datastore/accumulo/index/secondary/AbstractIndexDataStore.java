package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

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
import org.apache.accumulo.core.security.ColumnVisibility;

public abstract class AbstractIndexDataStore implements
		IndexDataStore
{
	private static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";
	private final Writer writer;

	public AbstractIndexDataStore(
			final AccumuloOperations accumuloOperations )
			throws InstantiationException {
		super();
		try {
			writer = accumuloOperations.createWriter(
					TABLE_PREFIX + getTableSuffix(),
					true,
					false);
		}
		catch (final TableNotFoundException e) {
			throw new InstantiationException(
					"Could not construct writer for secondary index: " + e.getMessage());
		}
	}

	public abstract String getTableSuffix();

	@Override
	public void store(
			final ByteArrayId indexID,
			final ByteArrayId dataId, // FIXME what is the purpose of dataId ???
			final List<ByteArrayId> ranges,
			final ByteArrayId visibility,
			final ByteArrayId dataLocationID,
			final List<ByteArrayId> dataRowIds,
			final List<FieldInfo<?>> attributeInfos ) {
		final ColumnVisibility columnVisibility = new ColumnVisibility(
				visibility.getBytes());
		for (final ByteArrayId range : ranges) {
			final Mutation m = new Mutation(
					range.getBytes());
			for (final ByteArrayId dataRowId : dataRowIds) {
				m.put(
						indexID.getBytes(),
						dataLocationID.getBytes(),
						columnVisibility,
						dataRowId.getBytes());
			}
			for (final FieldInfo<?> fieldInfo : attributeInfos) {
				m.put(
						indexID.getBytes(),
						fieldInfo.getDataValue().getId().getBytes(),
						columnVisibility,
						fieldInfo.getWrittenValue());
			}
			writer.write(m);
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

}
