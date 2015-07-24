package mil.nga.giat.geowave.core.store.index;

import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;

/**
 * This is responsible for persisting secondary index entries
 */
public interface IndexDataStore
{
	/**
	 * 
	 * @param indexID
	 * @param ranges
	 * @param visibility
	 * @param dataLocationID
	 * @param dataRowIds
	 */
	public void store(
			ByteArrayId indexID,
			List<ByteArrayId> ranges,
			ByteArrayId visibility,
			ByteArrayId dataLocationID,
			List<ByteArrayId> dataRowIds,
			List<FieldInfo<?>> attributeInfos );

	/**
	 * 
	 * @param indexID
	 * @param ranges
	 * @param visibility
	 * @param primaryIndexID
	 * @param dataRowIds
	 */
	public void remove(
			ByteArrayId indexID,
			List<ByteArrayId> ranges,
			ByteArrayId visibility,
			ByteArrayId dataLocationID,
			List<ByteArrayId> dataRowIds,
			List<FieldInfo<?>> attributeInfos );

	/**
	 * 
	 * @param indexID
	 *            secondary index ID
	 * @param ranges
	 * @param visibility
	 * @param used
	 *            for additional filtering
	 * @return Primary Index ID associated Range Values
	 */
	public Map<ByteArrayId, List<ByteArrayRange>> query(
			ByteArrayId indexID,
			List<ByteArrayRange> ranges,
			List<QueryFilter> constraints,
			String... visibility );
}
