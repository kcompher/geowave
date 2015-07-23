package mil.nga.giat.geowave.adapter.vector.ingest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.Writer;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.NumericSecondaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.SecondaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.TemporalSecondaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.TextSecondaryIndex;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class SecondaryIndexingIngestCallback implements
		IngestCallback<SimpleFeature>
{
	private static final String INDEX_KEY = "index";
	private final SecondaryIndex numericSecondaryIndex;
	private final SecondaryIndex temporalSecondaryIndex;
	private final SecondaryIndex textSecondaryIndex;
	private final List<Writer> allWriters = new ArrayList<>();

	public SecondaryIndexingIngestCallback(
			final AccumuloOperations accumuloOperations )
			throws InstantiationException {
		super();
		numericSecondaryIndex = NumericSecondaryIndex.getInstance(accumuloOperations);
		allWriters.add(numericSecondaryIndex.getWriter());
		temporalSecondaryIndex = TemporalSecondaryIndex.getInstance(accumuloOperations);
		allWriters.add(temporalSecondaryIndex.getWriter());
		textSecondaryIndex = TextSecondaryIndex.getInstance(
				accumuloOperations,
				3,
				3); // FIXME hard-coded for now
		allWriters.add(textSecondaryIndex.getWriter());
	}

	public List<Writer> getAllWriters() {
		return allWriters;
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final SimpleFeature feature ) {

		final List<ByteArrayId> rowIds = entryInfo.getRowIds();
		final SimpleFeatureType type = feature.getType();

		// iterate attributes...
		for (AttributeDescriptor desc : type.getAttributeDescriptors()) {

			// examining the user data of each attribute...
			final Map<Object, Object> userData = desc.getUserData();

			// looking for a key "index" with value true...
			if (userData.containsKey(INDEX_KEY) && userData.get(
					INDEX_KEY).equals(
					Boolean.TRUE)) {

				// we found an attribute to be indexed via secondary
				// indexing
				final String attributeName = desc.getLocalName();
				final Class<?> attributeType = desc.getType().getBinding();
				final Object attributeValue = feature.getAttribute(attributeName);
				final SecondaryIndex secondaryIndex = getSecondaryIndex(attributeType);

				// write to secondary index
				for (ByteArrayId rowId : rowIds) {
					secondaryIndex.write(
							attributeValue,
							attributeType,
							attributeName,
							rowId);
				}

			}
		}
	}

	private SecondaryIndex getSecondaryIndex(
			final Class<?> attributeType ) {
		if (Number.class.isAssignableFrom(attributeType)) {
			return numericSecondaryIndex;
		}
		else if (String.class.isAssignableFrom(attributeType)) {
			return textSecondaryIndex;
		}
		else if (Date.class.isAssignableFrom(attributeType)) {
			return temporalSecondaryIndex;
		}
		else {
			throw new UnsupportedOperationException(
					"Secondary indexing not supported for type " + attributeType.toString());
		}
	}

}
