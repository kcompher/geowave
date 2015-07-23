package mil.nga.giat.geowave.test.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.VectorDataStore;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.NumericSecondaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.TemporalSecondaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.TextSecondaryIndex;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * This class is currently a dirty test harness used to sanity check changes as
 * I go. It will likely be rewritten/replaced by a much more sophisticated
 * integration test for secondary indexing once the capability matures
 * 
 * @author yeagerdc
 * 
 */
public class SecondaryIndexingDriver extends
		GeoWaveTestEnvironment

{
	private static SimpleFeatureType schema;
	private static Random random = new Random();
	private static int NUM_FEATURES = 10;

	@BeforeClass
	public static void init()
			throws IOException,
			SchemaException,
			AccumuloException,
			AccumuloSecurityException {

		GeoWaveTestEnvironment.setup();

		// see https://github.com/ngageoint/geowave/wiki/Secondary-Indexing
		schema = DataUtilities.createType(
				"cannedData",
				"location:Geometry,persons:Double,record_date:Date,income_category:String,affiliation:String");

		// mark numeric attribute for secondary indexing
		schema.getDescriptor(
				"persons").getUserData().put(
				"index",
				Boolean.TRUE);

		// mark temporal attribute for secondary indexing
		schema.getDescriptor(
				"record_date").getUserData().put(
				"index",
				Boolean.TRUE);

		// mark text attribute for secondary indexing
		schema.getDescriptor(
				"affiliation").getUserData().put(
				"index",
				Boolean.TRUE);

		FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema);

		VectorDataStore dataStore = new VectorDataStore(
				accumuloOperations);

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		List<SimpleFeature> features = new ArrayList<>();
		for (int x = 0; x < NUM_FEATURES; x++) {
			features.add(buildSimpleFeature());
		}

		dataStore.ingest(
				dataAdapter,
				index,
				features.iterator());

		System.out.println("Feature(s) ingested");

		// final CloseableIterator<?> results = dataStore.query(new
		// SpatialQuery(
		// GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
		// -102,
		// 41))));
		//
		// while (results.hasNext()) {
		// SimpleFeature sf = (SimpleFeature) results.next();
		// System.out.println("Result: " + sf.getID());
		// }
		//
		// results.close();

	}

	@Test
	public void test()
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException {

		int numNumericEntries = countNumberOfEntriesInIndexTable(NumericSecondaryIndex.TABLE_NAME);
		Assert.assertTrue(numNumericEntries == NUM_FEATURES);

		int numTemporalEntries = countNumberOfEntriesInIndexTable(TemporalSecondaryIndex.TABLE_NAME);
		Assert.assertTrue(numTemporalEntries == NUM_FEATURES);

		int numTrigrams = 9; // text "a few words" produces 9 unique tri-grams:
		// {'a f', ' fe', 'few', 'ew ', 'w w', ' wo',
		// 'wor', 'ord', 'rds'}
		int numTextEntries = countNumberOfEntriesInIndexTable(TextSecondaryIndex.TABLE_NAME);
		// all features have the same affiliation text
		Assert.assertTrue(numTextEntries == (NUM_FEATURES * numTrigrams));
	}

	private int countNumberOfEntriesInIndexTable(
			final String tableName )
			throws TableNotFoundException {
		Scanner scanner = accumuloOperations.createScanner(tableName);
		int numEntries = 0;
		for (Entry<Key, Value> kv : scanner) {
			numEntries++;
		}
		scanner.close();
		return numEntries;
	}

	private static SimpleFeature buildSimpleFeature() {

		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				schema);

		int randomLng = random.nextInt(361) - 180; // generate random # between
													// -180, 180 inclusive
		int randomLat = random.nextInt(181) - 90; // generate random # between
													// -90, 90 inclusive
		int randomPersons = random.nextInt(2000000);

		builder.set(
				"location",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						randomLng,
						randomLat)));
		builder.set(
				"persons",
				randomPersons);
		builder.set(
				"record_date",
				new Date());
		builder.set(
				"income_category",
				"10-15");
		builder.set(
				"affiliation",
				"a few words");

		return builder.buildFeature(UUID.randomUUID().toString());
	}

}
