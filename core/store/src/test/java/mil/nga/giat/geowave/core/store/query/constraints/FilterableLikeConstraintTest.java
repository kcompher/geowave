package mil.nga.giat.geowave.core.store.query.constraints;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.text.FilterableLikeConstraint;

import org.junit.Test;

public class FilterableLikeConstraintTest
{
	final ByteArrayId fieldID = new ByteArrayId(
			"field");

	private IndexedPersistenceEncoding<String> create(
			String value ) {
		return new IndexedPersistenceEncoding<String>(
				fieldID,
				fieldID,
				fieldID,
				0,
				new PersistentDataset<String>(
						new PersistentValue<String>(
								fieldID,
								value)));
	}

	@Test
	public void test() {
		FilterableLikeConstraint constraint = new FilterableLikeConstraint(
				fieldID,
				"fRed%dog",
				true);
		QueryFilter filter = constraint.getFilter();
		assertTrue(filter.accept(create("fReddog")));
		assertTrue(filter.accept(create("fRedfkfdog")));
		assertFalse(filter.accept(create("fredddog")));
		assertFalse(filter.accept(create("xRedddog")));

		constraint = new FilterableLikeConstraint(
				fieldID,
				"fRed%",
				true);
		filter = constraint.getFilter();
		assertTrue(filter.accept(create("fReddog")));
		assertTrue(filter.accept(create("fRedfkfdog")));
		assertFalse(filter.accept(create("fredddog")));
		assertFalse(filter.accept(create("xRedddog")));

		constraint = new FilterableLikeConstraint(
				fieldID,
				"fRed%dog",
				false);
		filter = constraint.getFilter();
		assertTrue(filter.accept(create("fReddog")));
		assertTrue(filter.accept(create("fRedfkfdog")));
		assertTrue(filter.accept(create("freddDog")));
		assertFalse(filter.accept(create("xRedddog")));

		constraint = new FilterableLikeConstraint(
				fieldID,
				"fRed%",
				false);
		filter = constraint.getFilter();
		assertTrue(filter.accept(create("fReddog")));
		assertTrue(filter.accept(create("fRedfkfdog")));
		assertTrue(filter.accept(create("freddDog")));
		assertFalse(filter.accept(create("xRedddog")));
	}
}
