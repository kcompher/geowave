package mil.nga.giat.geowave.core.store.query.constraints;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;

import org.junit.Test;

public class FilterableTextRangeConstraintTest
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
	public void testOne() {
		FilterableTextRangeConstraint constraint = new FilterableTextRangeConstraint(
				fieldID,
				"RedDog",
				true);
		QueryFilter filter = constraint.getFilter();
		assertFalse(filter.accept(create("fReddog")));
		assertTrue(filter.accept(create("RedDog")));

		constraint = new FilterableTextRangeConstraint(
				fieldID,
				"RedDog",
				false);
		 filter = constraint.getFilter();
		assertFalse(filter.accept(create("fReddog")));
		assertTrue(filter.accept(create("RedDog")));
		assertTrue(filter.accept(create("reddog")));
	}

	@Test
	public void testRange() {
		FilterableTextRangeConstraint constraint = new FilterableTextRangeConstraint(
				fieldID,
				"RedDog",
				"SadDog",
				true);
		QueryFilter filter = constraint.getFilter();
		assertFalse(filter.accept(create("fReddog")));
		assertTrue(filter.accept(create("RedDog")));
		assertTrue(filter.accept(create("RodDog")));
		assertFalse(filter.accept(create("SidDog")));

		constraint = new FilterableTextRangeConstraint(
				fieldID,
				"RedDog",
				"SadDog",
				false);
		filter = constraint.getFilter();
		assertFalse(filter.accept(create("fReddog")));
		assertTrue(filter.accept(create("RedDog")));
		assertTrue(filter.accept(create("roddOg")));
		assertTrue(filter.accept(create("ridDog")));
	}

}
