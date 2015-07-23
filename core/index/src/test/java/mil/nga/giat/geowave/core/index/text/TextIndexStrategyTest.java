package mil.nga.giat.geowave.core.index.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.text.TextQueryConstraint.LikeConstraint;
import mil.nga.giat.geowave.core.index.text.TextQueryConstraint.TextRange;

import org.junit.Test;

public class TextIndexStrategyTest
{
	@Test
	public void testInsertions() {
		TextIndexStrategy strategy = new TextIndexStrategy();
		List<ByteArrayId> ids = strategy.getInsertionIds("inability to deal with or understand something complicated or unaccountable");
		assertTrue(ids.contains(new ByteArrayId(
				"\01i")));
		assertTrue(ids.contains(new ByteArrayId(
				"ity ")));
		assertTrue(ids.contains(new ByteArrayId(
				"le\01")));
		assertEquals(
				225,
				ids.size());
	}

	@Test
	public void testQueryTextRange() {
		TextIndexStrategy strategy = new TextIndexStrategy(
				3,
				4);
		List<ByteArrayRange> ranges = strategy.getQueryRanges(new TextRange(
				"deal",
				"dumn"));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dea"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dum")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new TextRange(
				"dealing",
				"durango"));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dea"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dur")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new TextRange(
				"d",
				"e"));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01d\00"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01e\177")))),
				ranges.get(0));

	}

	@Test
	public void testQueryTestLike() {
		TextIndexStrategy strategy = new TextIndexStrategy(
				3,
				4);
		List<ByteArrayRange> ranges = strategy.getQueryRanges(new LikeConstraint(
				"deal%"));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dea"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01dea")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new LikeConstraint(
				"de%"));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01de"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01de")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new LikeConstraint(
				"d%aling"));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01d\00"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01d\177")))),
				ranges.get(0));

		ranges = strategy.getQueryRanges(new TextRange(
				"d",
				"e"));
		assertEquals(
				new ByteArrayRange(
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01d\00"))),
						new ByteArrayId(
								TextIndexStrategy.toIndexByte(StringUtils.stringToBinary("\01e\177")))),
				ranges.get(0));

	}
}
