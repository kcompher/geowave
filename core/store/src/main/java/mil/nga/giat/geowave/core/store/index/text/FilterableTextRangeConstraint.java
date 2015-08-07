package mil.nga.giat.geowave.core.store.index.text;

import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;

public class FilterableTextRangeConstraint extends
		TextQueryConstraint
{

	private final ByteArrayId fieldId;
	private final boolean caseSensitive;
	private final String start;
	private final String end;

	/*
	 * Equals
	 */
	public FilterableTextRangeConstraint(
			final ByteArrayId fieldId,
			final String matchstring,
			final boolean caseSensitive ) {
		start = end = caseSensitive ? matchstring : matchstring.toLowerCase();
		this.fieldId = fieldId;
		this.caseSensitive = caseSensitive;
	}

	public FilterableTextRangeConstraint(
			final ByteArrayId fieldId,
			final String start,
			final String end,
			final boolean caseSensitive ) {
		super();
		this.start = caseSensitive ? start : start.toLowerCase();
		this.end = caseSensitive ? end : end.toLowerCase();
		this.fieldId = fieldId;
		this.caseSensitive = caseSensitive;
	}

	private int subStringSize(
			final int minNGramSize,
			final int maxNGramSize ) {
		return Math.min(
				maxNGramSize,
				Math.min(
						start.length(),
						end.length()));
	}

	private byte[] compose(
			final String expression,
			final int pad,
			final byte padCharacter ) {
		byte[] expressionBytes;
		expressionBytes = StringUtils.stringToBinary(expression);
		final byte[] result = new byte[expressionBytes.length + TextIndexStrategy.START_END_MARKER_BYTE.length + (pad < 0 ? 0 : pad)];
		System.arraycopy(
				TextIndexStrategy.START_END_MARKER_BYTE,
				0,
				result,
				0,
				TextIndexStrategy.START_END_MARKER_BYTE.length);
		System.arraycopy(
				expressionBytes,
				0,
				result,
				TextIndexStrategy.START_END_MARKER_BYTE.length,
				expressionBytes.length);
		int pos = expressionBytes.length + TextIndexStrategy.START_END_MARKER_BYTE.length;
		for (int i = 0; i < pad; i++) {
			result[pos] = padCharacter;
			pos++;
		}
		return TextIndexStrategy.toIndexByte(result);
	}

	@Override
	public List<ByteArrayRange> getRange(
			final int minNGramSize,
			final int maxNGramSize ) {
		// subtract one to account for the extra character
		final int subStringSize = subStringSize(
				minNGramSize - 1,
				maxNGramSize - 1);
		final int nGramSize = Math.max(
				minNGramSize,
				subStringSize);

		return Collections.singletonList(new ByteArrayRange(
				new ByteArrayId(
						compose(
								start.substring(
										0,
										subStringSize),
								nGramSize - start.length() - 1,
								(byte) 0)),
				new ByteArrayId(
						compose(
								end.substring(
										0,
										subStringSize),
								nGramSize - end.length() - 1,
								Byte.MAX_VALUE))));
	}

	@Override
	public QueryFilter getFilter() {
		return new QueryFilter() {

			@Override
			public boolean accept(
					final IndexedPersistenceEncoding<?> persistenceEncoding ) {
				String value = persistenceEncoding.getCommonData().getValue(
						fieldId).toString();
				value = caseSensitive ? value : value.toLowerCase();
				final int toStart = value.compareTo(start);
				final int toEnd = value.compareTo(end);
				return ((toStart >= 0) && (toEnd <= 0));
			}
		};
	}
}
