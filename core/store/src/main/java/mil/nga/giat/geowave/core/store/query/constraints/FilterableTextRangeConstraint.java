package mil.nga.giat.geowave.core.store.query.constraints;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.text.TextQueryConstraint;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

public class FilterableTextRangeConstraint extends
		TextQueryConstraint.TextRange implements
		FilterableConstraints
{

	private final ByteArrayId fieldId;
	private final boolean caseSensitive;

	public FilterableTextRangeConstraint(
			final ByteArrayId fieldId,
			final String matchstring,
			final boolean caseSensitive ) {
		super(
				caseSensitive ? matchstring : matchstring.toLowerCase());
		this.fieldId = fieldId;
		this.caseSensitive = caseSensitive;
	}

	public FilterableTextRangeConstraint(
			final ByteArrayId fieldId,
			final String start,
			final String end,
			final boolean caseSensitive ) {
		super(
				caseSensitive ? start : start.toLowerCase(),
				caseSensitive ? end : end.toLowerCase());
		this.fieldId = fieldId;
		this.caseSensitive = caseSensitive;
	}

	@Override
	public QueryFilter getFilter() {
		return new QueryFilter() {

			@Override
			public boolean accept(
					IndexedPersistenceEncoding<?> persistenceEncoding ) {
				String value = persistenceEncoding.getCommonData().getValue(
						fieldId).toString();
				value = caseSensitive ? value : value.toLowerCase();
				int toStart = value.compareTo(start);
				int toEnd = value.compareTo(end);
				return (toStart >= 0 && toEnd <= 0);
			}
		};
	}

}
