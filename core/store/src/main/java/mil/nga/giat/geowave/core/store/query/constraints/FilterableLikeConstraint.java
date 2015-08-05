package mil.nga.giat.geowave.core.store.query.constraints;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;
import mil.nga.giat.geowave.core.store.index.text.TextQueryConstraint;

public class FilterableLikeConstraint extends
		TextQueryConstraint.LikeConstraint implements
		FilterableConstraints
{

	private final ByteArrayId fieldId;
	private final Pattern regex;

	public FilterableLikeConstraint(
			final ByteArrayId fieldId,
			final String expression,
			final boolean caseSensitive ) {
		super(
				expression);
		this.fieldId = fieldId;
		regex = Pattern.compile(
				expression.replace(
						"%",
						".*"),
				caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
	}

	@Override
	public QueryFilter getFilter() {
		return new QueryFilter() {

			@Override
			public boolean accept(
					IndexedPersistenceEncoding<?> persistenceEncoding ) {
				String value = persistenceEncoding.getCommonData().getValue(
						fieldId).toString();
				final Matcher matcher = regex.matcher(value);
				return matcher.matches();
			}
		};
	}

}
