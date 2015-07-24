package mil.nga.giat.geowave.core.store.index;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.QueryConstraints;
import mil.nga.giat.geowave.core.store.filter.FilterList;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;

public class CompositeConstraints implements
		FilterableConstraints
{
	private List<QueryConstraints> constraints;

	public CompositeConstraints(
			List<QueryConstraints> constraints ) {
		super();
		this.constraints = constraints;
	}

	public List<QueryConstraints> getConstraints() {
		return constraints;
	}

	@Override
	public int getDimensionCount() {
		return constraints == null ? 0 : constraints.size();
	}

	@Override
	public boolean isEmpty() {
		return constraints == null || constraints.isEmpty();
	}

	@Override
	public QueryFilter getFilter() {
		List<QueryFilter> filters = new ArrayList<QueryFilter>();
		for (QueryConstraints constraint : constraints) {
			if (constraint instanceof FilterableConstraints) filters.add(((FilterableConstraints) constraint).getFilter());
		}
		return new FilterList(
				filters);
	}

}
