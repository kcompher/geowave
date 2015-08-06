package mil.nga.giat.geowave.core.store.index.numeric;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.index.FieldIndexStrategy;

public class NumericIndexStrategy implements
		FieldIndexStrategy<NumericQueryConstraint, Number>
{

	public NumericIndexStrategy() {}

	@Override
	public byte[] toBinary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			NumericQueryConstraint indexedRange ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			NumericQueryConstraint indexedRange,
			int maxEstimatedRangeDecomposition ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			List<FieldInfo<Number>> indexedData ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			List<FieldInfo<Number>> indexedData,
			int maxEstimatedDuplicateIds ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<FieldInfo<Number>> getRangeForId(
			ByteArrayId insertionId ) {
		// TODO Auto-generated method stub
		return null;
	}

}
