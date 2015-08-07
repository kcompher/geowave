package mil.nga.giat.geowave.core.store.index.text;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.index.FieldIndexStrategy;

import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class TextIndexStrategy implements
		FieldIndexStrategy<TextQueryConstraint, String>
{

	protected static final String START_END_MARKER = "\01";
	protected static final byte[] START_END_MARKER_BYTE = StringUtils.stringToBinary(START_END_MARKER);

	private int start = 2;
	private int end = 4;

	public TextIndexStrategy() {}

	public TextIndexStrategy(
			final int start,
			final int end ) {
		super();
		this.start = start;
		this.end = end;
	}

	@Override
	public byte[] toBinary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final TextQueryConstraint indexedRange ) {
		return indexedRange.getRange(
				start,
				end);
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final TextQueryConstraint indexedRange,
			final int maxEstimatedRangeDecomposition ) {
		return indexedRange.getRange(
				start,
				end);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<String>> indexedData ) {
		final List<ByteArrayId> insertionIds = new ArrayList<>();
		for (final FieldInfo<String> fieldInfo : indexedData) {
			insertionIds.addAll(grams(
					START_END_MARKER + fieldInfo.getDataValue().getValue() + START_END_MARKER,
					start,
					end));
		}
		return insertionIds;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<String>> indexedData,
			final int maxEstimatedDuplicateIds ) {
		// TODO handle maxEstimatedDuplicateIds
		return getInsertionIds(indexedData);
	}

	@Override
	public List<FieldInfo<String>> getRangeForId(
			final ByteArrayId insertionId ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		return "NGRAM_" + start + "_" + end;
	}

	protected static List<ByteArrayId> grams(
			final String value,
			final int start,
			final int end ) {

		final NGramTokenizer nGramTokenizer = new NGramTokenizer(
				start,
				end);
		final List<ByteArrayId> tokens = new ArrayList<ByteArrayId>();
		try {
			nGramTokenizer.setReader(new StringReader(
					value));
			final CharTermAttribute charTermAttribute = nGramTokenizer.addAttribute(CharTermAttribute.class);
			nGramTokenizer.reset();

			while (nGramTokenizer.incrementToken()) {
				tokens.add(new ByteArrayId(
						charTermAttribute.toString()));
			}
			nGramTokenizer.end();
			nGramTokenizer.close();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return tokens;
	}

	public static final byte[] toIndexByte(
			final String ngram ) {
		return toIndexByte(StringUtils.stringToBinary(ngram));
	}

	public static final byte[] toIndexByte(
			final byte[] ngramBytes ) {
		final ByteBuffer buffer = ByteBuffer.allocate(ngramBytes.length + 4);
		buffer.putInt(ngramBytes.length);
		buffer.put(ngramBytes);
		return buffer.array();
	}
}
