package mil.nga.giat.geowave.core.index.text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.QueryConstraints;
import mil.nga.giat.geowave.core.index.StringUtils;

public abstract class TextQueryConstraint implements
		QueryConstraints
{

	public abstract List<ByteArrayRange> getRange(
			int minNGramSize,
			int maxNGramSize );

	@Override
	public int getDimensionCount() {
		return 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	public static class TextRange extends
			TextQueryConstraint
	{

		private final String start;
		private final String end;

		/*
		 * Equals
		 */
		public TextRange(
				String single ) {
			this.start = this.end = single;
		}

		public TextRange(
				String start,
				String end ) {
			super();
			this.start = start;
			this.end = end;
		}

		private int subStringSize(
				int minNGramSize,
				int maxNGramSize ) {
			return Math.min(
					maxNGramSize,
					Math.min(
							start.length(),
							end.length()));
		}

		private byte[] compose(
				String expression,
				int pad,
				byte padCharacter ) {
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

		public List<ByteArrayRange> getRange(
				int minNGramSize,
				int maxNGramSize ) {
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
	}

	public static class LikeConstraint extends
			TextQueryConstraint
	{

		private final String expression;

		/*
		 * Equals
		 */
		public LikeConstraint(
				String expression ) {
			this.expression = expression;
		}

		public List<ByteArrayRange> getRange(
				int minNGramSize,
				int maxNGramSize ) {
			int percentIndex = expression.indexOf('%');
			if (percentIndex == 0) {
				// ends with case
				int count = 0;
				int maxSize = 0;
				for (int i = 0; i < expression.length(); i++) {
					if (expression.charAt(i) == '%') count = 0;
					count++;
					maxSize = Math.max(
							maxSize,
							count);
				}
				// find the largest possible ngrams
				final int minNGramSearchSize = Math.min(
						maxNGramSize,
						Math.max(
								minNGramSize,
								maxSize));
				final List<ByteArrayId> specificNGrams = TextIndexStrategy.grams(
						expression,
						minNGramSearchSize,
						maxNGramSize);
				final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
				for (ByteArrayId id : specificNGrams) {
					ranges.add(new ByteArrayRange(
							id,
							id));
					break; // actually only need to pick one...but the best one
							// based on stats
				}
				return ranges;
			}
			else if (percentIndex > 0) {
				// starts with case
				final String prefix = expression.substring(
						0,
						percentIndex);
				final String startSearchString = (prefix.length() == (expression.length() - 1)) ? prefix : prefix + "\000";
				final String endSearchString = (prefix.length() == (expression.length() - 1)) ? prefix : prefix + "\177";
				return new TextRange(
						startSearchString,
						endSearchString).getRange(
						minNGramSize,
						maxNGramSize);
			}
			else {
				return new TextRange(
						expression).getRange(
						minNGramSize,
						maxNGramSize);
			}
		}
	}
}
