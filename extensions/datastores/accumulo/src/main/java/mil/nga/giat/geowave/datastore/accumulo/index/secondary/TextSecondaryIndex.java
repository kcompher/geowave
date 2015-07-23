package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class TextSecondaryIndex extends
		AbstractSecondaryIndex<String>
{
	private final static Logger LOGGER = Logger.getLogger(TextSecondaryIndex.class);
	public static final String TABLE_NAME = TABLE_PREFIX + "TEXT";
	private static TextSecondaryIndex instance;
	private final NGramTokenizer nGramTokenizer;

	private TextSecondaryIndex(
			Writer writer,
			int minGram,
			int maxGram ) {
		super(
				writer);
		nGramTokenizer = new NGramTokenizer(
				minGram,
				maxGram);
	}

	/**
	 * Returns a Singleton instance of {@link TextSecondaryIndex}
	 * 
	 * @param accumuloOperations
	 * @return
	 * @throws InstantiationException
	 *             if unable to construct the necessary {@link Writer}
	 */
	public static TextSecondaryIndex getInstance(
			final AccumuloOperations accumuloOperations,
			int minGram,
			int maxGram )
			throws InstantiationException {
		if (instance == null) {
			try {
				instance = new TextSecondaryIndex(
						accumuloOperations.createWriter(
								TABLE_NAME,
								true,
								false),
						minGram,
						maxGram);
			}
			catch (TableNotFoundException e) {
				throw new InstantiationException(
						"Could not construct writer for TextSecondaryIndex: " + e.getMessage());
			}
		}
		return instance;
	}

	@Override
	public void write(
			final Object attributeValue,
			final Class<?> attributeType,
			final String attributeName,
			final ByteArrayId rowId ) {

		String stringValue = (String) attributeValue;
		List<String> tokens = new ArrayList<>();

		try {
			nGramTokenizer.setReader(new StringReader(
					stringValue));
			CharTermAttribute charTermAttribute = nGramTokenizer.addAttribute(CharTermAttribute.class);
			nGramTokenizer.reset();

			while (nGramTokenizer.incrementToken()) {
				tokens.add(charTermAttribute.toString());
			}
			nGramTokenizer.end();
			nGramTokenizer.close();

			for (String token : tokens) {
				super.write(
						token,
						attributeType,
						attributeName,
						rowId);
			}
		}
		catch (IOException e) {
			LOGGER.error(
					"Could not generate n-grams",
					e);
		}

	}

	@Override
	public byte[] constructRowId(
			String attributeValue ) {
		return StringUtils.stringToBinary(attributeValue);
	}

}
