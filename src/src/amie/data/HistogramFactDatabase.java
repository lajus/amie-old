package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

public class HistogramFactDatabase extends FactDatabase{

	private Map<ByteString, IntHashMap<Integer>> histograms = new HashMap<>();
	
	private Map<ByteString, IntHashMap<Integer>> cumulativeHistograms = new HashMap<>();
	
	private Map<ByteString, Long> sums = new HashMap<>();
	
	protected void load(File f, String message, boolean buildJoinTable)
			throws IOException {
		super.load(f, message, buildJoinTable);
		// Construct the histogram tables
		for (ByteString relation : predicateSize) {
			IntHashMap<Integer> counts;
			IntHashMap<Integer> cumulativeCounts = new IntHashMap<>();
			if (functionality(relation) > inverseFunctionality(relation)) {
				counts = buildHistogram(predicate2subject2object.get(relation));
			} else {
				counts = buildHistogram(predicate2object2subject.get(relation));
			}
			long total = counts.computeSum();
			sums.put(relation, total);
			int sum = 0;
			for (Integer count : counts.increasingKeys()) {
				sum =+ counts.get(count);
				cumulativeCounts.put(count, new Integer((int)(total - sum)));
			}
			histograms.put(relation, counts);
			cumulativeHistograms.put(relation, cumulativeCounts);
		}
		System.out.println(histograms);
		System.out.println(cumulativeHistograms);
		System.out.println(sums);
	}

	private IntHashMap<Integer> buildHistogram(
			Map<ByteString, IntHashMap<ByteString>> map) {
		IntHashMap<Integer> histogram = new IntHashMap<>();
		
		for (ByteString entity : map.keySet()) {
			histogram.increase(map.get(entity).size());
		}
		
		return histogram;
	}
	
	public int cardinalityEqualsTo(ByteString relation, int cardinality) {
		return cardinalityEqualsTo(relation, cardinality, histograms);
	}
	
	public int cardinalityGreaterThan(ByteString relation, int cardinality) {
		return cardinalityEqualsTo(relation, cardinality, cumulativeHistograms);
	}
	
	public double probabilityOfCardinalityEqualsTo(ByteString relation, 
			int cardinality) {
		return probabilityOfCardinalityGreaterThan(relation, cardinality, histograms);
	}
	
	public double probabilityOfCardinalityGreaterThan(ByteString relation, 
			int cardinality) {
		return probabilityOfCardinalityGreaterThan(relation, cardinality, cumulativeHistograms);
	}
	
	private int cardinalityEqualsTo(ByteString relation, int cardinality, Map<ByteString, IntHashMap<Integer>> histogram) {
		if (histogram.containsKey(relation)) {
			IntHashMap<Integer> buckets = histogram.get(relation);
			int value = buckets.get(cardinality);
			return value == -1 ? 0 : value;
		} else {
			return 0;
		}
	}
	
	public double probabilityOfCardinalityGreaterThan(ByteString relation, int cardinality,  Map<ByteString, IntHashMap<Integer>> histogram) {
		int value = cardinalityEqualsTo(relation, cardinality, histogram);
		if (value == 0) {
			return 0.0;
		} else {
			Long normalization = sums.get(relation);
			if (normalization == null) {
				return 0.0;
			} else {
				return value / normalization.doubleValue();
			}
		}
	}
	
	public static void main(String args[]) throws IOException {
		HistogramFactDatabase db = new HistogramFactDatabase();
		db.load(new File("/home/galarrag/workspace/AMIE/Data/yago2/yago2core.10kseedsSample.decoded.compressed.notypes.tsv"));
		System.out.println(db.probabilityOfCardinalityEqualsTo(ByteString.of("<dealsWith>"), 1));
		System.out.println(db.probabilityOfCardinalityGreaterThan(ByteString.of("<dealsWith>"), 1));		
		System.out.println(db.probabilityOfCardinalityEqualsTo(ByteString.of("<dealsWith>"), 2));
		System.out.println(db.probabilityOfCardinalityGreaterThan(ByteString.of("<dealsWith>"), 2));
		System.out.println(db.probabilityOfCardinalityEqualsTo(ByteString.of("<hasChild>"), 2));
		System.out.println(db.probabilityOfCardinalityGreaterThan(ByteString.of("<hasChild>"), 2));		
	}
	
}