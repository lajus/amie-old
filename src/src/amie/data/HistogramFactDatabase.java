package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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
			Object[] keys = counts.toArray();
			Arrays.sort(keys);
			for (Object count : keys) {
				sum += counts.get((Integer)count);
				cumulativeCounts.put((Integer)count, new Integer((int)(total - sum)));
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
		int result = cardinalityEqualsTo(relation, cardinality, histograms);
		return result == -1 ? 0 : result;
	}
	
	public int cardinalityGreaterThan(ByteString relation, int cardinality) {
		return (int) cardinalityGreaterThan(relation, cardinality, cumulativeHistograms, false);
	}
	
	public double probabilityOfCardinalityEqualsTo(ByteString relation, int cardinality) {
		int result = cardinalityEqualsTo(relation, cardinality);
		Long normalization = sums.get(relation);
		if (normalization == null) {
			return 0.0;
		} else {
			return result / normalization.doubleValue();
		}
	}
	
	public double probabilityOfCardinalityGreaterThan(ByteString relation, int cardinality) {
		return cardinalityGreaterThan(relation, cardinality, cumulativeHistograms, true);
	}
	
	private int cardinalityEqualsTo(ByteString relation, int cardinality, Map<ByteString, IntHashMap<Integer>> histogram) {
		if (histogram.containsKey(relation)) {
			IntHashMap<Integer> buckets = histogram.get(relation);
			return buckets.get(cardinality);
		} else {
			return 0;
		}
	}
	
	public double cardinalityGreaterThan(ByteString relation, int cardinality,  Map<ByteString, IntHashMap<Integer>> histogram, boolean normalized) {
		if (cardinality <= 0) {
			if (normalized) {
				return 1.0;
			} else {
				return sums.containsKey(relation) ? sums.get(relation) : 0.0;
			}
		}
		
		int value = cardinalityEqualsTo(relation, cardinality, histogram);
		if (value == -1) {
			// Look for the closest key that is there
			IntHashMap<Integer> buckets = histogram.get(relation);
			Object[] keys = buckets.toArray();
			int insertionPoint = Arrays.binarySearch(keys, cardinality);
			insertionPoint += 2;
			insertionPoint*= -1;
			if (insertionPoint < keys.length) {
				value = histogram.get(relation).get(keys[insertionPoint]);
			} else { 
				return 0.0;
			}
		} else if (value == 0) {
			return 0.0;
		}
		if (normalized) {
			Long normalization = sums.get(relation);
			if (normalization == null) {
				return 0.0;
			} else {
				return value / normalization.doubleValue();
			}
		} else {
			return value;
		}
	}
	
	public static void main(String args[]) throws IOException {
		HistogramFactDatabase db = new HistogramFactDatabase();
		db.load(new File("/home/galarrag/workspace/AMIE/Data/yago2/yago2core.10kseedsSample.decoded.compressed.notypes.tsv"));
		System.out.println("P(<dealsWith>) = 1 :" + db.probabilityOfCardinalityEqualsTo(ByteString.of("<dealsWith>"), 1));
		System.out.println("P(<dealsWith>) > 1 :" + db.probabilityOfCardinalityGreaterThan(ByteString.of("<dealsWith>"), 1));		
		
		System.out.println("P(<dealsWith>) = 2 :" + db.probabilityOfCardinalityEqualsTo(ByteString.of("<dealsWith>"), 2));
		System.out.println("P(<dealsWith>) > 2 :" + db.probabilityOfCardinalityGreaterThan(ByteString.of("<dealsWith>"), 2));
		
		System.out.println("P(<hasChild>) = 2 :" + db.probabilityOfCardinalityEqualsTo(ByteString.of("<hasChild>"), 2));
		System.out.println("P(<hasChild>) > 2 :" + db.probabilityOfCardinalityGreaterThan(ByteString.of("<hasChild>"), 2));
		
		System.out.println("P(<hasChild>) = 6 :" + db.probabilityOfCardinalityEqualsTo(ByteString.of("<hasChild>"), 6));
		System.out.println("P(<hasChild>) > 6 :" + db.probabilityOfCardinalityGreaterThan(ByteString.of("<hasChild>"), 6));
		
		System.out.println("P(<hasWonPrize>) = 0 :" + db.probabilityOfCardinalityEqualsTo(ByteString.of("<hasWonPrize>"), 0));
		System.out.println("P(<hasWonPrize>) > 0 :" + db.probabilityOfCardinalityGreaterThan(ByteString.of("<hasWonPrize>"), 0));
		
		System.out.println("P(<hasWonPrize>) = 2 :" + db.probabilityOfCardinalityEqualsTo(ByteString.of("<hasWonPrize>"), 2));
		System.out.println("P(<hasWonPrize>) > 2 :" +db.probabilityOfCardinalityGreaterThan(ByteString.of("<hasWonPrize>"), 2));
		
		System.out.println("P(<hasWonPrize>) = 10 :" +db.probabilityOfCardinalityEqualsTo(ByteString.of("<hasWonPrize>"), 10));
		System.out.println("P(<hasWonPrize>) > 10 :" + db.probabilityOfCardinalityGreaterThan(ByteString.of("<hasWonPrize>"), 10));
		
		System.out.println("P(<hasWonPrize>) = 7 :" +db.probabilityOfCardinalityEqualsTo(ByteString.of("<hasWonPrize>"), 7));
		System.out.println("P(<hasWonPrize>) > 7 :" + db.probabilityOfCardinalityGreaterThan(ByteString.of("<hasWonPrize>"), 7));
		
		System.out.println("P(<hasWonPrize>) = 6 :" +db.probabilityOfCardinalityEqualsTo(ByteString.of("<hasWonPrize>"), 6));
		System.out.println("P(<hasWonPrize>) > 6 :" + db.probabilityOfCardinalityGreaterThan(ByteString.of("<hasWonPrize>"), 6));
		
		System.out.println("P(<isPoliticianOf>) = 1 :" +db.probabilityOfCardinalityEqualsTo(ByteString.of("<isPoliticianOf>"), 1));
		System.out.println("P(<isPoliticianOf>) > 1 :" + db.probabilityOfCardinalityGreaterThan(ByteString.of("<isPoliticianOf>"), 1));
	}
	
}