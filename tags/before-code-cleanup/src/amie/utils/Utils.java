package amie.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javatools.datatypes.IntHashMap;

public class Utils {
	/**
	 * Prints a IntHashMap representing a histogram.
	 * @param histogram
	 */
	public static void printHistogram(IntHashMap<Integer> histogram) {
		for (Integer key : histogram.keys()) {			
			System.out.println(key + "\t" + histogram.get(key));
		}
	}
	
	/**
	 * It adds a key-value pair to a multimap.
	 * @param map
	 * @param key
	 * @param value
	 * @return
	 */
	public static <E, T> boolean add2Map(Map<T, List<E>> map, T key, E value) {
		List<E> items = map.get(key);
		if (items == null) {
			items = new ArrayList<>();
			map.put(key, items);
		}
		return items.add(value);
	}
	
	public static <E, T> IntHashMap<Integer> buildHistogram(Map<T, List<E>> map) {
		IntHashMap<Integer> histogram = new IntHashMap<>();
		for (T key : map.keySet()) {
			histogram.increase(map.get(key).size());
		}
		return histogram;
	}
}
