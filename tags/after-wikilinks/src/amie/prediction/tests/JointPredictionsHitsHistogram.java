package amie.prediction.tests;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.IntHashMap;
import javatools.filehandlers.TSVFile;

public class JointPredictionsHitsHistogram {

	private static void printHistogram(IntHashMap<Integer> histogram) {
		for (Integer key : histogram.keys()) {			
			System.out.println(key + "\t" + histogram.get(key));
		}
	}
	
	public static void main(String[] args) throws IOException {
		IntHashMap<Integer> hitsHistogram = new IntHashMap<>();
		IntHashMap<Integer> allPredictionsHistogram = new IntHashMap<>();
		
		try(TSVFile inferenceDump = new TSVFile(new File(args[0]))) {
			boolean nextIsHit = false;
			for (List<String> line : inferenceDump) {
				if (line.isEmpty()) {
					continue;
				}
				
				if (line.size() == 1) {
					if (line.get(0).equals("Hit")) {
						nextIsHit = true;
					}
					continue;
				}
				
				if (nextIsHit) {
					// # of records - 1 is the number of rules that predicted the fact 
					hitsHistogram.increase(line.size() - 1);
					nextIsHit = false;
				}
				
				allPredictionsHistogram.increase(line.size() - 1);
			}
		}

		System.out.println("Hits histogram");
		printHistogram(hitsHistogram);
		
		System.out.println("All predictions histogram");
		printHistogram(allPredictionsHistogram);
	}
}
