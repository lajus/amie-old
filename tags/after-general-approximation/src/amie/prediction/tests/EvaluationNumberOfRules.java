package amie.prediction.tests;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import amie.utils.Utils;
import javatools.datatypes.Triple;
import javatools.filehandlers.TSVFile;

public class EvaluationNumberOfRules {

	public static void main(String[] args) throws IOException {
		try (TSVFile tsvFile = new TSVFile(new File(args[0]))) {
			Map<Triple<String, String, String>, List<String>> trueTriplesToRules = 
					new HashMap<>();
			Map<Triple<String, String, String>, List<String>> falseTriplesToRules = 
							new HashMap<>();
			Map<Triple<String, String, String>, List<String>> unknownTriplesToRules = 
								new HashMap<>();							
			for (List<String> line : tsvFile) {
				if (line.size() < 6) {
					continue;
				}
				String ruleText = line.get(0);
				Triple<String, String, String> triple = new Triple<>(line.get(1), line.get(2), line.get(3));
				String evaluation = line.get(5);
				if (evaluation.toLowerCase().equals("true")) {
					Utils.add2Map(trueTriplesToRules, triple, ruleText);
				} else if (evaluation.toLowerCase().equals("false")) {
					Utils.add2Map(falseTriplesToRules, triple, ruleText);
				} else {
					Utils.add2Map(unknownTriplesToRules, triple, ruleText);
				}
			}
			
			System.out.println("True triples");
			Utils.printHistogram(Utils.buildHistogram(trueTriplesToRules));
			System.out.println("False triples");
			Utils.printHistogram(Utils.buildHistogram(falseTriplesToRules));
			System.out.println("Unknown triples");
			Utils.printHistogram(Utils.buildHistogram(unknownTriplesToRules));
		}

	}

}
