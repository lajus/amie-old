package amie.prediction.tests;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.Pair;
import javatools.filehandlers.TSVFile;

public class FindPotentialContradictions {
	
	private static List<String> functions = Arrays.asList("<wasBornIn>", "<isCitizenOf>", "<diedIn>", "<isPoliticianOf>", "<hasCurrency>");

	public static void main(String[] args) throws IOException {
		Map<String, Map<String, Set<Pair<String, String>>>> index = new HashMap<>();
		try(TSVFile tsvFile = new TSVFile(new File(args[0]))) {
			for (List<String> line : tsvFile) {
				if (line.size() >= 5) {
					String relation = line.get(1);
					if (functions.contains(relation)) {
						String subject = line.get(0);
						Map<String, Set<Pair<String, String>>> tail = index.get(subject);
						if (tail == null) {
							tail = new HashMap<>();
							index.put(subject, tail);
						}
						Set<Pair<String, String>> objects = tail.get(relation);
						if (objects == null) {
							objects = new LinkedHashSet<>();
							tail.put(relation, objects);
						}
						objects.add(new Pair<>(line.get(2), line.get(4)));
					}
				}
			}
		}
		
		for (String subject : index.keySet()) {
			StringBuilder strBuilder = new StringBuilder(subject + "\n");
			boolean print = false;

			Map<String, Set<Pair<String, String>>> tail = index.get(subject);
			for (String relation : tail.keySet()) {
				if (tail.get(relation).size() > 1) {
					print = true;
					for (Pair<String, String> pair : tail.get(relation)) {
						strBuilder.append(relation + "/" + pair + "\n");
					}
				}
			}
			
			if (print) {
				System.out.println(strBuilder.toString());
			}
		}
	}
}
