package amie.data;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Pair;
import javatools.datatypes.Triple;

public class OutputSignedRelationPhrases {

	public OutputSignedRelationPhrases() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FactDatabase db = new FactDatabase();
		List<File> files = new ArrayList<File>();
		for (int i = 0; i < args.length; ++i) {
			files.add(new File(args[i]));
		}
		db.load(files);
		Map<Triple<ByteString, ByteString, ByteString>, Set<Pair<ByteString, ByteString>>> signedRelations = 
				buildSignedRelations(db);
		List<Triple<ByteString, ByteString, ByteString>> signatures = new ArrayList<>(signedRelations.keySet());
		FileWriter out = new FileWriter(new File("relations.txt"));
		PrintWriter pWriter = new PrintWriter (out);
		for (int i = 0; i < signatures.size(); ++i) {
			pWriter.println(signatures.get(i).toString());
			Set<Pair<ByteString, ByteString>> seti = signedRelations.get(signatures.get(i));
			if (seti.size() < 3) {
				continue;
			}
			Set<ByteString> subjectsI = projectPairSet(seti);
			for (int j = i + 1; j < signatures.size(); ++j) {
				Set<Pair<ByteString, ByteString>> setj = signedRelations.get(signatures.get(j));				
				if (setj.size() < 3) {
					continue;
				}
				int intsr = intersectionSize(seti, setj);
				Set<ByteString> subjectsJ = projectPairSet(setj);
				int pcaCount1 = pcaIntersection(seti, subjectsJ); // ri => rj
				int pcaCount2 = pcaIntersection(setj, subjectsI); // rj => ri
				if (intsr > 1) {
					System.out.println(signatures.get(i) + "\t" + signatures.get(j) 
							+ "\t" + intsr + "\t" + seti.size() + "\t" + setj.size() + "\t" 
							+ pcaCount1 + "\t" + pcaCount2 + "\t" + ((double)intsr / pcaCount1) 
							+ "\t" + ((double)intsr / pcaCount2));
				}
			}
		}
		out.close();
	}

	private static int pcaIntersection(
			Set<Pair<ByteString, ByteString>> seti,
			Set<ByteString> subjectsJ) {
		int count = 0;
		for (Pair<ByteString, ByteString> pair : seti) {
			if (subjectsJ.contains(pair.first)) {
				++count;
			}
		}
		
		return count;
	}

	private static Set<ByteString> projectPairSet(
			Set<Pair<ByteString, ByteString>> seti) {
		Set<ByteString> result = new LinkedHashSet<>();
		for (Pair<ByteString, ByteString> pair : seti) {
			result.add(pair.first);
		}
		return result;
	}

	private static int intersectionSize(
			Set<Pair<ByteString, ByteString>> set,
			Set<Pair<ByteString, ByteString>> set2) {
		int count = 0;
		Set<Pair<ByteString, ByteString>> smaller = set.size() < set2.size() ? set : set2;
		Set<Pair<ByteString, ByteString>> bigger = smaller == set ? set2 : set;
		for (Pair<ByteString, ByteString> pair : smaller) {
			if (bigger.contains(pair)) {
				++count;
			}
		}
		
		return count;
	}

	private static Map<Triple<ByteString, ByteString, ByteString>, Set<Pair<ByteString, ByteString>>> buildSignedRelations(
			FactDatabase db) {
		// TODO Auto-generated method stub
		Map<Triple<ByteString, ByteString, ByteString>, Set<Pair<ByteString, ByteString>>> result 
		= new HashMap<Triple<ByteString, ByteString, ByteString>, Set<Pair<ByteString, ByteString>>>();
		ByteString typeRelation = ByteString.of("<rdf:type>");
		ByteString defaultStr = ByteString.of("default");
		for (ByteString relation : db.predicateSize) {
			if (!relation.equals(typeRelation)) {
				Map<ByteString, IntHashMap<ByteString>> tail = db.predicate2subject2object.get(relation);
				for (ByteString subject : tail.keySet()) {
					for (ByteString object : tail.get(subject)) {
						// Get the types
						IntHashMap<ByteString> subjectTypes = 
								db.predicate2subject2object.get(typeRelation).get(subject);
						IntHashMap<ByteString> objectTypes = 
								db.predicate2subject2object.get(typeRelation).get(object);
						if (subjectTypes == null) {
							subjectTypes = new IntHashMap<>();
							subjectTypes.add(defaultStr);
						}
						
						if (objectTypes == null) {
							objectTypes = new IntHashMap<>();
							objectTypes.add(defaultStr);
						}
						
						for (ByteString domain : subjectTypes) {
							for (ByteString range : objectTypes) {
								Triple<ByteString, ByteString, ByteString> triple = 
										new Triple<ByteString, ByteString, ByteString>(relation, domain, range);
								Set<Pair<ByteString, ByteString>> pairs = result.get(triple);
								if (pairs == null) {
									pairs = new LinkedHashSet<Pair<ByteString, ByteString>>();
									result.put(triple, pairs);
								}
								pairs.add(new Pair<>(subject, object));
							}
						}
					}
				}
			}
		}
		
		return result;
	}

}
