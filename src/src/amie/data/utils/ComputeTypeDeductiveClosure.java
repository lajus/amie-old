package amie.data.utils;

import java.io.IOException;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Map;

import amie.data.KB;
import amie.data.U;
import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

public class ComputeTypeDeductiveClosure {

	/**
	 * Given the instance information of a KB and its type hierarchy (subclass relationships), it computes
	 * the deductive closure.
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		KB kb = U.loadFiles(args);
		Map<ByteString, IntHashMap<ByteString>> allEntitiesAndTypes = 
				kb.resultsTwoVariables("?s", "?o", new String[]{"?s", amie.data.U.typeRelation, "?o"});
		for (ByteString entity : allEntitiesAndTypes.keySet()) {
			Set<ByteString> superTypes = new LinkedHashSet<>();
			for (ByteString type : allEntitiesAndTypes.get(entity)) {
				superTypes.addAll(U.getAllSuperTypes(kb, type));	
			}
			// And be sure we add only the new ones
			superTypes.removeAll(allEntitiesAndTypes.get(entity));
			output(entity, superTypes);
		}
	}

	/**
	 * Outputs statements of the form entity rdf:type type in TSV format
	 * @param entity
	 * @param superTypes
	 */
	private static void output(ByteString entity, Set<ByteString> superTypes) {
		for (ByteString type : superTypes) {
			System.out.println(entity + "\t" + amie.data.U.typeRelation + "\t" + type);
		}
	}
}
