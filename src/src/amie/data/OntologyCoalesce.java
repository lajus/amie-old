package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;


/**
 * It joins 2 ontologies which are linked via owl:sameAs statements for the entities
 * 
 * @author lgalarra
 *
 */
public class OntologyCoalesce {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(args.length < 2){
			System.err.println("OntologyCoalesce [--with-objects] source1 source2");
			System.exit(1);
		}
		
		FactDatabase source1, source2;
		
		source1 = new FactDatabase();
		source2 = new FactDatabase();
		
		source1.load(new File(args[1]));
		source2.load(new File(args[2]));
		
		coalesce(source1, source2, args[0].equals("--with-objects"));
		

	}

	private static void coalesce(FactDatabase source1, FactDatabase source2, boolean withObjs) {
		Set<ByteString> sourceEntities = new LinkedHashSet<>();
		sourceEntities.addAll(source1.subjectSize);
		sourceEntities.addAll(source1.objectSize);
		for(ByteString entity: sourceEntities){
			//Print all facts of the source ontology
			Map<ByteString, IntHashMap<ByteString>> tail1 = source1.subject2predicate2object.get(entity);
			Map<ByteString, IntHashMap<ByteString>> tail2 = source2.subject2predicate2object.get(entity);
			if(tail2 == null)
				continue;
						
			for(ByteString predicate: tail1.keySet()){
				for(ByteString object: tail1.get(predicate)){
					System.out.println(entity + "\t" + predicate + "\t" + object);
				}
			}
			//Print all facts in the target ontology
			for(ByteString predicate: tail2.keySet()){
				for(ByteString object: tail2.get(predicate)){
					System.out.println(entity + "\t" + predicate + "\t" + object);
				}
			}
		}
		
		if(withObjs){
			for(ByteString entity: source2.objectSize){
				if(sourceEntities.contains(entity)) continue;
				
				Map<ByteString, IntHashMap<ByteString>> tail2 = source2.subject2predicate2object.get(entity);
				if(tail2 == null) continue;
				
				//Print all facts in the target ontology
				for(ByteString predicate: tail2.keySet()){
					for(ByteString object: tail2.get(predicate)){
						System.out.println(entity + "\t" + predicate + "\t" + object);
					}
				}
			}
		}
	}
}