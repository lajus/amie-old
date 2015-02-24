package arm.data;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

public class TypesCleaner {
	
	public static ByteString[] keyTypes = { ByteString.of(" <wordnet_person_100007846>"),
		ByteString.of("<wordnet_organization_108008335>"),
		ByteString.of("<wordnet_building_102913152>"),
		ByteString.of("<yagoGeoEntity>"),
		ByteString.of("<wordnet_artifact_100021939>"),
		ByteString.of("<wordnet_abstraction_100002137>"),
		ByteString.of("<wordnet_physical_entity_100001930>")  
	};
	
	public static Set<ByteString> getSecondLevelTypes(FactDatabase source){
		Set<ByteString> resultTypes = new HashSet<>();
		List<ByteString[]> query1 = FactDatabase.triples(FactDatabase.triple("?x", "<rdfs:subClassOf>", "?s"));
		List<ByteString[]> query2 = FactDatabase.triples(FactDatabase.triple("?x", "<rdfs:subClassOf>", "?s"), FactDatabase.triple("?s", "<rdfs:subClassOf>", "?m"));
		
		Set<ByteString> typesWithChildren = source.selectDistinct(ByteString.of("?s"), query1);
		
		for(ByteString type: typesWithChildren){
			query2.get(1)[2] = type;
			if(source.countDistinct(ByteString.of("?x"), query2) == 0){
				resultTypes.add(type);
			}
		}
		
		return resultTypes;
	}
	
	public static Set<ByteString> getAllSuperTypes(FactDatabase source){
		List<ByteString[]> query1 = FactDatabase.triples(FactDatabase.triple("?x", "<rdfs:subClassOf>", "?s"));		
		return source.selectDistinct(ByteString.of("?s"), query1);
	}
	
	public static Set<ByteString> getSuperTypes(FactDatabase source, ByteString type){
		List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple(type, "<rdfs:subClassOf>", "?x"));		
		return new LinkedHashSet<ByteString>(source.selectDistinct(ByteString.of("?x"), query));
	}
	
	public static boolean isLeafDatatype(FactDatabase source, ByteString type){
		List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple("?x", "<rdfs:subClassOf>", type));		
		return source.countDistinct(ByteString.of("?x"), query) == 0;
	}
		
	private static Set<ByteString> calculateFilteredDeductiveClosure(FactDatabase source, ByteString type) {
		Set<ByteString> resultSet = new HashSet<ByteString>();
		Queue<ByteString> queue = new LinkedList<>();
		queue.addAll(getSuperTypes(source, type));
		List<ByteString> goodTypes = Arrays.asList(keyTypes);
		
		while(!queue.isEmpty()){
			ByteString currentType = queue.poll();
			if(goodTypes.contains(currentType)){
				resultSet.add(currentType);
			}
			queue.addAll(getSuperTypes(source, currentType));
		}
		
		return resultSet;
	}
	
	private static Set<ByteString> calculateDeductiveClosure(FactDatabase source, ByteString type) {
		Set<ByteString> resultSet = new HashSet<ByteString>();
		Queue<ByteString> queue = new LinkedList<>();
		queue.addAll(getSuperTypes(source, type));
		
		while(!queue.isEmpty()){
			ByteString currentType = queue.poll();
			resultSet.add(currentType);
			queue.addAll(getSuperTypes(source, currentType));
		}
		
		return resultSet;
	}
	
	//All penultimate types (the second deepest types)
	public static void cleanWithSingleType(String[] args) throws IOException{
		// TODO Auto-generated method stub
		FactDatabase source = new FactDatabase();
		
		source.load(new File(args[0]));
		
		Set<ByteString> goodTypes = getSecondLevelTypes(source);
		
		for(ByteString subject: source.subjectSize){
			Map<ByteString, IntHashMap<ByteString> > bindings = source.subject2predicate2object.get(subject);
						
			for(ByteString predicate: bindings.keySet()){
				IntHashMap<ByteString> objectBindings = bindings.get(predicate);
				
				for(ByteString object: objectBindings){
					if(predicate.equals(ByteString.of("rdf:type"))){
						//Only allow the good type
						if(goodTypes.contains(object)){
							System.out.println(subject + "\t" + predicate + "\t" + object);
						}
					}else{
						System.out.println(subject + "\t" + predicate + "\t" + object);
					}							
				}
			}
		}
	}
	
	
	public static void cleanWithAllTypes(String[] args) throws IOException{
		// TODO Auto-generated method stub
		FactDatabase source = new FactDatabase();
		
		source.load(new File(args[0]));
			
		for(ByteString subject: source.subjectSize){
			Set<ByteString> knownTypes = new HashSet<>();
			Map<ByteString, IntHashMap<ByteString> > bindings = source.subject2predicate2object.get(subject);
						
			for(ByteString predicate: bindings.keySet()){
				IntHashMap<ByteString> objectBindings = bindings.get(predicate);
				
				for(ByteString object: objectBindings){
					if(predicate.equals(ByteString.of("rdf:type"))){						
						if(isLeafDatatype(source, object)){
							//Calculate the deductive closure
							Set<ByteString> deductiveClosure = calculateDeductiveClosure(source, object);
							for(ByteString supertype: deductiveClosure){								
								if(!knownTypes.contains(supertype)){
									knownTypes.add(supertype);
									System.out.println(subject + "\t" + predicate + "\t" + supertype);
								}
							}
						}else{
							if(!knownTypes.contains(object)){
								knownTypes.add(object);
								System.out.println(subject + "\t" + predicate + "\t" + object);
							}
						}
					}else{
						System.out.println(subject + "\t" + predicate + "\t" + object);
					}							
				}
			}
		}
	}
	
	public static void cleanWithGoodTypes(String[] args) throws IOException{
		// TODO Auto-generated method stub
		FactDatabase source = new FactDatabase();		
		source.load(new File(args[0]));
			
		for(ByteString subject: source.subjectSize){
			Set<ByteString> knownTypes = new HashSet<>();
			Map<ByteString, IntHashMap<ByteString> > bindings = source.subject2predicate2object.get(subject);
						
			for(ByteString predicate: bindings.keySet()){
				IntHashMap<ByteString> objectBindings = bindings.get(predicate);
				
				for(ByteString object: objectBindings){
					if(predicate.equals(ByteString.of("rdf:type"))){						
						if(isLeafDatatype(source, object)){
							//Go up in the deductive closure and take only the key ones
							Set<ByteString> deductiveClosure = calculateFilteredDeductiveClosure(source, object);
							for(ByteString supertype: deductiveClosure){								
								if(!knownTypes.contains(supertype)){
									knownTypes.add(supertype);
									System.out.println(subject + "\t" + predicate + "\t" + supertype);
								}
							}
							
							//Be sure we take the second level guys
							Set<ByteString> secondLevelTypes = getSuperTypes(source, object);
							for(ByteString supertype: secondLevelTypes){
								if(!knownTypes.contains(supertype)){
									knownTypes.add(supertype);
									System.out.println(subject + "\t" + predicate + "\t" + supertype);
								}
							}							
						}else{
							//Whatever other type which YAGO2 considers important besides the low levels
							if(!knownTypes.contains(object)){
								knownTypes.add(object);
								System.out.println(subject + "\t" + predicate + "\t" + object);
							}
						}
					}else{
						System.out.println(subject + "\t" + predicate + "\t" + object);
					}							
				}
			}
		}		
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String[] files = Arrays.copyOfRange(args, 1, args.length);
		switch(args[0]){
		case "all":
			cleanWithAllTypes(files);
			break;
		case "good":
			cleanWithGoodTypes(files);
			break;
		case "single":
			cleanWithSingleType(files);
			break;
		default:
			System.out.println("First argument must be either: all|good|single");
			break;
		}
	}
}
