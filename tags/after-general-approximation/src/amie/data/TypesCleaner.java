package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.filehandlers.TSVFile;

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
	
	private static void joinWithTypesRelation(String[] files) throws IOException {
		// TODO Auto-generated method stub
		FactDatabase source1 = new FactDatabase();
		FactDatabase source2 = new FactDatabase();
		Map<ByteString, Set<ByteString>> entityToTypes = new HashMap<>();
		
		source1.load(new File(files[0]));
		source2.load(new File(files[1]));
		
		for(ByteString subject: source1.subjectSize){
			//Print all the facts about this subject
			Map<ByteString, IntHashMap<ByteString>> sourceTriples = source1.subject2predicate2object.get(subject);
			for(ByteString relation: sourceTriples.keySet()){
				for(ByteString object: sourceTriples.get(relation)){
					System.out.println(subject + "\t" + relation + "\t" + object);
				}
			}
			
			if(source2.subjectSize.get(subject) != 1){
				//and its types
				Set<ByteString> types = SchemaUtilities.getAllTypesForEntity(source2, subject);
				if(entityToTypes.containsKey(subject)){
					entityToTypes.get(subject).addAll(types);
				}else{
					entityToTypes.put(subject, types);
				}
			}
		}
		
		for(ByteString object: source1.objectSize){
			Set<ByteString> types = SchemaUtilities.getAllTypesForEntity(source2, object);
			if(entityToTypes.containsKey(object)){
				entityToTypes.get(object).addAll(types);
			}else{
				entityToTypes.put(object, types);
			}
		}
		
		for(ByteString entity: entityToTypes.keySet()){
			for(ByteString type: entityToTypes.get(entity)){
				System.out.println(entity + "\trdf:type\t" + type);
			}
		}
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
		List<File> files = new ArrayList<File>();
		for(String fileName: args){
			files.add(new File(fileName));
		}
		
		source.load(files);
			
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
	
	private static void outputUsingSchemaInformation(FactDatabase source){
		//Now for each subject and object, just pick the types we want. Assume the type hierarchy is correct
		List<ByteString> omittedRelations = new ArrayList<ByteString>();
		omittedRelations.add(ByteString.of("rdf:type"));
		omittedRelations.add(ByteString.of("<rdfs:subClassOf>"));
		omittedRelations.add(ByteString.of("<rdfs:domain>"));
		omittedRelations.add(ByteString.of("<rdfs:range>"));
		Map<ByteString, Set<ByteString>> entityTypes = new HashMap<ByteString, Set<ByteString>>();
		
		for(ByteString subject: source.subjectSize){
			Set<ByteString> finalTypes = new HashSet<>();
			
			Map<ByteString, IntHashMap<ByteString> > bindings = source.subject2predicate2object.get(subject);
			for(ByteString relation: bindings.keySet()){
				ByteString domain = SchemaUtilities.getRelationDomain(source, relation);
				if(domain != null){
					finalTypes.add(domain);
				}
			}
			
			entityTypes.put(subject, finalTypes);
		}
		
		for(ByteString object: source.objectSize){
			Set<ByteString> finalTypes = entityTypes.get(object);

			if(finalTypes == null){
				finalTypes = new HashSet<ByteString>();
				entityTypes.put(object, finalTypes);
			}				
			
			Map<ByteString, IntHashMap<ByteString> > bindings = source.object2predicate2subject.get(object);
			for(ByteString relation: bindings.keySet()){
				ByteString range = SchemaUtilities.getRelationRange(source, relation);
				if(range != null)
					finalTypes.add(range);
			}
		}
		
		//Print domain and schema information
		for(ByteString relation: source.predicateSize){
			ByteString domain = SchemaUtilities.getRelationDomain(source, relation);			
			if(domain != null)
				System.out.println(relation + "\t<rdfs:domain>\t" + domain);	
		}
		
		for(ByteString entity: entityTypes.keySet()){
			for(ByteString type: entityTypes.get(entity)){
				System.out.println(entity + "\trdf:type\t" + type);
			}
		}
		
		for(ByteString relation: source.predicateSize){
			ByteString range = SchemaUtilities.getRelationRange(source, relation);			
			if(range != null)			
				System.out.println(relation + "\t<rdfs:range>\t" + range);	
		}
		
		for(ByteString subject: source.subjectSize){
			Map<ByteString, IntHashMap<ByteString> > bindings = source.subject2predicate2object.get(subject);
						
			for(ByteString predicate: bindings.keySet()){
				IntHashMap<ByteString> objectBindings = bindings.get(predicate);
				
				for(ByteString object: objectBindings){
					if(!omittedRelations.contains(predicate)){
						System.out.println(subject + "\t" + predicate + "\t" + object);
					}
				}
			}
		}
	}
	
	private static void cleanWithRelationSignatureTypes(String[] args) throws IOException {
		FactDatabase source = new FactDatabase();		
		source.load(new File(args[0]));
		Map<ByteString, ByteString> domainsMap = new HashMap<ByteString, ByteString>();
		Map<ByteString, ByteString> rangesMap = new HashMap<ByteString, ByteString>();		
		//Build simple maps for the domain and ranges of relations	
		for(ByteString relation: source.predicateSize){
			ByteString domain = SchemaUtilities.getRelationDomain(source, relation);
			if(domain !=  null)			
				domainsMap.put(relation, domain);			
			
			ByteString range = SchemaUtilities.getRelationRange(source, relation);
			if(range !=  null)
				rangesMap.put(relation, range);
		}
		
		outputUsingSchemaInformation(source);
	}


	private static void cleanWithFixedRelationSignatureTypes(String[] args) throws IOException {
		FactDatabase source = new FactDatabase();		
		source.load(new File(args[1]));		
		Map<ByteString, ByteString> domainsMap = new HashMap<ByteString, ByteString>();
		Map<ByteString, ByteString> rangesMap = new HashMap<ByteString, ByteString>();
		
		//Interpret the first argument as the fixed domain and ranges
		TSVFile tsv = new TSVFile(new File(args[0]));
		for(List<String> record: tsv){
			domainsMap.put(ByteString.of(record.get(0)), ByteString.of(record.get(1)));
			rangesMap.put(ByteString.of(record.get(0)), ByteString.of(record.get(2)));			
		}
		tsv.close();
		outputUsingSignatureMaps(source, domainsMap, rangesMap);
	}	


	private static void outputUsingSignatureMaps(FactDatabase source, Map<ByteString, ByteString> domainsMap, Map<ByteString, ByteString> rangesMap) {
		//Now for each subject and object, just pick the types we want. Assume the type hierarchy is correct
		List<ByteString> omittedRelations = new ArrayList<ByteString>();
		omittedRelations.add(ByteString.of("rdf:type"));
		omittedRelations.add(ByteString.of("<rdfs:subClassOf>"));
		omittedRelations.add(ByteString.of("<rdfs:domain>"));
		omittedRelations.add(ByteString.of("<rdfs:range>"));
		Map<ByteString, Set<ByteString>> entityTypes = new HashMap<ByteString, Set<ByteString>>();
		
		for(ByteString subject: source.subjectSize){
			Set<ByteString> possibleTypes = new HashSet<>();
			Set<ByteString> finalTypes = new HashSet<>();
			
			Map<ByteString, IntHashMap<ByteString> > bindings = source.subject2predicate2object.get(subject);
			for(ByteString relation: bindings.keySet()){
				ByteString domain = domainsMap.get(relation);
				if(domain != null){
					possibleTypes.add(domain);
				}else{
					//Take it from the database
					domain = SchemaUtilities.getRelationDomain(source, relation);
					if(domain != null)
						finalTypes.add(domain);
				}
			}
			
			for(ByteString type: possibleTypes){
				Set<ByteString> inferredTypes = SchemaUtilities.getAllTypesForEntity(source, subject);
				if(inferredTypes.contains(type))
					finalTypes.add(type);
			}
			
			entityTypes.put(subject, finalTypes);
		}
		
		for(ByteString object: source.objectSize){
			Set<ByteString> possibleTypes = new HashSet<>();
			Set<ByteString> finalTypes = entityTypes.get(object);
						
			if(finalTypes == null){
				finalTypes = new HashSet<ByteString>();
				entityTypes.put(object, finalTypes);
			}				
			
			Map<ByteString, IntHashMap<ByteString> > bindings = source.object2predicate2subject.get(object);
			for(ByteString relation: bindings.keySet()){
				ByteString range = rangesMap.get(relation);
				if(range != null){
					possibleTypes.add(range);
				}else{
					range = SchemaUtilities.getRelationRange(source, relation);
					if(range != null){
						finalTypes.add(range);
					}
				}
			}
			
			for(ByteString type: possibleTypes){
				Set<ByteString> inferredTypes = SchemaUtilities.getAllTypesForEntity(source, object);
				if(inferredTypes.contains(type))
					finalTypes.add(type);	
			}
		}
		
		//Print domain and schema information
		for(ByteString relation: source.predicateSize){
			ByteString domain = domainsMap.get(relation);
			if(domain == null)
				domain = SchemaUtilities.getRelationDomain(source, relation);
			
			if(domain != null)
				System.out.println(relation + "\t<rdfs:domain>\t" + domain);	
		}
		
		for(ByteString entity: entityTypes.keySet()){
			for(ByteString type: entityTypes.get(entity)){
				System.out.println(entity + "\trdf:type\t" + type);
			}
		}
		
		for(ByteString relation: source.predicateSize){
			ByteString range = rangesMap.get(relation);
			if(range == null)
				range = SchemaUtilities.getRelationRange(source, relation);
			
			if(range != null)			
				System.out.println(relation + "\t<rdfs:range>\t" + range);	
		}
		
		for(ByteString subject: source.subjectSize){
			Map<ByteString, IntHashMap<ByteString> > bindings = source.subject2predicate2object.get(subject);
						
			for(ByteString predicate: bindings.keySet()){
				IntHashMap<ByteString> objectBindings = bindings.get(predicate);
				
				for(ByteString object: objectBindings){
					if(!omittedRelations.contains(predicate)){
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
		case "join":
			joinWithTypesRelation(files);
			break;
		case "all":
			cleanWithAllTypes(files);
			break;
		case "good":
			cleanWithGoodTypes(files);
			break;
		case "single":
			cleanWithSingleType(files);
			break;
		case "signature":
			cleanWithRelationSignatureTypes(files);
			break;
		case "fixed":
			cleanWithFixedRelationSignatureTypes(files);
			break;
		default:
			System.out.println("First argument must be either: all|good|single|signature|fixed");
			break;
		}
	}
}
