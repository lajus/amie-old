package amie.data;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

/**
 * Set of commonly used functions.
 * 
 * @author lgalarra
 *
 */
public class U {
	
	/** X rdf:type Class **/
	public static String typeRelation = "rdf:type";
	
	public static ByteString typeRelationBS = ByteString.of(typeRelation);
	
	/** Class1 rdfs:subClassOf Class2 **/
	public static String subClassRelation = "rdfs:subClassOf";
	
	public static ByteString subClassRelationBS = ByteString.of(subClassRelation);
	
	/** relation1 rdfs:subPropertyOf relation2 **/
	public static String subPropertyRelation = "rdfs:subPropertyOf";
	
	public static ByteString subPropertyRelationBS = ByteString.of(subPropertyRelation);
	
	/** Class rdfs:domain relation **/
	public static String domainRelation = "rdfs:domain";
	
	public static ByteString domainRelationBS = ByteString.of(domainRelation);
	
	/** Class rdfs:domain range **/
	public static String rangeRelation = "rdfs:range";
	
	public static ByteString rangeRelationBS = ByteString.of(rangeRelation);
	
	public static void loadSchemaConf() {
		try {
			List<String> lines = Files.readAllLines(Paths.get("conf/schema_properties"),
			        Charset.defaultCharset());
			for (String line : lines) {
				String[] lineParts = line.split("=");
				if (lineParts.length < 2)
					continue;
				try {
					amie.data.U.class.getField(lineParts[0]).set(null, lineParts[1]);
					amie.data.U.class.getField(lineParts[0] + "BS").set(null, ByteString.of(lineParts[1]));
				} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
						| SecurityException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			System.err.println("Using the default schema relations");
		}
		
	}

	/**
	 * Returns the domain of a given relation in a knowledge base
	 * @param source
	 * @param relation
	 * @return
	 */
	public static ByteString getRelationDomain(KB source, ByteString relation){
		List<ByteString[]> query = KB.triples(KB.triple(relation, domainRelation, "?x"));
		Set<ByteString> domains = source.selectDistinct(ByteString.of("?x"), query);
		if(!domains.isEmpty()){
			return domains.iterator().next();
		}
		
		//Try looking for the superproperty
		List<ByteString[]> query2 = KB.triples(KB.triple(relation, subPropertyRelation, "?y"), 
				KB.triple("?y", "rdfs:domain", "?x"));
		
		domains = source.selectDistinct(ByteString.of("?x"), query2);
		if(!domains.isEmpty()){
			return domains.iterator().next();
		}
		
		return null;
	}
	
	/**
	 * Returns the range of a given relation in a knowledge base.
	 * @param source
	 * @param relation
	 * @return
	 */
	public static ByteString getRelationRange(KB source, ByteString relation){
		List<ByteString[]> query = KB.triples(KB.triple(relation, rangeRelation, "?x"));
		Set<ByteString> ranges = source.selectDistinct(ByteString.of("?x"), query);
		if(!ranges.isEmpty()){
			return ranges.iterator().next();
		}
		
		//Try looking for the superproperty
		List<ByteString[]> query2 = KB.triples(KB.triple(relation, subPropertyRelation, "?y"), 
				KB.triple("?y", "rdfs:range", "?x"));
		
		ranges = source.selectDistinct(ByteString.of("?x"), query2);
		if(!ranges.isEmpty()){
			return ranges.iterator().next();
		}
		
		return null;		
	}
	
	/**
	 * It returns all the materialized types of an entity in a knowledge base.
	 * @param source
	 * @param entity
	 * @return
	 */
	public static Set<ByteString> getMaterializedTypesForEntity(KB source, ByteString entity){
		List<ByteString[]> query = KB.triples(KB.triple(entity, typeRelation, "?x"));
		return source.selectDistinct(ByteString.of("?x"), query);
	}
	
	/**
	 * Determines whether a given type is specific, that is, it does not have subclasses.
	 * @param source
	 * @param type
	 * @return
	 */
	public static boolean isLeafDatatype(KB source, ByteString type){
		List<ByteString[]> query = KB.triples(KB.triple("?x", subClassRelation, type));		
		return source.countDistinct(ByteString.of("?x"), query) == 0;
	}
	
	/**
	 * It returns the most specific types of an entity according to the type hierarchy
	 * of the knowledge base.
	 * @param source
	 * @param entity
	 * @return
	 */
	public static Set<ByteString> getLeafTypesForEntity(KB source, ByteString entity){
		Set<ByteString> tmpTypes = getMaterializedTypesForEntity(source, entity);
		Set<ByteString> resultTypes = new HashSet<ByteString>();
		
		for(ByteString type: tmpTypes){
			if(isLeafDatatype(source, type)){
				resultTypes.add(type);
			}
		}
		
		return resultTypes;
	}
	
	/**
	 * It returns all the types of a given entity.
	 * @param source
	 * @param entity
	 * @return
	 */
	public static Set<ByteString> getAllTypesForEntity(KB source, ByteString entity){
		Set<ByteString> leafTypes = getMaterializedTypesForEntity(source, entity);
		Set<ByteString> resultTypes = new HashSet<ByteString>(leafTypes);
		for(ByteString leafType: leafTypes){
			resultTypes.addAll(getAllSuperTypes(source, leafType));
		}
		return resultTypes;
	}
	
	/**
	 * It returns all the immediate super-types of a given type.
	 * @param source
	 * @param type
	 * @return
	 */
	public static Set<ByteString> getSuperTypes(KB source, ByteString type){
		List<ByteString[]> query = KB.triples(KB.triple(type, subClassRelation, "?x"));		
		return new LinkedHashSet<ByteString>(source.selectDistinct(ByteString.of("?x"), query));
	}
	
	/**
	 * It returns all the supertypes of a given type.
	 * @param source
	 * @param type
	 * @return
	 */
	public static Set<ByteString> getAllSuperTypes(KB source, ByteString type) {
		Set<ByteString> resultSet = new LinkedHashSet<ByteString>();
		Queue<ByteString> queue = new LinkedList<>();
		Set<ByteString> seenTypes = new LinkedHashSet<>();
		Set<ByteString> superTypes = getSuperTypes(source, type);
		queue.addAll(superTypes);
		seenTypes.addAll(superTypes);
		
		while (!queue.isEmpty()) {
			ByteString currentType = queue.poll();
			resultSet.add(currentType);
			superTypes = getSuperTypes(source, currentType);
			boolean proceed = true;
			for (ByteString st : superTypes) {
				if (seenTypes.contains(st)) {
					proceed = false;
				} else {
					seenTypes.add(st);
				}
			}
			if (proceed)
				queue.addAll(superTypes);
		}
		
		return resultSet;
	}
	
	/**
	 * It returns all the instances of a given type.
	 * @param source
	 * @param type
	 * @return
	 */
	public static Set<ByteString> getAllEntitiesForType(KB source, ByteString type) {
		List<ByteString[]> query = KB.triples(KB.triple("?x", typeRelation, type));		
		return new LinkedHashSet<ByteString>(source.selectDistinct(ByteString.of("?x"), query));	
	}
	
	/**
	 * Returns all present data types in the given KB
	 * @param kb
	 */
	public static Set<ByteString> getAllTypes(KB kb) {
		List<ByteString[]> query = KB.triples(KB.triple("?x", typeRelation, "?type"));		
		return new LinkedHashSet<ByteString>(kb.selectDistinct(ByteString.of("?type"), query));	
	}
	
	/**
	 * Gets all the entities of the type of the given relation's domain.
	 * @param source
	 * @param relation
	 * @return
	 */
	public static Set<ByteString> getDomainSet(KB source, ByteString relation) {
		ByteString domainType = getRelationDomain(source, relation);
		Set<ByteString> result = new LinkedHashSet<ByteString>();
		if (domainType != null) 
			result.addAll(getAllEntitiesForType(source, domainType));
		result.addAll(source.relation2subject2object.get(relation).keySet());
		return result;
	}
	
	/**
	 * Get all the immediate subtypes of a given type.
	 * @param source
	 * @param type
	 * @return
	 */
	public static Set<ByteString> getSubtypes(KB source, ByteString type) {
		List<ByteString[]> query = KB.triples(KB.triple(ByteString.of("?x"), subClassRelation, type));		
		return new LinkedHashSet<ByteString>(source.selectDistinct(ByteString.of("?x"), query));	
	}
	
	/**
	 * Gets all the entities of the type of the given relation's range.
	 * @param source
	 * @param relation
	 * @return
	 */
	public static Set<ByteString> getRangeSet(KB source, ByteString relation) {
		ByteString rangeType = getRelationRange(source, relation);
		Set<ByteString> result = new LinkedHashSet<ByteString>();
		if (rangeType != null) 
			result.addAll(getAllEntitiesForType(source, rangeType));
		result.addAll(source.relation2object2subject.get(relation).keySet());
		return result;
	}
	
	/**
	 * 
	 * @param source1
	 * @param source2
	 * @param withObjs
	 */
	public static void coalesce(KB source1, 
			KB source2, boolean withObjs) {
		Set<ByteString> sourceEntities = new LinkedHashSet<>();
		sourceEntities.addAll(source1.subjectSize);
		sourceEntities.addAll(source1.objectSize);
		for(ByteString entity: sourceEntities){
			//Print all facts of the source ontology
			Map<ByteString, IntHashMap<ByteString>> tail1 = source1.subject2relation2object.get(entity);
			Map<ByteString, IntHashMap<ByteString>> tail2 = source2.subject2relation2object.get(entity);
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
				
				Map<ByteString, IntHashMap<ByteString>> tail2 = source2.subject2relation2object.get(entity);
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
	
	/**
	 * 
	 * @param source
	 */
	public static void printOverlapTable(KB source) {
		//for each pair of relations, print the overlap table
		System.out.println("Relation1\tRelation2\tRelation1-subjects"
				+ "\tRelation1-objects\tRelation2-subjects\tRelation2-objects"
				+ "\tSubject-Subject\tSubject-Object\tObject-Subject\tObject-Object");
		for(ByteString r1: source.relationSize){
			Set<ByteString> subjects1 = source.relation2subject2object.get(r1).keySet();
			Set<ByteString> objects1 = source.relation2object2subject.get(r1).keySet();
			int nSubjectsr1 = subjects1.size();
			int nObjectsr1 = objects1.size();
			for(ByteString r2: source.relationSize){
				if(r1.equals(r2))
					continue;				
				System.out.print(r1 + "\t");
				System.out.print(r2 + "\t");
				Set<ByteString> subjects2 = source.relation2subject2object.get(r2).keySet();
				Set<ByteString> objects2 = source.relation2object2subject.get(r2).keySet();
				int nSubjectr2 = subjects2.size();
				int nObjectsr2 = objects2.size();
				System.out.print(nSubjectsr1 + "\t" + nObjectsr1 + "\t" + nSubjectr2 + "\t" + nObjectsr2 + "\t");
				System.out.print(computeOverlap(subjects1, subjects2) + "\t");
				System.out.print(computeOverlap(subjects1, objects2) + "\t");
				System.out.print(computeOverlap(subjects2, objects1) + "\t");
				System.out.println(computeOverlap(objects1, objects2));
			}
		}		
	}
	
	
	/**
	 * Returns a KB with the content of all the files referenced in the string array.
	 * @param args
	 * @return
	 * @throws IOException
	 */
	public static KB loadFiles(String args[]) throws IOException {
		// Load the data
		KB kb = new KB();
		List<File> files = new ArrayList<File>();
		for (int i = 0; i < args.length; ++i) {
			files.add(new File(args[i]));
		}
		kb.load(files);
		return kb;
	}

	/**
	 * 
	 * @param subjects1
	 * @param subjects2
	 * @return
	 */
	private static int computeOverlap(Set<ByteString> subjects1,
			Set<ByteString> subjects2) {
		int overlap = 0; 
		for(ByteString entity1 : subjects1){
			if(subjects2.contains(entity1))
				++overlap;
		}
		
		return overlap;
	}
	
	public static IntHashMap<Integer> getTypedHistogram(KB kb, ByteString relation) {
		IntHashMap<Integer> hist = new IntHashMap<>();
		List<ByteString[]> query = null;
		String queryVar = null;
		String existVar = null;
		ByteString targetType = null;
	
		if (kb.isFunctional(relation)) {
			queryVar = "?s";
			existVar = "?o";
			query = KB.triples(KB.triple("?s", relation, "?o"));
			targetType = getRelationDomain(kb, relation);
		} else {
			queryVar = "?o";
			existVar = "?s";
			query = KB.triples(KB.triple("?o", relation, "?s"));
			targetType = getRelationRange(kb, relation);
		}
		
		if (targetType == null) {
			return hist;
		}
		
		Set<ByteString> effectiveDomain = kb.selectDistinct(ByteString.of(queryVar), query);
		Set<ByteString> theorethicalDomain = getAllEntitiesForType(kb, targetType);
		effectiveDomain.retainAll(theorethicalDomain);
		for (ByteString entity : effectiveDomain) {
			long val;
			if (kb.isFunctional(relation)) {
				val = kb.count(KB.triple(entity, relation, ByteString.of(existVar)));
			} else {
				val = kb.count(KB.triple(ByteString.of(queryVar), relation, entity));
			}
			hist.increase((int)val);
		}
		kb.selectDistinct(ByteString.of(existVar), query);
		
		return hist;		
	}
	
	public static IntHashMap<ByteString> getTypesCount(KB kb) {
		List<ByteString[]> query = KB.triples(KB.triple("?s", typeRelation, "?o"));
		Map<ByteString, IntHashMap<ByteString>> types2Instances = 
				kb.selectDistinct(ByteString.of("?o"), ByteString.of("?s"), query);
		IntHashMap<ByteString> result = new IntHashMap<>();
		for (ByteString type : types2Instances.keySet()) {
			result.put(type, types2Instances.get(type).size());
		}
		return result;
	}

	
	public static void main(String args[]) throws IOException {
		KB d = new KB();
	    ArrayList<File> files = new ArrayList<File>();
	    for(String file: args)
	    	files.add(new File(file));
	    
	    d.load(files);
	    
	    for(ByteString relation: d.relationSize){
	    	System.out.println(relation + "\t" + getRelationDomain(d, relation) 
	    			+ "\t" + getRelationRange(d, relation));
	    }
	}

	/**
	 * It returns all the entities that occur as subjects or objects
	 * in the KB
	 * @param kb
	 * @return
	 */
	public static IntHashMap<ByteString> getAllEntities(KB kb) {
		IntHashMap<ByteString> result = new IntHashMap<>();
		result.addAll(kb.subjectSize);
		result.addAll(kb.objectSize);
		return result;
	}
}
