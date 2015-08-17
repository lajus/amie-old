package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javatools.datatypes.ByteString;

/**
 * Set of commonly used functions.
 * 
 * @author lgalarra
 *
 */
public class Utilities {

	/**
	 * Returns the domain of a given relation in a knowledge base
	 * @param source
	 * @param relation
	 * @return
	 */
	public static ByteString getRelationDomain(FactDatabase source, ByteString relation){
		List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple(relation, "rdfs:domain", "?x"));
		Set<ByteString> domains = source.selectDistinct(ByteString.of("?x"), query);
		if(!domains.isEmpty()){
			return domains.iterator().next();
		}
		
		//Try looking for the superproperty
		List<ByteString[]> query2 = FactDatabase.triples(FactDatabase.triple(relation, "rdfs:subPropertyOf", "?y"), 
				FactDatabase.triple("?y", "rdfs:domain", "?x"));
		
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
	public static ByteString getRelationRange(FactDatabase source, ByteString relation){
		List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple(relation, "rdfs:range", "?x"));
		Set<ByteString> ranges = source.selectDistinct(ByteString.of("?x"), query);
		if(!ranges.isEmpty()){
			return ranges.iterator().next();
		}
		
		//Try looking for the superproperty
		List<ByteString[]> query2 = FactDatabase.triples(FactDatabase.triple(relation, "rdfs:subPropertyOf", "?y"), 
				FactDatabase.triple("?y", "rdfs:range", "?x"));
		
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
	public static Set<ByteString> getMaterializedTypesForEntity(FactDatabase source, ByteString entity){
		List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple(entity, "rdf:type", "?x"));
		return source.selectDistinct(ByteString.of("?x"), query);
	}
	
	/**
	 * Determines whether a given type is specific, that is, it does not have subclasses.
	 * @param source
	 * @param type
	 * @return
	 */
	public static boolean isLeafDatatype(FactDatabase source, ByteString type){
		List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple("?x", "rdfs:subClassOf", type));		
		return source.countDistinct(ByteString.of("?x"), query) == 0;
	}
	
	/**
	 * It returns the most specific types of an entity according to the type hierarchy
	 * of the knowledge base.
	 * @param source
	 * @param entity
	 * @return
	 */
	public static Set<ByteString> getLeafTypesForEntity(FactDatabase source, ByteString entity){
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
	public static Set<ByteString> getAllTypesForEntity(FactDatabase source, ByteString entity){
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
	public static Set<ByteString> getSuperTypes(FactDatabase source, ByteString type){
		List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple(type, "rdfs:subClassOf", "?x"));		
		return new LinkedHashSet<ByteString>(source.selectDistinct(ByteString.of("?x"), query));
	}
	
	/**
	 * It returns all the supertypes of a given type.
	 * @param source
	 * @param type
	 * @return
	 */
	public static Set<ByteString> getAllSuperTypes(FactDatabase source, ByteString type) {
		Set<ByteString> resultSet = new LinkedHashSet<ByteString>();
		Queue<ByteString> queue = new LinkedList<>();
		queue.addAll(getSuperTypes(source, type));
		
		while(!queue.isEmpty()){
			ByteString currentType = queue.poll();
			resultSet.add(currentType);
			queue.addAll(getSuperTypes(source, currentType));
		}
		
		return resultSet;
	}
	
	/**
	 * It returns all the instances of a given type.
	 * @param source
	 * @param type
	 * @return
	 */
	public static Set<ByteString> getAllEntitiesForType(FactDatabase source, ByteString type) {
		List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple("?x", "rdf:type", type));		
		return new LinkedHashSet<ByteString>(source.selectDistinct(ByteString.of("?x"), query));	
	}
	
	/**
	 * Gets all the entities of the type of the given relation's domain.
	 * @param source
	 * @param relation
	 * @return
	 */
	public static Set<ByteString> getDomainSet(FactDatabase source, ByteString relation) {
		ByteString domainType = getRelationDomain(source, relation);
		Set<ByteString> result = new LinkedHashSet<ByteString>();
		if (domainType != null) 
			result.addAll(getAllEntitiesForType(source, domainType));
		result.addAll(source.predicate2subject2object.get(relation).keySet());
		return result;
	}
	
	/**
	 * Gets all the entities of the type of the given relation's range.
	 * @param source
	 * @param relation
	 * @return
	 */
	public static Set<ByteString> getRangeSet(FactDatabase source, ByteString relation) {
		ByteString rangeType = getRelationRange(source, relation);
		Set<ByteString> result = new LinkedHashSet<ByteString>();
		if (rangeType != null) 
			result.addAll(getAllEntitiesForType(source, rangeType));
		result.addAll(source.predicate2object2subject.get(relation).keySet());
		return result;
	}
	
	public static void main(String args[]) throws IOException{
		FactDatabase d = new FactDatabase();
	    ArrayList<File> files = new ArrayList<File>();
	    for(String file: args)
	    	files.add(new File(file));
	    
	    d.load(files);
	    
	    for(ByteString relation: d.predicateSize){
	    	System.out.println(relation + "\t" + getRelationDomain(d, relation) + "\t" + getRelationRange(d, relation));
	    }
	}
}
