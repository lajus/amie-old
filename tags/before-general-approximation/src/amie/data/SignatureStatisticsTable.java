package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;


public class SignatureStatisticsTable {

	private static ByteString omittedRelations[] = {ByteString.of("rdf:type")};
	
	public enum SignatureTarget{Domain, Range}
	
	public static void main(String args[]) throws IOException{
		FactDatabase source = new FactDatabase();
		Map<ByteString, Map<ByteString, long[]> > domainStatisticsTable;
		Map<ByteString, Map<ByteString, long[]> > rangeStatisticsTable;		
		
		int minSupport = Integer.parseInt(args[0]);
		List<File> files = new ArrayList<>();
		for(int i = 1; i < args.length; ++i){
			files.add(new File(args[i]));
		}
		source.load(files);
		domainStatisticsTable = findSignatureRelationAssociationRules(source, SignatureTarget.Domain, minSupport);
		rangeStatisticsTable = findSignatureRelationAssociationRules(source, SignatureTarget.Range, minSupport);
		
		System.out.println("relation(x,y) => isa(x, type)\t\tTrue positives\tHead size\tBody size");
		printStatisticsTable(domainStatisticsTable);
		System.out.println("relation(x,y) => isa(y, type)\t\tTrue positives\tHead size\tBody size");
		printStatisticsTable(rangeStatisticsTable);		
	}
	
	private static void printStatisticsTable(Map<ByteString, Map<ByteString, long[]> > statistics){
		for(ByteString relation: statistics.keySet()){
			Map<ByteString, long[]> relationSubmap = statistics.get(relation);
			for(ByteString type: relationSubmap.keySet()){
				System.out.print(relation + "\t" + type);
				for(long value: relationSubmap.get(type)){
					System.out.print("\t" + value);
				}
				System.out.println();
			}
		}		
	}
	
	private static long typeCardinality(FactDatabase db, ByteString type){
		ByteString[] triple = FactDatabase.triple("?s", "rdf:type", type);
		return db.count(triple);
	}

	private static Map<ByteString, Map<ByteString, long[]>> findSignatureRelationAssociationRules(
			FactDatabase source, SignatureTarget target, int minSupport) {
		Map<ByteString, Map<ByteString, long[]> > statistics = new HashMap<>();
		List<ByteString> omittedRelationsList = Arrays.asList(omittedRelations);
		IntHashMap<ByteString> typesCardinality = new IntHashMap<>();
		// TODO Auto-generated method stub
		for(ByteString relation: source.predicateSize){
			
			//Now let's get all the types ocurring with this relation
			if(omittedRelationsList.contains(relation))
				continue;
			
			List<ByteString[]> query = null;
			ByteString projectionVariable = null;
			if(target == SignatureTarget.Domain){
				query = FactDatabase.triples(FactDatabase.triple("?x", "rdf:type", "?y"), FactDatabase.triple("?x", relation, "?z"));
				projectionVariable = ByteString.of("?x");
			}else{
				query = FactDatabase.triples(FactDatabase.triple("?z", "rdf:type", "?y"), FactDatabase.triple("?x", relation, "?z"));
				projectionVariable = ByteString.of("?z");				
			}
			
			IntHashMap<ByteString> types = source.frequentBindingsOf(ByteString.of("?y"), projectionVariable, query);
			Map<ByteString, long[]> typesStatisticsMap = new HashMap<>();
			statistics.put(relation, typesStatisticsMap);
			for(ByteString type: types){
				long[] values = new long[3];
				values[0] = types.get(type);
				if(values[0] < minSupport)
					continue;
				
				values[1] = typesCardinality.get(type);
				if(values[1] == -1){
					values[1] = typeCardinality(source, type);
				}
				
				values[2] = source.predicateSize.get(relation);
				typesStatisticsMap.put(type, values);
			}
		}
		
		return statistics;
	}
}
