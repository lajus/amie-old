package arm.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

public class RelationsTrimmer {

	public RelationsTrimmer() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String args[]) throws Exception{
		FactDatabase inputSource = null;
		Set<ByteString> relationsToFilter = new HashSet<ByteString>();
		ArrayList<File> inputFiles = new ArrayList<File>();
		
		String[] filteredRelations = args[0].split(",");
		for(String filteredRelation: filteredRelations){
			relationsToFilter.add(ByteString.of(filteredRelation));
		}
		
		if(relationsToFilter.isEmpty()){
			System.err.println("No relations to filter");
			System.exit(1);
		}
		
		for(int i = 1; i < args.length; ++i){
			inputFiles.add(new File(args[i]));
		}
		
		
		if(inputFiles.isEmpty()){
			System.err.println("No input files");
			System.exit(1);			
		}
		
		inputSource = new FactDatabase();
		inputSource.load(inputFiles);
		FileWriter fstream = new FileWriter("output.tsv");
		BufferedWriter out = new BufferedWriter(fstream);
		
		//Now iterate over the trim source and only output triples whose subject appears in the facts source
		ByteString[] triple = new ByteString[3];
		triple[0] = FactDatabase.compress("?s");
		triple[1] = FactDatabase.compress("?p");
		triple[2] = FactDatabase.compress("?o");		
		
		Set<ByteString> predicates =  inputSource.selectDistinct(triple[1], FactDatabase.triples(FactDatabase.triple(triple)));
		
		for(ByteString predicate: predicates){
			if(relationsToFilter.contains(predicate))
				continue;
			
			Map<ByteString, IntHashMap<ByteString>> subjectsMap = inputSource.predicate2subject2object.get(predicate);			
			Set<ByteString> subjects = subjectsMap.keySet(); 
			for(ByteString subject: subjects){
				IntHashMap<ByteString> objects = subjectsMap.get(subject);
				for(ByteString object: objects){
					int nTimes = objects.get(object);
					for(int k = 0; k < nTimes; ++k){
						out.append(subject);
						out.append('\t');
						out.append(predicate);
						out.append('\t');
						out.append(object);
						out.append('\n');
					}
				}
			}
		}
		
		out.close();
	}

}
