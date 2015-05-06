package amie.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

public class FactsTrimmer {

	public FactsTrimmer() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String[] args) throws IOException{
		FactDatabase trimSource = null;
		FactDatabase factsSource = null;
		ArrayList<File> trimFiles = new ArrayList<File>();
		ArrayList<File> factsFiles = new ArrayList<File>();
		boolean includeObjects = false;
		int start = 0;
		
		if(args[0].equals("-withObjects")){
			includeObjects = true;
			start = 1;
		}
		
		for(int i = start; i < args.length; ++i){
			if(args[i].startsWith("-t")){
				trimFiles.add(new File(args[i].substring(2)));
			}else{
				factsFiles.add(new File(args[i]));
			}
		}
		
		if(trimFiles.isEmpty()){
			System.err.println("No files to trim");
			System.exit(1);
		}
		
		if(factsFiles.isEmpty()){
			System.err.println("No source files");
			System.exit(1);			
		}
		
		trimSource = new FactDatabase();
		factsSource = new FactDatabase();
		
		factsSource.load(factsFiles);
		trimSource.load(trimFiles);
		
		FileWriter fstream = new FileWriter("trimed.tsv");
		BufferedWriter out = new BufferedWriter(fstream);
		
		//Now iterate over the trim source and only output triples whose subject appears in the facts source
		ByteString[] triple = new ByteString[3];
		triple[0] = FactDatabase.compress("?s");
		triple[1] = FactDatabase.compress("?p");
		triple[2] = FactDatabase.compress("?o");		
		
		Set<ByteString> subjects =  factsSource.selectDistinct(triple[0], FactDatabase.triples(FactDatabase.triple(triple)));
		if(includeObjects)
			subjects.addAll(factsSource.selectDistinct(triple[2], FactDatabase.triples(FactDatabase.triple(triple))));
		
		for(ByteString subject: subjects){
			Map<ByteString, IntHashMap<ByteString>> subjectsMap = trimSource.subject2predicate2object.get(subject);
			if(subjectsMap == null) continue;
			
			Set<ByteString> predicates = subjectsMap.keySet(); 
			for(ByteString predicate: predicates){
				IntHashMap<ByteString> objects = subjectsMap.get(predicate);
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
