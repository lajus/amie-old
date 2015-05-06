package amie.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

public class EntityTrimmer {

	public EntityTrimmer() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FactDatabase factsSource = null;
		ArrayList<File> factsFiles = new ArrayList<File>();
		File entities = null;
				
		for(int i = 0; i < args.length; ++i){
			if(args[i].startsWith(":e")){
				entities = new File(args[i].substring(2));
			}else{
				factsFiles.add(new File(args[i]));
			}
		}
		
		if(entities == null){
			System.err.println("No entities input file");
			System.exit(1);
		}
		
		if(factsFiles.isEmpty()){
			System.err.println("No input data files");
			System.exit(1);			
		}

		factsSource = new FactDatabase();
		factsSource.load(factsFiles);
		
		FileWriter fstream = new FileWriter("output.tsv");
		BufferedWriter out = new BufferedWriter(fstream);
		
		FileReader fileReader = new FileReader(entities);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		Set<ByteString> seeds = new HashSet<ByteString>();
		//reading file line by line
		String line = bufferedReader.readLine().trim();
		while(line != null){
			seeds.add(ByteString.of("<" + line + ">"));
			line = bufferedReader.readLine();
		}
				
		Set<ByteString> subjects = factsSource.subject2predicate2object.keySet();
		for(ByteString subject: subjects){
			if(seeds.contains(subject)){
				//Then produce the facts
				Map<ByteString, IntHashMap<ByteString>> subjectsMap = factsSource.subject2predicate2object.get(subject);
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
		}	
		
		fileReader.close();
		bufferedReader.close();
		out.close();
	}
}
