package amie.data.eval.legacy;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import amie.data.FactDatabase;
import amie.query.Query;

import javatools.datatypes.ByteString;
import javatools.datatypes.Pair;
import javatools.filehandlers.TSVFile;

public class SummaryMatcher {
	public SummaryMatcher(){
		
	}
	
	public void match(File f1, File f2) throws IOException{
		TSVFile tsv1 = new TSVFile(f1);
		TSVFile tsv2 = new TSVFile(f2);
		
		Map<Query, int[]> s1 = load(tsv1);
		Map<Query, int[]> s2 = load(tsv2);
		
		for(Query q: s1.keySet()){
			int[] val1 = s2.get(q);
			if(val1 != null){
				int[] val2 = s1.get(q);
				if(Arrays.equals(val1, val2)){
					//Bingo
					System.out.println(q.getRuleString() + "\t" + val1[0] + "\t" + val1[1] + "\t" + val1[2]);
				}
			}
		}
				
		tsv1.close();
		tsv2.close();
	}
	
	private Map<Query, int[]> load(TSVFile tsv) {
		Map<Query, int[]> result = new HashMap<>();
		for(List<String> record: tsv){
			if(record.size() < 4) continue;
			
			String ruleStr = record.get(0);			
			int[] summary = new int[3];
			Pair<List<ByteString[]>, ByteString[]> rulePair = FactDatabase.rule(ruleStr);
			Query q = new Query();
			q.getTriples().add(rulePair.second);
			q.getTriples().addAll(rulePair.first);

			for(int i = 1; i < 4; ++i){
				summary[i - 1] = Integer.parseInt(record.get(i)); 
			}
			
			result.put(q, summary);			
		}
		
		return result;
	}

	public static void main(String[] args) throws IOException{
		File f1 = new File(args[0]);
		File f2 = new File(args[1]);

		SummaryMatcher matcher = new SummaryMatcher();		
		matcher.match(f1, f2);
	}

}
