package amie.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.Pair;
import javatools.filehandlers.FileLines;
import amie.data.FactDatabase;

/** 
 * Parses a file of AMIE rules
 * 
 * @author Fabian
 *
 */
public class AMIEreader {
	
	public static Query rule(String s) {	
		Pair<List<ByteString[]>, ByteString[]> rulePair = FactDatabase.rule(s);
		if(rulePair == null) return null;
		Query resultRule = new Query(rulePair.second, rulePair.first, 0);
		return resultRule;
	}	
  
	public static void normalizeRule(Query q){
		char c = 'a';
		Map<ByteString, Character> charmap = new HashMap<ByteString, Character>();
		for(ByteString[] triple: q.getTriples()){
			for(int i = 0;  i < triple.length; ++i){
				if(FactDatabase.isVariable(triple[i])){
					Character replace = charmap.get(triple[i]);
					if(replace == null){
						replace = new Character(c);
						charmap.put(triple[i], replace);
						c = (char) (c + 1);
					}
					triple[i] = ByteString.of("?" + replace);				
				}
			}
		}
	}	

	public static List<Query> rules(File f) throws IOException {
	    List<Query> result=new ArrayList<>();
	    for(String line : new FileLines(f)) {
	    	ArrayList<ByteString[]> triples=FactDatabase.triples(line);
	    	if(triples==null || triples.size()<2) continue;      
	    	ByteString[] last=triples.get(triples.size()-1);
	    	triples.remove(triples.size()-1);
	    	triples.add(0, last);
	    	Query query=new Query();
	 
	    	ArrayList<ByteString> variables = new ArrayList<ByteString>();
	    	for(ByteString[] triple: triples){
	    		if(!variables.contains(triple[0]))
	    			variables.add(triple[0]);
	    		if(!variables.contains(triple[2]))
	    			variables.add(triple[2]);
	    	}
	      
	    	query.setSupport(0);
	    	query.setTriples(triples);
	    	query.setFunctionalVariablePosition(0);
	    	result.add(query);
	    }
	    return(result);
	}
	  
	public static void main(String[] args) throws Exception {
	    System.out.println(AMIEreader.rule("=> ?a <hasChild> ?b"));
	}
}
