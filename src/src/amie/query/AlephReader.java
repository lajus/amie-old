package amie.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.filehandlers.FileLines;
import javatools.parsers.Char;
import javatools.parsers.NumberParser;
import amie.data.FactDatabase;

/**
 * Reads queries from an output file by ALEPH
 * 
 * @author Fabian M. Suchanek
 *
 */
public class AlephReader {

	public static List<Query> alephQueries(File f) throws IOException {
		List<Query> result=new ArrayList<>();
		String q="";
		Double score=null;
		for(String line : new FileLines(f)) {
			line=line.trim();
			if(line.contains(":-")) q=line;
	      		else if(line.endsWith("]")) score=NumberParser.parseDouble(Char.cutLast(line.substring(line.lastIndexOf('[')+1)));
	      		else if(line.contains(").")) {
	      			q+=line;
	      			q=q.replaceAll("\\b([A-Z])\\b","?$1");
	      			Query query=new Query();
	      			ArrayList<ByteString[]> triples = FactDatabase.triples(q);
	      			ArrayList<ByteString> variables = new ArrayList<ByteString>();
	      			for(ByteString[] triple: triples){
	      				triple[1] = ByteString.of("<" + triple[1].toString() + ">");
		      			if(!variables.contains(triple[0]))
		      				variables.add(triple[0]);
		      			if(!variables.contains(triple[2]))
		      				variables.add(triple[2]);
		      		}
		
		      		query.setTriples(triples);
		        
		      		if(score!=null) {
		      			query.setStdConfidence(score);
		      		}
		      		query.setSupport(0);
		      		query.setFunctionalVariable(query.getHead()[0]);
		      		result.add(query);
		      	}
		    }    
		  
		return(result);
	}
  
	public static void main(String[] args) throws Exception {
		Set<Query> rules = new HashSet<Query>(alephQueries(new File(args[0])));
		for(Query r: rules){
			System.out.println(r.getRuleString());
		}
	}
}
