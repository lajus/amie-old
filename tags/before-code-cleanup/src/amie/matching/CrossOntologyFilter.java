package amie.matching;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;

import amie.query.AMIEreader;
import amie.query.Query;

public class CrossOntologyFilter {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		List<Query> rules = AMIEreader.rules(new File(args[0]));		

		for(Query rule: rules){			
			ByteString r1, r2;
			r1 = rule.getHead()[1];
			r2 = rule.getBody().get(0)[1];
			if((r1.toString().startsWith("<ns:") && !r2.toString().startsWith("<ns:")) || 
				(r2.toString().startsWith("<ns:") && !r1.toString().startsWith("<ns:"))){
				System.out.println(rule.getRuleString());
			}
		}

	}

}
