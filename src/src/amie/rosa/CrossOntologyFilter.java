package amie.rosa;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;
import amie.rules.AMIEParser;
import amie.rules.Rule;

public class CrossOntologyFilter {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		List<Rule> rules = AMIEParser.rules(new File(args[0]));		

		for(Rule rule: rules){			
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
