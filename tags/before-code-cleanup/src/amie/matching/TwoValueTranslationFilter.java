package amie.matching;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.query.AMIEreader;
import amie.query.Query;

public class TwoValueTranslationFilter {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		List<Query> rules = AMIEreader.rules(new File(args[0]));
		
		for(Query rule: rules){
			if(followsAttributeValueTranslation(rule))
				System.out.println(rule.getRuleString());
		}
	}

	private static boolean followsAttributeValueTranslation(Query rule) {
		if(rule.getLength() != 3)
			return false;
		
		List<ByteString[]> body = rule.getBody();
		ByteString[] head = rule.getHead();
		boolean containsObject = !FactDatabase.isVariable(body.get(0)[2]) || !FactDatabase.isVariable(body.get(1)[2]);
		
		if(containsObject && (body.get(0)[1].toString().trim().startsWith("<ns:") && body.get(1)[1].toString().trim().startsWith("<ns:") && !head[1].toString().trim().startsWith("<ns:"))
				|| 	(!body.get(0)[1].toString().trim().startsWith("<ns:") && !body.get(1)[1].toString().trim().startsWith("<ns:") && head[1].toString().trim().startsWith("<ns:"))
			  )
				return true;
		
		return false;
	}

}
