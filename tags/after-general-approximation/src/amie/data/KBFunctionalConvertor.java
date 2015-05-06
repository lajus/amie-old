/**
 * @author lgalarra
 * @date Sep 13, 2013
 */
package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

/**
 * @author lgalarra
 *
 */
public class KBFunctionalConvertor {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FactDatabase db = new FactDatabase();
		
		db.load(new File(args[0]));
		for(ByteString object: db.objectSize){
			Map<ByteString, IntHashMap<ByteString> > predicates = db.object2predicate2subject.get(object);
			for(ByteString predicate: predicates.keySet()){
				if(db.functionality(predicate) >= db.inverseFunctionality(predicate)){
					for(ByteString subject: predicates.get(predicate))
						System.out.println(subject + "\t" + predicate + "\t" + object);
				}else{
					for(ByteString subject: predicates.get(predicate))
						System.out.println(object + "\t" + "<inv-" + predicate.subSequence(1, predicate.length()) + "\t" + subject);					
				}
			}
		}
	}

}
