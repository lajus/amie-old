package amie.data;

import java.io.IOException;

import javatools.datatypes.ByteString;

public class QueryKB {

	public static void main(String[] args) throws IOException {
		amie.data.U.loadSchemaConf();
		KB kb = amie.data.U.loadFiles(args);
/**		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(
				KB.triple("?s", "<livesIn>", "<Paris>"), KB.triple("?s", KB.hasNumberOfValuesEquals + "1", "<isCitizenOf>")))); 
		
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(
				KB.triple("?s", "<livesIn>", "<Paris>"), KB.triple("?s", KB.hasNumberOfValuesGreaterThan + "0", "<wasBornIn>"))));
		
		System.out.println(kb.selectDistinct(ByteString.of("?o"), KB.triples(
				KB.triple("?s", "<livesIn>", "<Paris>"), KB.triple("?s", "<wasBornIn>", "?o"))));
		
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(
				KB.triple("?s", "<livesIn>", "<Paris>"), KB.triple("?s", KB.hasNumberOfValuesEquals + "1", "<wasBornIn>"))));
		
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesGreaterThanInv + "2", "<hasChild>")))); **/
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesGreaterThanInv + "1", "<hasChild>"))));		

	}
}
