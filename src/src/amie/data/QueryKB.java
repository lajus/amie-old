package amie.data;

import java.io.IOException;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

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
		
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesGreaterThanInv + "0", "<hasChild>"))));
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesGreaterThanInv + "1", "<hasChild>"))));
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesGreaterThanInv + "2", "<hasChild>"))));
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesEquals + "0", "<isCitizenOf>"))));
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesEquals + "1", "<isCitizenOf>"))));
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesGreaterThan + "0", "<isCitizenOf>"))));
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesGreaterThan + "1", "<isCitizenOf>"))));
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(KB.triple("?s", KB.hasNumberOfValuesGreaterThan + "1", "<livesIn>"))));
		
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(
				KB.triple("?s", "<isCitizenOf>", "<France>"), KB.triple("?s", KB.hasNumberOfValuesGreaterThanInv + "1", "<hasChild>"))));
		System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(
				KB.triple("?s", "<isCitizenOf>", "?x"), KB.triple("?s", KB.hasNumberOfValuesGreaterThanInv + "1", "<hasChild>"))));**/
	/**	System.out.println(kb.countDistinct(ByteString.of("?s"), KB.triples(
				KB.triple("?s", "<isCitizenOf>", "?x"), KB.triple("?s", KB.hasNumberOfValuesEquals + "0", "<hasChild>"))));
		
		System.out.println(kb.count(KB.triple("?s", KB.hasNumberOfValuesEquals + "0", "<hasChild>"))); **/
		
		
		System.out.println(kb.maximalCardinality(ByteString.of("<hasChild>"), 1000));
		System.out.println(kb.maximalCardinalityInv(ByteString.of("<hasChild>"), 1000));

	}
}
