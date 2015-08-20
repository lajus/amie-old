package amie.tests;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.KB;

public class FactDatabaseTest {
	
	public static void case3(KB source){
		List<ByteString[]> q1 = KB.triples(KB.triple("?s29","rdf:type","<wikicategory_American_television_actors>"), KB.triple("?o29","rdf:type","<wikicategory_American_television_actors>"),  KB.triple("?o29","<isMarriedTo>","?s29"), KB.triple("?s29","<isMarriedTo>","?o29"));
		List<ByteString[]> q2 = KB.triples(KB.triple("?o29","<isMarriedTo>","?s29"), KB.triple("?s29","<isMarriedTo>","?o29"));
		List<ByteString[]> q3 = KB.triples(KB.triple("?o29","<isMarriedTo>","?s29"), KB.triple("?s29","<isMarriedTo>","?x"));
		List<ByteString[]> q4 = KB.triples(KB.triple("?s29","rdf:type","<wikicategory_American_television_actors>"), KB.triple("?o29","rdf:type","<wikicategory_American_television_actors>"),  KB.triple("?o29","<isMarriedTo>","?s29"), KB.triple("?s29","<isMarriedTo>","?x"));

		long t1, t2;
		t1 = System.currentTimeMillis();
		source.difference(ByteString.of("?s29"), q4, q1);
		t2 = System.currentTimeMillis();
		
		System.out.println("With types: " + (((double)(t2 - t1)) / 1000) + " seconds");
		
		t1 = System.currentTimeMillis();
		source.difference(ByteString.of("?s29"), q3, q2);
		t2 = System.currentTimeMillis();
		
		System.out.println("Without types: " + (((double)(t2 - t1)) / 1000) + " seconds");		
	}
	
	public static void case1(KB source){
		List<ByteString[]> q52 = KB.triples(KB.triple("?o34","rdf:type","<wikicategory_American_films>"), KB.triple("?s6815","?p","?s34"));
		List<ByteString[]> q51 = KB.triples(KB.triple("?s6815","?p","?s34"));
		long t1, t2;
		t1 = System.currentTimeMillis();
		source.countProjectionBindings(KB.triple("?s34","<directed>","?o34"), q52, ByteString.of("?s34"));
		t2 = System.currentTimeMillis();
		System.out.println("With types: " + (((double)(t2 - t1)) / 1000) + " seconds");

		t1 = System.currentTimeMillis();
		source.countProjectionBindings(KB.triple("?s34","<directed>","?o34"), q51, ByteString.of("?s34"));
		t2 = System.currentTimeMillis();
		System.out.println("Without types: " + (((double)(t2 - t1)) / 1000) + " seconds");
	}
	
	public static void case2(KB source){
		List<ByteString[]> q52 = KB.triples(KB.triple("?o25","rdf:type","<wordnet_university_108286163>"), KB.triple("?s25", "<hasAcademicAdvisor>", "?o235"), KB.triple("?o235", "<worksAt>", "?o25"), KB.triple("?o235","?p","?o25"));
		List<ByteString[]> q51 = KB.triples(KB.triple("?s25", "<hasAcademicAdvisor>", "?o235"), KB.triple("?o235", "<worksAt>", "?o25"), KB.triple("?o235","?p","?o25"));
		long t1, t2;
		t1 = System.currentTimeMillis();
		source.countProjectionBindings(KB.triple("?s25","<worksAt>","?o25"), q52, ByteString.of("?s25"));
		t2 = System.currentTimeMillis();
		System.out.println("With types: " + (((double)(t2 - t1)) / 1000) + " seconds");

		t1 = System.currentTimeMillis();
		source.countProjectionBindings(KB.triple("?s25","<worksAt>","?o25"), q51, ByteString.of("?s25"));
		t2 = System.currentTimeMillis();
		System.out.println("Without types: " + (((double)(t2 - t1)) / 1000) + " seconds");
	}
	
	public static void case4(KB source){
		long t1, t2;
		List<ByteString[]> q52 = KB.triples(KB.triple("?a","<dbo:subregion>","?b"), KB.triple("?k", "<dbo:location>", "?a"), KB.triple("?b","<dbo:country>","?k"));		
		t1 = System.currentTimeMillis();
		long result = source.countDistinctPairs(ByteString.of("?a"), ByteString.of("?b"), q52);
		t2 = System.currentTimeMillis();
		System.out.println("Result " + result + ", Running time: " + (((double)(t2 - t1)) / 1000) + " seconds");
	}
	
	public static void main(String[] args) throws IOException{
		KB source = new KB();
		
		source.load(new File(args[0]));
		
		//case1(source);
		//case2(source);
		//case3(source);
		case4(source);
	}

}
