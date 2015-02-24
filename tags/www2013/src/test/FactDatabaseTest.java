package test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;
import arm.data.FactDatabase;

public class FactDatabaseTest {
	
	public static void case3(FactDatabase source){
		List<ByteString[]> q1 = FactDatabase.triples(FactDatabase.triple("?s29","rdf:type","<wikicategory_American_television_actors>"), FactDatabase.triple("?o29","rdf:type","<wikicategory_American_television_actors>"),  FactDatabase.triple("?o29","<isMarriedTo>","?s29"), FactDatabase.triple("?s29","<isMarriedTo>","?o29"));
		List<ByteString[]> q2 = FactDatabase.triples(FactDatabase.triple("?o29","<isMarriedTo>","?s29"), FactDatabase.triple("?s29","<isMarriedTo>","?o29"));
		List<ByteString[]> q3 = FactDatabase.triples(FactDatabase.triple("?o29","<isMarriedTo>","?s29"), FactDatabase.triple("?s29","<isMarriedTo>","?x"));
		List<ByteString[]> q4 = FactDatabase.triples(FactDatabase.triple("?s29","rdf:type","<wikicategory_American_television_actors>"), FactDatabase.triple("?o29","rdf:type","<wikicategory_American_television_actors>"),  FactDatabase.triple("?o29","<isMarriedTo>","?s29"), FactDatabase.triple("?s29","<isMarriedTo>","?x"));

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
	
	public static void case1(FactDatabase source){
		List<ByteString[]> q52 = FactDatabase.triples(FactDatabase.triple("?o34","rdf:type","<wikicategory_American_films>"), FactDatabase.triple("?s6815","?p","?s34"));
		List<ByteString[]> q51 = FactDatabase.triples(FactDatabase.triple("?s6815","?p","?s34"));
		long t1, t2;
		t1 = System.currentTimeMillis();
		source.countProjectionBindings(FactDatabase.triple("?s34","<directed>","?o34"), q52, ByteString.of("?s34"));
		t2 = System.currentTimeMillis();
		System.out.println("With types: " + (((double)(t2 - t1)) / 1000) + " seconds");

		t1 = System.currentTimeMillis();
		source.countProjectionBindings(FactDatabase.triple("?s34","<directed>","?o34"), q51, ByteString.of("?s34"));
		t2 = System.currentTimeMillis();
		System.out.println("Without types: " + (((double)(t2 - t1)) / 1000) + " seconds");
	}
	
	public static void case2(FactDatabase source){
		List<ByteString[]> q52 = FactDatabase.triples(FactDatabase.triple("?o25","rdf:type","<wordnet_university_108286163>"), FactDatabase.triple("?s25", "<hasAcademicAdvisor>", "?o235"), FactDatabase.triple("?o235", "<worksAt>", "?o25"), FactDatabase.triple("?o235","?p","?o25"));
		List<ByteString[]> q51 = FactDatabase.triples(FactDatabase.triple("?s25", "<hasAcademicAdvisor>", "?o235"), FactDatabase.triple("?o235", "<worksAt>", "?o25"), FactDatabase.triple("?o235","?p","?o25"));
		long t1, t2;
		t1 = System.currentTimeMillis();
		source.countProjectionBindings(FactDatabase.triple("?s25","<worksAt>","?o25"), q52, ByteString.of("?s25"));
		t2 = System.currentTimeMillis();
		System.out.println("With types: " + (((double)(t2 - t1)) / 1000) + " seconds");

		t1 = System.currentTimeMillis();
		source.countProjectionBindings(FactDatabase.triple("?s25","<worksAt>","?o25"), q51, ByteString.of("?s25"));
		t2 = System.currentTimeMillis();
		System.out.println("Without types: " + (((double)(t2 - t1)) / 1000) + " seconds");
	}
	
	public static void main(String[] args) throws IOException{
		FactDatabase source = new FactDatabase();
		
		source.load(new File(args[0]));
		
		//case1(source);
		//case2(source);
		case3(source);
	}

}
