package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;

public class QueryKB {

	public static void main(String[] args) throws IOException {
		KB kb = new KB();
		kb.load(new File("/home/lgalarra/AMIE/Data/yago3/yago3.trainingset.final.tsv"),
				new File("/home/lgalarra/AMIE/Data/yago3/yagoTransitiveType.clean3.tsv"));
		List<ByteString[]> query = KB.triples(KB.triple("?s", "rdf:type", "?o"), 
				KB.triple("?s", "rdf:type", "<wikicat_Living_people>"), KB.triple("?s", "isComplete", "<diedIn>"));
		List<ByteString[]> query1 = KB.triples(KB.triple("?s", "rdf:type", "?o"), 
				KB.triple("?s", "rdf:type", "<wikicat_Living_people>"), KB.triple("?s", "isIncomplete", "<diedIn>"));
		System.out.println(kb.countDistinct(ByteString.of("?s"), query));
		System.out.println(kb.countDistinct(ByteString.of("?s"), query1));
		System.out.println(kb.selectDistinct(ByteString.of("?s"), query));
		System.out.println(kb.selectDistinct(ByteString.of("?s"), query1));
	}
}
