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
		List<ByteString[]> query = KB.triples(KB.triple("?a", "hasNumberOfValuesSmallerThan9", "<hasParent>"), 
				KB.triple("?a", "isIncomplete", "<hasParent>"));
		System.out.println(kb.countDistinct(ByteString.of("?a"), query));
		System.out.println(kb.selectDistinct(ByteString.of("?s"), query));
	}
}
