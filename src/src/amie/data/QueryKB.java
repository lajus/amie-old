package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;

public class QueryKB {

	public static void main(String[] args) throws IOException {
/**		KB kb = new KB();
		kb.load(new File("/home/luis/Dropbox/postdoc/artist/DB_Artist_+Transitive.rdf"));
		List<ByteString[]> query = KB.triples(KB.triple("?s", "http://dbpedia.org/ontology/birthYear", "?o"), 
				KB.triple("?x", "http://dbpedia.org/ontology/birthYear", "?o"),
				KB.triple("?s", "http://dbpedia.org/ontology/field", "http://dbpedia.org/resource/Painting"),
				KB.triple("?x", "http://dbpedia.org/ontology/field", "http://dbpedia.org/resource/Painting"),
				KB.triple("?s", KB.DIFFERENTFROMstr, "?x"));
		System.out.println(kb.selectDistinct(ByteString.of("?s"), ByteString.of("?x"), query));
		System.out.println(kb.selectDistinct(ByteString.of("?s"), ByteString.of("?o"), query)); **/
		
		KB kb = new KB();
		kb.load(new File("/home/galarrag/workspace/Prediction/data/training-from-sampling/yago3/annotations/yago3.trainingset.final.tsv"),
				new File("/home/galarrag/workspace/AMIE/Data/yago3/yagoFacts.clean.tsv"));
		List<ByteString[]> query = KB.triples(KB.triple("?a", "hasNumberOfValuesSmallerThan1", "<isMarriedTo>"),
				KB.triple("?a", "isIncomplete", "<isMarriedTo>"));
		List<ByteString[]> query1 = KB.triples(KB.triple("?a", "hasNumberOfValuesSmallerThan1", "<isMarriedTo>"),
				KB.triple("?a", "isComplete", "<isMarriedTo>"));
		System.out.println(kb.countDistinct(ByteString.of("?a"), query));
		System.out.println(kb.countDistinct(ByteString.of("?a"), query1));
//		System.out.println(kb.countDistinct(ByteString.of("?a"), query));
//		System.out.println(kb.selectDistinct(ByteString.of("?a"), query));
	}
}
