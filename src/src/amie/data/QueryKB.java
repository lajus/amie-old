package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;

public class QueryKB {

	public static void main(String[] args) throws IOException {
		KB kb = new KB();
		kb.load(new File("/home/galarrag/Dropbox/workspace/ConditionalKeys/Data/Wikidata/domains/album/album.final.tsv"));
		List<ByteString[]> query = KB.triples(KB.triple("?s", "freebase_identifier_p646", "?o"), 
				KB.triple("?x", "freebase_identifier_p646", "?o"), KB.triple("?s", KB.DIFFERENTFROMstr, "?x"));
		System.out.println(kb.selectDistinct(ByteString.of("?s"), ByteString.of("?x"), query));
		System.out.println(kb.selectDistinct(ByteString.of("?s"), ByteString.of("?o"), query));
	}
}
