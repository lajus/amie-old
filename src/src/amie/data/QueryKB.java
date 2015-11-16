package amie.data;

import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;

public class QueryKB {

	public static void main(String[] args) throws IOException {
		amie.data.U.loadSchemaConf();
		KB kb = amie.data.U.loadFiles(args);
		List<ByteString[]> query = KB.triples(KB.triple("<dbo:deathCause>", "~exists", "?d"));
		System.out.println(kb.selectDistinct(ByteString.of("?d"), query));
		query = KB.triples(KB.triple("<dbo:deathCause>", "~exists", "?d"), 
				KB.triple("?d", "<rdf:type>", "<dbo:Person>"));
		System.out.println(kb.selectDistinct(ByteString.of("?d"), query));
	}

}
