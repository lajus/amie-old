package amie.data;

import java.io.IOException;

import javatools.datatypes.ByteString;

public class QueryKB {

	public static void main(String[] args) throws IOException {
		amie.data.U.loadSchemaConf();
		KB kb = amie.data.U.loadFiles(args);
		System.out.println(U.getDomainSet(kb, ByteString.of("<diedIn>"), ByteString.of("<wikicat_Living_people>")));
	}

}
