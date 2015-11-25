package amie.data;

import java.io.IOException;

import javatools.datatypes.ByteString;

public class QueryKB {

	public static void main(String[] args) throws IOException {
		amie.data.U.loadSchemaConf();
		KB kb = amie.data.U.loadFiles(args);
		System.out.println(U.getRelationDomain(kb, ByteString.of("<place_of_birth_P19>")));
		System.out.println(U.getRelationDomain(kb, ByteString.of("<sex_or_gender_P21>")));
		System.out.println(U.getAllSubtypes(kb, ByteString.of("<human_Q5>")));
	}

}
