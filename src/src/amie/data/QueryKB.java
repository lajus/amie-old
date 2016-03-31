package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javatools.datatypes.ByteString;

public class QueryKB {

	public static void main(String[] args) throws IOException {
		KB kb = new KB();
		kb.load(new File("/home/galarrag/workspace/Prediction/data/training-from-sampling/yago3/annotations/yago3.trainingset.final.tsv"),
				new File("/home/galarrag/workspace/AMIE/Data/yago3/yagoFacts.clean.tsv"));
		List<ByteString[]> query = KB.triples(KB.triple("?a", "hasNumberOfValuesSmallerThan1", "<isCitizenOf>"), 
				KB.triple("?a", "isComplete", "<isCitizenOf>"));
		System.out.println(kb.countDistinct(ByteString.of("?a"), query));
		System.out.println(kb.selectDistinct(ByteString.of("?a"), query));
	}
}
