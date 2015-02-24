package amie.prediction;

import java.util.Comparator;

import javatools.datatypes.ByteString;

public class PredictionsComparator implements Comparator<Prediction> {

	@Override
	public int compare(Prediction o1, Prediction o2) {
		Double conf1 = o1.getConfidence();
		Double conf2 = o2.getConfidence();
		
		if (conf1.equals(conf2)) {
			ByteString[] triple1 = o1.getTriple();
			ByteString[] triple2 = o2.getTriple();
			String str1 = triple1[0].toString() + triple1[1].toString() + triple1[2].toString();
			String str2 = triple2[0].toString() + triple2[1].toString() + triple2[2].toString();
			return str2.compareTo(str1);
		} else {
			return conf2.compareTo(conf1);
		}
	}

}
