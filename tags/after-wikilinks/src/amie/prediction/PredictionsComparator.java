package amie.prediction;

import java.util.Comparator;

import javatools.datatypes.ByteString;

public class PredictionsComparator implements Comparator<Prediction> {

	public boolean naive;
	
	public PredictionsComparator() {
		naive = false;
	}
	
	/**
	 * @param naive Use the naive independence score.
	 */
	public PredictionsComparator(boolean naive) {
		this.naive = naive;
	}
	
	@Override
	public int compare(Prediction o1, Prediction o2) {
		Double conf1 = null;
		Double conf2 = null;

		if (naive) {
			conf1 = o1.getNaiveConfidence();
			conf2 = o2.getNaiveConfidence();			
		} else {
			conf1 = o1.getConfidence();
			conf2 = o2.getConfidence();			
		}
		
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
