package amie.prediction;

import java.util.Comparator;

import javatools.datatypes.ByteString;

public class PredictionsComparator implements Comparator<Prediction> {

	Metric metric;
	
	public PredictionsComparator() {
		metric = Metric.NaiveConfidence;
	}
	
	/**
	 * @param naive Use the naive independence score.
	 */
	public PredictionsComparator(Metric metric) {
		this.metric = metric;
	}
	
	@Override
	public int compare(Prediction o1, Prediction o2) {
		Double conf1 = null;
		Double conf2 = null;

		switch (this.metric) {
		case NaiveConfidence :
			conf1 = o1.getNaiveConfidence();
			conf2 = o2.getNaiveConfidence();			
			break;
		case JointConfidence :			
			conf1 = o1.getConfidence();
			conf2 = o2.getConfidence();
			break;
		case NaiveJointScore :
			conf1 = o1.getNaiveFullScore();
			conf2 = o2.getNaiveFullScore();
		case FullJointScore :
			conf1 = o1.getFullScore();
			conf2 = o2.getFullScore();
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
