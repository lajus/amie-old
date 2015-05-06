package amie.mining;

/**
 * Rule metrics defined by AMIE
 * @author lgalarra
 *
 */
public enum Metric {
	None, Support, Confidence, ImprovedConfidence, HeadCoverage, MinPredictiveness, MaxPredictiveness, 
	BodySize, ImprovedConfidenceTimesPredictivenessTimesBodySize, ConfidenceTimesPredictivenessTimesBodySize, ImprovedPredictivenessStd, ImprovedPredictiveness
}
