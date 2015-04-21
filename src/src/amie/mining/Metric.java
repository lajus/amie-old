package amie.mining;

/**
 * Rule metrics defined by AMIE
 * @author lgalarra
 *
 */
public enum Metric {
	None, Support, StandardConfidence, PCAConfidence, HeadCoverage, MinPredictiveness, MaxPredictiveness, 
	BodySize, ImprovedConfidenceTimesPredictivenessTimesBodySize, 
	ConfidenceTimesPredictivenessTimesBodySize, ImprovedPredictivenessStd, ImprovedPredictiveness
}