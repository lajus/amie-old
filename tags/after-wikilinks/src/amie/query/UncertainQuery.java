package amie.query;

import java.text.DecimalFormat;

public class UncertainQuery extends Query {

	/*
	 * Probabilistic version of the cardinality. Facts derived from
	 * uncertain bindings do not contribute with one unit of support.
	 */
	protected double uncertainCardinality;
	
	/**
	 * Probabilistic version of the body size (denominator of the standard confidence formula)
	 */
	protected double uncertainBodySize;
	
	/**
	 * Probabilistic version of the PCA body size (denominator of the PCA formula)
	 */
	protected double uncertainBodyStarSize;

	
	public UncertainQuery(Query rule, int cardinality) {
		super(rule, cardinality);
	}

	public double getUncertainBodyStarSize() {
		return uncertainBodyStarSize;
	}

	public void setUncertainBodySizeStar(double uncertainBodyStarSize) {
		this.uncertainBodyStarSize = uncertainBodyStarSize;
	}

	public double getUncertainCardinality() {
		return uncertainCardinality;
	}

	public void setUncertainCardinality(double uncertainCardinality) {
		this.uncertainCardinality = uncertainCardinality;
	}
	
	public double getUncertainBodySize() {
		return uncertainBodySize;
	}

	public void setUncertainBodySize(double uncertainBodySize) {
		this.uncertainBodySize = uncertainBodySize;
	}

	public String getFullRuleString() {
		DecimalFormat df = new DecimalFormat("#.#########");
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(getRuleString());
		
		strBuilder.append("\t" + df.format(getSupport()) );
		strBuilder.append("\t" + df.format(getHeadCoverage()));
		strBuilder.append("\t" + df.format(getConfidence()));
		strBuilder.append("\t" + df.format(getPcaConfidence()));
		strBuilder.append("\t" + getCardinality());		
		strBuilder.append("\t" + getBodySize());
		strBuilder.append("\t" + getBodyStarSize());
		strBuilder.append("\t" + getFunctionalVariable());

		return strBuilder.toString();
	}
	
	public String getBasicRuleString() {
		DecimalFormat df = new DecimalFormat("#.#########");
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(getRuleString());
		
		strBuilder.append("\t" + df.format(getSupport()) );
		strBuilder.append("\t" + df.format(getHeadCoverage()));
		strBuilder.append("\t" + df.format(getConfidence()));
		strBuilder.append("\t" + df.format(getPcaConfidence()));
		strBuilder.append("\t" + df.format(getUncertainCardinality()));		
		strBuilder.append("\t" + df.format(getUncertainBodySize()));
		strBuilder.append("\t" + df.format(getUncertainBodyStarSize()));
		strBuilder.append("\t" + getFunctionalVariable());

		return strBuilder.toString();
	}
	
}
