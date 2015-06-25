package amie.mining.assistant.experimental;

import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.data.SchemaUtilities;
import amie.mining.assistant.DefaultMiningAssistant;
import amie.query.Query;

public class RelationSignatureMiningAssistant extends DefaultMiningAssistant {
	/**
	 * @param dataSource
	 */
	public RelationSignatureMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
		// TODO Auto-generated constructor stub
	}
	
	public boolean testConfidenceThresholds(Query candidate) {
		boolean addIt = true;
		boolean queryChanged = false;
		
		if(!candidate.isClosed()){
			return false;
		}
		
		//Add the schema information to the rule
		ByteString domain, range, relation;
		relation = candidate.getHead()[1];
		domain = SchemaUtilities.getRelationDomain(source, relation);
		if(domain != null){
			ByteString[] domainTriple = new ByteString[3];
			domainTriple[0] = candidate.getHead()[0];
			domainTriple[1] = ByteString.of("rdf:type");
			domainTriple[2] = domain;
			candidate.getTriples().add(domainTriple);
			queryChanged = true;
		}
		
		range = SchemaUtilities.getRelationRange(source, relation);
		if(range != null){
			ByteString[] rangeTriple = new ByteString[3];
			rangeTriple[0] = candidate.getHead()[2];
			rangeTriple[1] = ByteString.of("rdf:type");
			rangeTriple[2] = range;
			candidate.getTriples().add(rangeTriple);
			queryChanged = true;
		}
		
		if(queryChanged)
			recalculateSupport(candidate);
		
		calculateConfidenceMetrics(candidate);
		if(candidate.getStdConfidence() >= minStdConfidence && candidate.getPcaConfidence() >= minPcaConfidence){
			//Now check the confidence with respect to its ancestors
			List<Query> ancestors = candidate.getAncestors();			
			for(int i = ancestors.size() - 2; i >= 0; --i){
				if(ancestors.get(i).isClosed() && (candidate.getStdConfidence() <= ancestors.get(i).getStdConfidence() || candidate.getPcaConfidence() <= ancestors.get(i).getPcaConfidence())){
					addIt = false;
					break;
				}
			}
		}else{
			return false;
		}
		
		return addIt;
	}

	private void recalculateSupport(Query candidate) {
		long cardinality = source.countProjection(candidate.getHead(), candidate.getAntecedent());
		candidate.setSupport(cardinality);
		candidate.setHeadCoverage((double)candidate.getSupport() / headCardinalities.get(candidate.getHeadRelation()));
		candidate.setSupportRatio((double)candidate.getSupport() / (double)source.size());
	}
}
