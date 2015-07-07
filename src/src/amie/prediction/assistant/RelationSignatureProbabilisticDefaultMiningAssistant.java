package amie.prediction.assistant;

import amie.data.FactDatabase;
import amie.mining.assistant.RelationSignatureDefaultMiningAssistant;

public class RelationSignatureProbabilisticDefaultMiningAssistant extends RelationSignatureDefaultMiningAssistant {
	
	public RelationSignatureProbabilisticDefaultMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
	}
}