package arm.mining;

import java.util.List;

import org.apache.commons.cli.CommandLine;

import arm.data.FactDatabase;
import arm.query.Query;

public class MiningResult {

	private FactDatabase trainingSource;
	
	private FactDatabase targetSource;
	
	private List<Query> rules;
	
	private CommandLine cli;
	
	public MiningResult(FactDatabase trainingSource, FactDatabase targetSource, List<Query> rules) {
		// TODO Auto-generated constructor stub
		this.setTrainingSource(trainingSource);
		this.setTargetSource(targetSource);
		this.setRules(rules);
	}

	public FactDatabase getTrainingSource() {
		return trainingSource;
	}

	public void setTrainingSource(FactDatabase trainingSource) {
		this.trainingSource = trainingSource;
	}

	public FactDatabase getTargetSource() {
		return targetSource;
	}

	public void setTargetSource(FactDatabase targetSource) {
		this.targetSource = targetSource;
	}
	
	public List<Query> getRules() {
		return rules;
	}

	public void setRules(List<Query> rules) {
		this.rules = rules;
	}

	public CommandLine getCli() {
		return cli;
	}

	public void setCli(CommandLine cli) {
		this.cli = cli;
	}


}
