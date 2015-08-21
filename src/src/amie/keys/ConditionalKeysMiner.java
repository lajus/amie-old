package amie.keys;

import java.util.List;

import amie.mining.AMIE;
import amie.rules.Rule;

/**
 * Small program that creates an AMIE instance so that Danai
 * can try the conditional keys model.
 * @author galarrag
 *
 */
public class ConditionalKeysMiner {

	public static void main(String[] args) throws Exception {
		AMIE amie = AMIE.getInstance(args);
		amie.setCheckParentsOfDegree2(true);		
		List<Rule> rules = amie.mine();
		if (!amie.isRealTime()) {
			for (Rule rule : rules) {
				System.out.println(rule.getFullRuleString());
			}
		}
	}
}
