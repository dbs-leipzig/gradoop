package org.gradoop.flink.model.impl.operators.fusion;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class FusionTest extends GradoopFlinkTestBase {
	@Test
	public void empty_empty_to_empty() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("empty:G[]");
		LogicalGraph left = loader.getLogicalGraphByVariable("empty");
		LogicalGraph right = loader.getLogicalGraphByVariable("empty");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("empty")));
	}

	@Test
	public void empty_single_to_empty() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("empty:G[]"+
			"single:G {single : \"graph\"}["+
			"()"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("empty");
		LogicalGraph right = loader.getLogicalGraphByVariable("single");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("empty")));
	}

	@Test
	public void single_empty_to_single() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("single:G {single : \"graph\"}["+
			"()"+
			"]"+
			"empty:G[]");
		LogicalGraph left = loader.getLogicalGraphByVariable("single");
		LogicalGraph right = loader.getLogicalGraphByVariable("empty");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("single")));
	}

	@Test
	public void single_aGraph_to_single() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"single:G {single : \"graph\"}["+
			"()"+
			"]"+
			"aGraph:G {aGraph : \"graph\"}["+
			"(a)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("single");
		LogicalGraph right = loader.getLogicalGraphByVariable("aGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("single")));
	}

	@Test
	public void single_single_to_singleInside() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(singleV:G {single : \"graph\"})"+
			"single:G {single : \"graph\"}["+
			"()"+
			"]"+
			"singleInside:G {single : \"graph\"}["+
			"(singleV)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("single");
		LogicalGraph right = loader.getLogicalGraphByVariable("single");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("singleInside")));
	}

	@Test
	public void aGraph_aGraph_to_aGraphLabels() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(aGraphV:G {aGraph : \"graph\"})"+
			"aGraph:G {aGraph : \"graph\"}["+
			"(a)"+
			"]"+
			"aGraphLabels:G {aGraph : \"graph\"}["+
			"(aGraphV)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aGraphLabels")));
	}

	@Test
	public void aGraph_empty_to_aGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"aGraph:G {aGraph : \"graph\"}["+
			"(a)"+
			"]"+
			"empty:G[]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("empty");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aGraph")));
	}

	@Test
	public void aGraph_single_to_aGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"aGraph:G {aGraph : \"graph\"}["+
			"(a)"+
			"]"+
			"single:G {single : \"graph\"}["+
			"()"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("single");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aGraph")));
	}

	@Test
	public void aAbGraph_aGraph_to_aggAbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(aGraphV:G {aGraph : \"graph\"})"+
			"aAbGraph:G {aAbGraph : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"aGraph:G {aGraph : \"graph\"}["+
			"(a)"+
			"]"+
			"aggAbGraph:G {aAbGraph : \"graph\"}["+
			"(aGraphV)-[:AlphaEdge]->(b)"+
			""+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aggAbGraph")));
	}

	@Test
	public void aAbGraph_empty_to_aAbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aAbGraph:G {aAbGraph : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"empty:G[]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("empty");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aAbGraph")));
	}

	@Test
	public void aAbGraph_single_to_aAbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aAbGraph:G {aAbGraph : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"single:G {single : \"graph\"}["+
			"()"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("single");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aAbGraph")));
	}

	@Test
	public void aAbGraph_aAbGraph_to_wholeaAbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(aAbGraphV:G {aAbGraph : \"graph\"})"+
			"aAbGraph:G {aAbGraph : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"wholeaAbGraph:G {aAbGraph : \"graph\"}["+
			"(aAbGraphV)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aAbGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("wholeaAbGraph")));
	}

	@Test
	public void aAbGraph_aBbGraph_to_aAbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aAbGraph:G {aAbGraph : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"aBbGraph:G {aBbGraph : \"graph\"}["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aBbGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aAbGraph")));
	}

	@Test
	public void aBbGraph_aGraph_to_aBbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aBbGraph:G {aBbGraph : \"graph\"}["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]"+
			"aGraph:G {aGraph : \"graph\"}["+
			"(a)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aBbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aBbGraph")));
	}

	@Test
	public void aBbGraph_empty_to_aBbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aBbGraph:G {aBbGraph : \"graph\"}["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]"+
			"empty:G[]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aBbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("empty");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aBbGraph")));
	}

	@Test
	public void aBbGraph_single_to_aBbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aBbGraph:G {aBbGraph : \"graph\"}["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]"+
			"single:G {single : \"graph\"}["+
			"()"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aBbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("single");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aBbGraph")));
	}

	@Test
	public void aBbGraph_aAbGraph_to_aBbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aBbGraph:G {aBbGraph : \"graph\"}["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]"+
			"aAbGraph:G {aAbGraph : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aBbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aAbGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aBbGraph")));
	}

	@Test
	public void aBbGraph_aBbGraph_to_abGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(aBbGraphV:G {aBbGraph : \"graph\"})"+
			"aBbGraph:G {aBbGraph : \"graph\"}["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]"+
			"abGraph:G {aBbGraph : \"graph\"}["+
			"(aBbGraphV)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aBbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aBbGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("abGraph")));
	}

	@Test
	public void aAbCdGraph_aAbCdGraph_to_abdGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(c:C {ctype : \"cvalue\"})"+
			"(aAbCdGraphV:G {aAbCdGraph : \"graph\"})"+
			"aAbCdGraph:G {aAbCdGraph : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:GammaEdge]->(c)"+
			""+
			"]"+
			"abdGraph:G {aAbCdGraph : \"graph\"}["+
			"(aAbCdGraphV)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbCdGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aAbCdGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("abdGraph")));
	}

	@Test
	public void aAbCdGraph_aAbGraph_to_abCdGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(c:C {ctype : \"cvalue\"})"+
			"(aAbGraphV:G {aAbGraph : \"graph\"})"+
			"aAbCdGraph:G {aAbCdGraph : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:GammaEdge]->(c)"+
			""+
			"]"+
			"aAbGraph:G {aAbGraph : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"abCdGraph:G {aAbCdGraph : \"graph\"}["+
			"(aAbGraphV)-[:GammaEdge]->(c)"+
			""+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbCdGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aAbGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("abCdGraph")));
	}

	@Test
	public void semicomplex_looplessPattern_to_firstmatch() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(c:C {ctype : \"cvalue\"})"+
			"(d:D {dtype : \"dvalue\"})"+
			"(e:E {etype : \"evalue\"})"+
			"(looplessPatternV:G {looplessPattern : \"graph\"})"+
			"semicomplex:G {semicomplex : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:loop]->(b)"+
			"(b)-->(c)"+
			"(c)-->(e)"+
			"(c)-[:BetaEdge]->(d)"+
			"(d)-->(e)"+
			""+
			"]"+
			"looplessPattern:G {looplessPattern : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(d)"+
			"]"+
			"firstmatch:G {semicomplex : \"graph\"}["+
			"(looplessPatternV)-->(c)"+
			"(looplessPatternV)-->(e)"+
			"(looplessPatternV)-[:loop]->(looplessPatternV)"+
			"(c)-[:BetaEdge]->(looplessPatternV)"+
			"(c)-->(e)"+
			""+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("semicomplex");
		LogicalGraph right = loader.getLogicalGraphByVariable("looplessPattern");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("firstmatch")));
	}

	@Test
	public void semicomplex_loopPattern_to_secondmatch() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(d:D {dtype : \"dvalue\"})"+
			"(c:C {ctype : \"cvalue\"})"+
			"(e:E {etype : \"evalue\"})"+
			"(loopPatternV:G {loopPattern : \"graph\"})"+
			"semicomplex:G {semicomplex : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:loop]->(b)"+
			"(b)-->(c)"+
			"(c)-->(e)"+
			"(c)-[:BetaEdge]->(d)"+
			"(d)-->(e)"+
			""+
			"]"+
			"loopPattern:G {loopPattern : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:loop]->(b)"+
			"(d)"+
			"]"+
			"secondmatch:G {semicomplex : \"graph\"}["+
			"(loopPatternV)-->(c)"+
			"(loopPatternV)-->(e)"+
			"(c)-[:BetaEdge]->(loopPatternV)"+
			"(c)-->(e)"+
			""+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("semicomplex");
		LogicalGraph right = loader.getLogicalGraphByVariable("loopPattern");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("secondmatch")));
	}

	@Test
	public void tricky_looplessPattern_to_thirdmatch() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(c:C {ctype : \"cvalue\"})"+
			"(d:D {dtype : \"dvalue\"})"+
			"(e:E {etype : \"evalue\"})"+
			"(looplessPatternV:G {looplessPattern : \"graph\"})"+
			"tricky:G {tricky : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(d)-[:loop]->(b)"+
			"(b)-->(c)"+
			"(c)-->(e)"+
			"(c)-[:BetaEdge]->(d)"+
			"(d)-->(e)"+
			""+
			"]"+
			"looplessPattern:G {looplessPattern : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(d)"+
			"]"+
			"thirdmatch:G {tricky : \"graph\"}["+
			"(looplessPatternV)-->(c)"+
			"(looplessPatternV)-->(e)"+
			"(looplessPatternV)-[:loop]->(looplessPatternV)"+
			"(c)-[:BetaEdge]->(looplessPatternV)"+
			"(c)-->(e)"+
			""+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("tricky");
		LogicalGraph right = loader.getLogicalGraphByVariable("looplessPattern");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("thirdmatch")));
	}

	@Test
	public void tricky_loopPattern_to_tricky() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(d:D {dtype : \"dvalue\"})"+
			"(c:C {ctype : \"cvalue\"})"+
			"(e:E {etype : \"evalue\"})"+
			"tricky:G {tricky : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(d)-[:loop]->(b)"+
			"(b)-->(c)"+
			"(c)-->(e)"+
			"(c)-[:BetaEdge]->(d)"+
			"(d)-->(e)"+
			""+
			"]"+
			"loopPattern:G {loopPattern : \"graph\"}["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:loop]->(b)"+
			"(d)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("tricky");
		LogicalGraph right = loader.getLogicalGraphByVariable("loopPattern");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("tricky")));
	}
}
