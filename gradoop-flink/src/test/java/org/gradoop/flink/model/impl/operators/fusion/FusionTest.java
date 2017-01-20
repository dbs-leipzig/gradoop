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
			"single:G["+
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
		FlinkAsciiGraphLoader loader = getLoaderFromString("single:G["+
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
	public void single_aVertex_to_single() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"single:G["+
			"()"+
			"]"+
			"aVertex:G["+
			"(a)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("single");
		LogicalGraph right = loader.getLogicalGraphByVariable("aVertex");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("single")));
	}

	@Test
	public void single_single_to_single() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("single:G["+
			"()"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("single");
		LogicalGraph right = loader.getLogicalGraphByVariable("single");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("single")));
	}

	@Test
	public void aVertex_aVertex_to_aVertex() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"aVertex:G["+
			"(a)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aVertex");
		LogicalGraph right = loader.getLogicalGraphByVariable("aVertex");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aVertex")));
	}

	@Test
	public void aVertex_empty_to_aVertex() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"aVertex:G["+
			"(a)"+
			"]"+
			"empty:G[]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aVertex");
		LogicalGraph right = loader.getLogicalGraphByVariable("empty");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aVertex")));
	}

	@Test
	public void aVertex_single_to_aVertex() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"aVertex:G["+
			"(a)"+
			"]"+
			"single:G["+
			"()"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aVertex");
		LogicalGraph right = loader.getLogicalGraphByVariable("single");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aVertex")));
	}

	@Test
	public void aAbGraph_aVertex_to_aAbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aAbGraph:G["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"aVertex:G["+
			"(a)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aVertex");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aAbGraph")));
	}

	@Test
	public void aAbGraph_empty_to_aAbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aAbGraph:G["+
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
			"aAbGraph:G["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"single:G["+
			"()"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("single");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aAbGraph")));
	}

	@Test
	public void aAbGraph_aAbGraph_to_abGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(ab:G {atype : \"avalue\",btype : \"bvalue\"})"+
			"aAbGraph:G["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"abGraph:G["+
			"(ab)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aAbGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("abGraph")));
	}

	@Test
	public void aAbGraph_aBbGraph_to_aAbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aAbGraph:G["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"aBbGraph:G["+
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
	public void aBbGraph_aVertex_to_aBbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aBbGraph:G["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]"+
			"aVertex:G["+
			"(a)"+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aBbGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aVertex");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("aBbGraph")));
	}

	@Test
	public void aBbGraph_empty_to_aBbGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"aBbGraph:G["+
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
			"aBbGraph:G["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]"+
			"single:G["+
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
			"aBbGraph:G["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]"+
			"aAbGraph:G["+
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
			"(ab:G {atype : \"avalue\",btype : \"bvalue\"})"+
			"aBbGraph:G["+
			"(a)-[:BetaEdge]->(b)"+
			""+
			"]"+
			"abGraph:G["+
			"(ab)"+
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
			"(ab:G {ctype : \"cvalue\",atype : \"avalue\",btype : \"bvalue\"})"+
			"aAbCdGraph:G["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:GammaEdge]->(c)"+
			""+
			"]"+
			"abdGraph:G["+
			"(abc)"+
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
			"(ab:G {atype : \"avalue\",btype : \"bvalue\"})"+
			"aAbCdGraph:G["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:GammaEdge]->(c)"+
			""+
			"]"+
			"aAbGraph:G["+
			"(a)-[:AlphaEdge]->(b)"+
			""+
			"]"+
			"abCdGraph:G["+
			"(ab)-[:GammaEdge]->(c)"+
			""+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("aAbCdGraph");
		LogicalGraph right = loader.getLogicalGraphByVariable("aAbGraph");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("abCdGraph")));
	}

	@Test
	public void semicomplex_loopPattern_to_secondmatch() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(d:D {dtype : \"dvalue\"})"+
			"(c:C {ctype : \"cvalue\"})"+
			"(e:E {etype : \"evalue\"})"+
			"(abd:G {ctype : \"cvalue\",atype : \"avalue\",btype : \"bvalue\",dtype : \"dvalue\"})"+
			"semicomplex:G["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:loop]->(b)"+
			"(b)-->(c)"+
			"(c)-->(e)"+
			"(c)-[:BetaEdge]->(d)"+
			"(d)-->(e)"+
			""+
			"]"+
			"loopPattern:G["+
			"(a)-[:AlphaEdge]->(b)"+
			"(b)-[:loop]->(b)"+
			"(d)"+
			"]"+
			"secondmatch:G["+
			"(abd)-->(c)"+
			"(abd)-->(e)"+
			"(c)-[:BetaEdge]->(abd)"+
			"(c)-->(d)"+
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
	public void tricky_looplessPattern_to_firstmatch() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(c:C {ctype : \"cvalue\"})"+
			"(d:D {dtype : \"dvalue\"})"+
			"(e:E {etype : \"evalue\"})"+
			"(abd:G {ctype : \"cvalue\",atype : \"avalue\",btype : \"bvalue\",dtype : \"dvalue\"})"+
			"tricky:G["+
			"(a)-[:AlphaEdge]->(b)"+
			"(d)-[:loop]->(b)"+
			"(b)-->(c)"+
			"(c)-->(e)"+
			"(c)-[:BetaEdge]->(d)"+
			"(d)-->(e)"+
			""+
			"]"+
			"looplessPattern:G["+
			"(a)-[:AlphaEdge]->(b)"+
			"(d)"+
			"]"+
			"firstmatch:G["+
			"(abd)-->(c)"+
			"(abd)-->(e)"+
			"(abd)-[:loop]->(abd)"+
			"(c)-[:BetaEdge]->(abd)"+
			"(c)-->(d)"+
			"(c)-->(e)"+
			""+
			"]");
		LogicalGraph left = loader.getLogicalGraphByVariable("tricky");
		LogicalGraph right = loader.getLogicalGraphByVariable("looplessPattern");
		Fusion f = null;
		LogicalGraph output = f.execute(left,right);
		collectAndAssertTrue(output.equalsByElementData(loader.getLogicalGraphByVariable("firstmatch")));
	}

	@Test
	public void tricky_loopPattern_to_tricky() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString("(a:A {atype : \"avalue\"})"+
			"(b:B {btype : \"bvalue\"})"+
			"(c:C {ctype : \"cvalue\"})"+
			"(d:D {dtype : \"dvalue\"})"+
			"(e:E {etype : \"evalue\"})"+
			"tricky:G["+
			"(a)-[:AlphaEdge]->(b)"+
			"(d)-[:loop]->(b)"+
			"(b)-->(c)"+
			"(c)-->(e)"+
			"(c)-[:BetaEdge]->(d)"+
			"(d)-->(e)"+
			""+
			"]"+
			"loopPattern:G["+
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
