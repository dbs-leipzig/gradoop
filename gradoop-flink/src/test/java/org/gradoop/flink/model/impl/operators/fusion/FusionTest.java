package org.gradoop.flink.model.impl.operators.fusion;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.junit.Assert;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class FusionTest extends GradoopFlinkTestBase {
	@Test
	public void empty_empty_to_empty() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"empty:G[]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("empty")));
	}

	@Test
	public void empty_emptyVertex_to_empty() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"empty:G[]"+
			"emptyVertex:G {emptyVertex : \"graph\"}[()]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("empty");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("empty")));
	}

	@Test
	public void emptyVertex_empty_to_emptyVertex() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"emptyVertex:G {emptyVertex : \"graph\"}[()]"+
			"empty:G[]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("emptyVertex")));
	}

	@Test
	public void emptyVertex_graphWithA_to_emptyVertex() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"emptyVertex:G {emptyVertex : \"graph\"}[()]"+
			"graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("emptyVertex")));
	}

	@Test
	public void emptyVertex_emptyVertex_to_singleInside() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"emptyVertex:G {emptyVertex : \"graph\"}[()]"+
			"singleInside:G {emptyVertex : \"graph\"}[(:G {emptyVertex : \"graph\"})]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("emptyVertex");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("singleInside")));
	}

	@Test
	public void graphWithA_graphWithA_to_aGraphLabels() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]"+
			"aGraphLabels:G {graphWithA : \"graph\"}[(:G {graphWithA : \"graph\"})]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("aGraphLabels")));
	}

	@Test
	public void graphWithA_empty_to_graphWithA() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]"+
			"empty:G[]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("graphWithA")));
	}

	@Test
	public void graphWithA_emptyVertex_to_graphWithA() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]"+
			"emptyVertex:G {emptyVertex : \"graph\"}[()]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("graphWithA");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("graphWithA")));
	}

	@Test
	public void ab_edgeWithAlpha_graphWithA_to_aggregatedASource() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]"+
			"aggregatedASource:G {ab_edgeWithAlpha : \"graph\"}[(:G {graphWithA : \"graph\"})-[:AlphaEdge {alphatype : \"alphavalue\"}]->(:B {btype : \"bvalue\"}) ]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("aggregatedASource")));
	}

	@Test
	public void ab_edgeWithAlpha_empty_to_ab_edgeWithAlpha() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"empty:G[]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("ab_edgeWithAlpha")));
	}

	@Test
	public void ab_edgeWithAlpha_emptyVertex_to_ab_edgeWithAlpha() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"emptyVertex:G {emptyVertex : \"graph\"}[()]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("ab_edgeWithAlpha")));
	}

	@Test
	public void ab_edgeWithAlpha_ab_edgeWithAlpha_to_fused_edgeWithAlpha() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"fused_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[(:G {ab_edgeWithAlpha : \"graph\"})]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("fused_edgeWithAlpha")));
	}

	@Test
	public void ab_edgeWithAlpha_ab_edgeWithBeta_to_ab_edgeWithBeta_loop() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"ab_edgeWithBeta_loop:G {ab_edgeWithAlpha : \"graph\"}[(:G {ab_edgeWithBeta : \"graph\"})-[:AlphaEdge {alphatype : \"alphavalue\"}]->(:G {ab_edgeWithBeta : \"graph\"}) ]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("ab_edgeWithBeta_loop")));
	}

	@Test
	public void ab_edgeWithBeta_graphWithA_to_ab_edgeWithBeta() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"graphWithA:G {graphWithA : \"graph\"}[(a:A {atype : \"avalue\"})]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("graphWithA");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("ab_edgeWithBeta")));
	}

	@Test
	public void ab_edgeWithBeta_empty_to_ab_edgeWithBeta() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"empty:G[]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("empty");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("ab_edgeWithBeta")));
	}

	@Test
	public void ab_edgeWithBeta_emptyVertex_to_ab_edgeWithBeta() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"emptyVertex:G {emptyVertex : \"graph\"}[()]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("emptyVertex");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("ab_edgeWithBeta")));
	}

	@Test
	public void ab_edgeWithBeta_ab_edgeWithBeta_to_fused_edgeWithBeta() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"ab_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"fused_edgeWithBeta:G {ab_edgeWithBeta : \"graph\"}[(:G {ab_edgeWithBeta : \"graph\"})]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithBeta");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("fused_edgeWithBeta")));
	}

	@Test
	public void abcdGraph_abcdGraph_to_abdGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"abcdGraph:G {abcdGraph : \"graph\"}[(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(c:C {ctype : \"cvalue\"}) ]"+
			"abdGraph:G {abcdGraph : \"graph\"}[(:G {abcdGraph : \"graph\"})]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("abcdGraph");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("abcdGraph");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("abdGraph")));
	}

	@Test
	public void abcdGraph_ab_edgeWithAlpha_to_ab_fusedGraph() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"abcdGraph:G {abcdGraph : \"graph\"}[(a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(c:C {ctype : \"cvalue\"}) ]"+
			"ab_edgeWithAlpha:G {ab_edgeWithAlpha : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"ab_fusedGraph:G {abcdGraph : \"graph\"}[(:G {ab_edgeWithAlpha : \"graph\"})-[:GammaEdge {gtype : \"gvalue\"}]->(:C {ctype : \"cvalue\"}) ]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("abcdGraph");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("ab_edgeWithAlpha");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("ab_fusedGraph")));
	}

	@Test
	public void semicomplex_looplessPattern_to_firstmatch() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"semicomplex:G {semicomplex : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  (c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  (c:C {ctype : \"cvalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : \"dvalue\"})  (d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) ]"+
			"looplessPattern:G {looplessPattern : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (d:D {dtype : \"dvalue\"})]"+
			"firstmatch:G {semicomplex : \"graph\"}[(:G {looplessPattern : \"graph\"})-->(:C {ctype : \"cvalue\"})  (:G {looplessPattern : \"graph\"})-->(:E {etype : \"evalue\"})  (:G {looplessPattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(:G {looplessPattern : \"graph\"})  (:C {ctype : \"cvalue\"})-[:BetaEdge {betatype : \"betavalue\"}]->(:G {looplessPattern : \"graph\"})  (:C {ctype : \"cvalue\"})-->(:E {etype : \"evalue\"}) ]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("semicomplex");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("looplessPattern");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("firstmatch")));
	}

	@Test
	public void semicomplex_loopPattern_to_secondmatch() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"semicomplex:G {semicomplex : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  (c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  (c:C {ctype : \"cvalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : \"dvalue\"})  (d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) ]"+
			"loopPattern:G {loopPattern : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  (d:D {dtype : \"dvalue\"})]"+
			"secondmatch:G {semicomplex : \"graph\"}[(:G {loopPattern : \"graph\"})-->(:C {ctype : \"cvalue\"})  (:G {loopPattern : \"graph\"})-->(:E {etype : \"evalue\"})  (:C {ctype : \"cvalue\"})-[:BetaEdge {betatype : \"betavalue\"}]->(:G {loopPattern : \"graph\"})  (:C {ctype : \"cvalue\"})-->(:E {etype : \"evalue\"}) ]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("semicomplex");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("loopPattern");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("secondmatch")));
	}

	@Test
	public void tricky_looplessPattern_to_thirdmatch() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"tricky:G {tricky : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (d:D {dtype : \"dvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  (c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  (c:C {ctype : \"cvalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : \"dvalue\"})  (d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) ]"+
			"looplessPattern:G {looplessPattern : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (d:D {dtype : \"dvalue\"})]"+
			"thirdmatch:G {tricky : \"graph\"}[(:G {looplessPattern : \"graph\"})-->(:C {ctype : \"cvalue\"})  (:G {looplessPattern : \"graph\"})-->(:E {etype : \"evalue\"})  (:G {looplessPattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(:G {looplessPattern : \"graph\"})  (:C {ctype : \"cvalue\"})-[:BetaEdge {betatype : \"betavalue\"}]->(:G {looplessPattern : \"graph\"})  (:C {ctype : \"cvalue\"})-->(:E {etype : \"evalue\"}) ]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("tricky");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("looplessPattern");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("thirdmatch")));
	}

	@Test
	public void tricky_loopPattern_to_tricky() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"tricky:G {tricky : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (d:D {dtype : \"dvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-->(c:C {ctype : \"cvalue\"})  (c:C {ctype : \"cvalue\"})-->(e:E {etype : \"evalue\"})  (c:C {ctype : \"cvalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(d:D {dtype : \"dvalue\"})  (d:D {dtype : \"dvalue\"})-->(e:E {etype : \"evalue\"}) ]"+
			"loopPattern:G {loopPattern : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (b:B {btype : \"bvalue\"})-[l:loop {ltype : \"lvalue\"}]->(b:B {btype : \"bvalue\"})  (d:D {dtype : \"dvalue\"})]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("tricky");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("loopPattern");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("tricky")));
	}

	@Test
	public void source_pattern_to_source_fusewith_pattern() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"source:G {source : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"})  (a:A {atype : \"avalue\"})-[l:loop {ltype : \"lvalue\"}]->(c:C {ctype : \"cvalue\"})  (c:C {ctype : \"cvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(d:D {dtype : \"dvalue\"}) ]"+
			"pattern:G {pattern : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"source_fusewith_pattern:G {source : \"graph\"}[(:G {pattern : \"graph\"})-[:BetaEdge {betatype : \"betavalue\"}]->(:G {pattern : \"graph\"})  (:G {pattern : \"graph\"})-[:loop {ltype : \"lvalue\"}]->(:C {ctype : \"cvalue\"})  (:C {ctype : \"cvalue\"})-[:GammaEdge {gtype : \"gvalue\"}]->(:D {dtype : \"dvalue\"}) ]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("source");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("pattern");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("source_fusewith_pattern")));
	}

	@Test
	public void pattern_source_to_pattern_fusewith_source() throws Exception {
		FlinkAsciiGraphLoader loader = getLoaderFromString(
"pattern:G {pattern : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"}) ]"+
			"source:G {source : \"graph\"}[(a:A {atype : \"avalue\"})-[alpha:AlphaEdge {alphatype : \"alphavalue\"}]->(b:B {btype : \"bvalue\"})  (a:A {atype : \"avalue\"})-[beta:BetaEdge {betatype : \"betavalue\"}]->(b:B {btype : \"bvalue\"})  (a:A {atype : \"avalue\"})-[l:loop {ltype : \"lvalue\"}]->(c:C {ctype : \"cvalue\"})  (c:C {ctype : \"cvalue\"})-[g:GammaEdge {gtype : \"gvalue\"}]->(d:D {dtype : \"dvalue\"}) ]"+
			"pattern_fusewith_source:G {pattern : \"graph\"}[(:G {source : \"graph\"})]");
		LogicalGraph searchGraph = loader.getLogicalGraphByVariable("pattern");
		LogicalGraph patternGraph = loader.getLogicalGraphByVariable("source");
		Fusion f = new Fusion();
		LogicalGraph output = f.execute(searchGraph,patternGraph);
		Assert.assertTrue("no match provided", FusionTestUtils.graphEquals(output,loader.getLogicalGraphByVariable("pattern_fusewith_source")));
	}
}
