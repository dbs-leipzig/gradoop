package org.gradoop.flink.model.impl.operators.fusion;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.TestUtils;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Created by Giacomo Bergami on 19/01/17.
 */
public class FusionTest  extends GradoopFlinkTestBase {

    StringBuilder databaseBuilder = new StringBuilder();
    private LogicalGraph empty, oneVertex;
    private LogicalGraph aVertex;
    private LogicalGraph aAbGraph;
    private LogicalGraph aBbGraph;
    private LogicalGraph abGraph;
    private LogicalGraph aAbCdGraph;
    private LogicalGraph abCdGraph;
    private LogicalGraph abdGraph;
    private LogicalGraph semicomplex;
    private LogicalGraph looplessPattern;
    private LogicalGraph loopPattern;
    private LogicalGraph firstmatch;
    private LogicalGraph secondmatch;
    private LogicalGraph tricky;

    private static void addTo(StringBuilder sb, Object o) {
        sb.append(o.toString()).append('\n');
    }
    private static TestUtils.VertexBuilder<?> simpleVertex(String var, String type) {
        TestUtils.VertexBuilder<?> a = new TestUtils.VertexBuilder<>();
        TestUtils.VertexBuilder.generateWithValueAndType(null,a,var,type).propList().put(var+"value",var+"type").plEnd();
        return a;
    }

    private void check(LogicalGraph left, LogicalGraph right, LogicalGraph toCheck, Fusion f) throws Exception {
        LogicalGraph result = f.execute(left, right);
        collectAndAssertTrue(result.equalsByElementData(toCheck));
    }

    //generally speaking, \Forall x. empty with to-be-fused x returns x
    @Test
    public void empty_3() throws Exception {
        Fusion f = null;
        check(empty,empty,empty,f);
    }

    @Test
    public void empty_single_Empty() throws Exception {
        Fusion f = null;
        check(empty,oneVertex,empty,f);
    }

    @Test
    public void single_empty_Single() throws Exception {
        Fusion f = null;
        check(oneVertex,empty,oneVertex,f);
    }

    @Test
    public void oneVertex_aVertex_oneVertex() throws Exception {
        Fusion f = null;
        check(oneVertex,aVertex,oneVertex,f);
    }

    @Test
    public void single_3() throws Exception {
        Fusion f = null;
        check(oneVertex,oneVertex,oneVertex,f);
    }

    @Test
    public void aVertex_3() throws Exception {
        Fusion f = null;
        check(aVertex,aVertex,aVertex,f);
    }

    @Test
    public void aVertex_empty_aVertex() throws Exception {
        Fusion f = null;
        check(aVertex,empty,aVertex,f);
    }

    @Test
    public void aVertex_oneVertex_aVertex() throws Exception {
        Fusion f = null;
        check(aVertex,oneVertex,aVertex,f);
    }

    @Test
    public void otherTest1() throws Exception {
        Fusion f = null;
        check( aAbGraph , aVertex , aAbGraph,f);
    }

    @Test
    public void otherTest2() throws Exception {
        Fusion f = null;
        check( aAbGraph , empty , aAbGraph,f);
    }

    @Test
    public void otherTest3() throws Exception {
        Fusion f = null;
        check( aAbGraph , oneVertex , aAbGraph,f);
    }

    @Test
    public void otherTest4() throws Exception {
        Fusion f = null;
        check( aAbGraph , aAbGraph , abGraph,f);
    }

    @Test
    public void otherTest5() throws Exception {
        Fusion f = null;
        check( aAbGraph , aBbGraph , aAbGraph,f);
    }

    @Test
    public void otherTest6() throws Exception {
        Fusion f = null;
        check( aBbGraph , aVertex , aBbGraph,f);
    }

    @Test
    public void otherTest7() throws Exception {
        Fusion f = null;
        check( aBbGraph , empty , aBbGraph,f);
    }

    @Test
    public void otherTest8() throws Exception {
        Fusion f = null;
        check( aBbGraph , oneVertex , aBbGraph,f);
    }

    @Test
    public void otherTest9() throws Exception {
        Fusion f = null;
        check( aBbGraph , aAbGraph , aBbGraph,f);
    }

    @Test
    public void otherTest10() throws Exception {
        Fusion f = null;
        check( aBbGraph , aBbGraph , abGraph,f);
    }

    @Test
    public void otherTest11() throws Exception {
        Fusion f = null;
        check( aAbCdGraph , aAbCdGraph , abdGraph,f);
    }

    @Test
    public void otherTest12() throws Exception {
        Fusion f = null;
        check( aAbCdGraph , aAbGraph , abCdGraph,f);
    }
    @Test
    public void otherTest13() throws Exception {
        Fusion f = null;
        check( semicomplex , looplessPattern , firstmatch,f);
    }

    @Test
    public void otherTest14() throws Exception {
        Fusion f = null;
        check( semicomplex , loopPattern , secondmatch,f);
    }

    @Test
    public void otherTest15() throws Exception {
        Fusion f = null;
        check( tricky , looplessPattern , firstmatch,f);
    }

    @Test
    public void otherTest16() throws Exception {
        Fusion f = null;
        check( tricky , loopPattern , tricky,f);
    }

    public FusionTest() {
        super();
        getTestGraphLoader();
    }

    private FlinkAsciiGraphLoader loader;
    private FlinkAsciiGraphLoader getTestGraphLoader() {
        if (loader==null) {
            declareVariables();

            //empty
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("empty", "G").t());

            //single
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("single", "G").t()
                    .pat().from().t().done());

            //aVertex
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("aVertex","G").t()
                    .pat()
                    .fromVariable("a").t()
                    .done());

            //aAbGraph
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("aAbGraph","G").t()
                    .pat()
                    .fromVariable("a").t()
                    .edgeVariableKey("α","AlphaEdge").t()
                    .toVariable("b").t()
                    .done());

            //aBbGraph
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("aBbGraph","G").t()
                    .pat()
                    .fromVariable("a").t()
                    .edgeVariableKey("β","BetaEdge").t()
                    .toVariable("b").t()
                    .done());

            // abVertex
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("abVertex","G").t()
                    .pat()
                    .fromVariable("ab").t()
                    .done());

            // aAbCdGraph
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("aAbCdGraph","G").t()
                    .pat()
                    .fromVariable("a").t()
                    .edgeVariableKey("α","AlphaEdge").t()
                    .toVariable("b").t()
                    .done()

                    .pat()
                    .fromVariable("b").t()
                    .edgeVariableKey("γ","GammaEdge").t()
                    .toVariable("c").t()
                    .done());

            // abCdGraph
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("abCdGraph","G").t()
                    .pat()
                    .fromVariable("ab").t()
                    .edgeVariableKey("γ","GammaEdge").t()
                    .toVariable("c").t()
                    .done());

            // abdGraph
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("abdGraph","G").t()
                    .pat()
                    .fromVariable("abc").t()
                    .done());

            // semicomplex
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("semicomplex","G").t()
                    .pat()
                    .fromVariable("a").t().edgeVariableKey("α","AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().edgeVariableKey("hook","loop").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariableKey("β","BetaEdge").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("d").t().toVariable("e").t()
                    .done());

            // looplessPattern
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("looplessPattern","G").t()
                    .pat()
                    .fromVariable("a").t().edgeVariableKey("α","AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t()
                    .done());

            // loopPattern
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("loopPattern","G").t()
                    .pat()
                    .fromVariable("a").t().edgeVariableKey("α","AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().edgeVariableKey("hook","loop").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t()
                    .done());

            // firstmatch
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("firstmatch","G").t()
                    .pat()
                    .fromVariable("abd").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("abd").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("abd").t().edgeVariableKey("hook","loop").t().toVariable("abd").t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariableKey("β","BetaEdge").t().toVariable("abd").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());

            // secondmatch
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("secondmatch","G").t()
                    .pat()
                    .fromVariable("abd").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("abd").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariableKey("β","BetaEdge").t().toVariable("abd").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());

            // tricky
            addTo(databaseBuilder, TestUtils.GraphWithinDatabase.labelType("tricky","G").t()
                    .pat()
                    .fromVariable("a").t().edgeVariableKey("α","AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t().edgeVariableKey("hook","loop").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariableKey("β","BetaEdge").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("d").t().toVariable("e").t()
                    .done());

            compileAndInitialize();
        }
        return loader;
    }

    /////
    private void declareVariables() {
        //Declaring vertices
        TestUtils.VertexBuilder<?> a = simpleVertex("a","A");
        TestUtils.VertexBuilder<?> b = simpleVertex("b","B");
        TestUtils.VertexBuilder<?> c = simpleVertex("c","C");
        TestUtils.VertexBuilder<?> d = simpleVertex("d","D");
        TestUtils.VertexBuilder<?> e = simpleVertex("e","E");
        TestUtils.VertexBuilder<?> ab = new TestUtils.VertexBuilder<>();
        TestUtils.VertexBuilder.generateWithValueAndType(null,ab,"ab","A+B")
                .propList().put("avalue","atype").put("bvalue","btype").plEnd();
        TestUtils.VertexBuilder<?> abc = new TestUtils.VertexBuilder<>();
        TestUtils.VertexBuilder.generateWithValueAndType(null,abc,"ab","A+B")
                .propList().put("avalue","atype").put("bvalue","btype").put("cvalue","ctype").plEnd();
        TestUtils.VertexBuilder<?> abd = new TestUtils.VertexBuilder<>();
        TestUtils.VertexBuilder.generateWithValueAndType(null,abd,"abd","A+B+D")
                .propList().put("avalue","atype").put("bvalue","btype").put("cvalue","ctype").put("dvalue","dtype").plEnd();
        addTo(databaseBuilder,a);
        addTo(databaseBuilder,b);
        addTo(databaseBuilder,c);
        addTo(databaseBuilder,d);
        addTo(databaseBuilder,e);
        addTo(databaseBuilder,ab);
        addTo(databaseBuilder,abc);
        addTo(databaseBuilder,abd);
    }

    private void compileAndInitialize() {
        loader = getLoaderFromString(databaseBuilder.toString());
        this.empty = loader.getLogicalGraphByVariable("empty");
        this.oneVertex = loader.getLogicalGraphByVariable("single");
        this.aVertex = loader.getLogicalGraphByVariable("aVertex");
        this.aAbGraph = loader.getLogicalGraphByVariable("aAbGraph");
        this.aBbGraph = loader.getLogicalGraphByVariable("aBbGraph");
        this.abGraph = loader.getLogicalGraphByVariable("abGraph");
        this.aAbCdGraph = loader.getLogicalGraphByVariable("aAbCdGraph");
        this.abCdGraph = loader.getLogicalGraphByVariable("abCdGraph");
        this.abdGraph = loader.getLogicalGraphByVariable("abdGraph");
        this.semicomplex = loader.getLogicalGraphByVariable("semicomplex");
        this.looplessPattern = loader.getLogicalGraphByVariable("looplessPattern");
        this.loopPattern = loader.getLogicalGraphByVariable("loopPattern");
        this.firstmatch = loader.getLogicalGraphByVariable("firstmatch");
        this.secondmatch = loader.getLogicalGraphByVariable("secondmatch");
        this.tricky = loader.getLogicalGraphByVariable("tricky");
    }

}
