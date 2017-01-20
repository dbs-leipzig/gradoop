package org.gradoop.flink.model.impl.operators.utils.testbuild;

import org.gradoop.flink.model.impl.operators.utils.GDLBuilder;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;

import java.io.File;
import java.io.IOException;

/**
 * Created by Giacomo Bergami on 19/01/17.
 */
public class FusionTestBuilder extends AbstractTestBuilder {

    public FusionTestBuilder() {
        super();
    }

    protected FlinkAsciiGraphLoader getTestGraphLoader() {
        if (loader==null) {
            declareVariables();

            //empty
            addTo("empty", GDLBuilder.GraphWithinDatabase.labelType("empty", "G").t());

            //single
            addTo("single", GDLBuilder.GraphWithinDatabase.labelType("single", "G").t()
                    .pat().from().t().done());

            //aVertex
            addTo("aVertex", GDLBuilder.GraphWithinDatabase.labelType("aVertex","G").t()
                    .pat()
                    .fromVariable("a").t()
                    .done());

            //aAbGraph
            addTo("aAbGraph", GDLBuilder.GraphWithinDatabase.labelType("aAbGraph","G").t()
                    .pat()
                    .fromVariable("a").t()
                    .edgeKey("AlphaEdge").t()
                    .toVariable("b").t()
                    .done());

            //aBbGraph
            addTo("aBbGraph", GDLBuilder.GraphWithinDatabase.labelType("aBbGraph","G").t()
                    .pat()
                    .fromVariable("a").t()
                    .edgeKey("BetaEdge").t()
                    .toVariable("b").t()
                    .done());

            // abVertex
            addTo("abGraph", GDLBuilder.GraphWithinDatabase.labelType("abGraph","G").t()
                    .pat()
                    .fromVariable("ab").t()
                    .done());

            // aAbCdGraph
            addTo("aAbCdGraph", GDLBuilder.GraphWithinDatabase.labelType("aAbCdGraph","G").t()
                    .pat()
                    .fromVariable("a").t()
                    .edgeKey("AlphaEdge").t()
                    .toVariable("b").t()
                    .done()

                    .pat()
                    .fromVariable("b").t()
                    .edgeKey("GammaEdge").t()
                    .toVariable("c").t()
                    .done());

            // abCdGraph
            addTo("abCdGraph", GDLBuilder.GraphWithinDatabase.labelType("abCdGraph","G").t()
                    .pat()
                    .fromVariable("ab").t()
                    .edgeKey("GammaEdge").t()
                    .toVariable("c").t()
                    .done());

            // abdGraph
            addTo("abdGraph", GDLBuilder.GraphWithinDatabase.labelType("abdGraph","G").t()
                    .pat()
                    .fromVariable("abc").t()
                    .done());

            // semicomplex
            addTo("semicomplex", GDLBuilder.GraphWithinDatabase.labelType("semicomplex","G").t()
                    .pat()
                    .fromVariable("a").t().edgeKey("AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().edgeKey("loop").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeKey("BetaEdge").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("d").t().toVariable("e").t()
                    .done());

            // looplessPattern
            addTo("looplessPattern", GDLBuilder.GraphWithinDatabase.labelType("looplessPattern","G").t()
                    .pat()
                    .fromVariable("a").t().edgeKey("AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t()
                    .done());

            // loopPattern
            addTo("loopPattern", GDLBuilder.GraphWithinDatabase.labelType("loopPattern","G").t()
                    .pat()
                    .fromVariable("a").t().edgeKey("AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().edgeKey("loop").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t()
                    .done());

            // firstmatch
            addTo("firstmatch", GDLBuilder.GraphWithinDatabase.labelType("firstmatch","G").t()
                    .pat()
                    .fromVariable("abd").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("abd").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("abd").t().edgeKey("loop").t().toVariable("abd").t()
                    .done().pat()
                    .fromVariable("c").t().edgeKey("BetaEdge").t().toVariable("abd").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());

            // secondmatch
            addTo("secondmatch", GDLBuilder.GraphWithinDatabase.labelType("secondmatch","G").t()
                    .pat()
                    .fromVariable("abd").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("abd").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeKey("BetaEdge").t().toVariable("abd").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());

            // tricky
            addTo("tricky", GDLBuilder.GraphWithinDatabase.labelType("tricky","G").t()
                    .pat()
                    .fromVariable("a").t().edgeKey("AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t().edgeKey("loop").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeKey("BetaEdge").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("d").t().toVariable("e").t()
                    .done());

        }
        return loader;
    }

    /////
    private void declareVariables() {
        //Declaring vertices
        GDLBuilder.VertexBuilder<?> a = simpleVertex("a","A");
        GDLBuilder.VertexBuilder<?> b = simpleVertex("b","B");
        GDLBuilder.VertexBuilder<?> c = simpleVertex("c","C");
        GDLBuilder.VertexBuilder<?> d = simpleVertex("d","D");
        GDLBuilder.VertexBuilder<?> e = simpleVertex("e","E");
        GDLBuilder.VertexBuilder<?> ab = new GDLBuilder.VertexBuilder<>();
        GDLBuilder.VertexBuilder.generateWithValueAndType(null,ab,"ab","G")
                .propList().put("avalue","atype").put("bvalue","btype").plEnd();
        GDLBuilder.VertexBuilder<?> abc = new GDLBuilder.VertexBuilder<>();
        GDLBuilder.VertexBuilder.generateWithValueAndType(null,abc,"ab","G")
                .propList().put("avalue","atype").put("bvalue","btype").put("cvalue","ctype").plEnd();
        GDLBuilder.VertexBuilder<?> abd = new GDLBuilder.VertexBuilder<>();
        GDLBuilder.VertexBuilder.generateWithValueAndType(null,abd,"abd","G")
                .propList().put("avalue","atype").put("bvalue","btype").put("cvalue","ctype").put("dvalue","dtype").plEnd();
        addTo("a",a);
        addTo("b",b);
        addTo("c",c);
        addTo("d",d);
        addTo("e",e);
        addTo("ab",ab);
        addTo("abc",abc);
        addTo("abd",abd);
    }

    public static void main(String args[]) throws IOException {
        FusionTestBuilder ft = new FusionTestBuilder();
        ft.check();
        ft.generateToFile(new File("./src/test/java/org/gradoop/flink/model/impl/operators/fusion/FusionTest.java"),"Fusion","empty empty empty\n" +
                "empty single empty\n" +
                "single empty single\n" +
                "single aVertex single\n" +
                "single single single\n" +
                "aVertex aVertex aVertex\n" +
                "aVertex empty aVertex\n" +
                "aVertex single aVertex\n" +
                "aAbGraph aVertex aAbGraph\n" +
                "aAbGraph empty aAbGraph\n" +
                "aAbGraph single aAbGraph\n" +
                "aAbGraph aAbGraph abGraph\n" +
                "aAbGraph aBbGraph aAbGraph\n" +
                "aBbGraph aVertex aBbGraph\n" +
                "aBbGraph empty aBbGraph\n" +
                "aBbGraph single aBbGraph\n" +
                "aBbGraph aAbGraph aBbGraph\n" +
                "aBbGraph aBbGraph abGraph\n" +
                "aAbCdGraph aAbCdGraph abdGraph\n" +
                "aAbCdGraph aAbGraph abCdGraph semicomplex looplessPattern firstmatch\n" +
                "semicomplex loopPattern secondmatch\n" +
                "tricky looplessPattern firstmatch\n" +
                "tricky loopPattern tricky");

    }

}
