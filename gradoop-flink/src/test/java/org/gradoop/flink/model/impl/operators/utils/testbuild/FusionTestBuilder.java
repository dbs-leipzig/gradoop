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
        super("org.gradoop.flink.model.impl.operators.fusion");
    }

    public static <P> GDLBuilder.PatternBuilder returnStartGraph(String name) {
        return GDLBuilder.GraphWithinDatabase.labelType(name,"G")
                .propList()
                .put("graph",name).plEnd()
                .pat();
    }

    public static GDLBuilder.PatternBuilder returnAggGraph(String name, String oldName) {
        return GDLBuilder.GraphWithinDatabase.labelType(name,"G")
                .propList()
                .put("graph",oldName).plEnd()
                .pat();
    }


    public void addSelfieGraphOf(String newName, String oldName) {
        addTo(newName, GDLBuilder.GraphWithinDatabase.labelType(newName,"G")
                .propList()
                .put("graph",oldName).plEnd()
                .pat()
                .fromVariable(belongToGraph(oldName)).t()
                .done());
    }

    public void returnStartGraphAndAdd(String element) {
        returnStartGraph(element);
    }


    protected FlinkAsciiGraphLoader getTestGraphLoader() {
        if (loader==null) {
            declareVariables();

            //empty
            addTo("empty", GDLBuilder.GraphWithinDatabase.labelType("empty", "G").t());

            //single
            addTo("single", returnStartGraph("single")
                    .from().t().done());

            //aVertex
            addTo("aGraph", returnStartGraph("aGraph")
                    .fromVariable("a").t()
                    .done());

            //aGraphLabels
            addSelfieGraphOf("aGraphLabels","aGraph");
            //////////////////

            /////////////////
            //aAbGraph
            addTo("aAbGraph", returnStartGraph("aAbGraph")
                    .fromVariable("a").t()
                    .edgeKey("AlphaEdge").t()
                    .toVariable("b").t()
                    .done());

            //a aggregated aAbGraph
            addTo("aggAbGraph", returnAggGraph("aggAbGraph","aAbGraph")
                    .fromVariable(belongToGraph("aGraph")).t()
                    .edgeKey("AlphaEdge").t()
                    .toVariable("b").t()
                    .done());

            //Whole aggregated aAbGraph
            addSelfieGraphOf("wholeaAbGraph","aAbGraph");
            /*addTo("wholeaAbGraph", GDLBuilder.GraphWithinDatabase.labelType("wholeaAbGraph","G")
                    .propList()
                        .put("graph","aAbGraph").plEnd()
                    .pat()
                    .fromVariable("aAbGraph").t()
                    .done());*/
            //////////////////

            //////////////////
            //aBbGraph
            addTo("aBbGraph", returnStartGraph("aBbGraph")
                    .fromVariable("a").t()
                    .edgeKey("BetaEdge").t()
                    .toVariable("b").t()
                    .done());

            // abVertex
            addSelfieGraphOf("abGraph","aBbGraph");
            //////////////////

            // aAbCdGraph
            addTo("aAbCdGraph",returnStartGraph("aAbCdGraph")
                    .fromVariable("a").t()
                    .edgeKey("AlphaEdge").t()
                    .toVariable("b").t()
                    .done()

                    .pat()
                    .fromVariable("b").t()
                    .edgeKey("GammaEdge").t()
                    .toVariable("c").t()
                    .done());


            addSelfieGraphOf("abdGraph","aAbCdGraph");



            addTo("abCdGraph", returnAggGraph("abCdGraph","aAbCdGraph")
                    .fromVariable(belongToGraph("aAbGraph")).t()
                    .edgeKey("GammaEdge").t()
                    .toVariable("c").t()
                    .done());




            /*


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
                    .done());*/


            addTo("semicomplex",returnStartGraph("semicomplex")
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

            addTo("looplessPattern",returnStartGraph("looplessPattern")
                    .fromVariable("a").t().edgeKey("AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t()
                    .done());

            addTo("firstmatch",returnAggGraph("firstmatch","semicomplex")
                    .fromVariable(belongToGraph("looplessPattern")).t().toVariable("c").t()
                    .done().pat()
                    .fromVariable(belongToGraph("looplessPattern")).t().toVariable("e").t()
                    .done().pat()
                    .fromVariable(belongToGraph("looplessPattern")).t().edgeKey("loop").t().toVariable(belongToGraph("looplessPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().edgeKey("BetaEdge").t().toVariable(belongToGraph("looplessPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());
            /////////////////

            addTo("loopPattern",returnStartGraph("loopPattern")
                    .fromVariable("a").t().edgeKey("AlphaEdge").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().edgeKey("loop").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t()
                    .done());

            addTo("secondmatch",returnAggGraph("secondmatch","semicomplex")
                    .fromVariable(belongToGraph("loopPattern")).t().toVariable("c").t()
                    .done().pat()
                    .fromVariable(belongToGraph("loopPattern")).t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeKey("BetaEdge").t().toVariable(belongToGraph("loopPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());
            /////////////////

            addTo("tricky",returnStartGraph("tricky")
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

            addTo("thirdmatch",returnAggGraph("thirdmatch","tricky")
                    .fromVariable(belongToGraph("looplessPattern")).t().toVariable("c").t()
                    .done().pat()
                    .fromVariable(belongToGraph("looplessPattern")).t().toVariable("e").t()
                    .done().pat()
                    .fromVariable(belongToGraph("looplessPattern")).t().edgeKey("loop").t().toVariable(belongToGraph("looplessPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().edgeKey("BetaEdge").t().toVariable(belongToGraph("looplessPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());


            /*

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
                    .done());*/

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

        /*GDLBuilder.VertexBuilder<?> ab = new GDLBuilder.VertexBuilder<>();
        GDLBuilder.VertexBuilder.generateWithVariableAndType(null,ab,"ab","G")
                .propList().put("avalue","atype").put("bvalue","btype").plEnd();
        GDLBuilder.VertexBuilder<?> abc = new GDLBuilder.VertexBuilder<>();
        GDLBuilder.VertexBuilder.generateWithVariableAndType(null,abc,"ab","G")
                .propList().put("avalue","atype").put("bvalue","btype").put("cvalue","ctype").plEnd();
        GDLBuilder.VertexBuilder<?> abd = new GDLBuilder.VertexBuilder<>();
        GDLBuilder.VertexBuilder.generateWithVariableAndType(null,abd,"abd","G")
                .propList().put("avalue","atype").put("bvalue","btype").put("cvalue","ctype").put("dvalue","dtype").plEnd();*/
        //addToGraphAttribute("aGraph","graph");
        //addToGraphAttribute("aAbGraph","graph");

        addTo("a",a);
        addTo("b",b);
        addTo("c",c);
        addTo("d",d);
        addTo("e",e);
        //addTo("ab",ab);
        //addTo("abc",abc);
        //addTo("abd",abd);

    }

    public static void main(String args[]) throws IOException {
        FusionTestBuilder ft = new FusionTestBuilder();
        ft.check();
        ft.generateToFile(new File("./src/test/java/org/gradoop/flink/model/impl/operators/fusion/FusionTest.java"),"Fusion","empty empty empty" +
                "\nempty single empty" +
                "\nsingle empty single" +
                "\nsingle aGraph single" +
                "\nsingle single single" +
                "\naGraph aGraph aGraphLabels" +
                "\naGraph empty aGraph" +
                "\naGraph single aGraph" +
                "\naAbGraph aGraph aggAbGraph" +
                "\naAbGraph empty aAbGraph" +
                "\naAbGraph single aAbGraph" +
                "\naAbGraph aAbGraph wholeaAbGraph" +
                "\naAbGraph aBbGraph aAbGraph" +
                "\naBbGraph aGraph aBbGraph" +
                "\naBbGraph empty aBbGraph" +
                "\naBbGraph single aBbGraph" +
                "\naBbGraph aAbGraph aBbGraph" +
                "\naBbGraph aBbGraph abGraph" +
                "\naAbCdGraph aAbCdGraph abdGraph" +
                "\naAbCdGraph aAbGraph abCdGraph" +
                "\nsemicomplex looplessPattern firstmatch" +
                "\nsemicomplex loopPattern secondmatch" +
                "\ntricky looplessPattern thirdmatch" +
                "\ntricky loopPattern tricky");

    }

}
