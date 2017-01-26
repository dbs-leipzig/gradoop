package org.gradoop.flink.model.impl.operators.utils.testbuild;

import org.gradoop.flink.model.impl.operators.utils.GDLBuilder;
import org.gradoop.flink.model.impl.operators.utils.IWithDependencies;
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

            //emptyVertex
            addTo("emptyVertex", returnStartGraph("emptyVertex")
                    .from().t().done());

            addSelfieGraphOf("singleInside","emptyVertex");

            //aVertex
            addTo("graphWithA", returnStartGraph("graphWithA")
                    .fromVariable("a").t()
                    .done());

            //aGraphLabels
            addSelfieGraphOf("aGraphLabels","graphWithA");
            //////////////////

            /////////////////
            //ab_edgeWithAlpha
            addTo("ab_edgeWithAlpha", returnStartGraph("ab_edgeWithAlpha")
                    .fromVariable("a").t()
                    .edgeVariable("alpha").t()
                    .toVariable("b").t()
                    .done());

            //a aggregated ab_edgeWithAlpha
            addTo("aggregatedASource", returnAggGraph("aggregatedASource","ab_edgeWithAlpha")
                    .fromVariable(belongToGraph("graphWithA")).t()
                    .edgeVariable("alpha").t()
                    .toVariable("b").t()
                    .done());

            //Whole aggregated ab_edgeWithAlpha
            addSelfieGraphOf("fused_edgeWithAlpha","ab_edgeWithAlpha");
            /*addTo("fused_edgeWithAlpha", GDLBuilder.GraphWithinDatabase.labelType("fused_edgeWithAlpha","G")
                    .propList()
                        .put("graph","ab_edgeWithAlpha").plEnd()
                    .pat()
                    .fromVariable("ab_edgeWithAlpha").t()
                    .done());*/
            //////////////////

            //////////////////
            //ab_edgeWithBeta
            addTo("ab_edgeWithBeta", returnStartGraph("ab_edgeWithBeta")
                    .fromVariable("a").t()
                    .edgeVariable("beta").t()
                    .toVariable("b").t()
                    .done());

            addTo("ab_edgeWithBeta_loop", returnAggGraph("ab_edgeWithBeta_loop","ab_edgeWithAlpha")
                    .fromVariable(belongToGraph("ab_edgeWithBeta")).t()
                    .edgeVariable("alpha").t()
                    .toVariable(belongToGraph("ab_edgeWithBeta")).t()
                    .done());

            // abVertex
            addSelfieGraphOf("fused_edgeWithBeta","ab_edgeWithBeta");
            //////////////////

            // abcdGraph
            addTo("abcdGraph",returnStartGraph("abcdGraph")
                    .fromVariable("a").t()
                    .edgeVariable("beta").t()
                    .toVariable("b").t()
                    .done()

                    .pat()
                    .fromVariable("b").t()
                    .edgeVariable("g").t()
                    .toVariable("c").t()
                    .done());


            addSelfieGraphOf("abdGraph","abcdGraph");



            addTo("ab_fusedGraph", returnAggGraph("ab_fusedGraph","abcdGraph")
                    .fromVariable(belongToGraph("ab_edgeWithAlpha")).t()
                    .edgeVariable("g").t()
                    .toVariable("c").t()
                    .done());


            addTo("semicomplex",returnStartGraph("semicomplex")
                    .fromVariable("a").t().edgeVariable("alpha").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().edgeVariable("l").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariable("beta").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("d").t().toVariable("e").t()
                    .done());

            addTo("looplessPattern",returnStartGraph("looplessPattern")
                    .fromVariable("a").t().edgeVariable("alpha").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t()
                    .done());

            addTo("firstmatch",returnAggGraph("firstmatch","semicomplex")
                    .fromVariable(belongToGraph("looplessPattern")).t().toVariable("c").t()
                    .done().pat()
                    .fromVariable(belongToGraph("looplessPattern")).t().toVariable("e").t()
                    .done().pat()
                    .fromVariable(belongToGraph("looplessPattern")).t().edgeVariable("l").t().toVariable(belongToGraph("looplessPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariable("beta").t().toVariable(belongToGraph("looplessPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());
            /////////////////

            addTo("loopPattern",returnStartGraph("loopPattern")
                    .fromVariable("a").t().edgeVariable("alpha").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().edgeVariable("l").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t()
                    .done());

            addTo("secondmatch",returnAggGraph("secondmatch","semicomplex")
                    .fromVariable(belongToGraph("loopPattern")).t().toVariable("c").t()
                    .done().pat()
                    .fromVariable(belongToGraph("loopPattern")).t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariable("beta").t().toVariable(belongToGraph("loopPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());
            /////////////////

            addTo("tricky",returnStartGraph("tricky")
                    .fromVariable("a").t().edgeVariable("alpha").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t().edgeVariable("l").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariable("beta").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("d").t().toVariable("e").t()
                    .done());

            addTo("thirdmatch",returnAggGraph("thirdmatch","tricky")
                    .fromVariable(belongToGraph("looplessPattern")).t().toVariable("c").t()
                    .done().pat()
                    .fromVariable(belongToGraph("looplessPattern")).t().toVariable("e").t()
                    .done().pat()
                    .fromVariable(belongToGraph("looplessPattern")).t().edgeVariable("l").t().toVariable(belongToGraph("looplessPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariable("beta").t().toVariable(belongToGraph("looplessPattern")).t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done());


            addTo("source",returnStartGraph("source")
                .fromVariable("a").t().edgeVariable("alpha").t().toVariable("b").t()
                .done().pat()
                .fromVariable("a").t().edgeVariable("beta").t().toVariable("b").t()
                .done().pat()
                .fromVariable("a").t().edgeVariable("l").t().toVariable("c").t()
                .done().pat()
                .fromVariable("c").t().edgeVariable("g").t().toVariable("d").t()
                .done()
            );

            addTo("pattern",returnStartGraph("pattern")
                    .fromVariable("a").t().edgeVariable("alpha").t().toVariable("b").t()
                    .done()
            );

            addTo("source_fusewith_pattern",returnAggGraph("source_fusewith_pattern","source")
                    .fromVariable(belongToGraph("pattern")).t().edgeVariable("beta").t().toVariable(belongToGraph("pattern")).t()
                    .done().pat()
                    .fromVariable(belongToGraph("pattern")).t().edgeVariable("l").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariable("g").t().toVariable("d").t()
                    .done()
            );

            addTo("pattern_fusewith_source",returnAggGraph("pattern_fusewith_source","pattern")
                    .fromVariable(belongToGraph("source")).t().done()
            );

            /*

            // tricky
            addTo("tricky", GDLBuilder.GraphWithinDatabase.labelType("tricky","G").t()
                    .pat()
                    .fromVariable("a").t().edgeVariable("alpha").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("d").t().edgeVariable("l").t().toVariable("b").t()
                    .done().pat()
                    .fromVariable("b").t().toVariable("c").t()
                    .done().pat()
                    .fromVariable("c").t().toVariable("e").t()
                    .done().pat()
                    .fromVariable("c").t().edgeVariable("beta").t().toVariable("d").t()
                    .done().pat()
                    .fromVariable("d").t().toVariable("e").t()
                    .done());*/

        }
        return loader;
    }


    public void addElement(String var, String type, boolean isVertex) {
        IWithDependencies a = isVertex ? simpleVertex(var, type) : simpleEdge(var,type);
        addTo(var,a);
    }

    public void addVertex(String var, String type) {
        addElement(var,type,true);
    }
    public void addEdge(String var, String type) {
        addElement(var,type,true);
    }

    /////
    private void declareVariables() {
        //Declaring vertices
        addVertex("a","A");
        addVertex("b","B");
        addVertex("c","C");
        addVertex("d","D");
        addVertex("e","E");
        addEdge("alpha","AlphaEdge");
        addEdge("beta","BetaEdge");
        addEdge("l","loop");
        addEdge("g","GammaEdge");

    }

    public static void main(String args[]) throws IOException {
        FusionTestBuilder ft = new FusionTestBuilder();
        ft.check();
        ft.generateToFile(new File("./src/test/java/org/gradoop/flink/model/impl/operators/fusion/FusionTest.java"),"Fusion","empty empty empty" +
                "\nempty emptyVertex empty" +
                "\nemptyVertex empty emptyVertex" +
                "\nemptyVertex graphWithA emptyVertex" +
                "\nemptyVertex emptyVertex singleInside" +
                "\ngraphWithA graphWithA aGraphLabels" +
                "\ngraphWithA empty graphWithA" +
                "\ngraphWithA emptyVertex graphWithA" +
                "\nab_edgeWithAlpha graphWithA aggregatedASource" +
                "\nab_edgeWithAlpha empty ab_edgeWithAlpha" +
                "\nab_edgeWithAlpha emptyVertex ab_edgeWithAlpha" +
                "\nab_edgeWithAlpha ab_edgeWithAlpha fused_edgeWithAlpha" +
                "\nab_edgeWithAlpha ab_edgeWithBeta ab_edgeWithBeta_loop" +
                "\nab_edgeWithBeta empty ab_edgeWithBeta" +
                "\nab_edgeWithBeta emptyVertex ab_edgeWithBeta" +
                "\nab_edgeWithBeta ab_edgeWithBeta fused_edgeWithBeta" +
                "\nabcdGraph abcdGraph abdGraph" +
                "\nabcdGraph ab_edgeWithAlpha ab_fusedGraph" +
                "\nsemicomplex looplessPattern firstmatch" +
                "\ntricky looplessPattern thirdmatch" +
                "\nsource pattern source_fusewith_pattern"+
                "\npattern source pattern_fusewith_source","searchGraph","patternGraph");

    }

}
