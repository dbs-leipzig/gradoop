package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.comparables.time.Duration;
import org.s1ck.gdl.model.comparables.time.TimeConstant;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.*;
import static org.s1ck.gdl.utils.Comparator.*;

public class CNFEstimationTest extends TemporalGradoopTestBase {

    TimeSelector aTxFrom = new TimeSelector("a", TX_FROM);
    TimeSelector aTxTo = new TimeSelector("a", TX_TO);
    TimeSelector aValFrom = new TimeSelector("a", VAL_FROM);
    TimeSelector aValTo = new TimeSelector("a", VAL_TO);

    TimeSelector bTxFrom = new TimeSelector("b", TX_FROM);
    TimeSelector bTxTo = new TimeSelector("b", TX_TO);
    TimeSelector bValFrom = new TimeSelector("b", VAL_FROM);
    TimeSelector bValTo = new TimeSelector("b", VAL_TO);

    TimeSelector cTxFrom = new TimeSelector("c", TX_FROM);
    TimeSelector cTxTo = new TimeSelector("c", TX_TO);
    TimeSelector cValFrom = new TimeSelector("c", VAL_FROM);
    TimeSelector cValTo = new TimeSelector("c", VAL_TO);

    Duration eValDuration = new Duration(new TimeSelector("e", VAL_FROM),
            new TimeSelector("e", VAL_TO));
    Duration eTxDuration = new Duration(new TimeSelector("e", TX_FROM),
            new TimeSelector("e", TX_TO));

    Duration fValDuration = new Duration(new TimeSelector("f", VAL_FROM),
            new TimeSelector("f", VAL_TO));

    Duration cTxDuration = new Duration(new TimeSelector("c", TX_FROM),
            new TimeSelector("c", TX_TO));

    @Test
    public void timeSelectorComparisonTest(){
        CNFEstimation estimator = getEstimator();

        //tx_from of v1 equally distributed from 100L to 200L
        // => 175L is part of the 76th bin
        // => 24 bins are greater => should yield 0.24
        Comparison comp1 = new Comparison(aTxFrom, GT, new TimeLiteral(175L));
        CNFElementTPGM e1 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp1)));
        TemporalCNF cnf1 = new TemporalCNF(Arrays.asList(e1));
        double estimation1 = estimator.estimateCNF(cnf1);
        assertEquals(estimation1, 0.24, 0.01);
        // does the cache work?
        double estimation1Cached = estimator.estimateCNF(cnf1);
        assertEquals(estimation1Cached, estimation1, .0);

        // switch sides
        Comparison comp2 = new Comparison(new TimeLiteral(175L), LT, aTxFrom);
        CNFElementTPGM e2 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp2)));
        TemporalCNF cnf2 = new TemporalCNF(Arrays.asList(e2));
        double estimation2 = estimator.estimateCNF(cnf2);
        assertEquals(estimation2, estimation1, 0.);

        // comparisons where lhs and rhs are both selectors are estimated to 1.
        // (no matter how much sense they make)
        Comparison comp3 = new Comparison(aTxTo, EQ, aTxFrom);
        CNFElementTPGM e3 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp3)));
        TemporalCNF cnf3 = new TemporalCNF(Arrays.asList(e3));
        double estimation3 = estimator.estimateCNF(cnf3);
        assertEquals(estimation3, 1., 0.00001);
    }

    @Test
    public void durationComparisonTest(){
        CNFEstimation estimator = getEstimator();

        // durations are equally distributed from 0 to 100
        Comparison comp1 = new Comparison(eTxDuration, GT, new TimeConstant(10));
        CNFElementTPGM e1 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp1)));
        TemporalCNF cnf1 = new TemporalCNF(Arrays.asList(e1));
        double estimation1 = estimator.estimateCNF(cnf1);
        assertEquals(estimation1, 0.9, 0.03);

        // switch sides
        Comparison comp2 = new Comparison(new TimeConstant(10), LT, eValDuration);
        CNFElementTPGM e2 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp2)));
        TemporalCNF cnf2 = new TemporalCNF(Arrays.asList(e2));
        double estimation2 = estimator.estimateCNF(cnf2);
        assertEquals(estimation2, estimation1, 0.);

        // durations that do not compare tx_from / val_duration with a constant
        // are evaluated to, i.e. neglected in the estimation
        Comparison comp3 = new Comparison(eTxDuration, LT, eValDuration);
        CNFElementTPGM e3 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp3)));
        TemporalCNF cnf3 = new TemporalCNF(Arrays.asList(e3));
        double estimation3 = estimator.estimateCNF(cnf3);
        assertEquals(estimation3, 1., 0.0001);

        Comparison comp4 = new Comparison(new Duration(
                new TimeSelector("e", TX_FROM),
                new TimeLiteral("2020-05-01")),
                LT, new TimeConstant(20L));
        CNFElementTPGM e4 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp4)));
        TemporalCNF cnf4 = new TemporalCNF(Arrays.asList(e4));
        double estimation4 = estimator.estimateCNF(cnf4);
        assertEquals(estimation4, 1., 0.0001);
    }

    @Test
    public void complexDurationComparisonTest(){
        CNFEstimation estimator = getEstimator();

        Comparison comp1 = new Comparison(eTxDuration, LTE, fValDuration);
        CNFElementTPGM e1 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp1)));
        TemporalCNF cnf1 = new TemporalCNF(Arrays.asList(e1));
        double estimation1 = estimator.estimateCNF(cnf1);
        assertEquals(estimation1, 0.5, 0.02);

        Comparison comp2 = new Comparison(eTxDuration, EQ, fValDuration);
        CNFElementTPGM e2 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp2)));
        TemporalCNF cnf2 = new TemporalCNF(Arrays.asList(e2));
        double estimation2 = estimator.estimateCNF(cnf2);
        assertEquals(estimation2, 0., 0.01);

        Comparison comp3 = new Comparison(eTxDuration, LT, fValDuration);
        CNFElementTPGM e3 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp3)));
        TemporalCNF cnf3 = new TemporalCNF(Arrays.asList(e3));
        double estimation3 = estimator.estimateCNF(cnf3);
        assertEquals(estimation3, 0.5, 0.02);
        assertTrue(estimation3 < estimation1);
    }

    @Test
    public void propertyComparisonTest(){
        CNFEstimation estimator = getEstimator();

        // categorical
        Comparison comp1 = new Comparison(
                new PropertySelector("a", "catProp1"),
                EQ,
                new Literal("x")
        );
        CNFElementTPGM e1 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp1)));
        TemporalCNF cnf1 = new TemporalCNF(Arrays.asList(e1));
        double estimation1 = estimator.estimateCNF(cnf1);
        assertEquals(estimation1, 0.06, 0.001);
        // sides switched
        Comparison comp2 = new Comparison(
                new Literal("x"),
                EQ,
                new PropertySelector("a", "catProp1")
        );
        CNFElementTPGM e2 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp2)));
        TemporalCNF cnf2 = new TemporalCNF(Arrays.asList(e2));
        double estimation2 = estimator.estimateCNF(cnf2);
        assertEquals(estimation2, 0.06, 0.001);

        // numerical
        Comparison comp3 = new Comparison(
                new PropertySelector("c", "numProp"),
                LTE,
                new Literal(50)
        );
        CNFElementTPGM e3 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp3)));
        TemporalCNF cnf3 = new TemporalCNF(Arrays.asList(e3));
        double estimation3 = estimator.estimateCNF(cnf3);
        assertEquals(estimation3, 0.15, 0.02);
        // switched sides
        Comparison comp4 = new Comparison(
                new Literal(50),
                GTE,
                new PropertySelector("c", "numProp")
        );
        CNFElementTPGM e4 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp4)));
        TemporalCNF cnf4 = new TemporalCNF(Arrays.asList(e4));
        double estimation4 = estimator.estimateCNF(cnf4);
        assertEquals(estimation4, estimation3, 0.);
    }

    @Test
    public void complexPropertyComparisonTest(){
        // property selector vs property selector
        CNFEstimation estimator = getEstimator();

        // categorical
        Comparison comp1 = new Comparison(
                new PropertySelector("b", "gender"),
                EQ,
                new PropertySelector("b", "gender")
        );
        CNFElementTPGM e1 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp1)));
        TemporalCNF cnf1 = new TemporalCNF(Arrays.asList(e1));
        double estimation1 = estimator.estimateCNF(cnf1);
        // (2/3)² * (1/3)²
        assertEquals(estimation1, 0.55, 0.01);


        // numerical
        Comparison comp2 = new Comparison(
                new PropertySelector("a", "numProp"),
                GTE,
                new PropertySelector("b", "numProp")
        );
        CNFElementTPGM e2 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp2)));
        TemporalCNF cnf2 = new TemporalCNF(Arrays.asList(e2));
        double estimation2 = estimator.estimateCNF(cnf2);
        // occurrences of numProp * prob(a.numProb >= b.numProb)
        // = (1/2)*(1/10) * (1/2) = 0.025
        assertEquals(estimation2, 0.025, 0.01);
        // switch sides
        Comparison comp3 = new Comparison(
                new PropertySelector("b", "numProp"),
                LT,
                new PropertySelector("a", "numProp")
        );
        CNFElementTPGM e3 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp3)));
        TemporalCNF cnf3 = new TemporalCNF(Arrays.asList(e3));
        double estimation3 = estimator.estimateCNF(cnf3);
        assertEquals(estimation3, estimation2, 0.01);
    }

    @Test
    public void complexLiteralComparisonTest(){
        CNFEstimation estimator = getEstimator();

        // cf. BinningTemporalGraphStatisticsTest

        Comparison comp1 = new Comparison(aValFrom, GTE, bTxTo);
        CNFElementTPGM e1 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp1)));
        double estimation1 = estimator.estimateCNF(new TemporalCNF(Collections.singletonList(e1)));
        assertEquals(estimation1, 0.05, 0.025);

        Comparison comp2 = new Comparison(aValTo, LT, cTxTo);
        CNFElementTPGM e2 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp2)));
        double estimation2 = estimator.estimateCNF(new TemporalCNF(Collections.singletonList(e2)));
        assertEquals(estimation2, 0.5, 0.1);

        Comparison comp3 = new Comparison(cTxTo, LTE, bTxFrom);
        CNFElementTPGM e3 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp3)));
        double estimation3 = estimator.estimateCNF(new TemporalCNF(Collections.singletonList(e3)));
        assertEquals(estimation3, 0.25, 0.05);
    }

    @Test
    public void conjunctionTest(){
        CNFEstimation estimator = getEstimator();
        // ~0.9
        Comparison comp1 = new Comparison(eTxDuration, GT, new TimeConstant(10));
        CNFElementTPGM e1 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp1)));
        // ~0.125
        Comparison comp2 = new Comparison(
                new PropertySelector("a", "numProp"),
                GTE,
                new PropertySelector("a", "numProp")
        );
        CNFElementTPGM e2 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp2)));
        // 1.
        Comparison comp3 = new Comparison(eTxDuration, LT, eValDuration);
        CNFElementTPGM e3 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp3)));
        // ~0.15
        Comparison comp4 = new Comparison(
                new Literal(50),
                GTE,
                new PropertySelector("c", "numProp")
        );
        CNFElementTPGM e4 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp4)));

        //0.9*0.125*0.15
        TemporalCNF cnf1 = new TemporalCNF(Arrays.asList(e1, e2, e4));
        double estimation1 = estimator.estimateCNF(cnf1);
        assertEquals(estimation1, 0.017, 0.002);

        TemporalCNF cnf2 = new TemporalCNF(Arrays.asList(e1, e2, e3, e4));
        double estimation2 = estimator.estimateCNF(cnf2);
        assertEquals(estimation1, 0.017, 0.002);

        //0.9 * 0.125
        TemporalCNF cnf3 = new TemporalCNF(Arrays.asList(e1, e2));
        double estimation3 = estimator.estimateCNF(cnf3);
        assertEquals(estimation3, 0.1125, 0.01);
    }

    @Test
    public void disjunctionTest(){
        CNFEstimation estimator = getEstimator();
        // ~0.9     (a)
        Comparison comp1 = new Comparison(eTxDuration, GT, new TimeConstant(10));
        ComparisonExpressionTPGM c1 = new ComparisonExpressionTPGM(comp1);
        // ~0.125      (b)
        Comparison comp2 = new Comparison(
                new PropertySelector("a", "numProp"),
                GTE,
                new PropertySelector("a", "numProp")
        );
        ComparisonExpressionTPGM c2 = new ComparisonExpressionTPGM(comp2);
        // 1.       (c)
        Comparison comp3 = new Comparison(eTxDuration, LT, eValDuration);
        ComparisonExpressionTPGM c3 = new ComparisonExpressionTPGM(comp3);
        // ~0.15    (d)
        Comparison comp4 = new Comparison(
                new Literal(50),
                GTE,
                new PropertySelector("c", "numProp")
        );
        ComparisonExpressionTPGM c4 = new ComparisonExpressionTPGM(comp4);

        // (a) OR (b) = P(a)+P(b)-P(a)*P(b) = 0.9+0.125 - 0.9*0.125 = 0.9125
        CNFElementTPGM e1 = new CNFElementTPGM(Arrays.asList(c1, c2));
        TemporalCNF cnf1 = new TemporalCNF(Arrays.asList(e1));
        double estimation1 = estimator.estimateCNF(cnf1);
        assertEquals(estimation1, 0.9125, 0.02);

        // (a) OR (b) OR (d)
        // = P(a) + P(b) + P(d) - P(a)*P(b) - P(a)*P(d) - P(b)*P(d) + P(a)*P(b)*P(d)
        // = 0.9+0.125+0.15 - 0.9*0.125 - 0.9*0.15 - 0.125*0.15 + 0.9*0.125*0.15
        // = 0.925625
        CNFElementTPGM e2 = new CNFElementTPGM(Arrays.asList(c1, c2, c4));
        TemporalCNF cnf2 = new TemporalCNF(Arrays.asList(e2));
        double estimation2 = estimator.estimateCNF(cnf2);
        assertEquals(estimation2, 0.925, 0.02);

        // (a) OR (b) OR (c) = 1
        CNFElementTPGM e3 = new CNFElementTPGM(Arrays.asList(c1, c2, c3));
        TemporalCNF cnf3 = new TemporalCNF(Arrays.asList(e3));
        double estimation3 = estimator.estimateCNF(cnf3);
        assertEquals(estimation3, 1., 0.01);

        // conjunction of disjunctions
        // independence assumption, should thus be ~ 0.9125*0.925*1. = 0.844
        TemporalCNF cnf4 = new TemporalCNF(Arrays.asList(e2, e1, e3));
        double estimation4 = estimator.estimateCNF(cnf4);
        assertEquals(estimation4, 0.84, 0.03);

        // check reordering of cnf4: sequence should be e1, e2, e3
        CNFElementTPGM e1Reordered = e1; // nothing to change here
        CNFElementTPGM e2Reordered = new CNFElementTPGM(Arrays.asList(c1, c4, c2));
        CNFElementTPGM e3Reordered = new CNFElementTPGM(Arrays.asList(c3, c1, c2));
        TemporalCNF cnf4Reordered = new TemporalCNF(Arrays.asList(
                e1Reordered, e2Reordered, e3Reordered));
        assertEquals(estimator.reorderCNF(cnf4), cnf4Reordered);

    }

    /**
     * Creates a CNFEstimation over a test graph (see {@link this::getDummyStats}.
     * Variable to label mapping: "a"->"v1", "b"->"v2", e->"edge"
     * Variable to type mapping "a"->VERTEX, "b"->VERTEX, "c"->VERTEX, "e"->EDGE
     * ("c" has no label specified)
     * @return CNFEstimation object
     */
    private CNFEstimation getEstimator(){
        BinningTemporalGraphStatistics stats = getDummyStats();

        HashMap<String, String> labelMap = new HashMap<>();
        labelMap.put("a", "v1");
        labelMap.put("b", "v2");
        labelMap.put("e", "edge");

        HashMap<String, TemporalGraphStatistics.ElementType> typeMap = new HashMap<>();
        typeMap.put("a", TemporalGraphStatistics.ElementType.VERTEX);
        typeMap.put("b", TemporalGraphStatistics.ElementType.VERTEX);
        typeMap.put("c", TemporalGraphStatistics.ElementType.VERTEX);
        typeMap.put("e", TemporalGraphStatistics.ElementType.EDGE);

        return new CNFEstimation(stats, typeMap, labelMap);
    }

    /**
     * Creates a CNF from a list of comparisons (conjunction of the comparisons)
     * @param comparisons list of comparisons
     * @return CNF (conjunction) from a list of comparisons
     */
    private CNF cnfFrom(List<ComparisonExpression> comparisons) {
        ArrayList<CNFElement> elements = new ArrayList<>();
        for(ComparisonExpression comparison: comparisons){
            elements.add(new CNFElement(
                    new ArrayList<>(Arrays.asList(comparison))));
        }
        return new CNF(elements);
    }

    /**
     * Creates a BinningTemporalGraphStatistics object
     * Vertices:
     *      100 "v1" vertices:
     *          "catProp"   = "x" (6 vertices)
     *                      = "y" (34 vertices)
     *          "numProp"   = 0,2,...,100 (50 vertices)
     *          "numProp2"  = 0,3,6,...,297 (100 vertices)
     *          tx_from goes from 100L to 200L, val_from from 150L to 250L (100 vertices)
     *          tx_to goes from 300L to 350L for half of the vertices, other half is unbounded
     *          val_to goes from 350L to 450L for half of the vertices, other half is unbounded
     *
     *      100 "v2" vertices:
     *          "catProp"   = "y" (20 vertices)
     *          "numProp"   = 0,10,...,90 (10 vertices)
     *          "gender"    = "m" (34 vertices)
     *                      = "f" (66 vertices)
     *          tx_from goes from 1000L to 2000L, val_from from 3000L to 4000L (100 vertices)
     *          tx_to goes from 1500L to 2500L (step 20) for half of the vertices, other half is unbounded
     *          val_to goes from 3500L to 4500L for half of the vertices (step 20), other half is unbounded
     *
     * Edges: identical tx and val times, their length equally distributed
     *          from 0 to 100L
     *
     * @return dummy statistics
     */
    private BinningTemporalGraphStatistics getDummyStats(){
        ArrayList<TemporalVertex> vertexList = new ArrayList<>();

        // first type of vertex has label1
        String vLabel1 = "v1";
        // tx_from goes from 100L to 200L, val_from from 150L to 250L (100 vertices)
        // tx_to goes from 300L to 350L for half of the vertices, other half is unbounded
        // val_to goes from 350L to 450L for half of the vertices, other half is unbounded
        int numL1Vertices = 100;
        for(int i=0; i<numL1Vertices; i++){
            TemporalVertex vertex = new TemporalVertex();
            vertex.setId(GradoopId.get());
            vertex.setLabel(vLabel1);

            vertex.setTxFrom(100L+i);
            Long txTo = i%2==0 ? 300L+i : Long.MAX_VALUE;
            vertex.setTxTo(txTo);

            vertex.setValidFrom(150L + i);
            Long valTo = i%2 == 0 ? 350L+i : Long.MAX_VALUE;
            vertex.setValidTo(valTo);

            // 6 nodes with property value x
            if(i%10==0){
                vertex.setProperty("catProp1", PropertyValue.create("x"));
            }
            // 34 nodes with property value y
            if(i%3==0){
                vertex.setProperty("catProp1", PropertyValue.create("y"));
            }

            // every second node has i as a property
            if(i%2==0){
                vertex.setProperty("numProp", PropertyValue.create(i));
            }

            vertex.setProperty("numProp2", PropertyValue.create(3*i));

            vertexList.add(vertex);
        }

        // first type of vertex has label1
        String vLabel2 = "v2";
        // tx_from goes from 1000L to 2000L, val_from from 3000L to 4000L (100 vertices)
        // tx_to goes from 1500L to 2500L (step 20) for half of the vertices, other half is unbounded
        // val_to goes from 3500L to 4500L for half of the vertices (step 20), other half is unbounded
        int numL2Vertices = 100;
        for(int i=0; i<numL2Vertices; i++){
            TemporalVertex vertex = new TemporalVertex();
            vertex.setId(GradoopId.get());
            vertex.setLabel(vLabel2);

            vertex.setTxFrom(1000L+i*10);
            Long txTo = i%2==0 ? 1500L+i*20 : Long.MAX_VALUE;
            vertex.setTxTo(txTo);

            vertex.setValidFrom(3000L + i*10);
            Long valTo = i%2 == 0 ? 3500L+i*20 : Long.MAX_VALUE;
            vertex.setValidTo(valTo);

            if(i%5 == 0){
                vertex.setProperty("catProp1", "y");
            }

            // every 10th node has i as property
            if(i%10==0){
                vertex.setProperty("numProp", PropertyValue.create(i));
            }
            vertexList.add(vertex);

            if(i%3==0){
                vertex.setProperty("gender", PropertyValue.create("m"));
            }
            else{
                vertex.setProperty("gender", PropertyValue.create("f"));
            }
        }

        //edges (only one type)
        ArrayList<TemporalEdge> edgeList = new ArrayList<>();
        String edgeLabel = "edge";
        int numEdges = 100;
        // identical tx and val times.
        // lengths are equally distributed from 0 to 100
        for(int i=0; i<numEdges; i++){
            TemporalEdge edge = new TemporalEdge();
            edge.setId(GradoopId.get());
            edge.setLabel(edgeLabel);
            edge.setTransactionTime(new Tuple2<>((long) i, (long)i+i));
            edge.setValidTime(edge.getTransactionTime());
            edgeList.add(edge);
        }

        TemporalGraph graph = new TemporalGraphFactory(getConfig()).fromCollections(
                vertexList, edgeList
        );

        return new BinningTemporalGraphStatisticsFactory()
                .fromGraphWithSampling(graph, 100);

    }

}
