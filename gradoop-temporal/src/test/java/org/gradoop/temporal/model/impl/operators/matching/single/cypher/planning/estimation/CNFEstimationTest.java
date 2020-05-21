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
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.TemporalElementStats;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation.util.CNFEstimation;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.*;
import static org.s1ck.gdl.utils.Comparator.GT;

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

    @Test
    public void timeSelectorComparisonTest(){
        CNFEstimation estimator = getEstimator();

        Comparison comp1 = new Comparison(aTxFrom, GT, new TimeLiteral(175L));
        CNFElementTPGM e1 = new CNFElementTPGM(Arrays.asList(new ComparisonExpressionTPGM(comp1)));
        TemporalCNF cnf1 = new TemporalCNF(Arrays.asList(e1));
        double estimation1 = estimator.estimateCNF(cnf1);
        System.out.println(estimation1);
    }


    /**
     * Creates a CNFEstimation over a test graph (see {@link this::getDummyStats}.
     * Variable to label mapping: "a"->"v1", "b"->"v2"
     * Variable to type mapping "a"->VERTEX, "b"->VERTEX, "c"->VERTEX
     * ("c" has no label specified)
     * @return CNFEstimation object
     */
    private CNFEstimation getEstimator(){
        BinningTemporalGraphStatistics stats = getDummyStats();

        HashMap<String, String> labelMap = new HashMap<>();
        labelMap.put("a", "v1");
        labelMap.put("b", "v2");

        HashMap<String, TemporalGraphStatistics.ElementType> typeMap = new HashMap<>();
        typeMap.put("a", TemporalGraphStatistics.ElementType.VERTEX);
        typeMap.put("b", TemporalGraphStatistics.ElementType.VERTEX);
        typeMap.put("c", TemporalGraphStatistics.ElementType.VERTEX);

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
                    new ArrayList<ComparisonExpression>(Arrays.asList(comparison))));
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
     *      100 "v2" vertices:
     *          "catProp"   = "y" (20 vertices)
     *          "numProp"   = 0,10,...,90 (10 vertices)
     *          "gender"    = "m" (34 vertices)
     *                      = "f" (66 vertices)
     * Edges: (not relevant)
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
        // identical tx and val times. From ranges from 0 to 100, to from 200 to 300 (all bounded)
        for(int i=0; i<numEdges; i++){
            TemporalEdge edge = new TemporalEdge();
            edge.setId(GradoopId.get());
            edge.setLabel(edgeLabel);
            edge.setTransactionTime(new Tuple2<>((long) i, 200L+i));
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
