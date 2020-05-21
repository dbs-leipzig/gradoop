package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeLiteralComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.TemporalElementStats;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;

import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BinningTemporalGraphStatisticsFactoryTest extends TemporalGradoopTestBase {

    @Test
    public void factoryTest() throws Exception {
        Tuple2<Long, Long> defaultTime = new Tuple2<>(Long.MIN_VALUE, Long.MAX_VALUE);

        ArrayList<TemporalVertex> vertexList = new ArrayList<>();
        String[] vertexLabels = new String[]{"v1", "v2"};
        for(int i = 0; i<10; i++){
            TemporalVertex vertex = new TemporalVertex();
            vertex.setId(GradoopId.get());
            String label = i%2 == 0 ? vertexLabels[0] : vertexLabels[1];
            vertex.setLabel(label);
            vertexList.add(vertex);
            vertex.setTransactionTime(defaultTime);
            vertex.setValidTime(defaultTime);
        }

        ArrayList<TemporalEdge> edgeList = new ArrayList<>();
        String[] edgeLabels = new String[]{"e1", "e2"};
        for(int i=0; i<5; i++){
            TemporalEdge edge = new TemporalEdge();
            edge.setId(GradoopId.get());
            edge.setSourceId(vertexList.get(2*i).getId());
            edge.setTargetId(vertexList.get(2*i+1).getId());
            String label = i%2==0 ? edgeLabels[0] : edgeLabels[1];
            edge.setLabel(label);
            edge.setTransactionTime(defaultTime);
            edge.setValidTime(defaultTime);
            edgeList.add(edge);
        }

        TemporalGraph graph = new TemporalGraphFactory(getConfig()).fromCollections(
                vertexList, edgeList
        );
        BinningTemporalGraphStatistics stat = new BinningTemporalGraphStatisticsFactory()
                .fromGraphWithSampling(graph, 10);

        Map<String, TemporalElementStats> edgeStats = stat.getEdgeStats();
        Map<String, TemporalElementStats> vertexStats = stat.getVertexStats();
        assertEquals(edgeStats.keySet().size(), 2);
        assertEquals(vertexStats.keySet().size(), 2);
        assertTrue(edgeStats.keySet().contains(edgeLabels[0]));
        assertTrue(edgeStats.keySet().contains(edgeLabels[1]));
        assertTrue(vertexStats.keySet().contains(vertexLabels[0]));
        assertTrue(vertexStats.keySet().contains(vertexLabels[1]));

    }

}
