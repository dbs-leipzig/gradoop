package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;

/**
 * Created by vasistas on 25/01/17.
 */
public class FusionUtils {

    public static LogicalGraph recreateGraph(LogicalGraph x) {
        return LogicalGraph.fromDataSets(x.getVertices(),x.getEdges(),x.getConfig());
    }

    public static MapOperator<GraphHead, GradoopId> getGraphId(LogicalGraph g) {
        return g.getGraphHead().map(new Id<>());
    }

    public static <P extends GraphElement> DataSet<P> areElementsInGraph(DataSet<P> head, LogicalGraph g, boolean inGraph) {
        return head
                .filter(inGraph ? new InGraphBroadcast<>() : new NotInGraphBroadcast<>())
                .withBroadcastSet(getGraphId(g), GraphContainmentFilterBroadcast.GRAPH_ID);
    }

    public static <P extends GraphElement,K> void mapDifferently(DataSet<P> head, LogicalGraph g, Function<P,K> ifInGraph, Function<P,K> ifNotInGraph) {

    }

    public static String getGraphLabel(LogicalGraph g) {
        throw new RuntimeException("getGraphLabel: the method has not been implemented yet");
    }

    public static Properties getGraphProperties(LogicalGraph g) {
        throw new RuntimeException("rightGraph: the method has not been implemented yet");
    }

    public static boolean isEdgeInGraph(Edge x, LogicalGraph rightGraph) {
        throw new RuntimeException("isEdgeInGraph: the method has not been implemented yet");
    }
}
