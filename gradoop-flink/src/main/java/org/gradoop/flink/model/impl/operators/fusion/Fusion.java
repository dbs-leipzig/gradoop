package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Created by Giacomo Bergami on 19/01/17.
 */
public class Fusion implements BinaryGraphToGraphOperator {
    @Override
    public String getName() {
        return Fusion.class.getName();
    }

    private static final GradoopFlinkConfig defaultConf = null;

    private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    @Override
    public LogicalGraph execute(final LogicalGraph leftGraph, final LogicalGraph rightGraph) {
        //Catching possible errors (null) and handling then as they were empty objects
        if (leftGraph == null) {
            return LogicalGraph.createEmptyGraph(rightGraph == null ? defaultConf : rightGraph.getConfig());
        } else if (rightGraph == null) {
            return FusionUtils.recreateGraph(leftGraph);
        } else {
            boolean exitCond = true;
            try {
                exitCond = rightGraph.isEmpty().first(1).collect().get(0);
            } catch (Exception ignored) {
            }
            if (exitCond) return FusionUtils.recreateGraph(leftGraph);

            exitCond = true;
            try {
                exitCond = leftGraph.isEmpty().first(1).collect().get(0);
            } catch (Exception ignore) {
            }
            if (exitCond) return FusionUtils.recreateGraph(leftGraph);


            DataSet<Vertex> leftVertices = leftGraph.getVertices();

            //TbmV
            DataSet<Vertex> toBeReplaced = FusionUtils.areElementsInGraph(leftVertices, rightGraph, true);
            exitCond = true;
            try {
                exitCond = toBeReplaced.count() == 0;
            } catch (Exception ignored) {
            }
            if (exitCond) return FusionUtils.recreateGraph(leftGraph);

            /// ERROR: Cannot perform a union between two DataSet belonging to different execution contexts
            DataSet<Vertex> finalVertices =
                    FusionUtils
                            .areElementsInGraph(leftVertices, rightGraph, false);


            DataSet<Vertex> toBeAdded;
            GradoopId vId = GradoopId.get();
            {
                Vertex v = new Vertex();
                v.setLabel(FusionUtils.getGraphLabel(rightGraph));
                v.setProperties(FusionUtils.getGraphProperties(rightGraph));
                v.setId(vId);
                toBeAdded = finalVertices.getExecutionEnvironment().fromElements(v);
            }
            finalVertices = finalVertices.union(toBeAdded);


            DataSet<Edge> leftEdges = leftGraph.getEdges();
            leftEdges = FusionUtils.areElementsInGraph(leftEdges, rightGraph, false);

            // Everything in once, maybe more efficient
            DataSet<Edge> updatedEdges = leftEdges
                    .fullOuterJoin(leftVertices)
                    .where((Edge x) -> x.getSourceId())
                    .equalTo((Vertex y) -> y.getId())
                    .with((FlatJoinFunction<Edge, Vertex, Edge>) (edge, vertex, collector) -> {
                        if (vertex == null) collector.collect(edge);
                        else if (edge != null) {
                            Edge e = new Edge();
                            e.setId(GradoopId.get());
                            e.setSourceId(vId);
                            e.setTargetId(edge.getTargetId());
                            e.setProperties(edge.getProperties());
                            e.setLabel(edge.getLabel());
                            collector.collect(e);
                        }
                    })
                    .returns(Edge.class)
                    .fullOuterJoin(leftVertices)
                    .where((Edge x) -> x.getTargetId())
                    .equalTo((Vertex y) -> y.getId())
                    .with((FlatJoinFunction<Edge, Vertex, Edge>) (edge, vertex, collector) -> {
                        if (vertex == null) collector.collect(edge);
                        else if (edge != null) {
                            Edge e = new Edge();
                            e.setId(GradoopId.get());
                            e.setTargetId(vId);
                            e.setSourceId(edge.getSourceId());
                            e.setProperties(edge.getProperties());
                            e.setLabel(edge.getLabel());
                            collector.collect(e);
                        }
                    })
                    .returns(Edge.class);


            return LogicalGraph.fromDataSets(leftGraph.getGraphHead(),finalVertices, updatedEdges, leftGraph.getConfig());


        }
    }

}
