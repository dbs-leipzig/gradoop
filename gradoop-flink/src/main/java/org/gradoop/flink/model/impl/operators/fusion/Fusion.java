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
            //????
            DataSet<LogicalGraph> l = rightGraph.isEmpty().first(1).map(isRightGraphEmpty -> {
                if (isRightGraphEmpty) {
                    return FusionUtils.recreateGraph(leftGraph);
                } else {
                    //????
                    DataSet<LogicalGraph> lk = leftGraph.isEmpty().first(1).map(isLeftGraphEmpty -> {
                        if (isLeftGraphEmpty)
                            return FusionUtils.recreateGraph(leftGraph);
                        else {
                            DataSet<Vertex> leftVertices = leftGraph.getVertices();

                            //TbmV
                            DataSet<Vertex> toBeReplaced = FusionUtils.areElementsInGraph(leftVertices, rightGraph, true);
                            if (toBeReplaced.count()==0) {
                                return FusionUtils.recreateGraph(leftGraph);
                            } else {

                                DataSet<Vertex> toBeAdded;
                                GradoopId vId = GradoopId.get();
                                {
                                    Vertex v = new Vertex();
                                    v.setLabel(FusionUtils.getGraphLabel(rightGraph)); // ?????????
                                    v.setProperties(FusionUtils.getGraphProperties(rightGraph));  // ?????????
                                    v.setId(vId);
                                    toBeAdded = env.fromElements(v);
                                }

                                DataSet<Vertex> finalVertices =
                                        FusionUtils
                                                .areElementsInGraph(leftVertices, rightGraph, false)
                                                .union(toBeAdded);


                                DataSet<Edge> leftEdges = leftGraph.getEdges();
                                leftEdges = FusionUtils.areElementsInGraph(leftEdges, rightGraph, false);

                                // Everything in once, maybe more efficient
                                DataSet<Edge> updatedEdges = leftEdges
                                        .fullOuterJoin(leftVertices)
                                        .where(Edge::getSourceId)
                                        .equalTo(Element::getId)
                                        .with((FlatJoinFunction<Edge, Vertex, Edge>) (edge, vertex, collector) -> {
                                            if (vertex == null) collector.collect(edge);
                                            else {
                                                Edge e = new Edge();
                                                e.setId(GradoopId.get());
                                                e.setSourceId(vId);
                                                e.setTargetId(edge.getTargetId());
                                                e.setProperties(edge.getProperties());
                                                collector.collect(e);
                                            }
                                        })
                                        .fullOuterJoin(leftVertices)
                                        .where(Edge::getTargetId)
                                        .equalTo(Element::getId)
                                        .with((FlatJoinFunction<Edge, Vertex, Edge>) (edge, vertex, collector) -> {
                                            if (vertex == null) collector.collect(edge);
                                            else {
                                                Edge e = new Edge();
                                                e.setId(GradoopId.get());
                                                e.setTargetId(vId);
                                                e.setSourceId(edge.getSourceId());
                                                e.setProperties(edge.getProperties());
                                                collector.collect(e);
                                            }
                                        });

                                return LogicalGraph.fromDataSets(finalVertices, updatedEdges, leftGraph.getConfig());

                            }
                        }
                    });
                    return lk.collect().get(0);
                }
            });
            try {
                return l.collect().get(0);
            } catch (Exception e) {
                return FusionUtils.recreateGraph(leftGraph);
            }
        }
    }

}
