package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Objects;

/**
 * Created by Giacomo Bergami on 19/01/17.
 */
public class Fusion implements BinaryGraphToGraphOperator {
    @Override
    public String getName() {
        return Fusion.class.getName();
    }

    public static boolean belongsTo(Vertex v, LogicalGraph secondGraph) {
        return false;
    }

    private static final GradoopFlinkConfig defaultConf = null;

    ///??????
    public static GradoopId graphId(LogicalGraph g) throws Exception {
        return g.getGraphHead().first(1).map(x -> x.getId()).collect().get(0);
    }

    public static LogicalGraph recreateGraph(LogicalGraph x) {
        return LogicalGraph.fromDataSets(x.getVertices(),x.getEdges(),x.getConfig());
    }

    @Override
    public LogicalGraph execute(final LogicalGraph firstGraph, final LogicalGraph secondGraph) {
        //Catching possible errors (null) and handling then as they were empty objects
        if (firstGraph==null) {
            return LogicalGraph.createEmptyGraph(secondGraph==null ? defaultConf : secondGraph.getConfig());
        } else if (secondGraph==null) {
            return recreateGraph(firstGraph);
        } else {
            secondGraph.isEmpty().first(1).map(isRightGraphEmpty -> {
                if (isRightGraphEmpty) {
                    return firstGraph;
                } else {
                    return firstGraph.isEmpty().first(1).map(isLeftGraphEmpty -> {
                        if (isLeftGraphEmpty)
                            return firstGraph;
                        else {
                            LogicalGraph toret = null;
                            // Do stuff here, they are both non empty.

                            //Check if the secondGraph is a subgraph of the firstGraph
                            FilterFunction<Vertex> xxx = null;
                            DataSet<Vertex> vertices = firstGraph.getVertices().filter(xxx);

                            //????
                            GradoopId sid = graphId(secondGraph);
                            DataSet<Vertex> toMergeVertices;

                            toMergeVertices = firstGraph.getVertices().filter(new NotInGraphBroadcast<>()).withBroadcastSet(secondGraph.getGraphHead().map(new Id<>()), GraphContainmentFilterBroadcast.GRAPH_ID);
                            if (toMergeVertices.count()!=secondGraph.getVertices().count()) { // the two graphs are not the same
                                return recreateGraph(firstGraph);
                            }

                            return toret;
                        }
                    });
                }
            });
            return null;
        }
    }
}
