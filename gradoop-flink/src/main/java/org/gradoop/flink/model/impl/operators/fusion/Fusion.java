package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
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

    @Override
    public LogicalGraph execute(final LogicalGraph leftGraph, final LogicalGraph rightGraph) {
        //Catching possible errors (null) and handling then as they were empty objects
        if (leftGraph==null) {
            return LogicalGraph.createEmptyGraph(rightGraph==null ? defaultConf : rightGraph.getConfig());
        } else if (rightGraph==null) {
            return FusionUtils.recreateGraph(leftGraph);
        } else {
            rightGraph.isEmpty().first(1).map(isRightGraphEmpty -> {


                if (isRightGraphEmpty) {
                    return leftGraph;
                } else {
                    return leftGraph.isEmpty().first(1).map(isLeftGraphEmpty -> {


                        if (isLeftGraphEmpty)
                            return leftGraph;
                        else {
                            // Do stuff here, they are both non empty.

                            {
                                //Return the vertices that appear both in the left and the right graph
                                DataSet<Vertex> leftVertices = leftGraph.getVertices();
                                leftVertices = FusionUtils.areElementsInGraph(leftVertices, rightGraph, true);

                                //If the number of vertices returned is the same as the one of the right graph
                                if (leftVertices.count() != rightGraph.getVertices().count()) { // the two graphs are not the same
                                    return FusionUtils.recreateGraph(leftGraph);
                                }

                                // Same thing I did for the edges I must do for the edges
                                DataSet<Edge> leftEdges = leftGraph.getEdges();
                                leftEdges = FusionUtils.areElementsInGraph(leftEdges, rightGraph, true);
                                if (leftEdges.count() != rightGraph.getEdges().count()) {
                                    return FusionUtils.recreateGraph(leftGraph);
                                }
                            }

                            //At this point, I shall create the graph that has to be returned
                            LogicalGraph toret = null;
                            {
                                DataSet<Vertex> leftVertices = leftGraph.getVertices();
                                leftVertices = FusionUtils.areElementsInGraph(leftVertices,rightGraph,false);
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
