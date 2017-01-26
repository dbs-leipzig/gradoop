/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
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

    private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    @Override
    public LogicalGraph execute(final LogicalGraph searchGraph, final LogicalGraph patternGraph) {
        //Catching possible errors (null) and handling then as they were empty objects
        if (searchGraph == null) {
            return LogicalGraph.createEmptyGraph(patternGraph == null ? defaultConf : patternGraph.getConfig());
        } else if (patternGraph == null) {
            return FusionUtils.recreateGraph(searchGraph);
        } else {
            /*
                 If either the search graph or the pattern graphs are empty, then I have to return the
                 search graph
             */
            boolean exitCond = true;
            try {
                exitCond = patternGraph.isEmpty().first(1).collect().get(0);
            } catch (Exception ignored) {
            }
            if (exitCond) return FusionUtils.recreateGraph(searchGraph);
            exitCond = true;
            try {
                exitCond = searchGraph.isEmpty().first(1).collect().get(0);
            } catch (Exception ignore) {
            }
            if (exitCond) return FusionUtils.recreateGraph(searchGraph);


            DataSet<Vertex> leftVertices = searchGraph.getVertices();

            //TbmV
            DataSet<Vertex> toBeReplaced = FusionUtils.areElementsInGraph(leftVertices, patternGraph, true);
            exitCond = true;
            try {
                exitCond = toBeReplaced.count() == 0;
            } catch (Exception ignored) {
            }
            if (exitCond) return FusionUtils.recreateGraph(searchGraph);

            /// ERROR: Cannot perform a union between two DataSet belonging to different execution contexts
            DataSet<Vertex> finalVertices =
                    FusionUtils
                            .areElementsInGraph(leftVertices, patternGraph, false);
            //

            DataSet<Vertex> toBeAdded;
            GradoopId vId = GradoopId.get();
            GradoopId searchGraphId = FusionUtils.getGraphId2(searchGraph);
            {
                Vertex v = new Vertex();
                v.setLabel(FusionUtils.getGraphLabel(patternGraph));
                v.setProperties(FusionUtils.getGraphProperties(patternGraph));
                v.setId(vId);
                v.addGraphId(searchGraphId);
                toBeAdded = finalVertices.getExecutionEnvironment().fromElements(v);
            }
            DataSet<Vertex> toBeReturned = finalVertices.union(toBeAdded);
            //System.err.println(FusionUtils.stringifyVerticesWithId(toBeReturned));

            DataSet<Edge> leftEdges = searchGraph.getEdges();
            leftEdges = FusionUtils.areElementsInGraph(leftEdges, patternGraph, false);

            // Everything in once, maybe more efficient
            DataSet<Edge> updatedEdges = leftEdges
                    .fullOuterJoin(toBeReplaced)
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
                            e.addGraphId(searchGraphId);
                            collector.collect(e);
                        }
                    })
                    .returns(Edge.class)
                    .fullOuterJoin(toBeReplaced)
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
                            e.addGraphId(searchGraphId);
                            collector.collect(e);
                        }
                    })
                    .returns(Edge.class);

            System.err.println(FusionUtils.stringifyEdgesFromGraph(updatedEdges,toBeReturned));

            return LogicalGraph.fromDataSets(searchGraph.getGraphHead(),toBeReturned, updatedEdges, searchGraph.getConfig());


        }
    }

}
