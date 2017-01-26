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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by vasistas on 25/01/17.
 */
public class FusionTestUtils {

    /**
     * Equality test that is not passed:
     *

     ab_edgeWithAlpha_ab_edgeWithBeta_to_ab_edgeWithBeta_loop()
     =====

     (G{ab_edgeWithBeta=graph:String})
     -[AlphaEdge{alphatype=alphavalue:String}]->(G{ab_edgeWithBeta=graph:String})
     <-[AlphaEdge{alphatype=alphavalue:String}]-(G{ab_edgeWithBeta=graph:String})

     (G{ab_edgeWithBeta=graph:String})
     -[AlphaEdge{alphatype=alphavalue:String}]->(G{ab_edgeWithBeta=graph:String})
     (G{ab_edgeWithBeta=graph:String})
     <-[AlphaEdge{alphatype=alphavalue:String}]-(G{ab_edgeWithBeta=graph:String})



     *
     *
     */

    public static final CanonicalAdjacencyMatrixBuilder can = new CanonicalAdjacencyMatrixBuilder(
            new GraphHeadToEmptyString(), new VertexToDataString(), new EdgeToDataString(), true);


    public static String stringifyGraph(LogicalGraph g) {
        try {
            return can.execute(GraphCollection.fromGraph(g)).collect().get(0);
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public static boolean epgmEquals(EPGMElement first, EPGMElement second) {
        boolean result = Objects.equals(first.getLabel(),second.getLabel());
        try {
            for (String k : first.getProperties().getKeys()) {
                result &= Objects.equals(first.getProperties().get(k), second.getProperties().get(k));
            }
        } catch (Exception e) {
            return (first.getProperties()==null||first.getProperties().isEmpty()) &&
                    (second.getProperties()==null|| second.getProperties().isEmpty());
        }
        return result;
    }

    public static String stringifyGraph2(LogicalGraph g) {
        StringBuilder sb = new StringBuilder();
        try {
            try {
                sb.append(FusionUtils.epgmString(g.getGraphHead().first(1).collect().get(0)));
            } catch (Exception ignore) {}
            sb.append("\nV=[\n");
            sb.append(FusionUtils.stringifyVertices(g.getVertices()));
            sb.append("\n]\nE=[\n");
            sb.append(FusionUtils.transformEdges(g.getEdges(),g.getVertices()).map((Tuple3<Vertex,Edge,Vertex> x)-> "("+ FusionUtils.epgmString(x.f0)+")-["+ FusionUtils.epgmString(x.f1)+"]->("+ FusionUtils.epgmString(x.f2)+")").returns(String.class).collect().stream().collect(Collectors.joining("\n")));
            sb.append("\n]");
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
        return sb.toString();
    }

    public static boolean graphEquals(LogicalGraph left, LogicalGraph right) {
        try {
            long leftC = left.getGraphHead().count();
            long rightC = right.getGraphHead().count();
            long test = 0;

            if (left.getGraphHead().count() != 0 && right.getGraphHead().count() != 0) {
                List<Boolean> gh = (left.getGraphHead()
                        .join(right.getGraphHead())
                        .where((GraphHead l) -> 0)
                        .equalTo((GraphHead r) -> 0)
                        .with(new FlatJoinFunction<GraphHead, GraphHead, Boolean>() {
                            @Override
                            public void join(GraphHead first, GraphHead second, Collector<Boolean> out) throws Exception {
                                if (epgmEquals(first,second)) out.collect(true);
                            }
                        }).returns(Boolean.class).collect());
                test = gh.size();
                if (test!=leftC || test!=rightC) return false;
            } else if (left.getVertices().count() == 0 && right.getVertices().count() == 0) {
            } else return false;

            leftC = left.getVertices().count();
            rightC = right.getVertices().count();
            test = 0;
            if (left.getVertices().count() != 0 && right.getVertices().count() != 0) {
                List<Boolean> gh = (left.getVertices()
                        .join(right.getVertices())
                        .where((Vertex l) -> 0)
                        .equalTo((Vertex r) -> 0)
                        .with(new FlatJoinFunction<Vertex, Vertex, Boolean>() {
                            @Override
                            public void join(Vertex first, Vertex second, Collector<Boolean> out) throws Exception {
                                if (epgmEquals(first,second)) out.collect(true);
                            }
                        }).returns(Boolean.class).collect());
                test = gh.size();
                if (test!=leftC || test!=rightC) return false;
            } else if (left.getVertices().count() == 0 && right.getVertices().count() == 0) {
            } else return false;

            leftC = left.getEdges().count();
            rightC = right.getEdges().count();
            if (left.getEdges().count() != 0 && right.getEdges().count() != 0) {
                List<Boolean> gh = (FusionUtils.transformEdges(left.getEdges(),left.getVertices())
                        .join(FusionUtils.transformEdges(right.getEdges(),right.getVertices()))
                        .where(new KeySelector<Tuple3<Vertex, Edge, Vertex>, Integer>() {
                            @Override
                            public Integer getKey(Tuple3<Vertex, Edge, Vertex> l) throws Exception {
                                return 0;
                            }
                        })
                        .equalTo(new KeySelector<Tuple3<Vertex, Edge, Vertex>, Integer>() {
                            @Override
                            public Integer getKey(Tuple3<Vertex, Edge, Vertex> r) throws Exception {
                                return 0;
                            }
                        })
                        .with(new FlatJoinFunction<Tuple3<Vertex,Edge,Vertex>, Tuple3<Vertex,Edge,Vertex>, Boolean>() {
                            @Override
                            public void join(Tuple3<Vertex,Edge,Vertex> first, Tuple3<Vertex,Edge,Vertex> second, Collector<Boolean> out) throws Exception {
                                if (epgmEquals(first.f0,second.f0) && epgmEquals(first.f1,second.f1) && epgmEquals(first.f2,second.f2)) out.collect(true);
                            }
                        }).returns(Boolean.class).collect());
                test = gh.size();
                if (test!=leftC || test!=rightC) return false;
            } else if (left.getEdges().count() == 0 && right.getEdges().count() == 0) {
            } else return false;

            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
