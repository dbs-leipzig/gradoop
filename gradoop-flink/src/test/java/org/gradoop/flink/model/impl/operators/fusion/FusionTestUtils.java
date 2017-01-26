package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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


    public static DataSet<Tuple3<Vertex,Edge,Vertex>> transformEdges(LogicalGraph lg) {
        return lg.getEdges()
                .join(lg.getVertices())
                .where((Edge e)->e.getSourceId())
                .equalTo((Vertex v)->v.getId())
                .with(new FlatJoinFunction<Edge, Vertex, Tuple2<Vertex,Edge>>() {
                    @Override
                    public void join(Edge first, Vertex second, Collector<Tuple2<Vertex, Edge>> out) throws Exception {
                        out.collect(new Tuple2<Vertex,Edge>(second,first));
                    }
                }).returns("Tuple2<org.gradoop.common.model.impl.pojo.Vertex,org.gradoop.common.model.impl.pojo.Edge>")
            .join(lg.getVertices())
        .where(new KeySelector<Tuple2<Vertex, Edge>, GradoopId>() {
            @Override
            public GradoopId getKey(Tuple2<Vertex, Edge> t) throws Exception {
                return t.f1.getTargetId();
            }
        })
        .equalTo((Vertex v)->v.getId()).with(new FlatJoinFunction<Tuple2<Vertex,Edge>, Vertex, Tuple3<Vertex,Edge,Vertex>>() {
            @Override
            public void join(Tuple2<Vertex,Edge> first, Vertex second, Collector<Tuple3<Vertex,Edge,Vertex>> out) throws Exception {
                out.collect(new Tuple3<Vertex,Edge,Vertex>(first.f0,first.f1,second));
            }
        }).returns("Tuple3<org.gradoop.common.model.impl.pojo.Vertex,org.gradoop.common.model.impl.pojo.Edge,org.gradoop.common.model.impl.pojo.Vertex>");
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

    public static boolean graphEquals(LogicalGraph left, LogicalGraph right) {
        try {
            if (left.getGraphHead().count() != 0 && right.getGraphHead().count() != 0) {
                Set<Boolean> gh = new HashSet<>(left.getGraphHead()
                        .join(right.getGraphHead())
                        .where((GraphHead l) -> 0)
                        .equalTo((GraphHead r) -> 0)
                        .with(new FlatJoinFunction<GraphHead, GraphHead, Boolean>() {
                            @Override
                            public void join(GraphHead first, GraphHead second, Collector<Boolean> out) throws Exception {
                                if (epgmEquals(first,second)) out.collect(true);
                            }
                        }).returns(Boolean.class).distinct().collect());
                if (gh.isEmpty()) return false;
            } else if (left.getVertices().count() == 0 && right.getVertices().count() == 0) {
            } else return false;

            if (left.getVertices().count() != 0 && right.getVertices().count() != 0) {
                Set<Boolean> gh = new HashSet<>(left.getVertices()
                        .join(right.getVertices())
                        .where((Vertex l) -> 0)
                        .equalTo((Vertex r) -> 0)
                        .with(new FlatJoinFunction<Vertex, Vertex, Boolean>() {
                            @Override
                            public void join(Vertex first, Vertex second, Collector<Boolean> out) throws Exception {
                                if (epgmEquals(first,second)) out.collect(true);
                            }
                        }).returns(Boolean.class).distinct().collect());
                if (gh.isEmpty()) return false;
            } else if (left.getVertices().count() == 0 && right.getVertices().count() == 0) {
            } else return false;

            if (left.getEdges().count() != 0 && right.getEdges().count() != 0) {
                Set<Boolean> gh = new HashSet<>(transformEdges(left)
                        .join(transformEdges(right))
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
                        }).returns(Boolean.class).distinct().collect());
            } else if (left.getEdges().count() == 0 && right.getEdges().count() == 0) {
            } else return false;

            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
