package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.Label;
import org.gradoop.flink.model.impl.functions.epgm.ToPropertyValue;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.ToIdWithLabel;

import java.util.stream.Collectors;

/**
 * Created by Giacomo Bergami on 25/01/17.
 */
public class FusionUtils {

    public static LogicalGraph recreateGraph(LogicalGraph x) {
        return LogicalGraph.fromDataSets(x.getGraphHead(),x.getVertices(),x.getEdges(),x.getConfig());
    }

    public static MapOperator<GraphHead, GradoopId> getGraphId(LogicalGraph g) {
        return g.getGraphHead().map(new Id<>());
    }

    public static <P extends GraphElement> DataSet<P> areElementsInGraph(DataSet<P> head, LogicalGraph g, boolean inGraph) {
        return head
                .filter(inGraph ? new InGraphBroadcast<>() : new NotInGraphBroadcast<>())
                .withBroadcastSet(getGraphId(g), GraphContainmentFilterBroadcast.GRAPH_ID);
    }


    public static String getGraphLabel(LogicalGraph g) {
        try {
            String toret = g.getGraphHead().map(new Label<>()).collect().get(0);
            return toret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties getGraphProperties(LogicalGraph g) {
        try {
            Properties toret = g.getGraphHead().map(new org.gradoop.flink.model.impl.functions.epgm.Properties<>()).collect().get(0);
            return toret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String stringifyVertices(DataSet<Vertex> g) {
        StringBuilder sb = new StringBuilder();
        try {
            return g.map((Vertex x)-> epgmString(x)).collect().stream().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
    public static String stringifyVerticesWithId(DataSet<Vertex> g) {
        StringBuilder sb = new StringBuilder();
        try {
            return g.map((Vertex x)-> epgmString(x,true)).collect().stream().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public static String stringifyEdgesFromGraph(DataSet<Edge> g, DataSet<Vertex> v) {
        StringBuilder sb = new StringBuilder();
        try {
            return transformEdges(g,v).map((Tuple3<Vertex,Edge,Vertex> x)-> "("+ FusionUtils.epgmString(x.f0)+")-["+ FusionUtils.epgmString(x.f1)+"]->("+ FusionUtils.epgmString(x.f2)+")").returns(String.class).collect().stream().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public static String epgmString(EPGMElement first) {
        return epgmString(first,false);
    }

    public static String epgmString(EPGMElement first, boolean withId) {
        StringBuilder sb = new StringBuilder();
        if (withId) sb.append("<").append(first.getId()).append(">");
        sb.append(first.getLabel()).append(" {");
        try {
            for (String k : first.getProperties().getKeys()) {
                sb.append(k).append("=").append(first.getProperties().get(k)).append(" ");
            }
        } catch (Exception e) {
        }
        sb.append("} ");
        return sb.toString();
    }

    public static DataSet<Tuple3<Vertex,Edge,Vertex>> transformEdges(DataSet<Edge> lg, DataSet<Vertex> v) {
        return lg.join(v)
                .where((Edge e)->e.getSourceId())
                .equalTo((Vertex x)->x.getId())
                .with(new FlatJoinFunction<Edge, Vertex, Tuple2<Vertex,Edge>>() {
                    @Override
                    public void join(Edge first, Vertex second, Collector<Tuple2<Vertex, Edge>> out) throws Exception {
                        out.collect(new Tuple2<Vertex,Edge>(second,first));
                    }
                }).returns("Tuple2<org.gradoop.common.model.impl.pojo.Vertex,org.gradoop.common.model.impl.pojo.Edge>")
            .join(v)
        .where(new KeySelector<Tuple2<Vertex, Edge>, GradoopId>() {
            @Override
            public GradoopId getKey(Tuple2<Vertex, Edge> t) throws Exception {
                return t.f1.getTargetId();
            }
        })
        .equalTo((Vertex x)->x.getId()).with(new FlatJoinFunction<Tuple2<Vertex,Edge>, Vertex, Tuple3<Vertex,Edge,Vertex>>() {
            @Override
            public void join(Tuple2<Vertex,Edge> first, Vertex second, Collector<Tuple3<Vertex,Edge,Vertex>> out) throws Exception {
                out.collect(new Tuple3<Vertex,Edge,Vertex>(first.f0,first.f1,second));
            }
        }).returns("Tuple3<org.gradoop.common.model.impl.pojo.Vertex,org.gradoop.common.model.impl.pojo.Edge,org.gradoop.common.model.impl.pojo.Vertex>");
    }
}
