package org.gradoop.flink.model.impl.operators.kmeans.functions;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

public class VertexPostProcessingMap<V extends Vertex>
        implements MapFunction<Tuple2<V, Tuple2<Centroid, Point>>, V> {
    final String LAT = "lat";
    final String LONG = "long";
    final String LAT_ORIGIN = LAT + "_origin";
    final String LONG_ORIGIN = LONG + "_origin";
    @Override
    public V map(Tuple2<V, Tuple2<Centroid, Point>> t2) throws Exception {
        V vertex = t2.f0;
        if (vertex.hasProperty(LAT) && vertex.hasProperty(LONG)) {
            vertex.setProperty(LAT_ORIGIN, vertex.getPropertyValue(LAT));
            vertex.setProperty(LONG_ORIGIN, vertex.getPropertyValue(LONG));
            vertex.removeProperty(LAT);
            vertex.removeProperty(LONG);
            vertex.setProperty(LAT, PropertyValue.create(t2.f1.f0.getLat()));
            vertex.setProperty(LONG, PropertyValue.create(t2.f1.f0.getLon()));
            vertex.setProperty("cluster_id", PropertyValue.create(t2.f1.f0.getId()));
        }
        return vertex;
    }
}
