package org.gradoop.flink.model.impl.operators.kmeans.functions;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;

public class VertexPostProcessingMap<V extends Vertex>
        implements MapFunction<Tuple2<V, Tuple2<Centroid, String>>, V> {
    final String LAT = "lat";
    final String LONG = "long";
    final String LAT_ORIGIN = LAT + "_origin";
    final String LONG_ORIGIN = LONG + "_origin";

    @Override
    public V map (Tuple2<V, Tuple2<Centroid, String>> t2) throws Exception {
        V vertex = t2.f0;
        if (vertex.hasProperty(LAT) && vertex.hasProperty(LAT)) {
            vertex.setProperty(LAT_ORIGIN, vertex.getPropertyValue(LAT));
            vertex.setProperty(LONG_ORIGIN, vertex.getPropertyValue(LONG));
            vertex.removeProperty(LAT);
            vertex.removeProperty(LONG);
            String[] latAndLon = t2.f1.f1.split(";");
            vertex.setProperty("cluster_lat", PropertyValue.create(Double.parseDouble(latAndLon[0])));
            vertex.setProperty("cluster_lon", PropertyValue.create(Double.parseDouble(latAndLon[1])));
            vertex.setProperty("cluster_id", PropertyValue.create(t2.f1.f0.getId()));
        }
        return vertex;
    }


}
