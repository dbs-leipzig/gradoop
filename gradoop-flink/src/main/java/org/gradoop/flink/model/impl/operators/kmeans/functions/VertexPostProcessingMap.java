/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.kmeans.functions;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;

/**
 * Extends the properties of the vertices by the clusterId and the spatial properties of the cluster
 *
 * @param <V> The vertex type
 */
public class VertexPostProcessingMap<V extends Vertex> implements
  MapFunction<Tuple2<V, Tuple2<Centroid, String>>, V> {

  /**
   * First spatial property name of the vertex
   */
  private String lat;

  /**
   * Second spatial property name of the vertex
   */
  private String lon;

  /**
   * First spatial property name of the vertex extended by 'origin'
   */
  private String latOrigin;

  /**
   * Second spatial property name of the vertex extended by 'origin'
   */
  private String lonOrigin;

  /**
   * Initializes a VertexProcessingMap instance with the spatial property names of the vertices
   *
   * @param lat First spatial property name of the vertex
   * @param lon Second spatial property name of the vertex
   */

  public VertexPostProcessingMap(String lat, String lon) {
    this.lat = lat;
    this.lon = lon;
    this.latOrigin = lat + "_origin";
    this.lonOrigin = lon + "_origin";
  }

  /**
   * Extends the vertex attributes by the clusterId and its spatial properties
   *
   * @param t2 Tuple of the vertex and a tuple containing its centroid and the unique vertex key
   * @return Returns the vertex with extended attributes
   */
  @Override
  public V map(Tuple2<V, Tuple2<Centroid, String>> t2) {
    V vertex = t2.f0;
    if (vertex.hasProperty(lat) && vertex.hasProperty(lat)) {
      vertex.setProperty(latOrigin, vertex.getPropertyValue(lat));
      vertex.setProperty(lonOrigin, vertex.getPropertyValue(lon));
      vertex.removeProperty(lat);
      vertex.removeProperty(lon);
      String[] latAndLon = t2.f1.f1.split(";");
      vertex.setProperty("cluster_" + lat, PropertyValue.create(Double.parseDouble(latAndLon[0])));
      vertex.setProperty("cluster_" + lon, PropertyValue.create(Double.parseDouble(latAndLon[1])));
      vertex.setProperty("cluster_id", PropertyValue.create(t2.f1.f0.getId()));
    }
    return vertex;
  }


}
