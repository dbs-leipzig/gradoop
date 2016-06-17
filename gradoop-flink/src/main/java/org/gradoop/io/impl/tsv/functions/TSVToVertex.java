package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.HashMap;
import java.util.regex.Pattern;

public class TSVToVertex<V extends EPGMVertex>
  implements MapFunction<String, V> {

  private static final Pattern FILE_SEPARATOR_TOKEN = Pattern.compile(" ");

  /**
   * Creates vertex data objects.
   */
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * Creates map function
   *
   * @param vertexFactory vertex data factory
   */
  public TSVToVertex(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }


  @Override
  public V map(String s) throws Exception {
    String[] token = FILE_SEPARATOR_TOKEN.split(s);
    GradoopId vertexID = GradoopId.get();
    String label = "";
    HashMap<String, Object> properties = new HashMap<>();
    properties.put("language", token[1]);
    properties.put("id", token[0]);
    PropertyList propertyList = PropertyList.createFromMap(properties);
    GradoopIdSet graphs = GradoopIdSet.fromExisting(GradoopId.get());

    return vertexFactory.initVertex(vertexID, label, propertyList, graphs);
  }
}
