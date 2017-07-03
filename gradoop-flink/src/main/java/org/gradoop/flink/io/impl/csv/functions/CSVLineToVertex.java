
package org.gradoop.flink.io.impl.csv.functions;

import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;

/**
 * /**
 * Creates a {@link Vertex} from a CSV string. The function uses a
 * {@link MetaData} object to correctly parse the property values.
 *
 * The string needs to be encoded in the following format:
 *
 * vertex-id;vertex-label;value_1|value_2|...|value_n
 */
public class CSVLineToVertex extends CSVLineToElement<Vertex> {
  /**
   * Used to instantiate the vertex.
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Constructor
   *
   * @param epgmVertexFactory EPGM vertex factory
   */
  public CSVLineToVertex(EPGMVertexFactory<Vertex> epgmVertexFactory) {
    this.vertexFactory = epgmVertexFactory;
  }

  @Override
  public Vertex map(String csvLine) throws Exception {
    String[] tokens = split(csvLine, 3);
    return vertexFactory.initVertex(
      GradoopId.fromString(tokens[0]),
      tokens[1],
      parseProperties(tokens[1], tokens[2]));
  }
}
