
package org.gradoop.flink.io.impl.csv.functions;

import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;

/**
 * Creates an {@link Edge} from a CSV string. The function uses a
 * {@link MetaData} object to correctly parse the property values.
 *
 * The string needs to be encoded in the following format:
 *
 * edge-id;source-id;target-id;edge-label;value_1|value_2|...|value_n
 */
public class CSVEdgeToEdge extends CSVLineToElement<Edge> {
  /**
   * Used to instantiate the edge.
   */
  private final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * Constructor.
   *
   * @param epgmEdgeFactory EPGM edge factory
   */
  public CSVEdgeToEdge(EPGMEdgeFactory<Edge> epgmEdgeFactory) {
    this.edgeFactory = epgmEdgeFactory;
  }

  @Override
  public Edge map(String csvLine) throws Exception {
    String[] tokens = split(csvLine, 5);
    return edgeFactory.initEdge(GradoopId.fromString(tokens[0]),
      tokens[3],
      GradoopId.fromString(tokens[1]),
      GradoopId.fromString(tokens[2]),
      parseProperties(tokens[3], tokens[4]));
  }
}
