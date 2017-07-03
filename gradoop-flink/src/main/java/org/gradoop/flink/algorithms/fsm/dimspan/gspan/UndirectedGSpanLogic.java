
package org.gradoop.flink.algorithms.fsm.dimspan.gspan;


import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.PatternEmbeddingsMap;

/**
 * Provides methods for logic related to the gSpan algorithm in undirected mode.
 */
public class UndirectedGSpanLogic extends GSpanLogicBase {

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public UndirectedGSpanLogic(DIMSpanConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  protected boolean getSingleEdgePatternIsOutgoing(int[] graph, int edgeId, boolean loop) {
    // extensions are always considered to be outgoing in undirected mode
    return true;
  }

  @Override
  protected boolean getExtensionIsOutgoing(int[] graph, int edgeId, boolean fromFrom) {
    // extensions are always considered to be outgoing in undirected mode
    return true;
  }

  @Override
  protected void storeSingleEdgePatternEmbeddings(PatternEmbeddingsMap patternEmbeddings,
    int[] pattern, int[] vertexIds, int[] edgeIds, int fromLabel, int toLabel, boolean loop) {

    patternEmbeddings.put(pattern, vertexIds, edgeIds);

    // create a second embedding for 1-edge automorphism
    if (fromLabel == toLabel && !loop) {
      patternEmbeddings.put(pattern, new int[] {vertexIds[1], vertexIds[0]}, edgeIds);
    }
  }
}
