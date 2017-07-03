
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 * Creates the fused vertex from the collection of the graph head of the pattern graph element.
 * The new vertex is stored as an occurence of the searchGraph
 *
 */
@FunctionAnnotation.ForwardedFieldsFirst("id")
@FunctionAnnotation.ForwardedFieldsSecond("label;properties")
public class CreateFusedVertex implements CrossFunction<GraphHead, GraphHead, Vertex> {

  /**
   * Basic vertex reused each time. It'll be the fused vertex
   */
  private static final Vertex REUSABLE_VERTEX = new Vertex();

  /**
   * newly generated vertex id for the new vertex
   */
  private GradoopId newVertexId;

  /**
   * Given the new vertex Id, it generates the joiner generating the new vertex
   * @param newVertexId   new vertex Id
   */
  public CreateFusedVertex(GradoopId newVertexId) {
    this.newVertexId = newVertexId;
  }

  @Override
  public Vertex cross(GraphHead searchGraphHead, GraphHead patternGraphSeachHead) throws Exception {
    REUSABLE_VERTEX.setLabel(patternGraphSeachHead.getLabel());
    REUSABLE_VERTEX.setProperties(patternGraphSeachHead.getProperties());
    REUSABLE_VERTEX.setId(newVertexId);
    REUSABLE_VERTEX.addGraphId(searchGraphHead.getId());
    return REUSABLE_VERTEX;
  }
}
