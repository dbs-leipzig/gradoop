package org.gradoop.algorithms;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.gradoop.io.formats.EPGEdgeValueWritable;
import org.gradoop.io.formats.EPGVertexIdentifierWritable;
import org.gradoop.io.formats.EPGVertexValueWritable;

import java.io.IOException;

/**
 * Connected Components from Giraph examples, edited
 */
public class ConnectedComponentsComputation extends
  BasicComputation<EPGVertexIdentifierWritable, EPGVertexValueWritable,
    EPGEdgeValueWritable, LongWritable> {
  /**
   * Propagates the smallest vertex id to all neighbors. Will always choose to
   * halt and only reactivate if a smaller id has been sent to it.
   *
   * @param vertex Vertex
   * @param messages Iterator of messages from the previous superstep.
   * @throws IOException
   */
  @Override
  public void compute(Vertex<EPGVertexIdentifierWritable,
    EPGVertexValueWritable, EPGEdgeValueWritable> vertex,
    Iterable<LongWritable> messages) throws IOException {

    //first superstep: take vertexID
    if (getSuperstep() == 0) {
      long current = vertex.getId().getID();
      // for each edge, check, if target vertex id is smaller than current id
      for (Edge<EPGVertexIdentifierWritable, EPGEdgeValueWritable> e :
        vertex.getEdges()) {
        long candidate = e.getTargetVertexId().getID();
        if (candidate < current) {
          current = candidate;
        }
      }
      vertex.getValue().resetGraphs();
      vertex.getValue().addGraph(current);
      for (Edge<EPGVertexIdentifierWritable, EPGEdgeValueWritable> edge :
        vertex.getEdges()) {
        EPGVertexIdentifierWritable neighbor = edge.getTargetVertexId();
        if (neighbor.getID() > current) {
          sendMessage(neighbor, new LongWritable(current));
        }
      }
      vertex.voteToHalt();
      return;
    }
    // next supersteps: take component from vertex.getValue().getGraphs()
    boolean changed = false;
    long current = vertex.getValue().getGraphs().iterator().next();
    for (LongWritable message : messages) {
      long candidate = message.get();
      if (candidate < current) {
        current = candidate;
        changed = true;
      }
    }
    // propagate new component id to the neighbors
    if (changed) {
      vertex.getValue().resetGraphs();
      vertex.getValue().addGraph(current);
      sendMessageToAllEdges(vertex, new LongWritable(current));
    }

    vertex.voteToHalt();
  }
}
