/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.algorithms.gelly.GradoopGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRAttractionFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRForceApplicator;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.GiLaDegreePruner;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Implementation of the GiLa-Layouting-Algorithm.
 * Good for sparsely-connected graphs. Really slow for dense graphs.
 */
public class GiLaLayouter extends
  GradoopGellyAlgorithm<GiLaLayouter.VertexValue, NullValue> implements LayoutingAlgorithm {

  /**
   * Default optimum distance.
   */
  protected static final double DEFAULT_OPT_DISTANCE = 100;
  /**
   * The MessageFunction to use
   */
  protected MsgFunc msgFunc;
  /**
   * Number of total iterations to perform
   */
  protected int iterations;
  /**
   * Width of the layouting-area
   */
  protected int width;
  /**
   * Height of the layouting-area
   */
  protected int height;
  /**
   * Only vertices in the kNeigboorhood of a vertex are used for repulsion calculation
   */
  protected int kNeighborhood;
  /**
   * This is the constance K from the FRLayouter. Renamed so it is not confused with
   * kNeighborhood
   */
  protected double optimumDistance;
  /**
   * (Approximate) Number of vertices in the graph. Used to compute default-values.
   */
  protected int numberOfVertices;

  /**
   * Construct a new GiLaLayouter-Instance
   *
   * @param iterations    Number of iterations to perform
   * @param vertexCount   (Approximate) Number of vertices in the graph. Used to compute
   *                      default-values.
   * @param kNeighborhood Only vertices in the kNeigboorhood of a vertex are used for repulsion
   *                      calculation.
   */
  public GiLaLayouter(int iterations, int vertexCount, int kNeighborhood) {
    super(new VertexToGellyVertex<VertexValue>() {
      @Override
      public Vertex<GradoopId, VertexValue> map(EPGMVertex vertex) {
        return new Vertex<>(vertex.getId(), new VertexValue(vertex));
      }
    }, new EdgeToGellyEdgeWithNullValue());
    this.iterations = iterations;
    this.kNeighborhood = kNeighborhood;
    this.numberOfVertices = vertexCount;
  }

  /**
   * Override default layout-space size
   * Default:  width = height = Math.sqrt(Math.pow(k, 2) * numberOfVertices) * 0.5
   *
   * @param width  new width
   * @param height new height
   * @return this (for method-chaining)
   */
  public GiLaLayouter area(int width, int height) {
    this.width = width;
    this.height = height;
    return this;
  }

  /**
   * Sets optional value optimumDistance
   *
   * @param optimumDistance the new value
   * @return this (for method-chaining)
   */
  public GiLaLayouter optimumDistance(double optimumDistance) {
    this.optimumDistance = optimumDistance;
    return this;
  }

  /**
   * Gets optimumDistance
   *
   * @return value of optimumDistance
   */
  public double getOptimumDistance() {
    return (optimumDistance != 0) ? optimumDistance : DEFAULT_OPT_DISTANCE;
  }

  @Override
  public int getWidth() {
    return (width != 0) ? width :
      (int) (Math.sqrt(Math.pow(DEFAULT_OPT_DISTANCE, 2) * numberOfVertices) * 0.5);
  }

  @Override
  public int getHeight() {
    return (height != 0) ? height :
      (int) (Math.sqrt(Math.pow(DEFAULT_OPT_DISTANCE, 2) * numberOfVertices) * 0.5);
  }


  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    msgFunc =
      new MsgFunc(getWidth(), getHeight(), getOptimumDistance(), kNeighborhood, numberOfVertices,
        iterations);

    // random start-layout
    RandomLayouter rl =
      new RandomLayouter(getWidth() / 10, getWidth() - (getWidth() / 10), getHeight() / 10,
        getHeight() - (getHeight() / 10));
    graph = rl.execute(graph);

    // remove vertices of degree 1
    GiLaDegreePruner pruner = new GiLaDegreePruner();
    graph = pruner.prune(graph);

    // GiLa needs an undirected graph. Transform the undirected into a directed Graph by copying
    // and reversing all edges.


    DataSet<EPGMEdge> edges = graph.getEdges().flatMap(
      new FlatMapFunction<EPGMEdge, EPGMEdge>() {

        @Override
        public void flatMap(EPGMEdge e,
          Collector<EPGMEdge> collector) throws Exception {
          EPGMEdge edgeCopy =
            new EPGMEdge(GradoopId.get(), e.getLabel(),
              e.getTargetId(), e.getSourceId(), new Properties(), null);
          collector.collect(e);
          collector.collect(edgeCopy);
        }
      });

    graph = graph.getConfig().getLogicalGraphFactory().fromDataSets(graph.getVertices(), edges);

    // perform the layouting
    graph = super.execute(graph);

    // reinsert pruned vertices
    graph = pruner.reinsert(graph);

    return graph;
  }

  @Override
  public LogicalGraph executeInGelly(Graph<GradoopId, VertexValue, NullValue> graph) {
    DataSet<Vertex<GradoopId, VertexValue>> result =
      graph.runVertexCentricIteration(msgFunc, null, iterations * kNeighborhood + 1).getVertices();


    DataSet<EPGMVertex> layoutedVertices =
      result.join(currentGraph.getVertices()).where(0).equalTo("id").with(
        new JoinFunction<Vertex<GradoopId, VertexValue>, EPGMVertex, EPGMVertex>() {
          @Override
          public EPGMVertex join(
            Vertex<GradoopId, VertexValue> gellyVertex,
            EPGMVertex vertex) throws Exception {
            gellyVertex.getValue().getPosition().setVertexPosition(vertex);
            return vertex;
          }
        });

    return currentGraph.getConfig().getLogicalGraphFactory()
      .fromDataSets(layoutedVertices, currentGraph.getEdges());
  }


  /**
   * The ComputeFunction for the GiLa-Algorithm
   */
  protected static class MsgFunc extends
    ComputeFunction<GradoopId, VertexValue, NullValue, Message> {

    /**
     * Only vertices in the kNeigboorhood of a vertex are used for repulsion calculation
     */
    protected int kNeighborhood;

    /**
     * Repulsion-function to use
     */
    protected FRRepulsionFunction repulsion;
    /**
     * Attraction-function to use
     */
    protected FRAttractionFunction attraction;
    /**
     * Application-Function to use
     */
    protected FRForceApplicator applicator;

    /**
     * For object-reuse
     */
    protected LVertex lvertex1 = new LVertex();

    /**
     * For object-reuse
     */
    protected LVertex lVertex2 = new LVertex();

    /**
     * For object-reuse
     */
    protected Tuple3<LVertex, LVertex, Integer> vertexTuple = new Tuple3<>();

    /**
     * Create new MsgFunc
     *
     * @param width           width of the layouting-area
     * @param height          height of the layouting-area
     * @param optimumDistance k of FRLayouter
     * @param kNeighborhood   kNeighborhood for repulsion-calculations
     * @param numVertices     Number of vertices in the graph
     * @param maxIterations  Number of iterations to perform
     */
    public MsgFunc(int width, int height, double optimumDistance, int kNeighborhood,
      int numVertices, int maxIterations) {
      this.kNeighborhood = kNeighborhood;

      this.repulsion = new FRRepulsionFunction(optimumDistance);
      this.attraction = new FRAttractionFunction(optimumDistance);
      //this.applicator = new GiLaForceApplicator(width,height,optimumDistance,numVertices);
      this.applicator = new FRForceApplicator(width, height, optimumDistance, maxIterations);
    }

    @Override
    public void compute(Vertex<GradoopId, VertexValue> vertex,
      MessageIterator<Message> messageIterator) {

      // substact one to get 0-based numbers
      int iteration = getSuperstepNumber() - 1;

      VertexValue value = vertex.getValue();

      List<Message> messagesToSend = new LinkedList<>();

      int receivedMessages = 0;
      for (Message msg : messageIterator) {
        if (!value.getMessages().contains(msg.f0)) {
          value.getMessages().add(msg.getSender());

          //Attraction from direct neighbors
          if (msg.f2 == kNeighborhood) {
            value.getForces().mAdd(
              attractionForce(vertex.getId(), value.getPosition(), msg.getSender(),
                msg.getPosition()));
          }

          //Repulsion from all
          value.getForces().mAdd(
            repulsionForce(vertex.getId(), value.getPosition(), msg.getSender(), msg.getPosition())
              .mMul(msg.getWeight() + 1));
          if (msg.getTTL() > 1) {
            msg.setTTL(msg.getTTL() - 1);
            messagesToSend.add(msg);
          }
        }
        receivedMessages++;
      }

      if (iteration % kNeighborhood == 0 && iteration != 0) {
        applicator.apply(value.getPosition(), value.getForces(),
          applicator.speedForIteration(iteration / kNeighborhood));
        value.getMessages().clear();
        value.getForces().reset();
      }

      if (iteration % kNeighborhood == 0 || iteration != 0 || receivedMessages == 0) {
        messagesToSend.add(
          new Message(vertex.getId(), value.getPosition(), kNeighborhood, vertex.getId(),
            value.getPrunedNeighbors()));
      }

      for (Edge<GradoopId, NullValue> e : getEdges()) {
        for (Message msg : messagesToSend) {
          // do not send message back to the vertex we received it from
          if (!msg.getLastHop().equals(e.getTarget())) {
            Message toSend = msg.copy();
            toSend.setLastHop(vertex.getId());
            sendMessageTo(e.getTarget(), toSend);
          }
        }
      }

      setNewVertexValue(value);
    }

    /**
     * Get attraction forces between two vertices
     *
     * @param id1  id of vertex 1
     * @param pos1 position of vertex 1
     * @param id2  id ov vertex 2
     * @param pos2 position of vertex 2
     * @return The calculated force-vector for vertex 1
     */
    protected Vector attractionForce(GradoopId id1, Vector pos1, GradoopId id2, Vector pos2) {
      lvertex1.setId(id1);
      lvertex1.setPosition(pos1.copy());
      lVertex2.setId(id2);
      lVertex2.setPosition(pos2.copy());
      vertexTuple.f0 = lvertex1;
      vertexTuple.f1 = lVertex2;
      vertexTuple.f2 = 1;
      return attraction.map(vertexTuple).getValue().copy();
    }

    /**
     * Get repulsion forces between two vertices
     *
     * @param id1  id of vertex 1
     * @param pos1 position of vertex 1
     * @param id2  id ov vertex 2
     * @param pos2 position of vertex 2
     * @return The calculated force-vector for vertex 1
     */
    protected Vector repulsionForce(GradoopId id1, Vector pos1, GradoopId id2, Vector pos2) {
      lvertex1.setId(id1);
      lvertex1.setPosition(pos1.copy());
      lVertex2.setId(id2);
      lVertex2.setPosition(pos2.copy());
      return repulsion.join(lvertex1, lVertex2).getValue().copy();
    }
  }

  /**
   * Represents a message transmitted between vertices. Just wraps Tuple4 for better readability.
   */
  protected static class Message extends Tuple5<GradoopId, Vector, Integer, GradoopId, Integer> {

    /**
     * Construct a new Message
     *
     * @param sender   Initial Sender of the message
     * @param position Position of the sender
     * @param ttl      TimeToLive of this message
     * @param lastHop  Id of the last vertex that retransmitted this message
     * @param weight   The weight of the vertex
     */
    public Message(GradoopId sender, Vector position, Integer ttl, GradoopId lastHop, int weight) {
      super(sender, position, ttl, lastHop, weight);
    }

    /**
     * Default constructor to conform with Pojo-rules
     */
    public Message() {
      super();
    }

    /**
     * Get the initial sender of the message
     *
     * @return The senders if
     */
    public GradoopId getSender() {
      return f0;
    }

    /**
     * Get the position of the sender
     *
     * @return The position
     */
    public Vector getPosition() {
      return f1;
    }

    /**
     * Get the ttl of this message
     *
     * @return the ttl
     */
    public Integer getTTL() {
      return f2;
    }

    /**
     * Get the last vertex send retransmitted this message
     *
     * @return The id of sais vertex
     */
    public GradoopId getLastHop() {
      return f3;
    }

    /**
     * Return the weight of the sending vertex
     *
     * @return weight
     */
    public int getWeight() {
      return f4;
    }

    /**
     * Set the sender id
     *
     * @param id New id
     */
    public void setSender(GradoopId id) {
      f0 = id;
    }

    /**
     * Set the position
     *
     * @param position New position
     */
    public void setPosition(Vector position) {
      f1 = position;
    }

    /**
     * Set the TTL for this message
     *
     * @param ttl New TTL
     */
    public void setTTL(Integer ttl) {
      f2 = ttl;
    }

    /**
     * Set the last hop id of this message
     *
     * @param hop New id
     */
    public void setLastHop(GradoopId hop) {
      f3 = hop;
    }

    /**
     * Set the weight of the sending vertex
     *
     * @param w The new weight
     */
    public void setWeight(int w) {
      f4 = w;
    }

    /**
     * Create a copy of this message.
     *
     * @return A shallow copy of this message
     */
    public Message copy() {
      return new Message(f0, f1, f2, f3, f4);
    }
  }

  /**
   * Represents the stored values for each vertex.
   */
  protected static class VertexValue extends Tuple4<Vector, Vector, HashSet<GradoopId>, Integer> {

    /**
     * Construct ne vertex-value
     *
     * @param v The gradoop-vertex to extract the position from
     */
    public VertexValue(EPGMVertex v) {
      f0 = Vector.fromVertexPosition(v);
      f1 = new Vector(0, 0);
      f2 = new HashSet<>();
      if (v.hasProperty(GiLaDegreePruner.NUM_PRUNED_NEIGHBORS_PROPERTY)) {
        f3 = v.getPropertyValue(GiLaDegreePruner.NUM_PRUNED_NEIGHBORS_PROPERTY).getInt();
      } else {
        f3 = 0;
      }
    }

    /**
     * Default constructor to conform woth POJO-rules
     */
    public VertexValue() {
      super();
    }

    /**
     * Gets current position of the vertex
     *
     * @return value of position
     */
    public Vector getPosition() {
      return f0;
    }

    /**
     * Sets current position of the vertex
     *
     * @param position the new value
     */
    public void setPosition(Vector position) {
      this.f0 = position;
    }

    /**
     * Gets current aggregated forces acting on this vertex. To be applied at the end of the round
     *
     * @return value of forces
     */
    public Vector getForces() {
      return f1;
    }

    /**
     * Sets forces current aggregated forces acting on this vertex. To be applied at the end of
     * the round
     *
     * @param forces the new value
     */
    public void setForces(Vector forces) {
      this.f1 = forces;
    }

    /**
     * Gets IDs of vertice whos broadcasts were received this round
     *
     * @return value of messages
     */
    public HashSet<GradoopId> getMessages() {
      return f2;
    }

    /**
     * Sets IDs of vertice whos broadcasts were received this round
     *
     * @param messages the new value
     */
    public void setMessages(HashSet<GradoopId> messages) {
      this.f2 = messages;
    }

    /**
     * Gets number of one-degree neighbors that were removed during pruning
     *
     * @return value of prunedNeighbors
     */
    public int getPrunedNeighbors() {
      return f3;
    }

    /**
     * Sets number of one-degree neighbors that were removed during pruning
     *
     * @param prunedNeighbors the new value
     */
    public void setPrunedNeighbors(int prunedNeighbors) {
      this.f3 = prunedNeighbors;
    }
  }

  @Override
  public String toString() {
    return "GiLaLayouter{" + "iterations=" + iterations + ", width=" + getWidth() + ", height=" +
      getHeight() + ", kNeighborhood=" + kNeighborhood + ", optimumDistance=" +
      getOptimumDistance() + ", numberOfVertices=" + numberOfVertices + '}';
  }
}
