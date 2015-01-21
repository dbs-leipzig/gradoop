package org.gradoop.algorithms;

import org.apache.commons.lang3.StringUtils;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.gradoop.io.PartitioningVertex;

import java.io.IOException;

/**
 * Adaptive Repartitioning (ADP) algorithm as described in:
 * <p/>
 * http://www.few.vu.nl/~cma330/papers/ICDCS14.pdf
 * <p/>
 * TODO: algorithm description
 */
public class AdaptiveRepartitioningComputation extends
  BasicComputation<IntWritable, PartitioningVertex, NullWritable, IntWritable> {
  /**
   * Number of partitions to create.
   */
  public static final String NUMBER_OF_PARTITIONS =
    "partitioning.num.partitions";
  /**
   * Default number of partitions if no value is given.
   */
  public static final int DEFAULT_NUMBER_OF_PARTITIONS = 4;
  /**
   * Number of iterations after which the calculation is stopped.
   */
  public static final String NUMBER_OF_ITERATIONS = "partitioning.iterations";
  /**
   * Default number of iterations if no value is given.
   */
  public static final int DEFAULT_NUMBER_OF_ITERATIONS = 50;
  /**
   * Number of migrations the Vertex can do
   */
  public static final String NUMBER_OF_STABILIZATION_ROUNDS =
    "partitioning.stabilization";
  /**
   * Default number of migrations if no value is given
   */
  public static final int DEFAULT_NUMBER_OF_STABILIZATION_ROUNDS = 30;
  /**
   * Threshold to calculate total partition capacity.
   */
  public static final String CAPACITY_THRESHOLD =
    "partitioning.capacity" + ".threshold";
  /**
   * Default capacityThreshold if no value is given.
   */
  public static final float DEFAULT_CAPACITY_THRESHOLD = .2f;
  /**
   * Prefix for capacity aggregator which is used by master and worker compute.
   */
  static final String CAPACITY_AGGREGATOR_PREFIX =
    AdaptiveRepartitioningComputation.class.getName() + ".capacity.aggregator.";
  /**
   * Prefix for demand aggregator which is used by master and worker compute.
   */
  static final String DEMAND_AGGREGATOR_PREFIX =
    AdaptiveRepartitioningComputation.class.getName() + ".demand.aggregator.";
  /**
   * Needed for aggregators.
   */
  private static final IntWritable POSITIVE_ONE = new IntWritable(1);
  /**
   * Needed for aggregators.
   */
  private static final IntWritable NEGATIVE_ONE = new IntWritable(-1);
  /**
   * Class logger.
   */
  private static final Logger LOG =
    Logger.getLogger(AdaptiveRepartitioningComputation.class);
  /**
   * Total number of available partitions.
   */
  private int k;
  /**
   * Capacity capacityThreshold
   */
  private float capacityThreshold;
  /**
   * Total number of possible migrations
   */
  private int stabilizationRoundMax;

  /**
   * Returns the desired partition of the given vertex based on the
   * neighbours and the partitions they are in.
   *
   * @param vertex   current vertex
   * @param messages messages sent to current vertex
   * @return desired partition
   */
  private int getDesiredPartition(
    final Vertex<IntWritable, PartitioningVertex, NullWritable> vertex,
    final Iterable<IntWritable> messages) {
    int currentPartition = vertex.getValue().getCurrentPartition().get();
    int desiredPartition = currentPartition;
    LOG.info("currentPartition: " + currentPartition);
    // got messages?
    if (messages.iterator().hasNext()) {
      // partition -> neighbours in partition
      int[] countNeighbours = getPartitionFrequencies(messages);
      LOG.info("count neighbours:");
      for (int i = 0; i < countNeighbours.length; i++) {
        LOG.info(String.format("%d => %d", i, countNeighbours[i]));
      }
      // partition -> desire to migrate
      double[] partitionWeights =
        getPartitionWeights(countNeighbours, vertex.getNumEdges());
      LOG.info("partition weights:");
      for (int i = 0; i < partitionWeights.length; i++) {
        LOG.info(String.format("%d => %f.2", i, partitionWeights[i]));
      }
      double firstMax = Integer.MIN_VALUE;
      double secondMax = Integer.MIN_VALUE;
      int firstK = -1;
      int secondK = -1;
      for (int i = 0; i < k; i++) {
        if (partitionWeights[i] > firstMax) {
          secondMax = firstMax;
          firstMax = partitionWeights[i];
          secondK = firstK;
          firstK = i;
        } else if (partitionWeights[i] > secondMax) {
          secondMax = partitionWeights[i];
          secondK = i;
        }
      }
      LOG.info("firstMax: " + firstMax);
      LOG.info("secondMax: " + secondMax);
      LOG.info("firstK: " + firstK);
      LOG.info("secondK: " + secondK);
      if (firstMax == secondMax) {
        if (currentPartition != firstK && currentPartition != secondK) {
          desiredPartition = firstK;
        }
      } else {
        desiredPartition = firstK;
      }
    }
    LOG.info("desiredPartition: " + desiredPartition);
    return desiredPartition;
  }

  /**
   * Calculates the partition frequencies among neighbour vertices.
   * Returns a field where element i represents the number of neighbours in
   * partition i.
   *
   * @param messages messages sent to the vertex
   * @return partition frequency
   */
  private int[] getPartitionFrequencies(final Iterable<IntWritable> messages) {
    int[] result = new int[k];
    for (IntWritable message : messages) {
      result[message.get()]++;
    }
    return result;
  }

  /**
   * Calculates a weight for each partition based on the partition frequency
   * and the number of outgoing edges of that vertex.
   *
   * @param partitionFrequencies partition frequencies
   * @param numEdges             number of outgoing edges
   * @return partition weights
   */
  private double[] getPartitionWeights(int[] partitionFrequencies,
    int numEdges) {
    double[] partitionWeights = new double[k];
    for (int i = 0; i < k; i++) {
      int load = getPartitionLoad(i);
      int freq = partitionFrequencies[i];
      double weight = (double) freq / (load * numEdges);
      partitionWeights[i] = weight;
    }
    return partitionWeights;
  }

  /**
   * Decides of a vertex is allowed to migrate to a given desired partition.
   * This is based on the free space in the partition and the demand for that
   * partition.
   *
   * @param desiredPartition desired partition
   * @return true if the vertex is allowed to migrate, false otherwise
   */
  private boolean doMigrate(int desiredPartition) {
    LOG.info("calculating migration probability");
    long totalCapacity = getTotalCapacity();
    LOG.info("totalCapacity: " + totalCapacity);
    int load = getPartitionLoad(desiredPartition);
    LOG.info("load: " + load);
    long availability = totalCapacity - load;
    LOG.info("availability: " + availability);
    double demand = getPartitionDemand(desiredPartition);
    LOG.info("demand: " + demand);
    double threshold = availability / demand;
    LOG.info("capacityThreshold: " + threshold);
    double randomRange = Math.random();
    LOG.info("randomRange: " + randomRange);
    return Double.compare(randomRange, threshold) < 0;
  }

  /**
   * Returns the total number of vertices a partition can store. This depends
   * on the strict capacity and the capacity threshold.
   *
   * @return total capacity of a partition
   */
  private int getTotalCapacity() {
    double strictCapacity = getTotalNumVertices() / (double) k;
    double buffer = strictCapacity * capacityThreshold;
    return (int) Math.ceil(strictCapacity + buffer);
  }

  /**
   * Returns the demand for the given partition.
   *
   * @param partition partition id
   * @return demand for partition
   */
  private int getPartitionDemand(int partition) {
    IntWritable demandWritable =
      getAggregatedValue(DEMAND_AGGREGATOR_PREFIX + partition);
    return demandWritable.get();
  }

  /**
   * Returns the current load of the given partition.
   *
   * @param partition partition id
   * @return load of partition
   */
  private int getPartitionLoad(int partition) {
    IntWritable loadWritable =
      getAggregatedValue(CAPACITY_AGGREGATOR_PREFIX + partition);
    return loadWritable.get();
  }

  /**
   * Moves a vertex from its old to its new partition.
   *
   * @param vertex           vertex
   * @param desiredPartition partition to move vertex to
   */
  private void migrateVertex(
    final Vertex<IntWritable, PartitioningVertex, NullWritable> vertex,
    int desiredPartition) {
    // add current partition to partition history
    vertex.getValue()
      .addToPartitionHistory(vertex.getValue().getCurrentPartition().get());
    LOG.info("Partition History: " +
      StringUtils.join(vertex.getValue().getPartitionHistory(), " "));

    // decrease capacity in old partition
    String oldPartition = CAPACITY_AGGREGATOR_PREFIX +
      vertex.getValue().getCurrentPartition().get();
    notifyAggregator(oldPartition, NEGATIVE_ONE);
    // increase capacity in new partition
    String newPartition = CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
    notifyAggregator(newPartition, POSITIVE_ONE);

    vertex.getValue().setCurrentPartition(new IntWritable(desiredPartition));
  }

  /**
   * Initializes the vertex with a partition id. This is calculated using
   * modulo (vertex-id % partition count).
   *
   * @param vertex vertex
   */
  private void setVertexStartValue(
    final Vertex<IntWritable, PartitioningVertex, NullWritable> vertex) {
    int startValue = vertex.getId().get() % k;
    vertex.getValue().setCurrentPartition(new IntWritable(startValue));
  }

  /**
   * Sends the given value to the given aggregator.
   *
   * @param aggregator aggregator to send value to
   * @param v          value to send
   */
  private void notifyAggregator(final String aggregator, final IntWritable v) {
    LOG.info(String.format("sending %d to %s", v.get(), aggregator));
    aggregate(aggregator, v);
  }

  @Override
  public void initialize(GraphState graphState,
    WorkerClientRequestProcessor<IntWritable, PartitioningVertex,
      NullWritable> workerClientRequestProcessor,
    GraphTaskManager<IntWritable, PartitioningVertex, NullWritable>
      graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
    super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
      workerGlobalCommUsage, workerContext);

    k = getConf().getInt(NUMBER_OF_PARTITIONS, DEFAULT_NUMBER_OF_PARTITIONS);
    stabilizationRoundMax = getConf().getInt(NUMBER_OF_STABILIZATION_ROUNDS,
      DEFAULT_NUMBER_OF_STABILIZATION_ROUNDS);
    capacityThreshold =
      getConf().getFloat(CAPACITY_THRESHOLD, DEFAULT_CAPACITY_THRESHOLD);

    LOG.info("=== Initialized Computation");
    LOG.info(String
      .format("k: %d, stabilizationRounds: %d, capacityTreshold:" + " %.2f", k,
        stabilizationRoundMax, capacityThreshold));
  }

  /**
   * The actual ADP computation.
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.
   * @throws IOException
   */
  @Override
  public void compute(
    Vertex<IntWritable, PartitioningVertex, NullWritable> vertex,
    Iterable<IntWritable> messages) throws IOException {
    LOG.info(String.format("=== ss: %d vertex-id: %d k: %d", getSuperstep(),
      vertex.getId().get(), k));
    if (getSuperstep() == 0) {
      LOG.info("== INIT PHASE");
      setVertexStartValue(vertex);
      String aggregator = CAPACITY_AGGREGATOR_PREFIX +
        vertex.getValue().getCurrentPartition().get();
      notifyAggregator(aggregator, POSITIVE_ONE);
      sendMessageToAllEdges(vertex, vertex.getValue().getCurrentPartition());
    } else {
      // even superstep: migrate phase
      if ((getSuperstep() % 2) == 0) {
        LOG.info("MIGRATION PHASE");
        int desiredPartition = vertex.getValue().getDesiredPartition().get();
        int currentPartition = vertex.getValue().getCurrentPartition().get();
        LOG.info("currentPartition: " + currentPartition);
        LOG.info("desiredPartition: " + desiredPartition);
        if (desiredPartition != currentPartition) {
          boolean migrate = doMigrate(desiredPartition);
          LOG.info("doMigrate: " + migrate);
          if (migrate) {
            migrateVertex(vertex, desiredPartition);
            sendMessageToAllEdges(vertex,
              vertex.getValue().getCurrentPartition());
          }
        }
        vertex.voteToHalt();
      } else { // odd supersteps: demand phase
        if (vertex.getValue().getPartitionHistoryCount() >=
          stabilizationRoundMax) {
          vertex.voteToHalt();
        } else {
          LOG.info("DEMAND PHASE");
          int desiredPartition = getDesiredPartition(vertex, messages);
          vertex.getValue()
            .setDesiredPartition(new IntWritable(desiredPartition));
          int currentValue = vertex.getValue().getCurrentPartition().get();
          boolean changed = currentValue != desiredPartition;
          if (changed) {
            notifyAggregator(DEMAND_AGGREGATOR_PREFIX + desiredPartition,
              POSITIVE_ONE);
          }
        }
      }
    }
  }
}
