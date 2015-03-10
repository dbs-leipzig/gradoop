package org.gradoop.io;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Custom vertex used by {@link org.gradoop.algorithms
 * .KwayPartitioningComputation}.
 */
public class PartitioningVertex implements Writable {
  /**
   * The desired partition the vertex want to migrate to
   */
  private int desiredPartition;

  /**
   * The actual partition
   */
  private int currentPartition;
  /**
   * Contains the partition history of the vertex
   */
  private List<Integer> partitionHistory;

  /**
   * Method to set the current partition
   *
   * @param currentPartition current partition
   */
  public void setCurrentPartition(IntWritable currentPartition) {
    this.currentPartition = currentPartition.get();
  }

  /**
   * Method to set the lastValue of the vertex
   *
   * @param desiredPartition the desired Partition
   */
  public void setDesiredPartition(IntWritable desiredPartition) {
    this.desiredPartition = desiredPartition.get();
  }


  /**
   * Get method to get the desired partition
   *
   * @return the desired Partition
   */
  public IntWritable getDesiredPartition() {
    return new IntWritable(this.desiredPartition);
  }

  /**
   * Get the current partition
   *
   * @return the current partition
   */
  public IntWritable getCurrentPartition() {
    return new IntWritable(this.currentPartition);
  }

  /**
   * Get the PartitionHistory
   *
   * @return partitionHistory list
   */
  public List<Integer> getPartitionHistory() {
    return this.partitionHistory;
  }

  /**
   * Method to add a partition to partition history
   * @param partition the vertex had
   */
  public void addToPartitionHistory(int partition) {
    initList();
    this.partitionHistory.add(partition);
  }

  /**
   * Returns the size of the partitionHistory list
   * @return size of partitionhistory list
   */
  public int getPartitionHistoryCount() {
    return (partitionHistory != null) ? partitionHistory.size() : 0;
  }

  /**
   * Initialize the PartitionHistoryList
   */
  private void initList() {
    if (partitionHistory == null) {
      this.partitionHistory = Lists.newArrayList();
    }
  }

  /**
   * Serializes the content of the vertex object.
   *
   * @param dataOutput data to be serialized
   * @throws IOException
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.desiredPartition);
    dataOutput.writeInt(this.currentPartition);
    if (partitionHistory == null || partitionHistory.isEmpty()) {
      dataOutput.writeInt(0);
    } else {
      dataOutput.writeInt(partitionHistory.size());
      for (Integer partitions : partitionHistory) {
        dataOutput.writeInt(partitions);
      }
    }
  }

  /**
   * Deserializes the content of the vertex object.
   *
   * @param dataInput data to be deserialized
   * @throws IOException
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.desiredPartition = dataInput.readInt();
    this.currentPartition = dataInput.readInt();
    final int partitionHistorySize = dataInput.readInt();
    if (partitionHistorySize > 0) {
      initList();
    }
    for (int i = 0; i < partitionHistorySize; i++) {
      partitionHistory.add(dataInput.readInt());
    }
  }
}
