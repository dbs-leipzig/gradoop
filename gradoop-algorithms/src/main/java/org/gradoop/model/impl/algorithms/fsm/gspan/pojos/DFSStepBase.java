package org.gradoop.model.impl.algorithms.fsm.gspan.pojos;

/**
 * Created by peet on 26.07.16.
 */
public abstract class DFSStepBase implements DFSStep {
  /**
   * discovery time of traversal start vertex
   */
  protected final int fromTime;
  /**
   * label of traversal start vertex
   */
  protected final int fromLabel;
  /**
   * label of the traversed edge
   */
  protected final int edgeLabel;
  /**
   * discovery time of traversal end vertex
   */
  protected final int toTime;
  /**
   * label of traversal end vertex
   */
  protected final int toLabel;

  public DFSStepBase(Integer fromLabel, Integer edgeLabel, Integer toLabel,
    int toTime, int fromTime) {
    this.fromLabel = fromLabel;
    this.edgeLabel = edgeLabel;
    this.toLabel = toLabel;
    this.toTime = toTime;
    this.fromTime = fromTime;
  }

  @Override
  public int getFromTime() {
    return fromTime;
  }

  @Override
  public Integer getFromLabel() {
    return fromLabel;
  }

  @Override
  public Integer getEdgeLabel() {
    return edgeLabel;
  }

  @Override
  public int getToTime() {
    return toTime;
  }

  @Override
  public Integer getToLabel() {
    return toLabel;
  }

  @Override
  public Boolean isLoop() {
    return fromTime == toTime;
  }

  @Override
  public Boolean isForward() {
    return getFromTime() < getToTime();
  }

  @Override
  public Boolean isBackward() {
    return !isForward();
  }

  @Override
  public int getMinVertexLabel() {
    return fromLabel < toLabel ? fromLabel : toLabel;
  }
}
