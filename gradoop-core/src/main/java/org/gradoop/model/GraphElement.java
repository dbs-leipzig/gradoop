package org.gradoop.model;

/**
 * A graph element is part of a graph. An element can be part of more than one
 * graph.
 */
public interface GraphElement {
  /**
   * Returns all graphs that element belongs to.
   *
   * @return all graphs of that element
   */
  Iterable<Long> getGraphs();

  /**
   * Adds that element to the given graph. If the element is already an element
   * of the given graph, nothing happens.
   *
   * @param graph the graph to be added to
   */
  void addGraph(Long graph);

  /**
   * Adds graphs to existing graph set. If an element is already element
   * of the given graph, nothing happens.
   *
   * @param graphs the graphs to be added
   */
  void addGraphs(Iterable<Long> graphs);

  /**
   * Resets all graph elements.
   */
  void resetGraphs();

  /**
   * Returns the number of graphs this element belongs to.
   *
   * @return number of graphs containing that element
   */
  int getGraphCount();
}
