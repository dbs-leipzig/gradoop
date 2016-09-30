package org.gradoop.benchmark.fsm;

/**
 * Transactional FSM benchmark parameters.
 */
public enum TFSMParam {
  /**
   * Flink parallelism
   */
  p,
  /**
   * Flink program class
   */
  c,
  /**
   * input file path
   */
  i,
  /**
   * log file pth
   */
  l,
  /**
   * minimum support
   */
  m,
  /**
   * directed mode (true/false)
   */
  d,
  /**
   * enable preprocessing (true/false)
   */
  r,
  /**
   * canonical label
   */
  n,
  /**
   * filter strategy
   */
  f,
  /**
   * growth strategy
   */
  g,
  /**
   * iteration strategy
   */
  t
}
