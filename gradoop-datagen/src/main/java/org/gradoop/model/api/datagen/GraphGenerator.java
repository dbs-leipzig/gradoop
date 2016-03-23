package org.gradoop.model.api.datagen;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;


public interface GraphGenerator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>{

  LogicalGraph<G, V, E> generate(Integer scaleFactor);

}
