package org.gradoop.model.api.datagen;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;

public interface GraphCollectionGenerator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>{

  GraphCollection<G, V, E> generate(Integer scaleFactor);
}
