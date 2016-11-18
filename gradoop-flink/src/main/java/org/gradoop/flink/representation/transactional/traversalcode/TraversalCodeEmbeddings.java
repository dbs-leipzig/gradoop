package org.gradoop.flink.representation.transactional.traversalcode;

import java.util.Collection;

public class TraversalCodeEmbeddings<C extends Comparable<C>> {

  private final TraversalCode<C> code;
  private final Collection<TraversalEmbedding> embeddings;


  public TraversalCodeEmbeddings(TraversalCode<C> code, Collection<TraversalEmbedding> embeddings) {
    this.code = code;
    this.embeddings = embeddings;
  }

  public TraversalCode<C> getCode() {
    return code;
  }

  public Collection<TraversalEmbedding> getEmbeddings() {
    return embeddings;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TraversalCodeEmbeddings<?> that = (TraversalCodeEmbeddings<?>) o;

    if (!code.equals(that.code)) {
      return false;
    }
    return embeddings.equals(that.embeddings);

  }

  @Override
  public int hashCode() {
    int result = code.hashCode();
    result = 31 * result + embeddings.hashCode();
    return result;
  }
}
