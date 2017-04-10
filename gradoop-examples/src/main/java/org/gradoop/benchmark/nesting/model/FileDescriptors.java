package org.gradoop.benchmark.nesting.model;

import org.gradoop.benchmark.nesting.serializers.DeserializeGradoopidFromFile;
import org.gradoop.benchmark.nesting.serializers.DeserializePairOfIdsFromFile;

import java.io.IOException;

/**
 * Created by vasistas on 10/04/17.
 */
public class FileDescriptors {

  private final DeserializeGradoopidFromFile headerFileDescriptor;
  private final DeserializePairOfIdsFromFile vertexFileDescriptor;
  private final DeserializePairOfIdsFromFile edgeFileDescriptor;

  public FileDescriptors(DeserializeGradoopidFromFile headerFileDescriptor,
    DeserializePairOfIdsFromFile vertexFileDescriptor,
    DeserializePairOfIdsFromFile edgeFileDescriptor) {
    this.headerFileDescriptor = headerFileDescriptor;
    this.vertexFileDescriptor = vertexFileDescriptor;
    this.edgeFileDescriptor = edgeFileDescriptor;
  }

  public void close() throws IOException {
    headerFileDescriptor.close();
    vertexFileDescriptor.close();
    edgeFileDescriptor.close();
  }

}
