package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

/**
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils}
 * adapted to temporal Embeddings
 */
public class JoinTestUtil {

    /**
     * Checks if the given data set contains at least one embedding that matches the given path.
     *
     * @param embeddings data set containing embedding
     * @param path expected path
     * @throws Exception on failure
     */
    public static void assertEmbeddingTPGMExists(DataSet<EmbeddingTPGM> embeddings, GradoopId... path)
            throws Exception {
        List<GradoopId> pathList = Lists.newArrayList(path);
        assertTrue(embeddings.collect().stream()
                .anyMatch(embedding -> pathList.equals(embeddingToIdList(embedding)))
        );
    }

    /**
     * Applies a consumer (e.g. containing an assertion) to each embedding in the given data set.
     *
     * @param dataSet data set containing embeddings
     * @param consumer consumer
     * @throws Exception on failure
     */
    public static void assertEveryEmbeddingTPGM(DataSet<EmbeddingTPGM> dataSet, Consumer<EmbeddingTPGM> consumer)
            throws Exception {
        dataSet.collect().forEach(consumer);
    }

    /**
     * Converts the given embedding to a list of identifiers.
     *
     * @param embedding embedding
     * @return id list
     */
    public static List<GradoopId> embeddingToIdList(EmbeddingTPGM embedding) {
        List<GradoopId> idList = new ArrayList<>();
        IntStream.range(0, embedding.size()).forEach(i -> idList.addAll(embedding.getIdAsList(i)));
        return idList;
    }

    /**
     * Creates an embedding from the given list of identifiers. The order of the identifiers
     * determines the order in the embedding.
     *
     * @param ids identifies to be contained in the embedding
     * @return embedding containing the specified ids
     */
    public static EmbeddingTPGM createEmbeddingTPGM(GradoopId... ids) {
        EmbeddingTPGM embedding = new EmbeddingTPGM();
        Arrays.stream(ids).forEach(embedding::add);
        return embedding;
    }
}
