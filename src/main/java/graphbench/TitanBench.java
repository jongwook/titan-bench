package graphbench;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.io.File;

import static java.lang.System.currentTimeMillis;

public class TitanBench {
    public static void main(String[] args) throws Exception {
        File db = new File("db");
        FileUtils.deleteQuietly(db);
        db.mkdirs();

        try (StandardTitanGraph graph = (StandardTitanGraph) GraphFactory.open(ImmutableMap.of(
            "gremlin.graph", "com.thinkaurelius.titan.core.TitanFactory",
            "storage.backend", "berkeleyje",
            "storage.directory", "db"
        ))) {
            long started = currentTimeMillis();

            for (int i = 0; i < 100000; i++) {
                TitanVertex v1 = graph.addVertex("left-" + i);
                TitanVertex v2 = graph.addVertex("right-" + i);
                v1.addEdge("e", v2);
                if (i % 100 == 0) {
                    System.out.printf("Added %d, %.3f seconds\n", i, (currentTimeMillis() - started) / 1000.0);
                }
            }
        }
    }
}
