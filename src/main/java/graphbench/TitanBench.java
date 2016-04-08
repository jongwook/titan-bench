package graphbench;

import com.google.common.collect.ImmutableMap;
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
                "storage.hostname", "127.0.0.1",
                "storage.directory", "db"
        ))) {
            long started = currentTimeMillis();

            for (int i = 0; i < 100000; i++) {
                graph.addVertex("vertex-" + i);
                if (i % 100 == 0) {
                    System.out.printf("Added %d, %.3f seconds\n", i, (currentTimeMillis() - started) / 1000.0);
                }
            }
        }
    }
}
