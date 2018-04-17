package golan.hello.spark.core;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;

public class SparkHttpQueryEngine {

    private static final int    HTTP_PORT      = 8888;

    private final SparkSession sparkSession;
    private final SparkContextJavaFunctions javaFunctions;

    public static void main(String[] args) throws Exception {
        new SparkHttpQueryEngine().start();

    }

    private void start() throws Exception {
        System.out.println("main - BEGIN");
        final Server server = new Server(HTTP_PORT);
        final RawDataQueryHandler handler = createRawDataHandler(server);
        System.out.println("starting...");
        server.start();
        System.out.println("joining...");
        server.join();
        System.out.println("main - END");
        handler.close();
    }

    private RawDataQueryHandler createRawDataHandler(Server server) {
        final ContextHandler context = new ContextHandler();
        context.setContextPath(RawDataQueryHandler.CONTEXT_PATH);
        context.setResourceBase(".");
        context.setClassLoader(Thread.currentThread().getContextClassLoader());
        server.setHandler(context);
        final RawDataQueryHandler handler = new RawDataQueryHandler(sparkSession);
        context.setHandler(handler);
        return handler;
    }

    private SparkHttpQueryEngine() {
        this.sparkSession = SparkSession
                .builder()
                .appName(SparkHttpQueryEngine.class.getSimpleName())
                .config("spark.cassandra.connection.host", CassandraShared.CASSANDRA_HOST)
                .config("spark.cassandra.connection.port", CassandraShared.CASSANDRA_PORT)
                .master("local[*]")
                .getOrCreate();

        this.javaFunctions = CassandraJavaUtil.javaFunctions(sparkSession.sparkContext());

    }
}
