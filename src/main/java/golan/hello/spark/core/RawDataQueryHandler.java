package golan.hello.spark.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

class RawDataQueryHandler extends AbstractHandler {

    public static final String CONTEXT_PATH = "/query/raw";

    private static final String SELECT_QUERY_TEMPLATE =
            "SELECT minutes, count(1) " +
                    "FROM " + CassandraShared.TABLE_NAME + " " +
                    "WHERE year=%d and month=%d and day=%d and hour=%d " +
                    "AND org_bucket='%s' and project_bucket='%s' " +
                    "AND org_id='%s' and project_id='%s' and environment='%s' " +
                    "GROUP BY org_bucket, project_bucket, year, month, day, hour, org_id, project_id, environment,minutes";



    private final SparkSession              sparkSession;
//    private final CassandraSQLContext       cassandraSQLContext;

    RawDataQueryHandler(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        System.out.println("~~~ Constructor");
        Dataset<Row> table = this.sparkSession.read().format("org.apache.spark.sql.cassandra")
                .options(new HashMap<String, String>() {
                    {
                        put("keyspace", CassandraShared.KEYSPACE);
                        put("table", CassandraShared.TABLE_NAME);
                    }
                }).load();
        table.createOrReplaceTempView(CassandraShared.TABLE_NAME);
    }

    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) throws IOException {

        final long a = System.nanoTime();
        response.setContentType("text/html; charset=utf-8");

        /*

        Integer year = getOrNull(request.getParameter("year"), null);
        Integer month = Integer.parseInt(request.getParameter("month"));
        Integer day = Integer.parseInt(request.getParameter("day"));
        Integer hour = Integer.parseInt(request.getParameter("hour"));
        String org_id = request.getParameter("org_id");
        String org_bucket = "user_bucket";
        String project_id = request.getParameter("project_id");
        String project_bucket = "project_bucket";
        String environment = request.getParameter("environment");

        final String sql = String.format(SELECT_QUERY_TEMPLATE,
                year, month, day, hour,
                org_bucket, project_bucket,
                org_id, project_id, environment);


         */

        final String sql = buildQuery(request);

        final PrintWriter out = response.getWriter();


        System.out.println("~~~ " + sql);



        final Dataset<Row> result = sparkSession.sql(sql);
        final long count = result.count();



        final long b = System.nanoTime();

        out.println("~~~ query=["+request.getQueryString()+"] sql=["+sql+"] count=["+count+"] time=["+nano2seconds(b-a)+"]");




//        final CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions
//                .cassandraTable(KEYSPACE, TABLE_NAME)
//                .where("year=2018 and month=1 and day=27 and hour=12  and  org_bucket='org_bucket' and project_bucket='project_bucket'");

        response.addHeader("Count", ""+ count);
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
    }

    static String buildQuery(ServletRequest request) {

        final StringBuilder sb = new StringBuilder();

        final String year = request.getParameter("year");
        if (year!=null && !year.isEmpty()) {
            if (sb.length()>0) sb.append(" and ");
            sb.append("year=").append(year);
        }

        final String month = request.getParameter("month");
        if (month!=null && !month.isEmpty()) {
            if (sb.length()>0) sb.append(" and ");
            sb.append("month=").append(month);
        }

        final String day = request.getParameter("day");
        if (day!=null && !day.isEmpty()) {
            if (sb.length()>0) sb.append(" and ");
            sb.append("day=").append(day);
        }

        final StringBuilder select = new StringBuilder("SELECT minutes, count(1) FROM " + CassandraShared.TABLE_NAME);
        if (sb.length()>0) {
            select.append(" WHERE ").append(sb);
        }
        select.append(" GROUP BY year, month, day, hour,minutes");

        return select.toString();
    }

    private static double nano2seconds(long a2b) {
        return Math.round(a2b/1000_000_000.0*100)/100.0;    //convert to sec and round to 2 digits after the decimal point
    }


    void close() {
        try {
            System.out.println("~~~ Close");
            this.sparkSession.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
