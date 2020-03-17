package home.postgres;

import com.google.common.base.Stopwatch;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.Futures;
import de.bytefish.pgbulkinsert.row.SimpleRow;
import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.HostSpec;

import java.net.Inet4Address;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class LoadingApp {

    private static final String SCHEMA_NAME = "public";
    private static final String TABLE_NAME = "event";
    private static final String[] COLUMN_NAMES = Arrays.stream(Column.values())
            .map(Column::name)
            .collect(Collectors.toList())
            .toArray(new String[0]);
    public static final int BATCH_COUNT = 5;
    public static final int BATCH_SIZE = 2000_000;

    enum Column {
        identity,
        ip_address,
        event_type,
        event_timestamp,
        tracking_id,
    }

    private static final SimpleRowWriter.Table TABLE_DEF = new SimpleRowWriter.Table(SCHEMA_NAME, TABLE_NAME, COLUMN_NAMES);

    private static final ExecutorService exec = Executors.newFixedThreadPool(1);

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("Starting loading app");

        Properties properties = new Properties();
        properties.setProperty("password", System.getenv("DB_PASSWORD"));
        try (PgConnection pgConnection = new PgConnection(
                new HostSpec[]{new HostSpec("localhost", 5435)}, "postgres", "events", properties, null);
        ) {
            perfBenchmark(pgConnection);
        }
    }

    private static void perfBenchmark(PgConnection pgConnection) throws SQLException, ExecutionException, InterruptedException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < BATCH_COUNT; i++) {
           futures.add(exec.submit(() ->  batchInsert(pgConnection)));
//            System.out.printf("Completed batch [%s], current rate is [%s]\n", i,
//                    (i+1) * BATCH_SIZE / stopwatch.elapsed(TimeUnit.SECONDS));
        }

        while (!futures.isEmpty()) {
            Future<?> first = futures.iterator().next();
            first.get();
            futures.remove(first);
            System.out.println("Completed future");
        }
        System.out.println(BATCH_COUNT * BATCH_SIZE / stopwatch.elapsed(TimeUnit.SECONDS));
        exec.shutdown();
    }

    private static void batchInsert(PgConnection pgConnection) {
        try {

            SimpleRowWriter writer = new SimpleRowWriter(TABLE_DEF);
            DataGenerator dataGenerator = new DataGenerator();
            writer.open(pgConnection);
            for (int i = 0; i < BATCH_SIZE; i++) {
                writer.startRow(dataGenerator::populateRow);

            }
            writer.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
