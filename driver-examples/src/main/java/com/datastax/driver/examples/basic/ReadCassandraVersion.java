/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.examples.basic;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connects to a Cassandra cluster and extracts basic information from it.
 *
 * <p>Preconditions: - a Cassandra cluster is running and accessible through the contacts points
 * identified by CONTACT_POINTS and PORT.
 *
 * <p>Side effects: none.
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class ReadCassandraVersion {

  static String[] CONTACT_POINTS = {"127.0.0.1"};
  static int PORT = 9042;

  public static void main(String[] args) {

    Executor countdown = Executors.newSingleThreadExecutor();
    try {
      // The Cluster object is the main entry point of the driver.
      // It holds the known state of the actual Cassandra cluster (notably the Metadata).
      // This class is thread-safe, you should create a single instance (per target Cassandra
      // cluster), and share
      // it throughout your application.
      final Cluster cluster =
          Cluster.builder()
                 .addContactPoints(CONTACT_POINTS)
                 .withPort(PORT)
//                 .withAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
                 .withCompression(ProtocolOptions.Compression.LZ4)
//                 .withProtocolVersion(ProtocolVersion.V4)
                 .allowBetaProtocolVersion()
              .build();

      // The Session is what you use to execute queries. Likewise, it is thread-safe and should be
      // reused.
      final Session session = cluster.connect();
//      if (true) System.exit(0);
      long start = System.nanoTime();
      // We use execute to send a query to Cassandra. This returns a ResultSet, which is essentially
      // a collection
      // of Row objects.
      ResultSet rs = session.execute("select release_version from system.local");
      //  Extract the first row (which is the only one in this case).
      Row row = rs.one();
      // Extract the value of the first (and only) column from the row.
      String releaseVersion = row.getString("release_version");
      System.out.printf("Cassandra version is: %s%n", releaseVersion);
      System.out.printf(
          "CQL Protocol version is: %s%n",
          cluster.getConfiguration().getProtocolOptions().getProtocolVersion());
      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
      session.execute(
          "CREATE TABLE IF NOT EXISTS test_ks.small_test (k int, c int, v int, PRIMARY KEY(k, c))");
      int partitions = 100;
      int rowsPerPartition = 100;
      List<ResultSetFuture> pending = new ArrayList<>();
      AtomicInteger inflight = new AtomicInteger(0);
      final AtomicInteger sleepCount = new AtomicInteger(0);
      int sleepTime = 2;
      for (int i = 0; i < partitions; i++) {
        for (int j = 0; j < rowsPerPartition; j++) {
          ResultSetFuture result =
              session.executeAsync(
                  String.format("INSERT INTO test_ks.small_test(k, c, v) VALUES (%s, %s, %s)", i, j, j));
          result.addListener(inflight::decrementAndGet, countdown);
          pending.add(result);
          if (inflight.incrementAndGet() >= 500) {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
            sleepCount.incrementAndGet();
          }
        }
      }
      ListenableFuture<List<ResultSet>> futures = Futures.allAsList(pending);
      futures.addListener(
          () -> {
            int sleeps = sleepCount.get();
            System.out.printf(
                "Done with %d inserts in %d ms (sleeping %d times for %d ms total) %n",
                partitions * rowsPerPartition,
                TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS),
                sleeps,
                sleeps * sleepTime);
            System.out.println("SELECT ONE ROW");
            printRow(session.execute("SELECT * FROM test_ks.small_test WHERE k=0").one());
            System.out.println("SELECT RANGE LIMIT 10");
            ResultSet rs1 = session.execute("SELECT * FROM test_ks.small_test LIMIT 10");
            rs1.forEach(ReadCassandraVersion::printRow);
          },
          countdown);
      System.out.println("EXECUTING PARALLEL SELECTS");
      inflight.set(0);
      sleepCount.set(0);
      pending.clear();
      long readStart = System.nanoTime();
      AtomicInteger cnt = new AtomicInteger(0);
      for (int i = 0; i < partitions; i++) {
        for (int j = 0; j < rowsPerPartition; j++) {
          ResultSetFuture result =
              session.executeAsync("SELECT * FROM test_ks.small_test WHERE k = ? AND c = ?", i, j);
          result.addListener(
              () -> {
                inflight.decrementAndGet();
                if (cnt.incrementAndGet() % 1000 == 0) {
                  System.out.printf("ROW SELECT %d: ", cnt.get());
                  printRow(result.getUninterruptibly().one());
                }
              },
              countdown);
          pending.add(result);
          if (inflight.incrementAndGet() >= 500) {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
            sleepCount.incrementAndGet();
          }
        }
      }
      futures = Futures.allAsList(pending);
      futures.addListener(
          () -> {
            int sleeps = sleepCount.get();
            System.out.printf(
                "Done with %d row selects in %d ms (sleeping %d times for %d ms total) %n",
                partitions * rowsPerPartition,
                TimeUnit.MILLISECONDS.convert(System.nanoTime() - readStart, TimeUnit.NANOSECONDS),
                sleeps,
                sleeps * sleepTime);

            System.out.println("AGGREGATION QUERY");
            ResultSet rs2 = session.execute("SELECT count(*) FROM test_ks.small_test");
            System.out.println("AGGREGATION RESULT: " + rs2.one().getLong("count"));
          },
          countdown);

      session.execute(
          "CREATE TABLE IF NOT EXISTS test_ks.large_test (k int, c int, v text, PRIMARY KEY(k, c))");
      StringBuilder builder = new StringBuilder();
      final int FRAME_SIZE_THRESHOLD = 1024 * 128; // kb
      for (int i = 0; i < (FRAME_SIZE_THRESHOLD * 2) + 1024; i++) builder.append('a');
      final String s = builder.toString();
      System.out.println("EXECUTING MULTI-FRAME INSERT");
      session.execute("INSERT INTO test_ks.large_test(k, c, v) VALUES (0, 0, ?)", s);

      System.out.println("EXECUTING MULTI-FRAME SELECT");
      ResultSetFuture result =
          session.executeAsync("SELECT * FROM test_ks.large_test WHERE k = 0 AND c = 0");
      result.addListener(
          () -> {
            Row r1 = result.getUninterruptibly().one();
            long finish = System.nanoTime();
            printRow(r1);
            String val = r1.getString("v");
            assert val.length() == s.length();
            System.out.printf(
                "Finished in %d ms%n",
                TimeUnit.MILLISECONDS.convert(finish - start, TimeUnit.NANOSECONDS));
            session.close();
            cluster.close();
            System.out.println("DONE");
          },
          countdown);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Close the cluster after we’re done with it. This will also close any session that was
      // created from this
      // cluster.
      // This step is important because it frees underlying resources (TCP connections, thread
      // pools...). In a
      // real application, you would typically do this at shutdown (for example, when undeploying
      // your webapp).
      //      if(session != null) session.close();
      //      if (cluster != null) cluster.close();
      //      System.out.println("CLOSED");

    }
    // The try-with-resources block automatically close the session after we’re done with it.
    // This step is important because it frees underlying resources (TCP connections, thread
    // pools...). In a real application, you would typically do this at shutdown
    // (for example, when undeploying your webapp).
  }

  private static void printRow(Row row) {
    assert row != null;
    System.out.printf("(%d, %d, %s)%n", row.getInt("k"), row.getInt("c"), row.getObject("v"));
  }
}
