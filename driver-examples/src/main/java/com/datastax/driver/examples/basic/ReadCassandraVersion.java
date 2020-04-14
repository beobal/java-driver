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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.*;

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

  public static void main(String[] args)
  {

    Cluster cluster = null;
    Session session = null;
    Executor countdown = Executors.newSingleThreadExecutor();
    try
    {
      // The Cluster object is the main entry point of the driver.
      // It holds the known state of the actual Cassandra cluster (notably the Metadata).
      // This class is thread-safe, you should create a single instance (per target Cassandra
      // cluster), and share
      // it throughout your application.
      cluster = Cluster.builder()
                       .addContactPoints(CONTACT_POINTS)
                       .withPort(PORT)
//                       .withProtocolVersion(ProtocolVersion.V4)
                       .allowBetaProtocolVersion()
                       .build();

      // The Session is what you use to execute queries. Likewise, it is thread-safe and should be
      // reused.
      session = cluster.connect();
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
      System.out.printf("CQL Protocol version is: %s%n", cluster.getConfiguration().getProtocolOptions().getProtocolVersion());

      session.execute("CREATE KEYSPACE IF NOT EXISTS sanity WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
      session.execute("CREATE TABLE IF NOT EXISTS sanity.t1 (k int, c int, v int, PRIMARY KEY(k, c))");
      int partitions = 100;
      int rowsPerPartition = 100;
      List<ResultSetFuture> pending = new ArrayList<>();
      AtomicInteger inflight = new AtomicInteger(0);
      final AtomicInteger sleepCount = new AtomicInteger(0);
      int sleepTime = 2;
      for (int i = 0; i < partitions; i++)
      {
        for (int j = 0; j < rowsPerPartition; j++)
        {
            ResultSetFuture result = session.executeAsync(String.format("INSERT INTO sanity.t1(k, c, v) VALUES (%s, %s, %s)", i, j, j));
            result.addListener(() -> inflight.decrementAndGet(), countdown);
            pending.add(result);
            if (inflight.incrementAndGet() >= 1000)
            {
              TimeUnit.MILLISECONDS.sleep(sleepTime);
              sleepCount.incrementAndGet();
            }
        }
      }
      ListenableFuture<List<ResultSet>> futures = Futures.allAsList(pending);
      futures.addListener(() -> {
          int sleeps = sleepCount.get();
          System.out.printf("Done with %d inserts (sleeping %d times for %d ms total) %n",
                            pending.size(),
                            sleeps,
                            sleeps * sleepTime);
        }, countdown);
      printRow(session.execute("SELECT * FROM sanity.t1 WHERE k=0").one());
      rs = session.execute("SELECT * FROM sanity.t1 LIMIT 10");
      rs.forEach(ReadCassandraVersion::printRow);

      rs = session.execute("SELECT count(*) FROM sanity.t1");
      System.out.println(rs.one().getLong("count"));
      System.out.printf("Finished in %d ms",
                        TimeUnit.MILLISECONDS.convert(System.nanoTime() - start,
                                                      TimeUnit.NANOSECONDS));
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      // Close the cluster after we’re done with it. This will also close any session that was
      // created from this
      // cluster.
      // This step is important because it frees underlying resources (TCP connections, thread
      // pools...). In a
      // real application, you would typically do this at shutdown (for example, when undeploying
      // your webapp).
      System.out.println("DONE");
      if(session != null) session.close();
      if (cluster != null) cluster.close();
      System.out.println("CLOSED");

    }
    // The try-with-resources block automatically close the session after we’re done with it.
    // This step is important because it frees underlying resources (TCP connections, thread
    // pools...). In a real application, you would typically do this at shutdown
    // (for example, when undeploying your webapp).
  }

  private static void printRow(Row row)
  {
    assert row != null;
    System.out.printf("(%d, %d, %d)%n", row.getInt("k"), row.getInt("c"), row.getInt("v"));
  }
}
