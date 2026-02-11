/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracted concurrency tests from ConditionalWriterIT.
 */
public class ConditionalWriterConcurrencyIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(ConditionalWriterConcurrencyIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new Callback());
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static class Callback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // Set the min span to 0 so we will definitely get all the traces back. See ACCUMULO-4365
      Map<String,String> siteConf = cfg.getSiteConfig();
      siteConf.put(Property.TRACE_SPAN_RECEIVER_PREFIX.getKey() + "tracer.span.min.ms", "0");
      cfg.setSiteConfig(siteConf);
    }
  }

  public static long abs(long l) {
    l = Math.abs(l); // abs(Long.MIN_VALUE) == Long.MIN_VALUE...
    if (l < 0)
      return 0;
    return l;
  }

  private SortedSet<Text> nss(String... splits) {
    TreeSet<Text> ret = new TreeSet<>();
    for (String split : splits)
      ret.add(new Text(split));

    return ret;
  }

  private static class Stats {

    ByteSequence row = null;
    int seq;
    long sum;
    int[] data = new int[10];

    public Stats(Iterator<Entry<Key,Value>> iterator) {
      while (iterator.hasNext()) {
        Entry<Key,Value> entry = iterator.next();

        if (row == null)
          row = entry.getKey().getRowData();

        String cf = entry.getKey().getColumnFamilyData().toString();
        String cq = entry.getKey().getColumnQualifierData().toString();

        if (cf.equals("data")) {
          data[Integer.parseInt(cq)] = Integer.parseInt(entry.getValue().toString());
        } else if (cf.equals("meta")) {
          if (cq.equals("sum")) {
            sum = Long.parseLong(entry.getValue().toString());
          } else if (cq.equals("seq")) {
            seq = Integer.parseInt(entry.getValue().toString());
          }
        }
      }

      long sum2 = 0;

      for (int datum : data) {
        sum2 += datum;
      }

      assertEquals(sum2, sum);
    }

    public Stats(ByteSequence row) {
      this.row = row;
      for (int i = 0; i < data.length; i++) {
        this.data[i] = 0;
      }
      this.seq = -1;
      this.sum = 0;
    }

    void set(int index, int value) {
      sum -= data[index];
      sum += value;
      data[index] = value;
    }

    ConditionalMutation toMutation() {
      Condition cond = new Condition("meta", "seq");
      if (seq >= 0)
        cond.setValue(seq + "");

      ConditionalMutation cm = new ConditionalMutation(row, cond);

      cm.put("meta", "seq", (seq + 1) + "");
      cm.put("meta", "sum", (sum) + "");

      for (int i = 0; i < data.length; i++) {
        cm.put("data", i + "", data[i] + "");
      }

      return cm;
    }

    @Override
    public String toString() {
      return row + " " + seq + " " + sum;
    }
  }

  private static class MutatorTask implements Runnable {
    String tableName;
    ArrayList<ByteSequence> rows;
    ConditionalWriter cw;
    AccumuloClient client;
    AtomicBoolean failed;

    public MutatorTask(String tableName, AccumuloClient client, ArrayList<ByteSequence> rows,
        ConditionalWriter cw, AtomicBoolean failed) {
      this.tableName = tableName;
      this.rows = rows;
      this.client = client;
      this.cw = cw;
      this.failed = failed;
    }

    @Override
    public void run() {
      try (Scanner scanner =
          new IsolatedScanner(client.createScanner(tableName, Authorizations.EMPTY))) {
        Random rand = new SecureRandom();

        for (int i = 0; i < 20; i++) {
          int numRows = rand.nextInt(10) + 1;

          ArrayList<ByteSequence> changes = new ArrayList<>(numRows);
          ArrayList<ConditionalMutation> mutations = new ArrayList<>();

          for (int j = 0; j < numRows; j++)
            changes.add(rows.get(rand.nextInt(rows.size())));

          for (ByteSequence row : changes) {
            scanner.setRange(new Range(row.toString()));
            Stats stats = new Stats(scanner.iterator());
            stats.set(rand.nextInt(10), rand.nextInt(Integer.MAX_VALUE));
            mutations.add(stats.toMutation());
          }

          ArrayList<ByteSequence> changed = new ArrayList<>(numRows);
          Iterator<Result> results = cw.write(mutations.iterator());
          while (results.hasNext()) {
            Result result = results.next();
            changed.add(new ArrayByteSequence(result.getMutation().getRow()));
          }

          Collections.sort(changes);
          Collections.sort(changed);

          assertEquals(changes, changed);
        }
      } catch (Exception e) {
        log.error("{}", e.getMessage(), e);
        failed.set(true);
      }
    }
  }

  @Test
  public void testThreads() throws Exception {
    // test multiple threads using a single conditional writer

    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(tableName);

      Random rand = new SecureRandom();

      switch (rand.nextInt(3)) {
        case 1:
          client.tableOperations().addSplits(tableName, nss("4"));
          break;
        case 2:
          client.tableOperations().addSplits(tableName, nss("3", "5"));
          break;
      }

      try (ConditionalWriter cw =
          client.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

        ArrayList<ByteSequence> rows = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
          rows.add(new ArrayByteSequence(
              FastFormat.toZeroPaddedString(abs(rand.nextLong()), 16, 16, new byte[0])));
        }

        ArrayList<ConditionalMutation> mutations = new ArrayList<>();

        for (ByteSequence row : rows)
          mutations.add(new Stats(row).toMutation());

        ArrayList<ByteSequence> rows2 = new ArrayList<>();
        Iterator<Result> results = cw.write(mutations.iterator());
        while (results.hasNext()) {
          Result result = results.next();
          assertEquals(Status.ACCEPTED, result.getStatus());
          rows2.add(new ArrayByteSequence(result.getMutation().getRow()));
        }

        Collections.sort(rows);
        Collections.sort(rows2);

        assertEquals(rows, rows2);

        AtomicBoolean failed = new AtomicBoolean(false);

        ExecutorService tp = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
          tp.submit(new MutatorTask(tableName, client, rows, cw, failed));
        }

        tp.shutdown();

        while (!tp.isTerminated()) {
          tp.awaitTermination(1, TimeUnit.MINUTES);
        }

        assertFalse("A MutatorTask failed with an exception", failed.get());
      }

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        RowIterator rowIter = new RowIterator(scanner);

        while (rowIter.hasNext()) {
          Iterator<Entry<Key,Value>> row = rowIter.next();
          new Stats(row);
        }
      }
    }
  }
}
