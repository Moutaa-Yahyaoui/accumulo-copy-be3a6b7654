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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.constraints.AlphaNumKeyConstraint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * Extracted batch tests from ConditionalWriterIT.
 */
public class ConditionalWriterBatchIT extends SharedMiniClusterBase {

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

  @Test
  public void testBatch() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      client.securityOperations().changeUserAuthorizations(getAdminPrincipal(),
          new Authorizations("A", "B"));

      ColumnVisibility cvab = new ColumnVisibility("A|B");

      ArrayList<ConditionalMutation> mutations = new ArrayList<>();

      ConditionalMutation cm0 =
          new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvab));
      cm0.put("name", "last", cvab, "doe");
      cm0.put("name", "first", cvab, "john");
      cm0.put("tx", "seq", cvab, "1");
      mutations.add(cm0);

      ConditionalMutation cm1 =
          new ConditionalMutation("59056", new Condition("tx", "seq").setVisibility(cvab));
      cm1.put("name", "last", cvab, "doe");
      cm1.put("name", "first", cvab, "jane");
      cm1.put("tx", "seq", cvab, "1");
      mutations.add(cm1);

      ConditionalMutation cm2 =
          new ConditionalMutation("19059", new Condition("tx", "seq").setVisibility(cvab));
      cm2.put("name", "last", cvab, "doe");
      cm2.put("name", "first", cvab, "jack");
      cm2.put("tx", "seq", cvab, "1");
      mutations.add(cm2);

      try (
          ConditionalWriter cw = client.createConditionalWriter(tableName,
              new ConditionalWriterConfig().setAuthorizations(new Authorizations("A")));
          Scanner scanner = client.createScanner(tableName, new Authorizations("A"))) {
        Iterator<Result> results = cw.write(mutations.iterator());
        int count = 0;
        while (results.hasNext()) {
          Result result = results.next();
          assertEquals(Status.ACCEPTED, result.getStatus());
          count++;
        }

        assertEquals(3, count);

        scanner.fetchColumn("tx", "seq");

        for (String row : new String[] {"99006", "59056", "19059"}) {
          scanner.setRange(new Range(row));
          Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
          assertEquals("1", entry.getValue().toString());
        }

        TreeSet<Text> splits = new TreeSet<>();
        splits.add(new Text("7"));
        splits.add(new Text("3"));
        client.tableOperations().addSplits(tableName, splits);

        mutations.clear();

        ConditionalMutation cm3 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvab).setValue("1"));
        cm3.put("name", "last", cvab, "Doe");
        cm3.put("tx", "seq", cvab, "2");
        mutations.add(cm3);

        ConditionalMutation cm4 =
            new ConditionalMutation("59056", new Condition("tx", "seq").setVisibility(cvab));
        cm4.put("name", "last", cvab, "Doe");
        cm4.put("tx", "seq", cvab, "1");
        mutations.add(cm4);

        ConditionalMutation cm5 = new ConditionalMutation("19059",
            new Condition("tx", "seq").setVisibility(cvab).setValue("2"));
        cm5.put("name", "last", cvab, "Doe");
        cm5.put("tx", "seq", cvab, "3");
        mutations.add(cm5);

        results = cw.write(mutations.iterator());
        int accepted = 0;
        int rejected = 0;
        while (results.hasNext()) {
          Result result = results.next();
          if (new String(result.getMutation().getRow()).equals("99006")) {
            assertEquals(Status.ACCEPTED, result.getStatus());
            accepted++;
          } else {
            assertEquals(Status.REJECTED, result.getStatus());
            rejected++;
          }
        }

        assertEquals("Expected only one accepted conditional mutation", 1, accepted);
        assertEquals("Expected two rejected conditional mutations", 2, rejected);

        for (String row : new String[] {"59056", "19059"}) {
          scanner.setRange(new Range(row));
          Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
          assertEquals("1", entry.getValue().toString());
        }

        scanner.setRange(new Range("99006"));
        Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
        assertEquals("2", entry.getValue().toString());

        scanner.clearColumns();
        scanner.fetchColumn("name", "last");
        entry = Iterables.getOnlyElement(scanner);
        assertEquals("Doe", entry.getValue().toString());
      }
    }
  }

  @Test
  public void testBigBatch() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      client.tableOperations().addSplits(tableName, nss("2", "4", "6"));

      sleepUninterruptibly(2, TimeUnit.SECONDS);

      int num = 100;

      ArrayList<byte[]> rows = new ArrayList<>(num);
      ArrayList<ConditionalMutation> cml = new ArrayList<>(num);

      Random r = new SecureRandom();
      byte[] e = new byte[0];

      for (int i = 0; i < num; i++) {
        rows.add(FastFormat.toZeroPaddedString(abs(r.nextLong()), 16, 16, e));
      }

      for (int i = 0; i < num; i++) {
        ConditionalMutation cm = new ConditionalMutation(rows.get(i), new Condition("meta", "seq"));

        cm.put("meta", "seq", "1");
        cm.put("meta", "tx", UUID.randomUUID().toString());

        cml.add(cm);
      }

      try (ConditionalWriter cw =
          client.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

        Iterator<Result> results = cw.write(cml.iterator());

        int count = 0;

        // TODO check got each row back
        while (results.hasNext()) {
          Result result = results.next();
          assertEquals(Status.ACCEPTED, result.getStatus());
          count++;
        }

        assertEquals("Did not receive the expected number of results", num, count);

        ArrayList<ConditionalMutation> cml2 = new ArrayList<>(num);

        for (int i = 0; i < num; i++) {
          ConditionalMutation cm =
              new ConditionalMutation(rows.get(i), new Condition("meta", "seq").setValue("1"));

          cm.put("meta", "seq", "2");
          cm.put("meta", "tx", UUID.randomUUID().toString());

          cml2.add(cm);
        }

        count = 0;

        results = cw.write(cml2.iterator());

        while (results.hasNext()) {
          Result result = results.next();
          assertEquals(Status.ACCEPTED, result.getStatus());
          count++;
        }

        assertEquals("Did not receive the expected number of results", num, count);
      }
    }
  }

  @Test
  public void testBatchErrors() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      client.tableOperations().addConstraint(tableName, AlphaNumKeyConstraint.class.getName());
      client.tableOperations().clone(tableName, tableName + "_clone", true, new HashMap<>(),
          new HashSet<>());

      client.securityOperations().changeUserAuthorizations(getAdminPrincipal(),
          new Authorizations("A", "B"));

      ColumnVisibility cvaob = new ColumnVisibility("A|B");
      ColumnVisibility cvaab = new ColumnVisibility("A&B");

      switch ((new SecureRandom()).nextInt(3)) {
        case 1:
          client.tableOperations().addSplits(tableName, nss("6"));
          break;
        case 2:
          client.tableOperations().addSplits(tableName, nss("2", "95"));
          break;
      }

      ArrayList<ConditionalMutation> mutations = new ArrayList<>();

      ConditionalMutation cm0 =
          new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvaob));
      cm0.put("name+", "last", cvaob, "doe");
      cm0.put("name", "first", cvaob, "john");
      cm0.put("tx", "seq", cvaob, "1");
      mutations.add(cm0);

      ConditionalMutation cm1 =
          new ConditionalMutation("59056", new Condition("tx", "seq").setVisibility(cvaab));
      cm1.put("name", "last", cvaab, "doe");
      cm1.put("name", "first", cvaab, "jane");
      cm1.put("tx", "seq", cvaab, "1");
      mutations.add(cm1);

      ConditionalMutation cm2 =
          new ConditionalMutation("19059", new Condition("tx", "seq").setVisibility(cvaob));
      cm2.put("name", "last", cvaob, "doe");
      cm2.put("name", "first", cvaob, "jack");
      cm2.put("tx", "seq", cvaob, "1");
      mutations.add(cm2);

      ConditionalMutation cm3 = new ConditionalMutation("90909",
          new Condition("tx", "seq").setVisibility(cvaob).setValue("1"));
      cm3.put("name", "last", cvaob, "doe");
      cm3.put("name", "first", cvaob, "john");
      cm3.put("tx", "seq", cvaob, "2");
      mutations.add(cm3);

      try (
          ConditionalWriter cw = client.createConditionalWriter(tableName,
              new ConditionalWriterConfig().setAuthorizations(new Authorizations("A")));
          Scanner scanner = client.createScanner(tableName, new Authorizations("A"))) {
        Iterator<Result> results = cw.write(mutations.iterator());
        HashSet<String> rows = new HashSet<>();
        while (results.hasNext()) {
          Result result = results.next();
          String row = new String(result.getMutation().getRow());
          if (row.equals("19059")) {
            assertEquals(Status.ACCEPTED, result.getStatus());
          } else if (row.equals("59056")) {
            assertEquals(Status.INVISIBLE_VISIBILITY, result.getStatus());
          } else if (row.equals("99006")) {
            assertEquals(Status.VIOLATED, result.getStatus());
          } else if (row.equals("90909")) {
            assertEquals(Status.REJECTED, result.getStatus());
          }
          rows.add(row);
        }

        assertEquals(4, rows.size());

        scanner.fetchColumn("tx", "seq");
        Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
        assertEquals("1", entry.getValue().toString());
      }
    }
  }

  @Test
  public void testSameRow() throws Exception {
    // test multiple mutations for same row in same batch

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      try (ConditionalWriter cw =
          client.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

        ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());

        ConditionalMutation cm2 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
        cm2.put("tx", "seq", "2");
        cm2.put("data", "x", "b");

        ConditionalMutation cm3 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
        cm3.put("tx", "seq", "2");
        cm3.put("data", "x", "c");

        ConditionalMutation cm4 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
        cm4.put("tx", "seq", "2");
        cm4.put("data", "x", "d");

        Iterator<Result> results = cw.write(Arrays.asList(cm2, cm3, cm4).iterator());

        int accepted = 0;
        int rejected = 0;
        int total = 0;

        while (results.hasNext()) {
          Status status = results.next().getStatus();
          if (status == Status.ACCEPTED)
            accepted++;
          if (status == Status.REJECTED)
            rejected++;
          total++;
        }

        assertEquals("Expected one accepted result", 1, accepted);
        assertEquals("Expected two rejected results", 2, rejected);
        assertEquals("Expected three total results", 3, total);
      }
    }
  }
}
