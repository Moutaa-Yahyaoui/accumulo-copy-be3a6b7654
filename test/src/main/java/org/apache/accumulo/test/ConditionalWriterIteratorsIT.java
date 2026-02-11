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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.constraints.AlphaNumKeyConstraint;
import org.apache.accumulo.test.functional.BadIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * Extracted iterator and constraint tests from ConditionalWriterIT.
 */
public class ConditionalWriterIteratorsIT extends SharedMiniClusterBase {

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

  @Test
  public void testConstraints() throws Exception {
    // ensure constraint violations are properly reported

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      client.tableOperations().addConstraint(tableName, AlphaNumKeyConstraint.class.getName());
      client.tableOperations().clone(tableName, tableName + "_clone", true, new HashMap<>(),
          new HashSet<>());

      try (
          ConditionalWriter cw =
              client.createConditionalWriter(tableName + "_clone", new ConditionalWriterConfig());
          Scanner scanner = client.createScanner(tableName + "_clone", new Authorizations())) {

        ConditionalMutation cm0 = new ConditionalMutation("99006+", new Condition("tx", "seq"));
        cm0.put("tx", "seq", "1");

        assertEquals(Status.VIOLATED, cw.write(cm0).getStatus());
        assertFalse("Should find no results in the table is mutation result was violated",
            scanner.iterator().hasNext());

        ConditionalMutation cm1 = new ConditionalMutation("99006", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");

        assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
        assertTrue("Accepted result should be returned when reading table",
            scanner.iterator().hasNext());
      }
    }
  }

  @Test
  public void testIterators() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName,
          new NewTableConfiguration().withoutDefaultIterators());

      try (BatchWriter bw = client.createBatchWriter(tableName)) {

        Mutation m = new Mutation("ACCUMULO-1000");
        m.put("count", "comments", "1");
        bw.addMutation(m);
        bw.addMutation(m);
        bw.addMutation(m);

        m = new Mutation("ACCUMULO-1001");
        m.put("count2", "comments", "1");
        bw.addMutation(m);
        bw.addMutation(m);

        m = new Mutation("ACCUMULO-1002");
        m.put("count2", "comments", "1");
        bw.addMutation(m);
        bw.addMutation(m);
      }

      IteratorSetting iterConfig = new IteratorSetting(10, SummingCombiner.class);
      SummingCombiner.setEncodingType(iterConfig, Type.STRING);
      SummingCombiner.setColumns(iterConfig,
          Collections.singletonList(new IteratorSetting.Column("count")));

      IteratorSetting iterConfig2 = new IteratorSetting(10, SummingCombiner.class);
      SummingCombiner.setEncodingType(iterConfig2, Type.STRING);
      SummingCombiner.setColumns(iterConfig2,
          Collections.singletonList(new IteratorSetting.Column("count2", "comments")));

      IteratorSetting iterConfig3 = new IteratorSetting(5, VersioningIterator.class);
      VersioningIterator.setMaxVersions(iterConfig3, 1);

      try (Scanner scanner = client.createScanner(tableName, new Authorizations())) {
        scanner.addScanIterator(iterConfig);
        scanner.setRange(new Range("ACCUMULO-1000"));
        scanner.fetchColumn("count", "comments");

        Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
        assertEquals("3", entry.getValue().toString());

        try (ConditionalWriter cw =
            client.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

          ConditionalMutation cm0 = new ConditionalMutation("ACCUMULO-1000",
              new Condition("count", "comments").setValue("3"));
          cm0.put("count", "comments", "1");
          assertEquals(Status.REJECTED, cw.write(cm0).getStatus());
          entry = Iterables.getOnlyElement(scanner);
          assertEquals("3", entry.getValue().toString());

          ConditionalMutation cm1 = new ConditionalMutation("ACCUMULO-1000",
              new Condition("count", "comments").setIterators(iterConfig).setValue("3"));
          cm1.put("count", "comments", "1");
          assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
          entry = Iterables.getOnlyElement(scanner);
          assertEquals("4", entry.getValue().toString());

          ConditionalMutation cm2 = new ConditionalMutation("ACCUMULO-1000",
              new Condition("count", "comments").setValue("4"));
          cm2.put("count", "comments", "1");
          assertEquals(Status.REJECTED, cw.write(cm1).getStatus());
          entry = Iterables.getOnlyElement(scanner);
          assertEquals("4", entry.getValue().toString());

          // run test with multiple iterators passed in same batch and condition with two iterators

          ConditionalMutation cm3 = new ConditionalMutation("ACCUMULO-1000",
              new Condition("count", "comments").setIterators(iterConfig).setValue("4"));
          cm3.put("count", "comments", "1");

          ConditionalMutation cm4 = new ConditionalMutation("ACCUMULO-1001",
              new Condition("count2", "comments").setIterators(iterConfig2).setValue("2"));
          cm4.put("count2", "comments", "1");

          ConditionalMutation cm5 =
              new ConditionalMutation("ACCUMULO-1002", new Condition("count2", "comments")
                  .setIterators(iterConfig2, iterConfig3).setValue("2"));
          cm5.put("count2", "comments", "1");

          Iterator<Result> results = cw.write(Arrays.asList(cm3, cm4, cm5).iterator());
          Map<String,Status> actual = new HashMap<>();

          while (results.hasNext()) {
            Result result = results.next();
            String k = new String(result.getMutation().getRow());
            assertFalse("Did not expect to see multiple resultus for the row: " + k,
                actual.containsKey(k));
            actual.put(k, result.getStatus());
          }

          Map<String,Status> expected = new HashMap<>();
          expected.put("ACCUMULO-1000", Status.ACCEPTED);
          expected.put("ACCUMULO-1001", Status.ACCEPTED);
          expected.put("ACCUMULO-1002", Status.REJECTED);

          assertEquals(expected, actual);
        }
      }
    }
  }

  public static class AddingIterator extends WrappingIterator {
    long amount = 0;

    @Override
    public Value getTopValue() {
      Value val = super.getTopValue();
      long l = Long.parseLong(val.toString());
      String newVal = (l + amount) + "";
      return new Value(newVal.getBytes(UTF_8));
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) {
      this.setSource(source);
      amount = Long.parseLong(options.get("amount"));
    }
  }

  public static class MultiplyingIterator extends WrappingIterator {
    long amount = 0;

    @Override
    public Value getTopValue() {
      Value val = super.getTopValue();
      long l = Long.parseLong(val.toString());
      String newVal = l * amount + "";
      return new Value(newVal.getBytes(UTF_8));
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) {
      this.setSource(source);
      amount = Long.parseLong(options.get("amount"));
    }
  }

  @Test
  public void testTableAndConditionIterators() throws Exception {

    // test w/ table that has iterators configured
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      IteratorSetting aiConfig1 = new IteratorSetting(30, "AI1", AddingIterator.class);
      aiConfig1.addOption("amount", "2");
      IteratorSetting aiConfig2 = new IteratorSetting(35, "MI1", MultiplyingIterator.class);
      aiConfig2.addOption("amount", "3");
      IteratorSetting aiConfig3 = new IteratorSetting(40, "AI2", AddingIterator.class);
      aiConfig3.addOption("amount", "5");

      client.tableOperations().create(tableName);

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        Mutation m = new Mutation("ACCUMULO-1000");
        m.put("count", "comments", "6");
        bw.addMutation(m);

        m = new Mutation("ACCUMULO-1001");
        m.put("count", "comments", "7");
        bw.addMutation(m);

        m = new Mutation("ACCUMULO-1002");
        m.put("count", "comments", "8");
        bw.addMutation(m);
      }

      client.tableOperations().attachIterator(tableName, aiConfig1, EnumSet.of(IteratorScope.scan));
      client.tableOperations().offline(tableName, true);
      client.tableOperations().online(tableName, true);

      try (
          ConditionalWriter cw =
              client.createConditionalWriter(tableName, new ConditionalWriterConfig());
          Scanner scanner = client.createScanner(tableName, new Authorizations())) {

        ConditionalMutation cm6 = new ConditionalMutation("ACCUMULO-1000",
            new Condition("count", "comments").setValue("8"));
        cm6.put("count", "comments", "7");
        assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

        scanner.setRange(new Range("ACCUMULO-1000"));
        scanner.fetchColumn("count", "comments");

        Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
        assertEquals("9", entry.getValue().toString());

        ConditionalMutation cm7 = new ConditionalMutation("ACCUMULO-1000",
            new Condition("count", "comments").setIterators(aiConfig2).setValue("27"));
        cm7.put("count", "comments", "8");
        assertEquals(Status.ACCEPTED, cw.write(cm7).getStatus());

        entry = Iterables.getOnlyElement(scanner);
        assertEquals("10", entry.getValue().toString());

        ConditionalMutation cm8 = new ConditionalMutation("ACCUMULO-1000",
            new Condition("count", "comments").setIterators(aiConfig2, aiConfig3).setValue("35"));
        cm8.put("count", "comments", "9");
        assertEquals(Status.ACCEPTED, cw.write(cm8).getStatus());

        entry = Iterables.getOnlyElement(scanner);
        assertEquals("11", entry.getValue().toString());

        ConditionalMutation cm3 = new ConditionalMutation("ACCUMULO-1000",
            new Condition("count", "comments").setIterators(aiConfig2).setValue("33"));
        cm3.put("count", "comments", "3");

        ConditionalMutation cm4 = new ConditionalMutation("ACCUMULO-1001",
            new Condition("count", "comments").setIterators(aiConfig3).setValue("14"));
        cm4.put("count", "comments", "3");

        ConditionalMutation cm5 = new ConditionalMutation("ACCUMULO-1002",
            new Condition("count", "comments").setIterators(aiConfig3).setValue("10"));
        cm5.put("count", "comments", "3");

        Iterator<Result> results = cw.write(Arrays.asList(cm3, cm4, cm5).iterator());
        Map<String,Status> actual = new HashMap<>();

        while (results.hasNext()) {
          Result result = results.next();
          String k = new String(result.getMutation().getRow());
          assertFalse("Did not expect to see multiple resultus for the row: " + k,
              actual.containsKey(k));
          actual.put(k, result.getStatus());
        }

        Map<String,Status> expected = new HashMap<>();
        expected.put("ACCUMULO-1000", Status.ACCEPTED);
        expected.put("ACCUMULO-1001", Status.ACCEPTED);
        expected.put("ACCUMULO-1002", Status.REJECTED);
        assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void testError() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      try (ConditionalWriter cw =
          client.createConditionalWriter(table, new ConditionalWriterConfig())) {

        IteratorSetting iterSetting = new IteratorSetting(5, BadIterator.class);

        ConditionalMutation cm1 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setIterators(iterSetting));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        Result result = cw.write(cm1);

        try {
          Status status = result.getStatus();
          assertEquals("Expected Status.UNKNOWN when using iterator which throws an error, Got status: " + status, Status.UNKNOWN, status);
        } catch (Exception ae) {
          // Expected
        }

      }
    }
  }
}
