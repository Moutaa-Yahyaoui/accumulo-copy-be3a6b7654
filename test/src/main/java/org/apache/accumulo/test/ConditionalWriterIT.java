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
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.tracer.TraceDump;
import org.apache.accumulo.tracer.TraceServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Basic tests for ConditionalWriter.
 * Tests extracted to:
 * <ul>
 *   <li>{@link ConditionalWriterIteratorsIT}</li>
 *   <li>{@link ConditionalWriterBatchIT}</li>
 *   <li>{@link ConditionalWriterSecurityIT}</li>
 *   <li>{@link ConditionalWriterConcurrencyIT}</li>
 * </ul>
 */
public class ConditionalWriterIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(ConditionalWriterIT.class);

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

  @Before
  public void deleteUsers() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Set<String> users = client.securityOperations().listLocalUsers();
      ClusterUser user = getUser(0);
      if (users.contains(user.getPrincipal())) {
        client.securityOperations().dropLocalUser(user.getPrincipal());
      }
    }
  }

  @Test
  public void testBasic() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      try (
          ConditionalWriter cw =
              client.createConditionalWriter(tableName, new ConditionalWriterConfig());
          Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {

        // mutation conditional on column tx:seq not existing
        ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq"));
        cm0.put("name", "last", "doe");
        cm0.put("name", "first", "john");
        cm0.put("tx", "seq", "1");
        assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
        assertEquals(Status.REJECTED, cw.write(cm0).getStatus());

        // mutation conditional on column tx:seq being 1
        ConditionalMutation cm1 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("1"));
        cm1.put("name", "last", "Doe");
        cm1.put("tx", "seq", "2");
        assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());

        // test condition where value differs
        ConditionalMutation cm2 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("1"));
        cm2.put("name", "last", "DOE");
        cm2.put("tx", "seq", "2");
        assertEquals(Status.REJECTED, cw.write(cm2).getStatus());

        // test condition where column does not exists
        ConditionalMutation cm3 =
            new ConditionalMutation("99006", new Condition("txtypo", "seq").setValue("1"));
        cm3.put("name", "last", "deo");
        cm3.put("tx", "seq", "2");
        assertEquals(Status.REJECTED, cw.write(cm3).getStatus());

        // test two conditions, where one should fail
        ConditionalMutation cm4 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("2"),
                new Condition("name", "last").setValue("doe"));
        cm4.put("name", "last", "deo");
        cm4.put("tx", "seq", "3");
        assertEquals(Status.REJECTED, cw.write(cm4).getStatus());

        // test two conditions, where one should fail
        ConditionalMutation cm5 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("1"),
                new Condition("name", "last").setValue("Doe"));
        cm5.put("name", "last", "deo");
        cm5.put("tx", "seq", "3");
        assertEquals(Status.REJECTED, cw.write(cm5).getStatus());

        // ensure rejected mutations did not write
        scanner.fetchColumn("name", "last");
        scanner.setRange(new Range("99006"));
        Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
        assertEquals("Doe", entry.getValue().toString());

        // test w/ two conditions that are met
        ConditionalMutation cm6 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("2"),
                new Condition("name", "last").setValue("Doe"));
        cm6.put("name", "last", "DOE");
        cm6.put("tx", "seq", "3");
        assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

        entry = Iterables.getOnlyElement(scanner);
        assertEquals("DOE", entry.getValue().toString());

        // test a conditional mutation that deletes
        ConditionalMutation cm7 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("3"));
        cm7.putDelete("name", "last");
        cm7.putDelete("name", "first");
        cm7.putDelete("tx", "seq");
        assertEquals(Status.ACCEPTED, cw.write(cm7).getStatus());

        assertFalse("Did not expect to find any results", scanner.iterator().hasNext());

        // add the row back
        assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
        assertEquals(Status.REJECTED, cw.write(cm0).getStatus());

        entry = Iterables.getOnlyElement(scanner);
        assertEquals("doe", entry.getValue().toString());
      }
    }
  }

  @Test
  public void testFields() throws Exception {

    try (AccumuloClient client1 = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      String user = null;

      ClusterUser user1 = getUser(0);
      user = user1.getPrincipal();
      if (saslEnabled()) {
        // The token is pointless for kerberos
        client1.securityOperations().createLocalUser(user, null);
      } else {
        client1.securityOperations().createLocalUser(user, new PasswordToken(user1.getPassword()));
      }

      Authorizations auths = new Authorizations("A", "B");

      client1.securityOperations().changeUserAuthorizations(user, auths);
      client1.securityOperations().grantSystemPermission(user, SystemPermission.CREATE_TABLE);

      try (AccumuloClient client2 =
          Accumulo.newClient().from(client1.properties()).as(user, user1.getToken()).build()) {
        client2.tableOperations().create(tableName);

        try (
            ConditionalWriter cw = client2.createConditionalWriter(tableName,
                new ConditionalWriterConfig().setAuthorizations(auths));
            Scanner scanner = client2.createScanner(tableName, auths)) {

          ColumnVisibility cva = new ColumnVisibility("A");
          ColumnVisibility cvb = new ColumnVisibility("B");

          ConditionalMutation cm0 =
              new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cva));
          cm0.put("name", "last", cva, "doe");
          cm0.put("name", "first", cva, "john");
          cm0.put("tx", "seq", cva, "1");
          assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());

          scanner.setRange(new Range("99006"));
          // TODO verify all columns
          scanner.fetchColumn("tx", "seq");
          Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
          assertEquals("1", entry.getValue().toString());
          long ts = entry.getKey().getTimestamp();

          // test wrong colf
          ConditionalMutation cm1 = new ConditionalMutation("99006",
              new Condition("txA", "seq").setVisibility(cva).setValue("1"));
          cm1.put("name", "last", cva, "Doe");
          cm1.put("name", "first", cva, "John");
          cm1.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm1).getStatus());

          // test wrong colq
          ConditionalMutation cm2 = new ConditionalMutation("99006",
              new Condition("tx", "seqA").setVisibility(cva).setValue("1"));
          cm2.put("name", "last", cva, "Doe");
          cm2.put("name", "first", cva, "John");
          cm2.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm2).getStatus());

          // test wrong colv
          ConditionalMutation cm3 = new ConditionalMutation("99006",
              new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
          cm3.put("name", "last", cva, "Doe");
          cm3.put("name", "first", cva, "John");
          cm3.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm3).getStatus());

          // test wrong timestamp
          ConditionalMutation cm4 = new ConditionalMutation("99006",
              new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts + 1).setValue("1"));
          cm4.put("name", "last", cva, "Doe");
          cm4.put("name", "first", cva, "John");
          cm4.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm4).getStatus());

          // test wrong timestamp
          ConditionalMutation cm5 = new ConditionalMutation("99006",
              new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts - 1).setValue("1"));
          cm5.put("name", "last", cva, "Doe");
          cm5.put("name", "first", cva, "John");
          cm5.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm5).getStatus());

          // ensure no updates were made
          entry = Iterables.getOnlyElement(scanner);
          assertEquals("1", entry.getValue().toString());

          // set all columns correctly
          ConditionalMutation cm6 = new ConditionalMutation("99006",
              new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts).setValue("1"));
          cm6.put("name", "last", cva, "Doe");
          cm6.put("name", "first", cva, "John");
          cm6.put("tx", "seq", cva, "2");
          assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

          entry = Iterables.getOnlyElement(scanner);
          assertEquals("2", entry.getValue().toString());
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoConditions() throws AccumuloException, AccumuloSecurityException,
      TableExistsException, TableNotFoundException {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      try (ConditionalWriter cw =
          client.createConditionalWriter(table, new ConditionalWriterConfig())) {

        ConditionalMutation cm1 = new ConditionalMutation("r1");
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        cw.write(cm1);
      }
    }
  }

  @Test
  public void testTrace() throws Exception {
    // Need to add a getClientConfig() to AccumuloCluster
    Process tracer = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      MiniAccumuloClusterImpl mac = getCluster();
      if (!client.tableOperations().exists("trace")) {
        tracer = mac.exec(TraceServer.class).getProcess();
        while (!client.tableOperations().exists("trace")) {
          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      }

      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      TraceUtil.enableClientTraces("localhost", "testTrace", mac.getClientProperties());
      sleepUninterruptibly(1, TimeUnit.SECONDS);
      long rootTraceId;
      try (TraceScope root = Trace.startSpan("traceTest", Sampler.ALWAYS); ConditionalWriter cw =
          client.createConditionalWriter(tableName, new ConditionalWriterConfig())) {
        rootTraceId = root.getSpan().getTraceId();

        // mutation conditional on column tx:seq not exiting
        ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq"));
        cm0.put("name", "last", "doe");
        cm0.put("name", "first", "john");
        cm0.put("tx", "seq", "1");
        assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
      }

      try (Scanner scanner = client.createScanner("trace", Authorizations.EMPTY)) {
        scanner.setRange(new Range(new Text(Long.toHexString(rootTraceId))));
        loop: while (true) {
          final StringBuilder finalBuffer = new StringBuilder();
          int traceCount = TraceDump.printTrace(scanner, line -> {
            try {
              finalBuffer.append(line).append("\n");
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          });
          String traceOutput = finalBuffer.toString();
          log.info("Trace output:" + traceOutput);
          if (traceCount > 0) {
            String[] parts = ("traceTest, startScan,startConditionalUpdate,conditionalUpdate"
                + ",Check conditions,apply conditional mutations").split(",");
            int lastPos = 0;
            for (String part : parts) {
              log.info("Looking in trace output for '" + part + "'");
              int pos = traceOutput.indexOf(part);
              if (pos == -1) {
                log.info("Trace output doesn't contain '" + part + "'");
                Thread.sleep(1000);
                break loop;
              }
              assertTrue("Did not find '" + part + "' in output", pos > 0);
              assertTrue("'" + part + "' occurred earlier than the previous element unexpectedly",
                  pos > lastPos);
              lastPos = pos;
            }
            break;
          } else {
            log.info("Ignoring trace output as traceCount not greater than zero: " + traceCount);
            Thread.sleep(1000);
          }
        }
        if (tracer != null) {
          tracer.destroy();
        }
      }
    }
  }
}
