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
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Extracted security and error tests from ConditionalWriterIT.
 */
public class ConditionalWriterSecurityIT extends SharedMiniClusterBase {

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
  public void testBadColVis() throws Exception {
    // test when a user sets a col vis in a condition that can never be seen

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      Authorizations auths = new Authorizations("A", "B");

      client.securityOperations().changeUserAuthorizations(getAdminPrincipal(), auths);

      Authorizations filteredAuths = new Authorizations("A");

      ColumnVisibility cva = new ColumnVisibility("A");
      ColumnVisibility cvb = new ColumnVisibility("B");
      ColumnVisibility cvc = new ColumnVisibility("C");

      try (ConditionalWriter cw = client.createConditionalWriter(tableName,
          new ConditionalWriterConfig().setAuthorizations(filteredAuths))) {

        // User has authorization, but didn't include it in the writer
        ConditionalMutation cm0 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb));
        cm0.put("name", "last", cva, "doe");
        cm0.put("name", "first", cva, "john");
        cm0.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm0).getStatus());

        ConditionalMutation cm1 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
        cm1.put("name", "last", cva, "doe");
        cm1.put("name", "first", cva, "john");
        cm1.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm1).getStatus());

        // User does not have the authorization
        ConditionalMutation cm2 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvc));
        cm2.put("name", "last", cva, "doe");
        cm2.put("name", "first", cva, "john");
        cm2.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm2).getStatus());

        ConditionalMutation cm3 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvc).setValue("1"));
        cm3.put("name", "last", cva, "doe");
        cm3.put("name", "first", cva, "john");
        cm3.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm3).getStatus());

        // if any visibility is bad, good visibilities don't override
        ConditionalMutation cm4 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb),
                new Condition("tx", "seq").setVisibility(cva));

        cm4.put("name", "last", cva, "doe");
        cm4.put("name", "first", cva, "john");
        cm4.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm4).getStatus());

        ConditionalMutation cm5 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvb).setValue("1"),
            new Condition("tx", "seq").setVisibility(cva).setValue("1"));
        cm5.put("name", "last", cva, "doe");
        cm5.put("name", "first", cva, "john");
        cm5.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm5).getStatus());

        ConditionalMutation cm6 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvb).setValue("1"),
            new Condition("tx", "seq").setVisibility(cva));
        cm6.put("name", "last", cva, "doe");
        cm6.put("name", "first", cva, "john");
        cm6.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm6).getStatus());

        ConditionalMutation cm7 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb),
                new Condition("tx", "seq").setVisibility(cva).setValue("1"));
        cm7.put("name", "last", cva, "doe");
        cm7.put("name", "first", cva, "john");
        cm7.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm7).getStatus());

      }

      // test passing auths that exceed users configured auths

      Authorizations exceedingAuths = new Authorizations("A", "B", "D");
      try (ConditionalWriter cw2 = client.createConditionalWriter(tableName,
          new ConditionalWriterConfig().setAuthorizations(exceedingAuths))) {

        ConditionalMutation cm8 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb),
                new Condition("tx", "seq").setVisibility(cva).setValue("1"));
        cm8.put("name", "last", cva, "doe");
        cm8.put("name", "first", cva, "john");
        cm8.put("tx", "seq", cva, "1");

        try {
          Status status = cw2.write(cm8).getStatus();
          fail(
              "Writing mutation with Authorizations the user doesn't have should fail. Got status: "
                  + status);
        } catch (AccumuloSecurityException ase) {
          // expected
        }
      }
    }
  }

  @Test
  public void testSecurity() throws Exception {
    // test against table user does not have read and/or write permissions for
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String user = null;

      // Create a new user
      ClusterUser user1 = getUser(0);
      user = user1.getPrincipal();
      if (saslEnabled()) {
        client.securityOperations().createLocalUser(user, null);
      } else {
        client.securityOperations().createLocalUser(user, new PasswordToken(user1.getPassword()));
      }

      String[] tables = getUniqueNames(3);
      String table1 = tables[0], table2 = tables[1], table3 = tables[2];

      // Create three tables
      client.tableOperations().create(table1);
      client.tableOperations().create(table2);
      client.tableOperations().create(table3);

      // Grant R on table1, W on table2, R/W on table3
      client.securityOperations().grantTablePermission(user, table1, TablePermission.READ);
      client.securityOperations().grantTablePermission(user, table2, TablePermission.WRITE);
      client.securityOperations().grantTablePermission(user, table3, TablePermission.READ);
      client.securityOperations().grantTablePermission(user, table3, TablePermission.WRITE);

      ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
      cm1.put("tx", "seq", "1");
      cm1.put("data", "x", "a");

      try (
          AccumuloClient client2 =
              Accumulo.newClient().from(client.properties()).as(user, user1.getToken()).build();
          ConditionalWriter cw1 =
              client2.createConditionalWriter(table1, new ConditionalWriterConfig());
          ConditionalWriter cw2 =
              client2.createConditionalWriter(table2, new ConditionalWriterConfig());
          ConditionalWriter cw3 =
              client2.createConditionalWriter(table3, new ConditionalWriterConfig())) {

        // Should be able to conditional-update a table we have R/W on
        assertEquals(Status.ACCEPTED, cw3.write(cm1).getStatus());

        // Conditional-update to a table we only have read on should fail
        try {
          Status status = cw1.write(cm1).getStatus();
          fail("Expected exception writing conditional mutation to table"
              + " the user doesn't have write access to, Got status: " + status);
        } catch (AccumuloSecurityException ase) {
          // expected
        }

        // Conditional-update to a table we only have writer on should fail
        try {
          Status status = cw2.write(cm1).getStatus();
          fail("Expected exception writing conditional mutation to table"
              + " the user doesn't have read access to. Got status: " + status);
        } catch (AccumuloSecurityException ase) {
          // expected
        }
      }
    }
  }

  @Test
  public void testTimeout() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String table = getUniqueNames(1)[0];

      client.tableOperations().create(table);

      try (
          ConditionalWriter cw = client.createConditionalWriter(table,
              new ConditionalWriterConfig().setTimeout(3, TimeUnit.SECONDS));
          Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {

        ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        assertEquals(cw.write(cm1).getStatus(), Status.ACCEPTED);

        IteratorSetting is = new IteratorSetting(5, SlowIterator.class);
        SlowIterator.setSeekSleepTime(is, 5000);

        ConditionalMutation cm2 = new ConditionalMutation("r1",
            new Condition("tx", "seq").setValue("1").setIterators(is));
        cm2.put("tx", "seq", "2");
        cm2.put("data", "x", "b");

        assertEquals(cw.write(cm2).getStatus(), Status.UNKNOWN);

        for (Entry<Key,Value> entry : scanner) {
          String cf = entry.getKey().getColumnFamilyData().toString();
          String cq = entry.getKey().getColumnQualifierData().toString();
          String val = entry.getValue().toString();

          if (cf.equals("tx") && cq.equals("seq"))
            assertEquals("Unexpected value in tx:seq", "1", val);
          else if (cf.equals("data") && cq.equals("x"))
            assertEquals("Unexpected value in data:x", "a", val);
          else
            fail("Saw unexpected column family and qualifier: " + entry);
        }

        ConditionalMutation cm3 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
        cm3.put("tx", "seq", "2");
        cm3.put("data", "x", "b");

        assertEquals(cw.write(cm3).getStatus(), Status.ACCEPTED);
      }
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      try {
        client.createConditionalWriter(table, new ConditionalWriterConfig());
        fail("Creating conditional writer for table that doesn't exist should fail");
      } catch (TableNotFoundException e) {}

      client.tableOperations().create(table);

      try (ConditionalWriter cw =
          client.createConditionalWriter(table, new ConditionalWriterConfig())) {

        client.tableOperations().delete(table);

        ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        Result result = cw.write(cm1);

        try {
          Status status = result.getStatus();
          fail("Expected exception writing conditional mutation to deleted table. Got status: "
              + status);
        } catch (AccumuloException ae) {
          assertEquals(TableDeletedException.class, ae.getCause().getClass());
        }
      }
    }
  }

  @Test
  public void testOffline() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      try (ConditionalWriter cw =
          client.createConditionalWriter(table, new ConditionalWriterConfig())) {

        client.tableOperations().offline(table, true);

        ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        Result result = cw.write(cm1);

        try {
          Status status = result.getStatus();
          fail("Expected exception writing conditional mutation to offline table. Got status: "
              + status);
        } catch (AccumuloException ae) {
          assertEquals(TableOfflineException.class, ae.getCause().getClass());
        }

        try {
          client.createConditionalWriter(table, new ConditionalWriterConfig());
          fail("Expected exception creating conditional writer to offline table");
        } catch (TableOfflineException e) {}
      }
    }
  }
}
