package org.projectx.hive;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.TestHiveConnectionFactory;
import org.projectx.hive.session.FailoverHiveSession;

import com.google.common.collect.Lists;

public class FailoverHiveSessioIntegrationTest {

  @Test
  public void testCreateConnection() {
    final Host host1 = new Host("localhost", 10000);
    final Host host2 = new Host("localhost", 10000);
    final FailoverHiveSession session = new FailoverHiveSession(new TestHiveConnectionFactory(),
        Lists.newArrayList(host1, host2), 100000, "select 1 from dual", 10000, 2);
    assertNotNull("session was null", session);
    final HiveConnection connection = session.createConnection();
    assertNotNull("connection was null", connection);
  }

  @Test
  public void testCreateConnectionWithBadConnectionSucceeds() {
    final Host host1 = new Host("TEST_DUMMY_HOST", 10000);
    final Host host2 = new Host("localhost", 10000);
    final FailoverHiveSession session = new FailoverHiveSession(new TestHiveConnectionFactory(),
        Lists.newArrayList(host1, host2), 10000, "select 1 from dual", 10000, 2);
    assertNotNull("session was null", session);
    final HiveConnection connection = session.createConnection();
    assertNotNull("connection was null", connection);
  }
}
