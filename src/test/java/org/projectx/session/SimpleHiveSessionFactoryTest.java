package org.projectx.session;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectx.hive.Host;
import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.HiveConnectionFactory;
import org.projectx.hive.session.HiveSession;
import org.projectx.hive.session.HiveSessionParams;
import org.projectx.hive.session.SimpleHiveSessionFactory;

import com.google.common.collect.Maps;

@RunWith(MockitoJUnitRunner.class)
public class SimpleHiveSessionFactoryTest {

  private static final int TEST_PORT = 10010;

  private static final int TEST_TIMEOUT = 100;

  private static final String TEST_HOST = "TEST_HOST";

  private SimpleHiveSessionFactory classUnderTest;
  @Mock
  private HiveConnectionFactory mockFactory;

  @Mock
  private HiveConnection mockConnection;

  @Mock
  private HiveSessionParams mockParams;

  @Mock
  private Host mockHost;

  private final Map<String, String> params = Maps.newHashMap();

  @Before
  public void before() {
    classUnderTest = new SimpleHiveSessionFactory(mockFactory, TEST_HOST, TEST_PORT, TEST_TIMEOUT);
    params.put("TEST_KEY", "TEST_VALUE");
  }

  @Test
  public void testCreateSesssion() {
    when(mockFactory.createHiveConnection(any(Host.class), anyInt())).thenReturn(mockConnection);
    when(mockParams.getParameters()).thenReturn(params);
    final HiveSession $ = classUnderTest.createSession(mockParams);
    assertNotNull("returned session is null", $);
    verify(mockConnection).open();
    verify(mockConnection).execute("SET TEST_KEY=TEST_VALUE");
  }
}
