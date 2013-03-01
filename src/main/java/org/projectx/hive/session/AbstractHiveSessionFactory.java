package org.projectx.hive.session;

import java.util.Map;
import java.util.Map.Entry;

import org.projectx.hive.connection.HiveConnection;


public abstract class AbstractHiveSessionFactory implements HiveSessionFactory {

  @Override
  public HiveSession createSession() {
    return createSession(new HiveSessionParams());
  }

  /**
   * Apply the session parameters by executing them one-by-one on the connections
   * @param connection an instance of a {@link HiveConnection}
   * @param sessionParameters the session parameters to set
   */
  protected void setSessionParameters(final HiveConnection connection, final HiveSessionParams sessionParameters) {
    final Map<String, String> params = sessionParameters.getParameters();
    for (final Entry<String, String> param : params.entrySet()) {
      final String key = param.getKey();
      final String value = param.getValue();
      connection.execute(String.format("SET %s=%s", key, value));
    }
  }
}
