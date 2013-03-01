package org.projectx.hive.session;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class HiveSessionParams {

  public static final String HIVE_PARAM_JOBNAME = "mapred.job.name";
  public static final String HIVE_PARAM_MAX_JOBNAME_LENGTH = "hive.jobname.length";

  private Map<String, String> params = new LinkedHashMap<String, String>();

  public HiveSessionParams() {
    params = new LinkedHashMap<String, String>();
  }

  public HiveSessionParams(final Map<String, String> params) {
    this.params = params;
  }

  public HiveSessionParams(final HiveSessionParams sessionParams) {
    params = new LinkedHashMap<String, String>(sessionParams.getParameters());
  }

  public Map<String, String> getParameters() {
    return Collections.unmodifiableMap(params);
  }

  public String getParameter(final String key) {
    return params.get(key);
  }

  public void setParameter(final String key, final String value) {
    params.put(key, value);
  }

  public boolean isEmpty() {
    return params.isEmpty();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof HiveSessionParams == false) {
      return false;
    }
    if (obj == this) {
      return true;
    }

    final HiveSessionParams other = (HiveSessionParams) obj;
    return params.equals(other.params);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(params).toHashCode();
  }

  @Override
  public String toString() {
    return String.format("HiveSessionParameters[%s]", params.toString());
  }

  public void update(final HiveSessionParams sessionParams) {
    params.putAll(sessionParams.getParameters());
  }
}
