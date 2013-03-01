package org.projectx.hive.session;

public class HiveSessionParamsBuilder {
  private final HiveSessionParams hiveSessionParams;

  public HiveSessionParamsBuilder() {
    hiveSessionParams = new HiveSessionParams();
  }

  public HiveSessionParamsBuilder withJobName(final String name) {
    hiveSessionParams.setParameter(HiveSessionParams.HIVE_PARAM_JOBNAME, name);
    hiveSessionParams.setParameter(HiveSessionParams.HIVE_PARAM_MAX_JOBNAME_LENGTH, String.valueOf(name.length()));
    return this;
  }

  public HiveSessionParams build() {
    return hiveSessionParams;
  }
}
