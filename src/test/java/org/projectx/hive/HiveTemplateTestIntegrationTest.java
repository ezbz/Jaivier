package org.projectx.hive;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ContextConfiguration(locations = { "classpath*:applicationContext-test-hive.xml",
                                   "classpath*:applicationContext-properties.xml" })
@RunWith(SpringJUnit4ClassRunner.class)
public class HiveTemplateTestIntegrationTest {

  @Resource
  private HiveOperations hiveTemplate;

  @Test
  public void testQuery() throws InterruptedException {
    hiveTemplate.queryForRawList("select 1 from dual");
  }
}