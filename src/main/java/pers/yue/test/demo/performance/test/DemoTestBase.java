package pers.yue.test.demo.performance.test;

import org.testng.annotations.BeforeSuite;
import pers.yue.util.PropertiesUtil;

/**
 * Demo base test class of implementing performance test tool with the performance test core module.
 * Assume that you read test env properties and setup your test client in this class.
 *
 * Created by Zhang Yue on 6/8/2019
 */
public class DemoTestBase {
    private static final String ENDPOINT_PROPERTY_NAME = "endpoint";
    private static final String DEFAULT_ENDPOINT = "127.0.0.1";
    String endpoint;

    @BeforeSuite(alwaysRun = true)
    public void beforeSuiteDemoTestBase() {
        endpoint = PropertiesUtil.loadProperty(ENDPOINT_PROPERTY_NAME, System.getProperties(), null, DEFAULT_ENDPOINT);
    }
}
