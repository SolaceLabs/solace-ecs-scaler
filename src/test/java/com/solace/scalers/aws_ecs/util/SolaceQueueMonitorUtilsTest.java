package com.solace.scalers.aws_ecs.util;

import com.solace.scalers.aws_ecs.SolaceQueueMonitor;
import com.solace.scalers.aws_ecs.model.ScalerConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfigParser;
import org.junit.Test;

import static com.solace.scalers.aws_ecs.SolaceQueueMonitor.ACTIVE_SEMP_CONFIG;
import static com.solace.scalers.aws_ecs.SolaceQueueMonitor.STANDBY_SEMP_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SolaceQueueMonitorUtilsTest {

    @Test
    public void createSolaceQueueMonitorWithBasicAuth_ActiveStandby() throws Exception {
        String configFile = "src/test/resources/configs/valid-config.yaml";
        ScalerConfig scalerConfig = ScalerConfigParser.validateScalerConfig(ScalerConfigParser.parseScalerConfig(configFile));

        SolaceQueueMonitor solaceQueueMonitor = SolaceQueueMonitorUtils.createSolaceQueueMonitorWithBasicAuth(scalerConfig.getBrokerConfig(), scalerConfig.getEcsServiceConfig().get(0));

        assertEquals(solaceQueueMonitor.getQueueName(), "service-queue-1");
        assertEquals(solaceQueueMonitor.getSempConfigMap().get(ACTIVE_SEMP_CONFIG).getBrokerSempUrl(), "http://my.solace.broker.com:943");
        assertEquals(solaceQueueMonitor.getSempConfigMap().get(STANDBY_SEMP_CONFIG).getBrokerSempUrl(), "http://standby.solace.broker.com:943");
    }

    @Test
    public void createSolaceQueueMonitorWithBasicAuth_ActiveOnly() throws Exception {
        String configFile = "src/test/resources/configs/valid-standalone-config.yaml";
        ScalerConfig scalerConfig = ScalerConfigParser.validateScalerConfig(ScalerConfigParser.parseScalerConfig(configFile));

        SolaceQueueMonitor solaceQueueMonitor = SolaceQueueMonitorUtils.createSolaceQueueMonitorWithBasicAuth(scalerConfig.getBrokerConfig(), scalerConfig.getEcsServiceConfig().get(0));

        assertEquals(solaceQueueMonitor.getQueueName(), "service-queue-1");
        assertEquals(solaceQueueMonitor.getSempConfigMap().get(ACTIVE_SEMP_CONFIG).getBrokerSempUrl(), "http://my.solace.broker.com:943");
        assertNull(solaceQueueMonitor.getSempConfigMap().get(STANDBY_SEMP_CONFIG));
    }


}
