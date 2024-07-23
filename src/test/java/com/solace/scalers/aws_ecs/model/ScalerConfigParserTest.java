package com.solace.scalers.aws_ecs.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ScalerConfigParserTest {

    @Test
    public void testParse_valid() throws Exception {
        String configFile = "src/test/resources/configs/valid-config.yaml";

        ScalerConfig scalerConfig = ScalerConfigParser.validateScalerConfig(ScalerConfigParser.parseScalerConfig(configFile));

        assertEquals(scalerConfig.brokerConfig.msgVpnName, "testVpn");
        assertEquals(scalerConfig.brokerConfig.activeMsgVpnSempConfig.brokerSempUrl, "http://my.solace.broker.com:943");
        assertEquals(scalerConfig.brokerConfig.standbyMsgVpnSempConfig.brokerSempUrl, "http://standby.solace.broker.com:943");
        assertEquals(scalerConfig.ecsServiceConfig.size(), 3);
        assertEquals(scalerConfig.ecsServiceConfig.get(0).ecsCluster, "ecsCluster1");
        assertEquals(scalerConfig.ecsServiceConfig.get(0).ecsService, "ecsService1");
    }

    @Test(expected = NullPointerException.class)
    public void testParse_incompleteBrokerConfig() throws Exception {
        String configFile = "src/test/resources/configs/invalid-broker-config.yaml";

        ScalerConfig scalerConfig = ScalerConfigParser.validateScalerConfig(ScalerConfigParser.parseScalerConfig(configFile));
    }

    @Test(expected = Exception.class)
    public void testParse_duplicateECSServiceConfig() throws Exception {
        String configFile = "src/test/resources/configs/duplicate-ecs-service-config.yaml";

        ScalerConfig scalerConfig = ScalerConfigParser.validateScalerConfig(ScalerConfigParser.parseScalerConfig(configFile));
    }

    @Test(expected = Exception.class)
    public void testParse_duplicateQueueNameConfig() throws Exception {
        String configFile = "src/test/resources/configs/duplicate-queue-name-config.yaml";

        ScalerConfig scalerConfig = ScalerConfigParser.validateScalerConfig(ScalerConfigParser.parseScalerConfig(configFile));
    }
}
