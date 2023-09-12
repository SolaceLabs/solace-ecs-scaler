package com.solace.demo;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import com.solace.scalers.aws_ecs.model.ScalerConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfig.BrokerConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfig.EcsServiceConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfig.ScalerBehaviorConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfig.ScalerOperation;

public class TestUtilities {
    
    @Test
    public void createConfigFile() {

        ScalerConfig scalerConfig = new ScalerConfig();

        scalerConfig.setBrokerConfig(new BrokerConfig());
        scalerConfig.setEcsServiceConfig( new ArrayList<EcsServiceConfig>() );

        scalerConfig.getBrokerConfig().setBrokerSempUrl("http://my.solace.broker.com:943");
        scalerConfig.getBrokerConfig().setMsgVpnName("testVpn");
        scalerConfig.getBrokerConfig().setUsername("thisIsMe");
        scalerConfig.getBrokerConfig().setPassword("secretPassword");
        scalerConfig.getBrokerConfig().setPollingInterval(10);

        EcsServiceConfig ecs1 = new EcsServiceConfig();
        EcsServiceConfig ecs2 = new EcsServiceConfig();
        EcsServiceConfig ecs3 = new EcsServiceConfig();

        ScalerBehaviorConfig sbc1 = new ScalerBehaviorConfig();
        ScalerBehaviorConfig sbc2 = new ScalerBehaviorConfig();
        ScalerBehaviorConfig sbc3 = new ScalerBehaviorConfig();

        ecs1.setEcsCluster("ecsCluster1");
        ecs1.setEcsService("ecsService1");
        ecs1.setQueueName("service-queue-1");

        ecs2.setEcsCluster("ecsCluster2");
        ecs2.setEcsService("ecsService2");
        ecs2.setQueueName("service-queue-2");

        ecs3.setEcsCluster("ecsCluster3");
        ecs3.setEcsService("ecsService3");
        ecs3.setQueueName("service-queue-3");

        sbc1.setMinReplicaCount(1);
        sbc1.setMaxReplicaCount(10);
        sbc1.setMessageCountTarget(50);
        sbc1.setMessageReceiveRateTarget(20);
        sbc1.setMessageSpoolUsageTarget(100);

        sbc2.setMinReplicaCount(0);
        sbc2.setMaxReplicaCount(25);
        sbc2.setMessageCountTarget(30);
        sbc2.setMessageReceiveRateTarget(15);
        sbc2.setMessageSpoolUsageTarget(200);

        sbc3.setMinReplicaCount(1);
        sbc3.setMaxReplicaCount(12);
        sbc3.setMessageCountTarget(100);
        sbc3.setMessageReceiveRateTarget(30);
        sbc3.setMessageSpoolUsageTarget(200);

        ScalerOperation soOut = new ScalerOperation();
        ScalerOperation soIn  = new ScalerOperation();

        soOut.setCooldownPeriod(30);
        soOut.setMaxScaleStep(5);
        soOut.setStabilizationWindow(10);

        soIn.setCooldownPeriod(60);
        soIn.setMaxScaleStep(2);
        soIn.setStabilizationWindow(120);
        
        sbc1.setScaleInConfig(soIn);
        sbc1.setScaleOutConfig(soOut);

        sbc2.setScaleInConfig(soIn);
        sbc2.setScaleOutConfig(soOut);

        sbc3.setScaleInConfig(soIn);
        sbc3.setScaleOutConfig(soOut);

        ecs1.setScalerBehaviorConfig(sbc1);
        ecs2.setScalerBehaviorConfig(sbc2);
        ecs3.setScalerBehaviorConfig(sbc3);

        scalerConfig.getEcsServiceConfig().add(ecs1);
        scalerConfig.getEcsServiceConfig().add(ecs2);
        scalerConfig.getEcsServiceConfig().add(ecs3);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory().enable(Feature.MINIMIZE_QUOTES));

        try {
            mapper.writeValue( new File("src/test/resources/configs/generated-config.yaml"), scalerConfig );
        } catch ( Exception exc ) {
            System.out.println(exc.getMessage());
            System.out.println(exc.getStackTrace());
        }

        assertTrue( true );
    }

}
