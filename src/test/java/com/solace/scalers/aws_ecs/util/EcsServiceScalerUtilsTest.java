package com.solace.scalers.aws_ecs.util;

import com.solace.scalers.aws_ecs.model.ScalerConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfigParser;
import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerDesiredReplicaTargets;
import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerObservations;
import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerReplicaTarget;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.solace.scalers.aws_ecs.util.EcsServiceScalerUtils.*;
import static org.junit.Assert.*;

public class EcsServiceScalerUtilsTest {

    ScalerConfig scalerConfig;

    @Before
    public void setUp() throws Exception {
        String configFile = "src/test/resources/configs/valid-config.yaml";
        scalerConfig = ScalerConfigParser.validateScalerConfig(ScalerConfigParser.parseScalerConfig(configFile));
    }

    @Test
    public void getEcsServiceScalerObservations_scaleOut() {
        long evaluationTimeInstant = System.currentTimeMillis();
        Integer currentDesiredReplicas = 5;
        ScalerConfig.EcsServiceConfig ecsServiceConfig = scalerConfig.getEcsServiceConfig().get(0);
        Map<Long, Map<String, Long>> metricObservations = new ConcurrentHashMap<>(250, 0.75F, 3);
        metricObservations.put(evaluationTimeInstant - (60L * 1000L), generateMetricsObservations(5, 20));
        metricObservations.put(evaluationTimeInstant - (2L * 60L * 1000L), generateMetricsObservations(10, 1000));

        EcsServiceScalerObservations observations = EcsServiceScalerUtils.getEcsServiceScalerObservations(evaluationTimeInstant, metricObservations, 5000L, 5000L);

        assertNotNull(observations);
        assertEquals(1000, observations.getMaxAverageMessageRateObservationUp());
        assertEquals(1000, observations.getMaxAverageMessageRateObservationDown());
        assertEquals(10, observations.getMaxMessageCountObservationUp());
        assertEquals(10, observations.getMaxMessageCountObservationDown());

        EcsServiceScalerDesiredReplicaTargets desiredReplicaTargets = EcsServiceScalerUtils.getReplicaTargets(currentDesiredReplicas, observations, ecsServiceConfig);
        assertNotNull(desiredReplicaTargets);
        assertEquals(56, (int) desiredReplicaTargets.getDesiredScaleInTarget());
        assertEquals(10, (int)desiredReplicaTargets.getDesiredScaleOutTarget());

        EcsServiceScalerReplicaTarget ecsServiceScalerReplicaTarget = EcsServiceScalerUtils.getReplicaTarget(desiredReplicaTargets.getDesiredScaleInTarget(), desiredReplicaTargets.getDesiredScaleOutTarget(), currentDesiredReplicas, evaluationTimeInstant, evaluationTimeInstant - 100000L, evaluationTimeInstant - 200000L, ecsServiceConfig);
        assertNotNull(ecsServiceScalerReplicaTarget);
        assertTrue(ecsServiceScalerReplicaTarget.isScaleOutDecision());
        assertFalse(ecsServiceScalerReplicaTarget.isScaleInDecision());
        assertEquals(Integer.valueOf(10), ecsServiceScalerReplicaTarget.getReplicaTarget());
    }

    @Test
    public void getEcsServiceScalerObservations_scaleIn() {
        long evaluationTimeInstant = System.currentTimeMillis();
        Integer currentDesiredReplicas = 5;
        ScalerConfig.EcsServiceConfig ecsServiceConfig = scalerConfig.getEcsServiceConfig().get(0);
        Map<Long, Map<String, Long>> metricObservations = new ConcurrentHashMap<>(250, 0.75F, 3);
        metricObservations.put(evaluationTimeInstant - (60L * 1000L), generateMetricsObservations(5, 20));
        metricObservations.put(evaluationTimeInstant - (2L * 60L * 1000L), generateMetricsObservations(10, 50));

        EcsServiceScalerObservations observations = EcsServiceScalerUtils.getEcsServiceScalerObservations(evaluationTimeInstant, metricObservations, 5000L, 5000L);

        assertNotNull(observations);
        assertEquals(50, observations.getMaxAverageMessageRateObservationUp());
        assertEquals(50, observations.getMaxAverageMessageRateObservationDown());
        assertEquals(10, observations.getMaxMessageCountObservationUp());
        assertEquals(10, observations.getMaxMessageCountObservationDown());

        EcsServiceScalerDesiredReplicaTargets desiredReplicaTargets = EcsServiceScalerUtils.getReplicaTargets(currentDesiredReplicas, observations, ecsServiceConfig);
        assertNotNull(desiredReplicaTargets);
        assertEquals(3, (int) desiredReplicaTargets.getDesiredScaleInTarget());
        assertEquals(3, (int)desiredReplicaTargets.getDesiredScaleOutTarget());

        EcsServiceScalerReplicaTarget ecsServiceScalerReplicaTarget = EcsServiceScalerUtils.getReplicaTarget(desiredReplicaTargets.getDesiredScaleInTarget(), desiredReplicaTargets.getDesiredScaleOutTarget(), currentDesiredReplicas, evaluationTimeInstant, evaluationTimeInstant - 100000L, evaluationTimeInstant - 200000L, ecsServiceConfig);
        assertNotNull(ecsServiceScalerReplicaTarget);
        assertTrue(ecsServiceScalerReplicaTarget.isScaleInDecision());
        assertFalse(ecsServiceScalerReplicaTarget.isScaleOutDecision());
        assertEquals(Integer.valueOf(3), ecsServiceScalerReplicaTarget.getReplicaTarget());
    }


    // Generate metrics observations
    private HashMap<String, Long> generateMetricsObservations(long messageCount, long messageReceiveRate) {
        HashMap<String, Long> metricMap = new HashMap<>();
        metricMap.put(METRIC_MSG_COUNT, messageCount);
        metricMap.put(METRIC_AVG_RX_RATE, messageReceiveRate);
        metricMap.put(METRIC_SPOOL_USAGE, 0L);
        return metricMap;
    }
}
