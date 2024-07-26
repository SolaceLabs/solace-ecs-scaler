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
    public void scaleEcsService_scaleOut() {
        long evaluationTimeInstant = System.currentTimeMillis();
        Integer currentDesiredReplicas = 5;
        ScalerConfig.EcsServiceConfig ecsServiceConfig = scalerConfig.getEcsServiceConfig().get(0);
        Map<Long, Map<String, Long>> metricObservations = new ConcurrentHashMap<>(250, 0.75F, 3);
        metricObservations.put(evaluationTimeInstant - getMillisForNumberOfSeconds(60L), generateMetricsObservations(5, 20));
        metricObservations.put(evaluationTimeInstant - getMillisForNumberOfSeconds(120L), generateMetricsObservations(10, 1000));

        EcsServiceScalerObservations observations = EcsServiceScalerUtils.getEcsServiceScalerObservations(evaluationTimeInstant, metricObservations, getMillisForNumberOfSeconds((long) ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getStabilizationWindow()), getMillisForNumberOfSeconds((long) ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getStabilizationWindow()));

        assertNotNull(observations);
        assertEquals(1000, observations.getMaxAverageMessageRateObservationUp());
        assertEquals(1000, observations.getMaxAverageMessageRateObservationDown());
        assertEquals(10, observations.getMaxMessageCountObservationUp());
        assertEquals(10, observations.getMaxMessageCountObservationDown());

        EcsServiceScalerDesiredReplicaTargets desiredReplicaTargets = EcsServiceScalerUtils.getReplicaTargets(currentDesiredReplicas, observations, ecsServiceConfig);
        assertNotNull(desiredReplicaTargets);
        assertEquals(56, (int) desiredReplicaTargets.getDesiredScaleInTarget());
        assertEquals(10, (int)desiredReplicaTargets.getDesiredScaleOutTarget());

        EcsServiceScalerReplicaTarget ecsServiceScalerReplicaTarget = EcsServiceScalerUtils.getReplicaTarget(desiredReplicaTargets.getDesiredScaleInTarget(), desiredReplicaTargets.getDesiredScaleOutTarget(), currentDesiredReplicas, evaluationTimeInstant, evaluationTimeInstant - getMillisForNumberOfSeconds(100L), evaluationTimeInstant - getMillisForNumberOfSeconds(200L), ecsServiceConfig);
        assertNotNull(ecsServiceScalerReplicaTarget);
        assertTrue(ecsServiceScalerReplicaTarget.isScaleOutDecision());
        assertFalse(ecsServiceScalerReplicaTarget.isScaleInDecision());
        assertEquals(Integer.valueOf(10), ecsServiceScalerReplicaTarget.getReplicaTarget());
    }

    @Test
    public void scaleEcsService_scaleIn() {
        long evaluationTimeInstant = System.currentTimeMillis();
        Integer currentDesiredReplicas = 5;
        ScalerConfig.EcsServiceConfig ecsServiceConfig = scalerConfig.getEcsServiceConfig().get(0);
        Map<Long, Map<String, Long>> metricObservations = new ConcurrentHashMap<>(250, 0.75F, 3);
        metricObservations.put(evaluationTimeInstant - getMillisForNumberOfSeconds(60L), generateMetricsObservations(5, 20));
        metricObservations.put(evaluationTimeInstant - getMillisForNumberOfSeconds(45L), generateMetricsObservations(10, 50));

        EcsServiceScalerObservations observations = EcsServiceScalerUtils.getEcsServiceScalerObservations(evaluationTimeInstant, metricObservations, evaluationTimeInstant - getMillisForNumberOfSeconds((long) ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getStabilizationWindow()), evaluationTimeInstant - getMillisForNumberOfSeconds((long) ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getStabilizationWindow()));

        assertNotNull(observations);
        assertEquals(50, observations.getMaxAverageMessageRateObservationUp());
        assertEquals(50, observations.getMaxAverageMessageRateObservationDown());
        assertEquals(10, observations.getMaxMessageCountObservationUp());
        assertEquals(10, observations.getMaxMessageCountObservationDown());

        EcsServiceScalerDesiredReplicaTargets desiredReplicaTargets = EcsServiceScalerUtils.getReplicaTargets(currentDesiredReplicas, observations, ecsServiceConfig);
        assertNotNull(desiredReplicaTargets);
        assertEquals(3, (int) desiredReplicaTargets.getDesiredScaleInTarget());
        assertEquals(3, (int)desiredReplicaTargets.getDesiredScaleOutTarget());

        EcsServiceScalerReplicaTarget ecsServiceScalerReplicaTarget = EcsServiceScalerUtils.getReplicaTarget(desiredReplicaTargets.getDesiredScaleInTarget(), desiredReplicaTargets.getDesiredScaleOutTarget(), currentDesiredReplicas, evaluationTimeInstant, evaluationTimeInstant - getMillisForNumberOfSeconds(10L), evaluationTimeInstant - getMillisForNumberOfSeconds(200L), ecsServiceConfig);
        assertNotNull(ecsServiceScalerReplicaTarget);
        assertTrue(ecsServiceScalerReplicaTarget.isScaleInDecision());
        assertFalse(ecsServiceScalerReplicaTarget.isScaleOutDecision());
        assertEquals(Integer.valueOf(3), ecsServiceScalerReplicaTarget.getReplicaTarget());
    }

    @Test
    public void scaleEcsService_oldMetrics() {
        long evaluationTimeInstant = System.currentTimeMillis();
        Integer currentDesiredReplicas = 1;
        ScalerConfig.EcsServiceConfig ecsServiceConfig = scalerConfig.getEcsServiceConfig().get(0);
        Map<Long, Map<String, Long>> metricObservations = new ConcurrentHashMap<>(250, 0.75F, 3);
        metricObservations.put(evaluationTimeInstant - getMillisForNumberOfSeconds(60L), generateMetricsObservations(5, 20));
        metricObservations.put(evaluationTimeInstant - getMillisForNumberOfSeconds(120L), generateMetricsObservations(10, 1000));

        EcsServiceScalerObservations observations = EcsServiceScalerUtils.getEcsServiceScalerObservations(evaluationTimeInstant, metricObservations, evaluationTimeInstant - getMillisForNumberOfSeconds((long) ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getStabilizationWindow()), evaluationTimeInstant - getMillisForNumberOfSeconds((long) ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getStabilizationWindow()));

        assertNotNull(observations);
        assertEquals(20, observations.getMaxAverageMessageRateObservationUp());
        assertEquals(20, observations.getMaxAverageMessageRateObservationDown());
        assertEquals(5, observations.getMaxMessageCountObservationUp());
        assertEquals(5, observations.getMaxMessageCountObservationDown());

        EcsServiceScalerDesiredReplicaTargets desiredReplicaTargets = EcsServiceScalerUtils.getReplicaTargets(currentDesiredReplicas, observations, ecsServiceConfig);
        assertNotNull(desiredReplicaTargets);
        assertEquals(2, (int) desiredReplicaTargets.getDesiredScaleInTarget());
        assertEquals(1, (int)desiredReplicaTargets.getDesiredScaleOutTarget());

        EcsServiceScalerReplicaTarget ecsServiceScalerReplicaTarget = EcsServiceScalerUtils.getReplicaTarget(desiredReplicaTargets.getDesiredScaleInTarget(), desiredReplicaTargets.getDesiredScaleOutTarget(), currentDesiredReplicas, evaluationTimeInstant, evaluationTimeInstant - getMillisForNumberOfSeconds(10L), evaluationTimeInstant - getMillisForNumberOfSeconds(20L), ecsServiceConfig);
        assertNotNull(ecsServiceScalerReplicaTarget);
        assertFalse(ecsServiceScalerReplicaTarget.isScaleInDecision());
        assertFalse(ecsServiceScalerReplicaTarget.isScaleOutDecision());
        assertNull(ecsServiceScalerReplicaTarget.getReplicaTarget());
    }

    private Long getMillisForNumberOfSeconds(Long numberOfSeconds) {
        return numberOfSeconds * 1000L;
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
