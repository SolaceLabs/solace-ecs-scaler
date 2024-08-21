package com.solace.scalers.aws_ecs.util;

import com.solace.scalers.aws_ecs.model.ScalerConfig;
import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerDesiredReplicaTargets;
import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerObservations;
import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerReplicaTarget;
import lombok.extern.log4j.Log4j2;

import java.util.*;

@Log4j2
public class EcsServiceScalerUtils {
    // String values used to identify metric types
    public static final String METRIC_MSG_COUNT = "messageCount",
            METRIC_AVG_RX_RATE = "messageReceiveRate",
            METRIC_SPOOL_USAGE = "messageSpoolUsage";

    /**
     * Get the Max metric value in the stabilization window for each scale-out/in operations
     * and get the newest metric in case there are none in the window (e.g. stabilization window == 0)
     *
     * @param metricObservations
     * @param scaleOutMetricHorizon
     * @param scaleInMetricHorizon
     * @return
     */
    public static EcsServiceScalerObservations getEcsServiceScalerObservations(long evaluationTimeInstant, final Map<Long, Map<String, Long>> metricObservations, final long scaleOutMetricHorizon, final long scaleInMetricHorizon) {
        EcsServiceScalerObservations ecsServiceScalerObservations = new EcsServiceScalerObservations();
        for (Map.Entry<Long, Map<String, Long>> entry : metricObservations.entrySet()) {
            if (entry.getKey() > ecsServiceScalerObservations.getNewestObservationTime()) {
                ecsServiceScalerObservations.setNewestObservationTime(entry.getKey());
                ecsServiceScalerObservations.setNewestMessageCountObservation(entry.getValue().get(METRIC_MSG_COUNT));
                ecsServiceScalerObservations.setNewestAverageMessageRateObservation(entry.getValue().get(METRIC_AVG_RX_RATE));
                ecsServiceScalerObservations.setFoundNewObservation(true);
            }
            if (entry.getKey() > scaleOutMetricHorizon) {
                ecsServiceScalerObservations.setMaxMessageCountObservationUp(Math.max(ecsServiceScalerObservations.getMaxMessageCountObservationUp(), entry.getValue().get(METRIC_MSG_COUNT)));
                ecsServiceScalerObservations.setMaxAverageMessageRateObservationUp(Math.max(ecsServiceScalerObservations.getMaxAverageMessageRateObservationUp(), entry.getValue().get(METRIC_AVG_RX_RATE)));
                ecsServiceScalerObservations.setFoundWindowObservationUp(true);
            }
            if (entry.getKey() > scaleInMetricHorizon) {
                ecsServiceScalerObservations.setMaxMessageCountObservationDown(Math.max(ecsServiceScalerObservations.getMaxMessageCountObservationDown(), entry.getValue().get(METRIC_MSG_COUNT)));
                ecsServiceScalerObservations.setMaxAverageMessageRateObservationDown(Math.max(ecsServiceScalerObservations.getMaxAverageMessageRateObservationDown(), entry.getValue().get(METRIC_AVG_RX_RATE)));
                ecsServiceScalerObservations.setFoundWindowObservationDown(true);
            }
        }

        // Nothing found in the scaling window, use newest metric observation if it's < 5 minutes old
        // This is to account for users who may set very short stabilization windows or very long metric poll cycles
        // TODO - Make time window for "new" observations configurable, currently coded to 5 minutes
        if (!ecsServiceScalerObservations.isFoundWindowObservationUp()) {
            if (ecsServiceScalerObservations.isFoundNewObservation() &&
                    ecsServiceScalerObservations.getNewestObservationTime() > (evaluationTimeInstant - (5L * 60L * 1000L))) {
                ecsServiceScalerObservations.setMaxMessageCountObservationUp(ecsServiceScalerObservations.getNewestMessageCountObservation());
                ecsServiceScalerObservations.setMaxAverageMessageRateObservationUp(ecsServiceScalerObservations.getNewestAverageMessageRateObservation());
            } else {
                ecsServiceScalerObservations.setComputeScaleOut(false);
            }
        }
        if (!ecsServiceScalerObservations.isFoundWindowObservationDown()) {
            if (ecsServiceScalerObservations.isFoundNewObservation() && ecsServiceScalerObservations.getNewestObservationTime() > (evaluationTimeInstant - (5L * 60L * 1000L))) {
                ecsServiceScalerObservations.setMaxMessageCountObservationDown(ecsServiceScalerObservations.getNewestMessageCountObservation());
                ecsServiceScalerObservations.setMaxAverageMessageRateObservationDown(ecsServiceScalerObservations.getNewestAverageMessageRateObservation());
            } else {
                ecsServiceScalerObservations.setComputeScaleIn(false);
            }
        }

        return ecsServiceScalerObservations;
    }

    /**
     * Helper method to compute the desired replica count based on the most recent observations
     *
     * @param currentDesiredReplicas
     * @param ecsServiceScalerObservations
     * @param ecsServiceConfig
     * @return
     */
    public static EcsServiceScalerDesiredReplicaTargets getReplicaTargets(Integer currentDesiredReplicas, EcsServiceScalerObservations ecsServiceScalerObservations, ScalerConfig.EcsServiceConfig ecsServiceConfig) {
        List<Integer> scaleOutReplicaTargets = new ArrayList<>();
        List<Integer> scaleInReplicaTargets = new ArrayList<>();

        scaleOutReplicaTargets.add(computeDesiredReplicas(
                ecsServiceConfig.getScalerBehaviorConfig().getMessageCountTarget(),
                ecsServiceScalerObservations.getMaxMessageCountObservationUp(),
                ecsServiceConfig.getScalerBehaviorConfig().getMaxReplicaCount(),
                ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getMaxScaleStep(),
                true,
                currentDesiredReplicas
        ));
        scaleOutReplicaTargets.add(computeDesiredReplicas(
                ecsServiceConfig.getScalerBehaviorConfig().getMessageReceiveRateTarget(),
                ecsServiceScalerObservations.getMaxAverageMessageRateObservationUp(),
                ecsServiceConfig.getScalerBehaviorConfig().getMaxReplicaCount(),
                ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getMaxScaleStep(),
                true,
                currentDesiredReplicas
        ));

        scaleInReplicaTargets.add(computeDesiredReplicas(
                ecsServiceConfig.getScalerBehaviorConfig().getMessageCountTarget(),
                ecsServiceScalerObservations.getMaxMessageCountObservationDown(),
                ecsServiceConfig.getScalerBehaviorConfig().getMinReplicaCount(),
                ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getMaxScaleStep(),
                false,
                currentDesiredReplicas
        ));
        scaleInReplicaTargets.add(computeDesiredReplicas(
                ecsServiceConfig.getScalerBehaviorConfig().getMessageReceiveRateTarget(),
                ecsServiceScalerObservations.getMaxAverageMessageRateObservationDown(),
                ecsServiceConfig.getScalerBehaviorConfig().getMinReplicaCount(),
                ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getMaxScaleStep(),
                false,
                currentDesiredReplicas
        ));

        EcsServiceScalerDesiredReplicaTargets desiredReplicaTargets = new EcsServiceScalerDesiredReplicaTargets();

        scaleInReplicaTargets.stream().max(Integer::compare).ifPresent(desiredReplicaTargets::setDesiredScaleInTarget);
        scaleOutReplicaTargets.stream().max(Integer::compare).ifPresent(desiredReplicaTargets::setDesiredScaleOutTarget);

        return desiredReplicaTargets;

    }

    /**
     * Test for conditions to prevent scaling operation
     * @param desiredScaleInTarget
     * @param desiredScaleOutTarget
     * @param currentDesiredReplicas
     * @param evaluationTimeInstant
     * @param lastScaleOutTime
     * @param lastScaleInTime
     * @param ecsServiceConfig
     * @return
     */
    public static EcsServiceScalerReplicaTarget getReplicaTarget(Integer desiredScaleInTarget,
                                                                           Integer desiredScaleOutTarget,
                                                                           Integer currentDesiredReplicas,
                                                                           long evaluationTimeInstant,
                                                                           long lastScaleOutTime,
                                                                           long lastScaleInTime,
                                                                           ScalerConfig.EcsServiceConfig ecsServiceConfig
    ) {
        EcsServiceScalerReplicaTarget ecsServiceScalerReplicaTarget = new EcsServiceScalerReplicaTarget();

        // Let's make some decisions
        if (desiredScaleOutTarget != null && desiredScaleInTarget != null && desiredScaleInTarget < desiredScaleOutTarget) {
            // - We should not scale-in to a replica count that is less than the value computed for the scale-out target
            // - this scenario is unlikely but technically possible:
            // - if the scale-in stabilization window is smaller than the scale-out stabilization window
            desiredScaleInTarget = desiredScaleOutTarget;
        }
        if (desiredScaleOutTarget != null && desiredScaleOutTarget > currentDesiredReplicas) {

            log.info("Service={} -- Scaler computes desiredReplicas={} > currentReplicas={}",
                    LogUtils.getServiceDesignation(ecsServiceConfig),
                    desiredScaleOutTarget,
                    currentDesiredReplicas);
            if (lastScaleOutTime < (evaluationTimeInstant - (ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getCooldownPeriod() * 1000L))) {
                ecsServiceScalerReplicaTarget.setScaleOutDecision(true);
                ecsServiceScalerReplicaTarget.setReplicaTarget(desiredScaleOutTarget);
            } else {
                log.info("Service={} -- Service Scaling in Cooldown; Scale Out Operation blocked",
                        LogUtils.getServiceDesignation(ecsServiceConfig));
                return ecsServiceScalerReplicaTarget;
            }
        } else if (desiredScaleInTarget != null && desiredScaleInTarget < currentDesiredReplicas) {
            log.info("Service={} -- Scaler computes desiredReplicas={} < currentReplicas={}",
                    LogUtils.getServiceDesignation(ecsServiceConfig),
                    desiredScaleInTarget,
                    currentDesiredReplicas);
            if (lastScaleInTime < (evaluationTimeInstant - (ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getCooldownPeriod() * 1000L))) {
                ecsServiceScalerReplicaTarget.setScaleInDecision(true);
                ecsServiceScalerReplicaTarget.setReplicaTarget(desiredScaleInTarget);
            } else {
                log.info("Service={} -- Scaling in Cooldown; Scale In Operation blocked",
                        LogUtils.getServiceDesignation(ecsServiceConfig));
                return ecsServiceScalerReplicaTarget;
            }
        } else {
            log.info("Service={} -- Scaler computes Steady State - currentReplicas={}",
                    LogUtils.getServiceDesignation(ecsServiceConfig),
                    currentDesiredReplicas);
            return ecsServiceScalerReplicaTarget;
        }

        return ecsServiceScalerReplicaTarget;

    }

    /**
     * This method computes the desired target for a given metric
     * The method takes into account min/max replicas and max step size
     *
     * @param target         - target metric
     * @param observation    - observations reduced to a scalar
     * @param boundary       - min or max replicas for scale down/up
     * @param step           - max replica adjustment permitted
     * @param ScaleOut       - Set true for scale up computation; Set false for scale down
     * @param currentDesired - required to account for step size
     * @return
     */
    private static Integer computeDesiredReplicas(
            Integer target,
            Long observation,
            Integer boundary,
            Integer step,
            boolean ScaleOut,
            Integer currentDesired) {

        if (target == null ||
                target < 1 ||
                observation == null ||
                observation < 0L ||
                currentDesired == null ||
                currentDesired < 0 ||
                boundary == null ||
                boundary < 0) {
            return null;
        }
        if (step == null || step < 0) {
            step = 0;
        }

        // TODO - adjustmentFactor is to moderate scale-down operations; should make value a config item
        double adjustmentFactor = (ScaleOut ? 1.0 : 0.9);

        double rawNewDesired = (double) observation / ((double) target * adjustmentFactor);

        Integer newDesired = (int) Math.ceil(rawNewDesired);

        if (ScaleOut) {
            // apply step (up)
            if (step > 0) {
                newDesired = Math.min(newDesired, currentDesired + step);
            }
            // apply max boundary
            newDesired = Math.min(newDesired, boundary);
        } else {
            // apply step (down)
            if (step > 0) {
                newDesired = Math.max(newDesired, currentDesired - step);
            }
            // apply min boundary
            newDesired = Math.max(newDesired, boundary);
        }

        return newDesired;
    }
}
