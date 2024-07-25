package com.solace.scalers.aws_ecs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerObservations;
import com.solace.scalers.aws_ecs.util.EcsServiceScalerUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.UpdateServiceRequest;
import com.amazonaws.services.ecs.model.UpdateServiceResult;
import com.solace.scalers.aws_ecs.model.ScalerConfig.EcsServiceConfig;
import com.solace.scalers.aws_ecs.util.LogUtils;

/**
 * Class to retain metrics and perform scaling operations.
 * TODO - Add messageSpoolUsage to active metrics
 */
public class EcsServiceScaler {
    


    private static final Logger     logger = LogManager.getLogger( EcsServiceScaler.class );

    // TODO - Evaluate if default AmazonECS client is acceptable for production
    private static final AmazonECS  ecs = AmazonECSClientBuilder.defaultClient();

    private final Map<Long, Map<String, Long>> metricObservations = new ConcurrentHashMap<>( 250, 0.75F, 3 );

    private volatile EcsServiceConfig ecsServiceConfig;

    // Used to compute cooldown periods; can be distinct for scale-in/scale-out
    private long lastScaleOutTime = 0L;
    private long lastScaleInTime = 0L;

    // lastScaledReplicaCount is needed because replica counts returned from Cloudwatch can
    // take a while before synchronizing with the last update request to the service.
    // When lastScaledReplicaCount == running replica count, a subsequent scaling operation may proceed
    // If lastScaledReplicaCount != running replica count, then need to wait
    private int     lastScaledReplicaCount = 0;
    private boolean lastScaledReplicaCountInitialized = false;

    /**
     * Constructor passing EcsServiceConfig object
     * @param ecsServiceConfig
     */
    public EcsServiceScaler( EcsServiceConfig ecsServiceConfig ) {
        this.ecsServiceConfig = ecsServiceConfig;
    }

    public Map<Long, Map<String, Long>> getMetricObservations() {
        return this.metricObservations;
    }

    public EcsServiceConfig getEcsServiceConfig() {
        return ecsServiceConfig;
    }

    /*
     * Method purges all metrics older than 2X the larger stabilization window
     * Or 120 seconds, whichever is greater
     */
    public void purgeOldMetrics() {

        final long retainMetricsWindow = Math.max(
                Math.max(
                        ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getStabilizationWindow(),
                        ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getStabilizationWindow() 
                        ) * 2L * 1000L,
                120L * 1000L );                 // Retain for a minimum of 120 seconds

        int  counter = 0;
        long timeInstance = System.currentTimeMillis();
        for ( Long key : metricObservations.keySet() ) {
            if ( ( key + retainMetricsWindow ) < timeInstance ) {
                metricObservations.remove( key );
                counter++;
            }
        }
        logger.debug( "Service={} -- Purged {} metric entries older than {} seconds", 
                                LogUtils.getServiceDesignation(ecsServiceConfig), 
                                counter, 
                                ( retainMetricsWindow / 1000L ) );
    }

    /**
     * Method performs the following actions:
     * - Determine if the scaling is possible based on the (known) current state of the target service and the scaler
     * - Compute the target replica count for scale-up and scale-down based upon accumulated metrics
     * - Call private scaleEcsService() to perform scaling operation
     * @param currentDesiredReplicas
     * @param currentRunningReplicas
     * @throws Exception
     */
    public void scalingOperation( Integer currentDesiredReplicas, Integer currentRunningReplicas ) throws Exception {

        if(!determineIfScalingOperationIsPossible(currentDesiredReplicas, currentRunningReplicas)) {
            return;
        }

        if ( !lastScaledReplicaCountInitialized ) {
            lastScaledReplicaCount = currentRunningReplicas;
            lastScaledReplicaCountInitialized = true;
        }

        final long  evaluationTimeInstant = System.currentTimeMillis();

        final long  scaleOutMetricHorizon = evaluationTimeInstant - ( ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getStabilizationWindow() * 1000L ),
                scaleInMetricHorizon = evaluationTimeInstant - ( ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getStabilizationWindow() * 1000L );

        EcsServiceScalerObservations ecsServiceScalerObservations = EcsServiceScalerUtils.getEcsServiceScalerObservations(evaluationTimeInstant, metricObservations, scaleOutMetricHorizon, scaleInMetricHorizon);


        if ( !ecsServiceScalerObservations.isComputeScaleOut() && !ecsServiceScalerObservations.isComputeScaleIn()) {
            logger.warn( "Service={} -- No recent metrics to use for scaling computations, skipping this cycle",
                            LogUtils.getServiceDesignation(ecsServiceConfig) );
            return;
        }

        Map<String, List<Integer>> replicaTargets = EcsServiceScalerUtils.getReplicaTargets(currentDesiredReplicas,ecsServiceScalerObservations,ecsServiceConfig);

        
        final Integer desiredScaleInTarget = getMaximumFromList(replicaTargets.get(EcsServiceScalerUtils.SCALE_IN_REPLICA_TARGET_KEY));
        final Integer desiredScaleOutTarget = getMaximumFromList(replicaTargets.get(EcsServiceScalerUtils.SCALE_OUT_REPLICA_TARGET_KEY));

        // We have our scale-in / scale-out targets, make some decisions and scale
        scaleEcsService(desiredScaleInTarget, desiredScaleOutTarget, currentDesiredReplicas, evaluationTimeInstant);
    }

    /**
     * Determine if a scaling operation is possible. We must know both the current desired replica count and the current replica count of the ecs service before performing a scaling operation.
     * Additionally, a scaling operation must not already by in progress
     * @param currentDesiredReplicas
     * @param currentRunningReplicas
     * @return boolean
     */
    private boolean determineIfScalingOperationIsPossible(Integer currentDesiredReplicas, Integer currentRunningReplicas ) {
        // Check state to see if a scaling operation is possible at the current time
        // TODO - Need a timeout for this condition, otherwise could end up in a state where additional scaling never happens
        if ( lastScaledReplicaCountInitialized && currentRunningReplicas != null && lastScaledReplicaCount != currentRunningReplicas ) {
            logger.info( "Service={} -- Scaling Operation in Progress - Waiting for lastScaledReplicaCount={} == currentRunningReplicas={}",
                    LogUtils.getServiceDesignation(ecsServiceConfig),
                    lastScaledReplicaCount,
                    currentRunningReplicas );
            return false;
        }
        if ( currentDesiredReplicas == null || currentRunningReplicas == null ) {
            logger.warn( "Service={} -- Current replica values not known - currentDesiredReplcas={} currentRunningReplicas={}; Both must be non-null to proceed with scaling",
                    LogUtils.getServiceDesignation(ecsServiceConfig),
                    currentDesiredReplicas,
                    currentRunningReplicas );
            return false;
        }
        if ( currentDesiredReplicas != currentRunningReplicas ) {
            logger.info( "Service={} -- Scaled Service not in steady state - currentDesiredReplcas={} currentRunningReplicas={}; Values must be equal to proceed with scaling",
                    LogUtils.getServiceDesignation(ecsServiceConfig),
                    currentDesiredReplicas,
                    currentRunningReplicas );
            return false;
        }
        return true;
    }

    /**
     * Method tests for conditions that should prevent a scaling operation
     * If none exixt, then scale out/in. Calls AWS ECS API to update the desired task count directly
     * @param desiredScaleInTarget
     * @param desiredScaleOutTarget
     * @param currentDesiredReplicas
     * @param evaluationTimeInstant
     * @throws Exception
     */
    private void scaleEcsService( 
                        Integer desiredScaleInTarget, 
                        Integer desiredScaleOutTarget, 
                        Integer currentDesiredReplicas,
                        long    evaluationTimeInstant ) throws Exception {

        boolean     scaleOutDecision = false,
                    scaleInDecision = false;
        
        Integer     replicaTarget = null;

        // Let's make some decisions
        if ( desiredScaleOutTarget != null && desiredScaleInTarget != null && desiredScaleInTarget < desiredScaleOutTarget ) {
            // - We should not scale-in to a replica count that is less than the value computed for the scale-out target
            // - this scenario is unlikely but technically possible:
            // - if the scale-in stabilization window is smaller than the scale-out stabilization window
            desiredScaleInTarget = desiredScaleOutTarget;
        }
        if ( desiredScaleOutTarget != null && desiredScaleOutTarget > currentDesiredReplicas ) {

            logger.info("Service={} -- Scaler computes desiredReplicas={} > currentReplicas={}",
                                LogUtils.getServiceDesignation(ecsServiceConfig),
                                desiredScaleOutTarget, 
                                currentDesiredReplicas );
            if ( lastScaleOutTime < ( evaluationTimeInstant - ( ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getCooldownPeriod() * 1000L ) ) ) {
                scaleOutDecision = true;
                replicaTarget = desiredScaleOutTarget;
            } else {
                logger.info("Service={} -- Service Scaling in Cooldown; Scale Out Operation blocked",
                                LogUtils.getServiceDesignation(ecsServiceConfig) );
                return;
            }
        } else if ( desiredScaleInTarget != null && desiredScaleInTarget < currentDesiredReplicas ) {
            logger.info("Service={} -- Scaler computes desiredReplicas={} < currentReplicas={}",
                                LogUtils.getServiceDesignation(ecsServiceConfig),
                                desiredScaleInTarget, 
                                currentDesiredReplicas );
            if ( lastScaleInTime < ( evaluationTimeInstant - ( ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getCooldownPeriod() * 1000L ) ) ) {
                scaleInDecision = true;
                replicaTarget = desiredScaleInTarget;
            } else {
                logger.info("Service={} -- Scaling in Cooldown; Scale In Operation blocked",
                                LogUtils.getServiceDesignation(ecsServiceConfig) );
                return;
            }
        } else {
            logger.info( "Service={} -- Scaler computes Steady State - currentReplicas={}",
                                LogUtils.getServiceDesignation(ecsServiceConfig),
                                currentDesiredReplicas );
            return;
        }

        logger.info("Service={} -- Preparing to scale from {} to {} instances",
                            LogUtils.getServiceDesignation(ecsServiceConfig),
                            currentDesiredReplicas,
                            replicaTarget );

        // If we're here, we're scaling something
        try {
            UpdateServiceRequest updateServiceRequest = 
                                    new UpdateServiceRequest()
                                            .withCluster(ecsServiceConfig.getEcsCluster())
                                            .withService(ecsServiceConfig.getEcsService())
                                            .withDesiredCount(replicaTarget);

            logger.debug( "Service={} -- Update Request Body:\n" + updateServiceRequest.toString(),
                                LogUtils.getServiceDesignation(ecsServiceConfig) );
            UpdateServiceResult updateServiceResult = ecs.updateService(updateServiceRequest);

            // TODO - Keep a running count of failures and exit the scaler if > threshold
            // TODO - Verify that 200 <= result <= 204 are all success
            if (    updateServiceResult.getSdkHttpMetadata().getHttpStatusCode() < 200 &&
                    updateServiceResult.getSdkHttpMetadata().getHttpStatusCode() > 204 ) {
                logger.error( "Service={} -- HTTP Status Code={}", 
                                LogUtils.getServiceDesignation(ecsServiceConfig),
                                updateServiceResult.getSdkHttpMetadata().getHttpStatusCode() );
                logger.error( "Service={} -- Scaling Operation FAILED to update ECS Service to {} instances",
                                LogUtils.getServiceDesignation(ecsServiceConfig),
                                replicaTarget );
                return;
            }

            lastScaledReplicaCount = replicaTarget;
        } catch ( Exception exc ) {
            logger.error( "Service={} -- Exception attempting to update from {} to {} instances",
                                LogUtils.getServiceDesignation(ecsServiceConfig),
                                currentDesiredReplicas,
                                replicaTarget );
            logger.error( "Service={} -- Exception Type: {}",
                                LogUtils.getServiceDesignation(ecsServiceConfig), 
                                exc.getClass() );
            logger.error( "Service={} -- Exception Message: {}", 
                                LogUtils.getServiceDesignation(ecsServiceConfig), 
                                exc.getMessage() );
            logger.error( exc.getStackTrace() );
            throw exc;
        }

        logger.info("Service={} -- Successfully scaled to {} instances",
                                LogUtils.getServiceDesignation(ecsServiceConfig), 
                                replicaTarget );

        if ( scaleOutDecision ) {
            lastScaleOutTime = System.currentTimeMillis();
        }
        if ( scaleInDecision ) {
            lastScaleInTime = System.currentTimeMillis();
        }
    }

    /**
     * Iterate over array of Integer objects and return the maximum or null
     * @param eval
     * @return
     */
    private Integer getMaximumFromList( List<Integer> eval ) {
        Integer maxValue = null;

        for ( Integer i : eval ) {
            if ( i != null && ( maxValue == null ? true : i > maxValue ) ) {
                maxValue = i;
            }
        }

        return maxValue;
    }


}
