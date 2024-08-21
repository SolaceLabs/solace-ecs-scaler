package com.solace.scalers.aws_ecs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerDesiredReplicaTargets;
import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerObservations;
import com.solace.scalers.aws_ecs.model.util.ecs.EcsServiceScalerReplicaTarget;
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

        EcsServiceScalerDesiredReplicaTargets ecsServiceScalerDesiredReplicaTargets = EcsServiceScalerUtils.getReplicaTargets(currentDesiredReplicas,ecsServiceScalerObservations,ecsServiceConfig);

        // We have our scale-in / scale-out targets, make some decisions and scale
        scaleEcsService(ecsServiceScalerDesiredReplicaTargets.getDesiredScaleInTarget(), ecsServiceScalerDesiredReplicaTargets.getDesiredScaleOutTarget(), currentDesiredReplicas, evaluationTimeInstant);
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
     * If none exit, then scale out/in. Calls AWS ECS API to update the desired task count directly
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

        EcsServiceScalerReplicaTarget replicaTarget = EcsServiceScalerUtils.getReplicaTarget(
                desiredScaleInTarget,
                desiredScaleOutTarget,
                currentDesiredReplicas,
                evaluationTimeInstant,
                lastScaleOutTime,
                lastScaleInTime,
                ecsServiceConfig);

        if(!replicaTarget.isScaleInDecision() && !replicaTarget.isScaleOutDecision()) {
            return;
        }

        logger.info("Service={} -- Preparing to scale from {} to {} instances",
                            LogUtils.getServiceDesignation(ecsServiceConfig),
                            currentDesiredReplicas,
                            replicaTarget.getReplicaTarget() );

        // If we're here, we're scaling something
        try {
            UpdateServiceRequest updateServiceRequest = 
                                    new UpdateServiceRequest()
                                            .withCluster(ecsServiceConfig.getEcsCluster())
                                            .withService(ecsServiceConfig.getEcsService())
                                            .withDesiredCount(replicaTarget.getReplicaTarget());

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

            lastScaledReplicaCount = replicaTarget.getReplicaTarget();
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

        if ( replicaTarget.isScaleOutDecision() ) {
            lastScaleOutTime = System.currentTimeMillis();
        }
        if ( replicaTarget.isScaleInDecision() ) {
            lastScaleInTime = System.currentTimeMillis();
        }
    }


}
