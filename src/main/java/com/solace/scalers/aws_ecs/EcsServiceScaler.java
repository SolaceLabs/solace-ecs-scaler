package com.solace.scalers.aws_ecs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    
    // String values used to identify metric types
    public static final String      METRIC_MSG_COUNT    = "messageCount",
                                    METRIC_AVG_RX_RATE  = "messageReceiveRate",
                                    METRIC_SPOOL_USAGE  = "messageSpoolUsage";

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

        // Check state to see if a scaling operation is possible at the current time
        // TODO - Need a timeout for this condition, otherwise could end up in a state where additional scaling never happens
        if ( lastScaledReplicaCountInitialized && currentRunningReplicas != null && lastScaledReplicaCount != currentRunningReplicas ) {
            logger.info( "Service={} -- Scaling Operation in Progress - Waiting for lastScaledReplicaCount={} == currentRunningReplicas={}",
                            LogUtils.getServiceDesignation(ecsServiceConfig),
                            lastScaledReplicaCount,
                            currentRunningReplicas );
            return;
        }
        if ( currentDesiredReplicas == null || currentRunningReplicas == null ) {
            logger.warn( "Service={} -- Current replica values not known - currentDesiredReplcas={} currentRunningReplicas={}; Both must be non-null to proceed with scaling",
                            LogUtils.getServiceDesignation(ecsServiceConfig),
                            currentDesiredReplicas,
                            currentRunningReplicas );
            return;
        }
        if ( currentDesiredReplicas != currentRunningReplicas ) {
            logger.info( "Service={} -- Scaled Service not in steady state - currentDesiredReplcas={} currentRunningReplicas={}; Values must be equal to proceed with scaling",
                            LogUtils.getServiceDesignation(ecsServiceConfig),
                            currentDesiredReplicas,
                            currentRunningReplicas );
            return;
        }
        if ( !lastScaledReplicaCountInitialized ) {
            lastScaledReplicaCount = currentRunningReplicas;
            lastScaledReplicaCountInitialized = true;
        }

        final long  evaluationTimeInstant = System.currentTimeMillis();
        boolean     computeScaleOut = true, 
                    computeScaleIn = true;

        long        maxMessageCountObservationUp = 0L, 
                    maxAverageMessageRateObservationUp = 0L,
                    maxMessageCountObservationDown = 0L,
                    maxAverageMessageRateObservationDown = 0L;

        boolean     foundWindowObservationUp = false, 
                    foundWindowObservationDown = false, 
                    foundNewObservation = false;

        final long  ScaleOutMetricHorizon = evaluationTimeInstant - ( ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getStabilizationWindow() * 1000L ),
                    ScaleInMetricHorizon = evaluationTimeInstant - ( ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getStabilizationWindow() * 1000L );

        long        newestObservationTime = 0L;
        long        newestMessageCountObservation = 0L, 
                    newestMessageRateObservation = 0L;

        // Get the Max metric value in the stabilization window for each scale-out/in operations
        // And get the newest metric in case there are none in the window (e.g. stabilization window == 0)
        for ( Map.Entry<Long, Map<String, Long>> entry : metricObservations.entrySet() ) {
            if ( entry.getKey() > newestObservationTime ) {
                newestObservationTime = entry.getKey();
                newestMessageCountObservation = entry.getValue().get(METRIC_MSG_COUNT);
                newestMessageRateObservation = entry.getValue().get(METRIC_AVG_RX_RATE);
                foundNewObservation = true;
            }
            if ( entry.getKey() > ScaleOutMetricHorizon ) {
                maxMessageCountObservationUp = Math.max( maxMessageCountObservationUp, entry.getValue().get( METRIC_MSG_COUNT ) );
                maxAverageMessageRateObservationUp = Math.max( maxAverageMessageRateObservationUp, entry.getValue().get( METRIC_AVG_RX_RATE ) );
                foundWindowObservationUp = true;
            }
            if ( entry.getKey() > ScaleInMetricHorizon ) {
                maxMessageCountObservationDown = Math.max( maxMessageCountObservationDown, entry.getValue().get( METRIC_MSG_COUNT ) );
                maxAverageMessageRateObservationDown = Math.max( maxAverageMessageRateObservationDown, entry.getValue().get( METRIC_AVG_RX_RATE ) );
                foundWindowObservationDown = true;
            }
        }

        // Nothing found in the scaling window, use newest metric observation if it's < 5 minutes old
        // This is to account for users who may set very short stabilization windows or very long metric poll cycles
        // TODO - Make time window for "new" observations configurable, currently coded to 5 minutes
        if ( !foundWindowObservationUp ) {
            if ( foundNewObservation && newestObservationTime > ( evaluationTimeInstant - ( 5L * 60L * 1000L ) ) ) {
                maxMessageCountObservationUp = newestMessageCountObservation;
                maxAverageMessageRateObservationUp = newestMessageRateObservation;
            } else {
                computeScaleOut = false;
            }
        }
        if ( !foundWindowObservationDown ) {
            if ( foundNewObservation && newestObservationTime > ( evaluationTimeInstant - ( 5L * 60L * 1000L ) ) ) {
                maxMessageCountObservationDown = newestMessageCountObservation;
                maxAverageMessageRateObservationDown = newestMessageRateObservation;
            } else {
                computeScaleIn = false;
            }
        }
        if ( !computeScaleOut && !computeScaleIn ) {
            logger.warn( "Service={} -- No recent metrics to use for scaling computations, skipping this cycle",
                            LogUtils.getServiceDesignation(ecsServiceConfig) );
            return;
        }

        // Add largest metric targets found to value array for comparison
        List<Integer> scaleOutReplicaTargets = new ArrayList<Integer>();
        List<Integer> scaleInReplicaTargets = new ArrayList<Integer>();
        scaleOutReplicaTargets.add( computeDesiredReplicas(
                        ecsServiceConfig.getScalerBehaviorConfig().getMessageCountTarget(),
                        maxMessageCountObservationUp,
                        ecsServiceConfig.getScalerBehaviorConfig().getMaxReplicaCount(),
                        ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getMaxScaleStep(),
                        true,
                        currentDesiredReplicas
                        ) );
        scaleOutReplicaTargets.add( computeDesiredReplicas(
                        ecsServiceConfig.getScalerBehaviorConfig().getMessageReceiveRateTarget(),
                        maxAverageMessageRateObservationUp,
                        ecsServiceConfig.getScalerBehaviorConfig().getMaxReplicaCount(),
                        ecsServiceConfig.getScalerBehaviorConfig().getScaleOutConfig().getMaxScaleStep(),
                        true,
                        currentDesiredReplicas
                        ) );

        scaleInReplicaTargets.add( computeDesiredReplicas(
                        ecsServiceConfig.getScalerBehaviorConfig().getMessageCountTarget(),
                        maxMessageCountObservationDown,
                        ecsServiceConfig.getScalerBehaviorConfig().getMinReplicaCount(),
                        ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getMaxScaleStep(),
                        false,
                        currentDesiredReplicas
                        ) );
        scaleInReplicaTargets.add( computeDesiredReplicas(
                        ecsServiceConfig.getScalerBehaviorConfig().getMessageReceiveRateTarget(),
                        maxAverageMessageRateObservationDown,
                        ecsServiceConfig.getScalerBehaviorConfig().getMinReplicaCount(),
                        ecsServiceConfig.getScalerBehaviorConfig().getScaleInConfig().getMaxScaleStep(),
                        false,
                        currentDesiredReplicas
                        ) );
        
        final Integer desiredScaleInTarget = getMaximumFromList(scaleInReplicaTargets);
        final Integer desiredScaleOutTarget = getMaximumFromList(scaleOutReplicaTargets);

        // We have our scale-in / scale-out targets, make some decisions and scale
        scaleEcsService(desiredScaleInTarget, desiredScaleOutTarget, currentDesiredReplicas, evaluationTimeInstant);
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

    /**
     * This method computes the desired target for a given metric
     * The method takes into account min/max replicas and max step size
     * @param target - target metric
     * @param observation - observations reduced to a scalar
     * @param boundary - min or max replicas for scale down/up
     * @param step - max replica adjustment permitted
     * @param ScaleOut - Set true for scale up computation; Set false for scale down
     * @param currentDesired - required to account for step size
     * @return
     */
    private Integer computeDesiredReplicas( 
                        Integer target, 
                        Long    observation, 
                        Integer boundary, 
                        Integer step, 
                        boolean ScaleOut, 
                        Integer currentDesired ) {

        if (    target == null || 
                target < 1 || 
                observation == null || 
                observation < 0L || 
                currentDesired == null ||
                currentDesired < 0 ||
                boundary == null ||
                boundary < 0 ) {
            return null;
        }
        if ( step == null || step < 0 ) {
            step = 0;
        }

        // TODO - adjustmentFactor is to moderate scale-down operations; should make value a config item
        double adjustmentFactor = ( ScaleOut ? 1.0 : 0.9 );

        double rawNewDesired = ( double )observation / ( ( double )target * adjustmentFactor );

        Integer newDesired = ( int )Math.ceil( rawNewDesired );

        if ( ScaleOut ) {
            // apply step (up)
            if ( step > 0 ) {
                newDesired = Math.min( newDesired, currentDesired + step );
            }
            // apply max boundary
            newDesired = Math.min( newDesired, boundary );
        } else {
            // apply step (down)
            if ( step > 0 ) {
                newDesired = Math.max( newDesired, currentDesired - step );
            }
            // apply min boundary
            newDesired = Math.max( newDesired, boundary );
        }

        return newDesired;
    }
}
