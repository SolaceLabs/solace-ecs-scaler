package com.solace.scalers.aws_ecs;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.solace.scalers.aws_ecs.model.ScalerConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfigParser;
import com.solace.scalers.aws_ecs.model.ScalerConfig.EcsServiceConfig;
import com.solace.scalers.aws_ecs.util.EcsServiceScalerUtils;
import com.solace.scalers.aws_ecs.util.HealthUtil;
import com.solace.scalers.aws_ecs.util.LogUtils;
import com.solace.scalers.aws_ecs.util.SolaceQueueMonitorUtils;

import lombok.extern.log4j.Log4j2;

/**
 * Class to scale ECS Services for applications bound to Solace Queues
 * - Obtains metrics from Solace SEMP
 * - Obtains ECS service task counts from Cloudwatch
 * - Scales applications based on observations and configuration settings
 */
@Log4j2
public class SolaceEcsAutoscalerApp
{
    private static final long       INIT_POLLING_DELAY_SEMP = 5L,
                                    INIT_POLLING_DELAY_PURGE = 10L,
                                    INIT_POLLING_DELAY_ECS_METRICS = 2L;

    private static final long       POLLING_INTERVAL_PURGE = 120L,
                                    POLLING_INTERVAL_ECS_METRICS = 20L,
                                    SCALING_OPERATION_INTERVAL_MILLIS = 10000L,
                                    SHUTDOWN_THREAD_DELAY_MILLIS = 5000L;

    // configuration file arg
    public static final String		ARG_SCALER_CONFIG = "--config-file=";

    // shutdown if set to false
    private static volatile boolean isRunning = true;

    /**
     * Accepts a config file in format defined by ScalerConfig --> --config-file=path/to/file.properties
     * @param args
     * @throws MalformedURLException
     * @throws InterruptedException
     * @throws IOException
     */
    public static void main( String[] args ) throws MalformedURLException, InterruptedException, IOException
    {
        // Get configuration file from command line argument
        String configFile = "src/main/resources/scaler-config.yaml";
    	for (String arg : args) {
			if (arg.startsWith(ARG_SCALER_CONFIG) && arg.length() > ARG_SCALER_CONFIG.length()) {
				configFile = arg.replace(ARG_SCALER_CONFIG, "");
			}
		}

        // Parse config file
        ScalerConfig scalerConfig = null;
        try {
            scalerConfig = ScalerConfigParser.
                                validateScalerConfig(
                                    ScalerConfigParser.parseScalerConfig( configFile )
                                );
        } catch ( Exception exc ) {
            log.error( "Could not parse scaler config at: {} -- Exiting", configFile);
            System.exit(1);
        }

        log.info("Starting Solace/ECS Scaler -- Monitoring Solace Messaging Service at URL: {} / MsgVpn: {}",
                scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig().getBrokerSempUrl(),
                scalerConfig.getBrokerConfig().getMsgVpnName());

        // Create Scaler Object maps indexed by queueName: SolaceQueueMonitor, EcsServiceScaler, and EcsServiceMetrics
        final Map<String, SolaceQueueMonitor>   solaceQueueMonitorMap   = new ConcurrentHashMap<>( scalerConfig.getEcsServiceConfig().size(), 0.75F, 2 );
        final Map<String, EcsServiceScaler>     ecsServiceScalerMap     = new ConcurrentHashMap<>( scalerConfig.getEcsServiceConfig().size(), 0.75F, 3 );
        final Map<String, EcsServiceMetrics>    ecsServiceMetricsMap    = new ConcurrentHashMap<>( scalerConfig.getEcsServiceConfig().size(), 0.75F, 3 );

        // Load up the maps with one entry for each ECS Service/Queue in the configuration file
        // Entries execute tasks and track scaler state for corresponding services
        for ( EcsServiceConfig ecsServiceConfig : scalerConfig.getEcsServiceConfig() ) {
            try {
                solaceQueueMonitorMap.put( 
                                ecsServiceConfig.getQueueName(), 
                                SolaceQueueMonitorUtils.createSolaceQueueMonitorWithBasicAuth( 
                                    scalerConfig.getBrokerConfig(), 
                                    ecsServiceConfig)
                                );
                ecsServiceScalerMap.put(
                                    ecsServiceConfig.getQueueName(), 
                                    new EcsServiceScaler( ecsServiceConfig )
                                );
                ecsServiceMetricsMap.put(
                                    ecsServiceConfig.getQueueName(), 
                                    new EcsServiceMetrics(ecsServiceConfig)
                                );
                log.info( "Configured Scaler for Service={} -- on Solace Queue: {}",
                                LogUtils.getServiceDesignation(ecsServiceConfig), ecsServiceConfig.getQueueName() );
            } catch ( Exception exc ) {
                log.error("Error configuring Scaler for Service={} on Solace Queue: {}",
                                LogUtils.getServiceDesignation(ecsServiceConfig), ecsServiceConfig.getQueueName());
                log.error("Exiting Program");
                throw exc;
            }
        }

        /**
         * Configuration is complete
         * Start up the scaler processes!
         */
        
        // CREATE THREAD -- Monitor Solace Service using SEMP
        // Configure and start thread to get Queue Metrics from SEMP
        ScheduledExecutorService solaceSempQueryThread = Executors.newSingleThreadScheduledExecutor();
        solaceSempQueryThread.scheduleAtFixedRate(() -> {

            for ( Map.Entry<String, SolaceQueueMonitor> entry : solaceQueueMonitorMap.entrySet() ) {

                if ( !isRunning ) return;

                try {
//                    TODO: Check Broker Status
                    Map<String, Long> metricsEntry = SolaceQueueMonitorUtils.getQueueMetricsFromQueueResponse( entry.getValue().getSempMonitorForQueue() );
                    ecsServiceScalerMap.get( entry.getValue().getQueueName() ).getMetricObservations().put( System.currentTimeMillis(), metricsEntry );
                    log.info( "Service={} -- Stored Metrics: {}: {}, {}: {}, {}: {}",
                                LogUtils.getServiceDesignation( ecsServiceScalerMap.get( entry.getValue().getQueueName() ).getEcsServiceConfig() ),
                                EcsServiceScalerUtils.METRIC_MSG_COUNT, metricsEntry.get(EcsServiceScalerUtils.METRIC_MSG_COUNT),
                                EcsServiceScalerUtils.METRIC_AVG_RX_RATE, metricsEntry.get(EcsServiceScalerUtils.METRIC_AVG_RX_RATE),
                            EcsServiceScalerUtils.METRIC_SPOOL_USAGE, metricsEntry.get(EcsServiceScalerUtils.METRIC_SPOOL_USAGE) );
                } catch ( Exception exc ) {
                    log.error( "Service={} -- Error Obtaining/Storing Metrics -- Exception: {}",
                                LogUtils.getServiceDesignation( ecsServiceScalerMap.get( entry.getValue().getQueueName() ).getEcsServiceConfig() ),
                                exc.getMessage() );
                    log.error( "Exiting" );
                    isRunning = false;
                }
            }

        }, INIT_POLLING_DELAY_SEMP, ( long )scalerConfig.getBrokerConfig().getPollingInterval(), TimeUnit.SECONDS);

        // CREATE THREAD -- Purge Old Metrics
        // Configure+Exec thread to purge old metrics so they don't get out of hand
        ScheduledExecutorService purgeMetricsThread = Executors.newSingleThreadScheduledExecutor();
        purgeMetricsThread.scheduleAtFixedRate(() -> {
            for ( Map.Entry<String, EcsServiceScaler> ecsScalerEntry : ecsServiceScalerMap.entrySet() ) {
                if ( !isRunning ) return;
                ecsScalerEntry.getValue().purgeOldMetrics();
            }
        }, INIT_POLLING_DELAY_PURGE, POLLING_INTERVAL_PURGE, TimeUnit.SECONDS);

        // CREATE THREAD -- Monitor ECS Metrics from Cloudwatch
        // TODO - Make ECS Metrics Monitor polling interval configurable
        ScheduledExecutorService getEcsMetricsThread = Executors.newSingleThreadScheduledExecutor();
        getEcsMetricsThread.scheduleAtFixedRate(() -> {
            for ( Map.Entry<String, EcsServiceMetrics> ecsServiceMetricsEntry : ecsServiceMetricsMap.entrySet() ) {
                if ( !isRunning ) return;
                ecsServiceMetricsEntry.getValue().retrieveMetricsFromCloudwatch();
            }
        }, INIT_POLLING_DELAY_ECS_METRICS, POLLING_INTERVAL_ECS_METRICS, TimeUnit.SECONDS);

        // Intercept SIGTERM and SIGINT for graceful shutdown
        var shutdownListener = new Thread() {
            public void run() {
                isRunning = false;
                try {
                    log.info("*** Shutdown Signal Detected -- Shutting Down Scaler ***");
                    Thread.sleep(SHUTDOWN_THREAD_DELAY_MILLIS);
                    if (!purgeMetricsThread.isShutdown()) {
                        purgeMetricsThread.shutdown();
                    }
                    if (!solaceSempQueryThread.isShutdown()) {
                        solaceSempQueryThread.shutdown();
                    }
                    if (!getEcsMetricsThread.isShutdown()) {
                        getEcsMetricsThread.shutdown();
                    }
                } catch (InterruptedException e) { }
            }
        };
        Runtime.getRuntime().addShutdownHook(shutdownListener);

        // Delay main thread (and scaling operations) for the greater of: 
        //          60 seconds OR the SEMP Polling period
        //          + INIT_POLLING_DELAY_SEMP + 5 Seconds
        // Should ensure that SEMP metrics have been acquired
        final long initializationDelay = 
                            System.currentTimeMillis() + 
                            ( INIT_POLLING_DELAY_SEMP + 5L ) * 1000L +
                            Math.max( ( long )scalerConfig.getBrokerConfig().getPollingInterval(), 60L ) * 1000L;
        boolean isScaling = false;
        while ( !isScaling && isRunning ) {
            if ( System.currentTimeMillis() < initializationDelay ) {
                log.info( "Metrics are initializing, no scaling operations will be performed" );
            } else {
                log.info( "Metrics initialization complete -- Going Active!" );
                isScaling = true;
            }
            // TODO - make this value configurable -- initializationPeriod
            Thread.sleep(SCALING_OPERATION_INTERVAL_MILLIS);
        }

        // MAIN THREAD -- Scaling Operations
        // TODO - Make scaling operation cycle configurable (currently 10 seconds)
        while ( isRunning ) {
            // Creates tmp healthcheck file
            HealthUtil.updateHealthStatus(isRunning);

            for ( Map.Entry<String, EcsServiceScaler> ecsServiceScalerEntry : ecsServiceScalerMap.entrySet() ) {
                try {
                    if ( !isRunning ) break;
                    ecsServiceScalerEntry.getValue().scalingOperation(
                            ecsServiceMetricsMap.get(ecsServiceScalerEntry.getKey()).getDesiredTaskCount(), 
                            ecsServiceMetricsMap.get(ecsServiceScalerEntry.getKey()).getRunningTaskCount() );
                } catch ( Exception exc ) {
                    log.warn( "Service={} -- Caught exception from ECS Service Scaler -- Shutting down", LogUtils.getServiceDesignation(ecsServiceScalerEntry.getValue().getEcsServiceConfig()) );
                    isRunning = false;
                }
            }

            // Exit before thread sleep if done
            if ( !isRunning ) {
                // Delete tmp healthcheck file
                HealthUtil.updateHealthStatus(isRunning);
                break;
            }

            // TODO - make this value configurable -- scaler interval
            Thread.sleep(SCALING_OPERATION_INTERVAL_MILLIS);
        }

        System.exit(0);
    }
}
