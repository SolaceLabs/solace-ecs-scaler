package com.solace.scalers.aws_ecs;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricDataResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.cloudwatch.model.MetricDataQuery;
import com.amazonaws.services.cloudwatch.model.MetricDataResult;
import com.amazonaws.services.cloudwatch.model.MetricStat;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.solace.scalers.aws_ecs.model.ScalerConfig.EcsServiceConfig;
import com.solace.scalers.aws_ecs.util.LogUtils;


/**
 * Class designed to retrieve Desired and Running Task Counts from AWS CloudWatch
 * from ECS/ContainerInsights. Requires that these metrics are enabled
 * 
 * Set up to retrieve these metrics from Cloudwatch and then make availble to calling apps
 * using getter methods.
 * TODO - Upldate this class to acquire and return other application metrics, such as
 * CPU and Memory utilization. These must be treated as data points and stored for
 * a configurable window. Whereas replica counts can continue to be treated as scalars.
 */
@Log4j2
public class EcsServiceMetrics {
    
    public static final String  CW_ECS_NAMESPACE                = "ECS/ContainerInsights",
                                CW_DIM_CLUSTER_NAME             = "ClusterName",
                                CW_DIM_SERVICE_NAME             = "ServiceName",
                                CW_METRIC_DESIRED_TASK_COUNT    = "DesiredTaskCount",
                                CW_METRIC_RUNNING_TASK_COUNT    = "RunningTaskCount",
                                CW_METRIC_STAT                  = "Maximum";

    public static final Integer CW_METRIC_RESOLUTION_PERIOD     = 60;

   
    // TODO - Evaluate if AmazonCloudWatch default client config is sufficient for production
    private static final AmazonCloudWatch cw = AmazonCloudWatchClientBuilder.defaultClient();

    private volatile Integer desiredTaskCount;

    private volatile Integer runningTaskCount;

    private EcsServiceConfig ecsServiceConfig;
    
    /**
     * Constructor requires target ECS service configuration
     * @param ecsServiceConfig
     */
    public EcsServiceMetrics( EcsServiceConfig ecsServiceConfig ) {
        this.ecsServiceConfig = ecsServiceConfig;
    }

    public Integer getDesiredTaskCount() {
        return desiredTaskCount;
    }

    public Integer getRunningTaskCount() {
        return runningTaskCount;
    }

    /**
     * Should make metric period a variable for general use
     * TODO - Catch/handle exceptions
     */
    public void retrieveMetricsFromCloudwatch() {

        // Get Timestamps to Bind Metric Range, 1 minute resolution
        Calendar startTime = Calendar.getInstance(), endTime = Calendar.getInstance();
        endTime.setTime( new Date() ) ;
        endTime.set(Calendar.MILLISECOND, 0);       // Truncate seconds + milliseconds
        endTime.set(Calendar.SECOND, 0);
        endTime.add(Calendar.MINUTE, 1);           // Add 1 Minute to End-Time
        startTime.setTime( endTime.getTime() );
        startTime.add( Calendar.MINUTE, -8 );

        log.debug("Service={} -- Retrieving AWS CloudWatch metrics from ECS", 
                            LogUtils.getServiceDesignation(ecsServiceConfig));
        log.debug("Service={} -- Metric Query: Start time={} End Time={}", 
                            LogUtils.getServiceDesignation(ecsServiceConfig),
                            startTime.getTime(), 
                            endTime.getTime());

        GetMetricDataRequest metricRequest = new GetMetricDataRequest()
                        .withStartTime( startTime.getTime() )
                        .withEndTime( endTime.getTime() )
                        .withMetricDataQueries( formatMetricDataQuery( CW_METRIC_DESIRED_TASK_COUNT ) )
                        .withMetricDataQueries( formatMetricDataQuery( CW_METRIC_RUNNING_TASK_COUNT ) );
        GetMetricDataResult metricResult = cw.getMetricData( metricRequest );

        if ( metricResult == null || metricResult.getSdkHttpMetadata() == null ) {
            log.error( "Service={} -- Could not retrieve ECS metrics from CloudWatch; Credentials initialized?",
                            LogUtils.getServiceDesignation(ecsServiceConfig) );
        }

        log.debug( "Service={} -- HTTP Status Code: {}", 
                            LogUtils.getServiceDesignation(ecsServiceConfig),
                            metricResult.getSdkHttpMetadata().getHttpStatusCode() );

        if ( metricResult.getMetricDataResults() == null ) {
            log.warn("Service={} -- Results returned no data",
                            LogUtils.getServiceDesignation(ecsServiceConfig));
            return;
        }

        Iterator<MetricDataResult> it = metricResult.getMetricDataResults().iterator();

        if ( it == null || !it.hasNext() ) {
            log.warn("Service={} -- Call to AWS Cloudwatch for ECS did not return metric data",
                            LogUtils.getServiceDesignation(ecsServiceConfig));
            log.warn("Service={} -- Scaling will be prevented until a successful call for the task counts is made",
                            LogUtils.getServiceDesignation(ecsServiceConfig));

            // Prevent Scaling Operations
            desiredTaskCount = null;
            runningTaskCount = null;

            return;
        }

        // Get the ECS Service metrics from CloudWatch return
        // Evaluate to make sure that the returned values are recent enough to by useful
        Calendar timeBoundary = Calendar.getInstance();
        timeBoundary.setTime( endTime.getTime() );
        timeBoundary.add(Calendar.MINUTE, -3);          // This is ridiculous
        Integer desired = null, running = null;

        while ( it.hasNext() ) {

            MetricDataResult result = it.next();

            log.debug( "Service={} -- Number of timestamps == {}", 
                            LogUtils.getServiceDesignation(ecsServiceConfig),
                            result.getTimestamps().size() );
            log.debug("Service={} -- Most recent datapoint Time={} -- Boundary Time={}", 
                                LogUtils.getServiceDesignation(ecsServiceConfig),
                                ( result.getTimestamps().size() > 0 ? result.getTimestamps().get(0) : "NONE" ), 
                                timeBoundary.getTime() );

            if ( result.getTimestamps().size() > 0 ) {
                if ( result.getTimestamps().get(0).compareTo( timeBoundary.getTime() ) >= 0 ) {
                    if ( result.getId().toLowerCase( ).contentEquals( CW_METRIC_DESIRED_TASK_COUNT.toLowerCase() ) ) {
                        desired = ( int )Math.round( result.getValues().get(0) );
                    } else if ( result.getId().toLowerCase( ).contentEquals( CW_METRIC_RUNNING_TASK_COUNT.toLowerCase() ) ) {
                        running = ( int )Math.round( result.getValues().get(0) );
                    }
                }
            }
        }

        /**
         * TODO - Enable scale-to-zero instances?
         * As it is, this component should not be used to scale ECS services to zero running instances.
         * The reason is that when scaled to zero, no data points are reported to AWS CloudWatch for the
         * service. Calls to get the current desired/running counts will return no data points/NULL.
         * This is the same result that would be returned for a query to a non-existent service.
         */
        if ( running == null || desired == null ) {
            log.warn("Service={} -- Retrieved desiredTaskCount={}; runningTaskCount={} from ECS Cloudwatch -- One or both values is NULL; Scaling operations will be prevented", 
                                LogUtils.getServiceDesignation(ecsServiceConfig),
                                desired, running);
        } else {
            log.info("Service={} -- Retrieved desiredTaskCount={}; runningTaskCount={} from ECS CloudWatch", 
                                LogUtils.getServiceDesignation(ecsServiceConfig),
                                desired, running);
        }

        // Set the object variables
        desiredTaskCount = desired;
        runningTaskCount = running;
    }

    /**
     * This method makes plenty of assumptions; will need to be re-factored for general use
     * @param metricName
     * @return
     */
    private MetricDataQuery formatMetricDataQuery( String metricName ) {
        MetricDataQuery metricDataQuery = new MetricDataQuery()
        .withId( metricName.toLowerCase() )
        .withMetricStat( new MetricStat()
            .withMetric( new Metric()
                    .withNamespace( CW_ECS_NAMESPACE )
                    .withMetricName( metricName )
                    .withDimensions( new Dimension()
                        .withName(CW_DIM_CLUSTER_NAME).withValue(ecsServiceConfig.getEcsCluster()) )
                    .withDimensions( new Dimension()
                        .withName(CW_DIM_SERVICE_NAME).withValue(ecsServiceConfig.getEcsService()) )
            )
            .withPeriod(CW_METRIC_RESOLUTION_PERIOD)
            .withStat(CW_METRIC_STAT)
            .withUnit(StandardUnit.Count)
        )
        .withLabel( metricName + "_Label" )
        .withReturnData(true);

        return metricDataQuery;
    }
}
