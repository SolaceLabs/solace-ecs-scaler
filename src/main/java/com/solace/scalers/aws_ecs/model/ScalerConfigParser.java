package com.solace.scalers.aws_ecs.model;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.solace.scalers.aws_ecs.model.ScalerConfig.EcsServiceConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfig.ScalerBehaviorConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfig.ScalerOperation;
import com.solace.scalers.aws_ecs.util.LogUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to parse and validate the Solace ECS Scaler Configuration File
 * Static methods only
 */
public class ScalerConfigParser {

    private static final Logger logger = LogManager.getLogger( ScalerConfigParser.class );  // log4j2, but could also use SLF4J, JCL, etc.

    /**
     * Parse Solace ECS Scaler configuration file
     * @param configFile - Path to configuration file
     * @return
     * @throws Exception
     */
    public static ScalerConfig parseScalerConfig( String configFile ) throws Exception {
        
        ScalerConfig scalerConfig = null;
    	ObjectMapper mapper = new ObjectMapper( new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES) );

        try {
        	scalerConfig = mapper.readValue(new File(configFile), ScalerConfig.class);
        } catch (DatabindException dbexc) {
        	logger.error("Failed to parse the config file: {}", dbexc.getMessage());
            logger.error(dbexc.getStackTrace());
            throw dbexc;
		} catch (StreamReadException srexc ) {
        	logger.error("Failed to parse the config file: {}", srexc.getMessage());
            logger.error(srexc.getStackTrace());
        	throw srexc;
		} catch (IOException ioexc) {
			logger.error("There was an error reading the input file: {}", configFile);
			logger.error(ioexc.getMessage());
            logger.error(ioexc.getStackTrace());
        	throw ioexc;
		}

        return scalerConfig;
    }

    /**
     * Validate parsed configuration file
     * @param scalerConfig - Parsed from ScalerConfig object
     * @return Valid scalerConfig object
     * @throws Exception If one or more validation errors
     */
    public static ScalerConfig validateScalerConfig( ScalerConfig scalerConfig ) throws Exception {

        int errorCount = 0;

        if ( scalerConfig.getEcsServiceConfig().size() < 1 ) {
            logger.error("At least one [ecsServiceConfig] entry is required");
            return null;
        }
        if ( scalerConfig.getEcsServiceConfig().size() > 100 ) {
            logger.error("Too many scaled apps in the configuration: {}. " + 
                            "Maximum number of scaled applications for the Scaler is 100.",
                            scalerConfig.getEcsServiceConfig().size() );
        }

        // For duplicate detection
        List<String> services = new ArrayList<>();
        List<String> queues = new ArrayList<>();

        for ( EcsServiceConfig ecsServiceConfig : scalerConfig.getEcsServiceConfig() ) {

            ScalerBehaviorConfig scalerBehaviorConfig = ecsServiceConfig.getScalerBehaviorConfig();

            // Min replicas >= 1
            if ( scalerBehaviorConfig.getMinReplicaCount() < 1 ) {
                errorCount++;
                logger.error("service={} minReplicaCount must be > 0", 
                                LogUtils.getServiceDesignation(ecsServiceConfig));
            }

            // Max Replicas > min replicas
            if ( scalerBehaviorConfig.getMaxReplicaCount() <= scalerBehaviorConfig.getMinReplicaCount() ) {
                errorCount++;
                logger.error("service={} maxReplicatCount must be > minReplicaCount", 
                                LogUtils.getServiceDesignation(ecsServiceConfig));
            }

            // Validate that metrics values are >= 0
            // If not specified in the config, metric values will default to 0
            if ( scalerBehaviorConfig.getMessageCountTarget() < 0 ||
                scalerBehaviorConfig.getMessageReceiveRateTarget() < 0 ||
                scalerBehaviorConfig.getMessageSpoolUsageTarget() < 0 ) {
                    logger.error("service={} Metric values must be >= 0", 
                                LogUtils.getServiceDesignation(ecsServiceConfig) );
                    errorCount++;
            }

            // Validate that at least one metric values > 0
            if ( scalerBehaviorConfig.getMessageCountTarget() == 0 &&
                scalerBehaviorConfig.getMessageReceiveRateTarget() == 0 
                // Uncomment if implementing messageSpoolUsageTarget:
                // && scalerBehaviorConfig.getMessageSpoolUsageTarget() == 0
                ) {
                    logger.error("service={} At least one metric value must be > 0 for each service", 
                                    LogUtils.getServiceDesignation(ecsServiceConfig) );
                    errorCount++;
            }

            // Create default scaler operation configuration if not specified in the input file -- for bothe scale-in and scale-out
            if ( scalerBehaviorConfig.getScaleInConfig() == null ) {
                scalerBehaviorConfig.setScaleInConfig( new ScalerOperation( 0, 0, 0 ) );
            }
            if ( scalerBehaviorConfig.getScaleOutConfig() == null ) {
                scalerBehaviorConfig.setScaleOutConfig( new ScalerOperation(0, 0, 0) );
            }

            // Validate that scaler operations are >= 0
            if ( !validateScalerOperation( scalerBehaviorConfig.getScaleInConfig() ) ) {
                errorCount++;
                logger.error( "service={} ScaleIn Config: cooldownPeriod, maxScaleStep, stabilizationWindow values must be >= 0", 
                                    LogUtils.getServiceDesignation(ecsServiceConfig) );
            }
            if ( !validateScalerOperation( scalerBehaviorConfig.getScaleOutConfig() ) ) {
                errorCount++;
                logger.error( "service={} ScaleOut Config: cooldownPeriod, maxScaleStep, stabilizationWindow values must be >= 0", 
                                    LogUtils.getServiceDesignation(ecsServiceConfig) );
            }

            queues.add( ecsServiceConfig.getQueueName() );
            services.add( LogUtils.getServiceDesignation(ecsServiceConfig) );
        }

        // Check for duplicate queue names and service names; competing scalers == BAD
        List<String> duplicateQueues = findDuplicatesInList( queues );
        List<String> duplicateServices = findDuplicatesInList( services );

        for ( String s : duplicateQueues ) {
            logger.error( "Found duplicate queueName == [{}] in configuration", s );
            errorCount++;
        }
        for ( String s : duplicateServices ) {
            logger.error( "Found duplicate Service Name == [{}] in configuration", s );
            errorCount++;
        }

        if ( errorCount > 0 ) {
            logger.error( "There were {} validation errors detected in the configuration", errorCount );
            throw new Exception(String.format("There were %d validation errors detected in the configuration", errorCount));
        }
        return scalerConfig;
    }

    /**
     * Simple method to report duplicate string values in a list
     * @param list of strings to check for duplicates
     * @return list of duplicates
     */
    public static List<String> findDuplicatesInList( List<String> list ) {
        List<String> duplicates = new ArrayList<>();
        Set<String> set = list.stream()
                                .filter(i -> Collections.frequency(list, i) > 1)
                                .collect(Collectors.toSet());
        duplicates.addAll(set);
        return duplicates;
    }

    /**
     * Checks scaler operation object for valid values
     * @param scalerOperation
     * @return true for valid; false not valid
     */
    private static boolean validateScalerOperation( ScalerOperation scalerOperation ) {

        if (scalerOperation.getCooldownPeriod() == null ||
            scalerOperation.getMaxScaleStep() == null ||
            scalerOperation.getStabilizationWindow() == null ) {
            return false;
        }
        if (scalerOperation.getCooldownPeriod() < 0 ||
            scalerOperation.getMaxScaleStep() < 0 ||
            scalerOperation.getStabilizationWindow() < 0 ) {
            return false;
        }
        return true;
    }
}
