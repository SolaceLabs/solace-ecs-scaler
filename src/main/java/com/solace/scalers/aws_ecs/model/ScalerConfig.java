package com.solace.scalers.aws_ecs.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Builder.Default;

/**
 * ECS Scaler Configuration File
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ScalerConfig {
    
    @JsonProperty
    @NonNull
    protected BrokerConfig              brokerConfig;

    @JsonProperty
    @NonNull
    protected List<EcsServiceConfig>    ecsServiceConfig;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class BrokerConfig {

        @JsonProperty
        @NonNull
        private String  brokerSempUrl;

        @JsonProperty
        private String  username;

        @JsonProperty
        private String  password;

        @JsonProperty
        @NonNull
        private String  msgVpnName;
    
        @JsonProperty
        @NonNull
        private Integer pollingInterval;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EcsServiceConfig {

        @JsonProperty
        @NonNull
        private String ecsCluster;

        @JsonProperty
        @NonNull
        private String ecsService;

        @JsonProperty
        @NonNull
        private String queueName;

        @JsonProperty
        @NonNull
        private ScalerBehaviorConfig scalerBehaviorConfig;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ScalerBehaviorConfig {

        @JsonProperty
        @NonNull
        private Integer minReplicaCount;

        @JsonProperty
        @NonNull
        private Integer maxReplicaCount;

        @JsonProperty
        @Default
        private Integer messageCountTarget = 0;
        
        @JsonProperty
        @Default
        private Integer messageReceiveRateTarget = 0;

        @JsonProperty
        @Default
        private Integer messageSpoolUsageTarget = 0;
    
        @JsonProperty
        private ScalerOperation scaleOutConfig;

        @JsonProperty
        private ScalerOperation scaleInConfig;

    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ScalerOperation {

        @JsonProperty
        @Default
        private Integer maxScaleStep = 0;
    
        @JsonProperty
        @Default
        private Integer cooldownPeriod = 0;
    
        @JsonProperty
        @Default
        private Integer stabilizationWindow = 0;
    }
}
