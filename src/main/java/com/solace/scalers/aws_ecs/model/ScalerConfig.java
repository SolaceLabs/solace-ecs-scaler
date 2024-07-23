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
        protected SempConfig activeMsgVpnSempConfig;

        @JsonProperty
        protected SempConfig standbyMsgVpnSempConfig;

        @JsonProperty
        @NonNull
        protected String  msgVpnName;
    
        @JsonProperty
        @NonNull
        protected Integer pollingInterval;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SempConfig {
        @JsonProperty
        @NonNull
        protected String  brokerSempUrl;

        @JsonProperty
        protected String  username;

        @JsonProperty
        protected String  password;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EcsServiceConfig {

        @JsonProperty
        @NonNull
        protected String ecsCluster;

        @JsonProperty
        @NonNull
        protected String ecsService;

        @JsonProperty
        @NonNull
        protected String queueName;

        @JsonProperty
        @NonNull
        protected ScalerBehaviorConfig scalerBehaviorConfig;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ScalerBehaviorConfig {

        @JsonProperty
        @NonNull
        protected Integer minReplicaCount;

        @JsonProperty
        @NonNull
        protected Integer maxReplicaCount;

        @JsonProperty
        @Default
        protected Integer messageCountTarget = 0;
        
        @JsonProperty
        @Default
        protected Integer messageReceiveRateTarget = 0;

        @JsonProperty
        @Default
        protected Integer messageSpoolUsageTarget = 0;
    
        @JsonProperty
        protected ScalerOperation scaleOutConfig;

        @JsonProperty
        protected ScalerOperation scaleInConfig;

    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ScalerOperation {

        @JsonProperty
        @Default
        protected Integer maxScaleStep = 0;
    
        @JsonProperty
        @Default
        protected Integer cooldownPeriod = 0;
    
        @JsonProperty
        @Default
        protected Integer stabilizationWindow = 0;
    }
}
