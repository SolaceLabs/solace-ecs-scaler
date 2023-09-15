package com.solace.scalers.aws_ecs.model.semp_v2;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SempQueueResponse {
    
    @JsonProperty
    protected QueueCollections collections;

    @JsonProperty
    protected QueueData data;

    @Data
    @NoArgsConstructor
    public static class QueueCollections {

        @JsonProperty
        protected Messages msgs;

    }

    @Data
    @NoArgsConstructor
    public static class QueueData {

        @JsonProperty
        protected Long averageRxMsgRate;

        @JsonProperty
        protected Long averageTxMsgRate;

        @JsonProperty
        protected Long msgSpoolUsage;

        @JsonProperty
        protected String msgVpnName;

        @JsonProperty
        protected String queueName;
    
    }

    @Data
    @NoArgsConstructor
    public static class Messages {

        @JsonProperty
        protected Long count;

    }
}
