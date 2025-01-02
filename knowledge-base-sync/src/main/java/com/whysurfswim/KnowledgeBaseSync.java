package com.whysurfswim;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockagent.BedrockAgentClient;
import software.amazon.awssdk.services.bedrockagent.model.*;
import software.amazon.awssdk.services.ssm.SsmClient;

@Slf4j
public class KnowledgeBaseSync implements RequestHandler<SQSEvent, String> {
    @Override
    public String handleRequest(SQSEvent input, Context context) {
        ObjectMapper objectMapper = new ObjectMapper();
        String bucketName;
        String objectKey;
        String knowledgeBaseIds;

        // Extracting the message body from the event
        String messageBody = input.getRecords().get(0).getBody();

        try {
            // Parse the message body as JSON
            JsonNode jsonNode = objectMapper.readTree(messageBody);
            JsonNode detailNode = jsonNode.get("detail");
            bucketName = detailNode.get("bucket").get("name").asText();
            objectKey = detailNode.get("object").get("key").asText();
            log.info("Bucket Name: {}, ObjectKey: {} will be data synced", bucketName, objectKey);
        } catch (Exception e) {
            log.error("Failed to parse SQS message body", e);
            return "Error occurred while parsing SQS message body: " + input;
        }

        // Extracting the knowledge base ID from the Environment Variable
        String knowledgebaseidParameter = System.getenv("KNOWLEDGEBASEID_PARAMETER");
        if (knowledgebaseidParameter == null || knowledgebaseidParameter.isEmpty()) {
            log.error("KNOWLEDGEBASEID_PARAMETER key is not set or empty!");
            return "Error: KNOWLEDGEBASEID_PARAMETER not provided.";
        }

        // Extracting the knowledge base ID values from SSM Parameter store
        try (SsmClient ssmClient = SsmClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {
            knowledgeBaseIds = ssmClient.getParameter(builder -> builder
                    .name(knowledgebaseidParameter)
                    .withDecryption(true)
            ).parameter().value();
        } catch (Exception e) {
            log.error(String.valueOf(e));
            return "Error: Failed to retrieve knowledge base IDs from SSM Parameter Store.";
        }

        String[] knowledgeBaseIdArray = knowledgeBaseIds.split(",");
        for (String kbId : knowledgeBaseIdArray) {
            kbId = kbId.trim();
            log.info("Knowledge Base ID: {}", kbId);

            // Get List of all Datasources from bedrock knowledgebase Id
            try (BedrockAgentClient bedrockAgentClient = BedrockAgentClient.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .region(Region.US_EAST_1)
                    .build()) {
                ListDataSourcesResponse dataSourcesResponse = bedrockAgentClient.listDataSources(ListDataSourcesRequest.builder()
                        .knowledgeBaseId(kbId).build());
                for (DataSourceSummary ds : dataSourcesResponse.dataSourceSummaries()) {
                    //Start data sync for updated bucket
                    log.info("Data Source details: {}", ds);
                    bedrockAgentClient.startIngestionJob(StartIngestionJobRequest.builder()
                            .knowledgeBaseId(kbId).dataSourceId(ds.dataSourceId())
                            .build());
                }
            } catch (Exception e) {
                log.error(String.valueOf(e));
                return "Error: Failed to retrieve knowledge base IDs from SSM Parameter Store.";
            }

        }

        return "KnowledgeBaseSync executed successfully";
    }
}
