package com.whysurfswim;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.transcribe.TranscribeClient;
import software.amazon.awssdk.services.transcribe.model.Media;
import software.amazon.awssdk.services.transcribe.model.StartTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.StartTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.TranscribeException;

import java.util.UUID;

@Slf4j
public class TranscriptionJobInvokation implements RequestHandler<SQSEvent, String> {

    @Override
    public String handleRequest(SQSEvent input, Context context) {
        ObjectMapper objectMapper = new ObjectMapper();
        String bucketName;
        String objectKey;

        // Extracting the message body from the event
        String messageBody = input.getRecords().get(0).getBody();

        try {
            // Parse the message body as JSON
            JsonNode jsonNode = objectMapper.readTree(messageBody);
            JsonNode detailNode = jsonNode.get("detail");
            bucketName = detailNode.get("bucket").get("name").asText();
            objectKey = detailNode.get("object").get("key").asText();
        } catch (Exception e) {
            log.error("Failed to parse SQS message body", e);
            return "Error occurred while parsing SQS message body: " + input;
        }

        String transcriptionJobName = "transcription-job-" + objectKey + "-" + UUID.randomUUID();

        try (TranscribeClient transcribeClient = TranscribeClient.create()) {
            // Set up the request for transcription
            StartTranscriptionJobRequest transcriptionJobRequest = StartTranscriptionJobRequest.builder()
                    .transcriptionJobName(transcriptionJobName)
                    .languageCode("en-US")
                    .media(Media.builder().mediaFileUri("s3://" + bucketName + "/" + objectKey).build())
                    .mediaFormat("mp4")
                    .outputBucketName(System.getenv("VIDEO_OUTPUT_BUCKET_NAME"))
                    .build();

            // Start the transcription job
            StartTranscriptionJobResponse transcriptionJobResponse = transcribeClient.startTranscriptionJob(transcriptionJobRequest);
            log.info("Started transcription job: {}", transcriptionJobResponse.transcriptionJob().transcriptionJobName());

            // Log the status of the transcription job
            log.info("Transcription job status: {}", transcriptionJobResponse.transcriptionJob().transcriptionJobStatus());

        } catch (TranscribeException e) {
            log.error("Failed to start transcription job", e);
            return "Error occurred: " + e.getMessage();
        }

        return "Transcription Job submitted successfully";
    }
}