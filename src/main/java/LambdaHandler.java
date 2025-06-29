import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;


import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;


/**
 * The main AWS Lambda handler class. This class implements RequestHandler
 * to correctly receive events from AWS CloudWatch (EventBridge).
 *
 * This class orchestrates the calls to DropshipzoneAPIClient methods,
 * including loading SKUs from S3 and updating Neto items in parallel,
 * and now also generates an output CSV with cost data.
 */
public class LambdaHandler implements RequestHandler<ScheduledEvent, Void> {

    private static final int NETO_UPDATE_THREAD_POOL_SIZE = 10;
    private final ExecutorService executorService = Executors.newFixedThreadPool(NETO_UPDATE_THREAD_POOL_SIZE);

    private S3Client s3Client; // S3Client instance

    // S3 client-side timeouts
    private static final int S3_CONNECT_TIMEOUT_MS = 5000;
    private static final int S3_READ_TIMEOUT_MS = 30000;
    private static final int S3_API_CALL_TIMEOUT_MS = 45000;


    // Add a public zero-argument constructor as required by AWS Lambda
    public LambdaHandler() {
        System.out.println("LambdaHandler constructor invoked. Initializing S3Client.");
        String awsRegion = System.getenv("AWS_REGION");

        // Build ClientOverrideConfiguration for timeouts
        ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMillis(S3_API_CALL_TIMEOUT_MS))
                .apiCallAttemptTimeout(Duration.ofMillis(S3_READ_TIMEOUT_MS)) // This applies to each retry attempt
                .build();

        // Explicitly set HTTP client to UrlConnectionHttpClient to avoid "Multiple HTTP implementations" error.
        // This also explicitly uses S3Client.Builder which resolves symbol issues when using 'var'.
        if (awsRegion != null && !awsRegion.isEmpty()) {
            this.s3Client = S3Client.builder()
                    .region(Region.of(awsRegion))
                    .overrideConfiguration(clientConfig)
                    .httpClientBuilder(UrlConnectionHttpClient.builder()
                            .connectionTimeout(Duration.ofMillis(S3_CONNECT_TIMEOUT_MS))
                            .socketTimeout(Duration.ofMillis(S3_READ_TIMEOUT_MS)))
                    .build();
            System.out.println("S3Client initialized with region: " + awsRegion);
        } else {
            System.err.println("AWS_REGION environment variable not found. Using default Region.US_EAST_1.");
            this.s3Client = S3Client.builder()
                    .region(Region.US_EAST_1) // Fallback region
                    .overrideConfiguration(clientConfig)
                    .httpClientBuilder(UrlConnectionHttpClient.builder()
                            .connectionTimeout(Duration.ofMillis(S3_CONNECT_TIMEOUT_MS))
                            .socketTimeout(Duration.ofMillis(S3_READ_TIMEOUT_MS)))
                    .build();
            System.out.println("S3Client initialized with fallback region: " + Region.US_EAST_1.id());
        }
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        context.getLogger().log("Lambda function invoked by CloudWatch Event at: " + event.getTime());

        // --- NEW DEBUGGING LOGS FOR ENVIRONMENT VARIABLES ---
        context.getLogger().log("Checking environment variable: S3_INPUT_BUCKET_NAME = " + System.getenv("S3_INPUT_BUCKET_NAME"));
        context.getLogger().log("Checking environment variable: S3_INPUT_FILE_KEY = " + System.getenv("S3_INPUT_FILE_KEY"));
        // --- END NEW DEBUGGING LOGS ---


        String s3InputBucketName = System.getenv("S3_INPUT_BUCKET_NAME"); // Renamed for clarity
        String s3InputFileKey = System.getenv("S3_INPUT_FILE_KEY");       // Renamed for clarity
        String s3OutputBucketName = System.getenv("S3_OUTPUT_BUCKET_NAME"); // New env var for output bucket
        String s3OutputFilePrefix = System.getenv("S3_OUTPUT_FILE_PREFIX"); // New env var for output file prefix

        if (s3InputBucketName == null || s3InputFileKey == null || s3InputBucketName.isEmpty() || s3InputFileKey.isEmpty()) {
            context.getLogger().log("Error: S3_INPUT_BUCKET_NAME or S3_INPUT_FILE_KEY environment variables not set. Aborting execution.");
            throw new RuntimeException("S3 input bucket or file key not configured.");
        }
        if (s3OutputBucketName == null || s3OutputFilePrefix == null || s3OutputBucketName.isEmpty() || s3OutputFilePrefix.isEmpty()) {
             context.getLogger().log("Warning: S3_OUTPUT_BUCKET_NAME or S3_OUTPUT_FILE_PREFIX environment variables not set. Output CSV will not be generated.");
             // Don't throw RuntimeException here, allow main logic to proceed if only output is affected.
        }


        try {
            // 1. Authenticate with Dropshipzone API
            String token = DropshipzoneAPIClient.authenticate();
            if (token == null) {
                context.getLogger().log("Failed to extract Dropshipzone token. Aborting execution.");
                throw new RuntimeException("Authentication failed.");
            }
            context.getLogger().log("Token acquired successfully.");

            // 2. Load SKUs from the S3 CSV file
            context.getLogger().log("Attempting to load SKUs from S3 Input Bucket: " + s3InputBucketName + ", Key: " + s3InputFileKey);
            List<String> skuList = DropshipzoneAPIClient.loadSkusFromCSV(s3Client, s3InputBucketName, s3InputFileKey);
            if (skuList.isEmpty()) {
                context.getLogger().log("No SKUs found in S3 file: " + s3InputFileKey + ". Aborting execution.");
                return null;
            }
            context.getLogger().log("Loaded SKUs from S3: " + skuList.size() + " SKUs.");

            // 3. Fetch stock data from Dropshipzone API
            String stockResponse = DropshipzoneAPIClient.fetchStock(token, skuList);

            JSONObject stockJson;
            try {
                stockJson = new JSONObject(stockResponse);
            } catch (org.json.JSONException jsonE) {
                context.getLogger().log("CRITICAL ERROR: Failed to parse combined stock response into JSONObject.");
                context.getLogger().log("Raw response content that failed parsing: " + stockResponse);
                jsonE.printStackTrace();
                throw new RuntimeException("Failed to parse main stock JSON response.", jsonE);
            }

            // 4. Process stock data in memory (applying quantity < 25 rule and extracting cost)
            JSONArray resultArray = new JSONArray();
            if (stockJson.has("result") && stockJson.getJSONArray("result").length() > 0) {
                resultArray = stockJson.getJSONArray("result");
                context.getLogger().log("Total stock items fetched from Dropshipzone API: " + resultArray.length());
            } else {
                context.getLogger().log("No stock data found in the 'result' field of the response.");
            }

            // Processed data now includes cost
            List<Map<String, String>> processedSkuData = DropshipzoneAPIClient.processStockData(resultArray, skuList);
            context.getLogger().log("Processed stock data for " + processedSkuData.size() + " SKUs (including cost).");


            // 5. Update Neto items in parallel
            context.getLogger().log("\n--- Updating Neto Items in Parallel ---");
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Map<String, String> entry : processedSkuData) {
                String sku = entry.get("sku");
                int quantity = Integer.parseInt(entry.get("quantity"));
                String cost = entry.get("cost"); // Get the cost for logging/output

                // Log the SKU, quantity, and cost being sent to Neto or for output
                context.getLogger().log(String.format("Prepared for Neto Update/CSV Output: SKU=%s, Quantity=%d, Cost=%s", sku, quantity, cost));

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    DropshipzoneAPIClient.updateNetoItem(sku, quantity);
                }, executorService);

                futures.add(future);
            }

            // Wait for all parallel Neto update operations to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            context.getLogger().log("All SKUs processed for update in Neto.");

            // 6. Generate and upload new CSV to S3
            if (s3OutputBucketName != null && !s3OutputBucketName.isEmpty() &&
                s3OutputFilePrefix != null && !s3OutputFilePrefix.isEmpty()) {
                context.getLogger().log("\n--- Generating and Uploading Output CSV to S3 ---");
                String csvContent = generateCsvContent(processedSkuData);
                String outputS3Key = s3OutputFilePrefix + "_" +
                                      DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneOffset.UTC).format(Instant.now()) +
                                      ".csv";
                uploadCsvToS3(s3Client, s3OutputBucketName, outputS3Key, csvContent);
                context.getLogger().log("Output CSV uploaded to s3://" + s3OutputBucketName + "/" + outputS3Key);
            }


        } catch (Exception e) {
            context.getLogger().log("An unhandled error occurred during Lambda execution:");
            e.printStackTrace();
            throw new RuntimeException("Lambda execution failed: " + e.getMessage(), e);
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    context.getLogger().log("Executor service did not terminate gracefully within 5 seconds.");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                context.getLogger().log("Executor service termination interrupted.");
            }
            // Removed S3Client close here. S3Client should be managed by the Lambda container lifecycle.
            // if (s3Client != null) {
            //     s3Client.close();
            //     context.getLogger().log("S3Client closed.");
            // }
        }
        return null;
    }

    /**
     * Generates CSV content from the processed SKU data.
     * The CSV will have a header: SKU,Quantity,Cost
     * @param data The list of maps containing SKU, quantity, and cost.
     * @return A String containing the CSV content.
     */
    private String generateCsvContent(List<Map<String, String>> data) {
        StringBuilder csvBuilder = new StringBuilder();
        // Add CSV Header
        csvBuilder.append("SKU,Quantity,Cost\n");

        for (Map<String, String> entry : data) {
            String sku = entry.get("sku");
            String quantity = entry.get("quantity");
            String cost = entry.get("cost");
            csvBuilder.append(String.format("%s,%s,%s\n", escapeCsv(sku), escapeCsv(quantity), escapeCsv(cost)));
        }
        return csvBuilder.toString();
    }

    /**
     * Escapes a string for CSV output.
     * Doubles inner quotes and wraps the string in quotes if it contains commas or quotes.
     * @param value The string to escape.
     * @return The escaped string.
     */
    private String escapeCsv(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }


    /**
     * Uploads the generated CSV content to an S3 bucket.
     * @param s3Client The S3Client instance.
     * @param bucketName The name of the S3 bucket to upload to.
     * @param key The object key (path) for the new CSV file.
     * @param content The CSV content as a string.
     * @throws IOException If an error occurs during upload.
     */
    private void uploadCsvToS3(S3Client s3Client, String bucketName, String key, String content) throws IOException {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("text/csv")
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromString(content));
            System.out.println("Successfully uploaded CSV to S3: s3://" + bucketName + "/" + key);
        } catch (Exception e) {
            System.err.println("Error uploading CSV to S3 bucket '" + bucketName + "' with key '" + key + "': " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to upload CSV to S3.", e);
        }
    }
}
