import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request; // For listing objects
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response; // For listing objects
import software.amazon.awssdk.services.s3.model.S3Object;           // For S3 object details
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest; // For deleting objects
import software.amazon.awssdk.core.sync.RequestBody;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator; // For sorting objects

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

import java.net.http.HttpClient;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * The main AWS Lambda handler class. This class implements RequestHandler
 * to correctly receive events from AWS CloudWatch (EventBridge).
 *
 * This class orchestrates the calls to DropshipzoneAPIClient methods,
 * including loading SKUs from S3 and updating Neto items in parallel,
 * and now also generates an output CSV with cost and selling price data.
 */
public class LambdaHandler implements RequestHandler<ScheduledEvent, Void> {

    private static final int NETO_UPDATE_THREAD_POOL_SIZE = 20; // Increased thread pool size
    private final ExecutorService executorService = Executors.newFixedThreadPool(NETO_UPDATE_THREAD_POOL_SIZE);

    private S3Client s3Client; // S3Client instance
    private HttpClient httpClient; // Shared HttpClient instance for Dropshipzone and Neto APIs

    // S3 client-side timeouts
    private static final int S3_CONNECT_TIMEOUT_MS = 5000;
    private static final int S3_READ_TIMEOUT_MS = 30000;
    private static final int S3_API_CALL_TIMEOUT_MS = 45000;


    // Add a public zero-argument constructor as required by AWS Lambda
    public LambdaHandler() {
        System.out.println("LambdaHandler constructor invoked. Initializing S3Client and HttpClient.");
        String awsRegion = System.getenv("AWS_REGION");

        // --- S3Client Initialization (remains similar) ---
        ClientOverrideConfiguration s3ClientConfig = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMillis(S3_API_CALL_TIMEOUT_MS))
                .apiCallAttemptTimeout(Duration.ofMillis(S3_READ_TIMEOUT_MS))
                .build();

        if (awsRegion != null && !awsRegion.isEmpty()) {
            this.s3Client = S3Client.builder()
                    .region(Region.of(awsRegion))
                    .overrideConfiguration(s3ClientConfig)
                    .httpClientBuilder(UrlConnectionHttpClient.builder()
                            .connectionTimeout(Duration.ofMillis(S3_CONNECT_TIMEOUT_MS))
                            .socketTimeout(Duration.ofMillis(S3_READ_TIMEOUT_MS)))
                    .build();
            System.out.println("S3Client initialized with region: " + awsRegion);
        } else {
            System.err.println("AWS_REGION environment variable not found. Using default Region.US_EAST_1.");
            this.s3Client = S3Client.builder()
                    .region(Region.US_EAST_1) // Fallback region
                    .overrideConfiguration(s3ClientConfig)
                    .httpClientBuilder(UrlConnectionHttpClient.builder()
                            .connectionTimeout(Duration.ofMillis(S3_CONNECT_TIMEOUT_MS))
                            .socketTimeout(Duration.ofMillis(S3_READ_TIMEOUT_MS)))
                    .build();
            System.out.println("S3Client initialized with fallback region: " + Region.US_EAST_1.id());
        }

        // --- Shared HttpClient Initialization for Dropshipzone and Neto APIs ---
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(DropshipzoneAPIClient.CONNECT_TIMEOUT_MS))
                .build();
        System.out.println("Shared HttpClient initialized for external APIs.");
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        context.getLogger().log("Lambda function invoked by CloudWatch Event at: " + event.getTime());

        // --- DEBUGGING LOGS FOR ENVIRONMENT VARIABLES (Keep these for now) ---
        context.getLogger().log("Checking environment variable: S3_INPUT_BUCKET_NAME = " + System.getenv("S3_INPUT_BUCKET_NAME"));
        context.getLogger().log("Checking environment variable: S3_INPUT_FILE_KEY = " + System.getenv("S3_INPUT_FILE_KEY"));
        // --- END DEBUGGING LOGS ---


        String s3InputBucketName = System.getenv("S3_INPUT_BUCKET_NAME");
        String s3InputFileKey = System.getenv("S3_INPUT_FILE_KEY");
        String s3OutputBucketName = System.getenv("S3_OUTPUT_BUCKET_NAME");
        String s3OutputFilePrefix = System.getenv("S3_OUTPUT_FILE_PREFIX");

        // New environment variable for price multiplier
        double priceMultiplier = 1.4; // Default value
        try {
            priceMultiplier = Double.parseDouble(System.getenv().getOrDefault("PRICE_MULTIPLIER", "1.4"));
        } catch (NumberFormatException e) {
            context.getLogger().log("Warning: Invalid PRICE_MULTIPLIER environment variable. Using default value 1.4.");
        }

        // New environment variable for max files to keep
        int s3OutputMaxFiles = Integer.parseInt(System.getenv().getOrDefault("S3_OUTPUT_MAX_FILES", "5"));


        if (s3InputBucketName == null || s3InputFileKey == null || s3InputBucketName.isEmpty() || s3InputFileKey.isEmpty()) {
            context.getLogger().log("Error: S3_INPUT_BUCKET_NAME or S3_INPUT_FILE_KEY environment variables not set. Aborting execution.");
            throw new RuntimeException("S3 input bucket or file key not configured.");
        }
        if (s3OutputBucketName == null || s3OutputFilePrefix == null || s3OutputBucketName.isEmpty() || s3OutputFilePrefix.isEmpty()) {
             context.getLogger().log("Warning: S3_OUTPUT_BUCKET_NAME or S3_OUTPUT_FILE_PREFIX environment variables not set. Output CSV will not be generated and cleanup will not run.");
        }


        try {
            // 1. Authenticate with Dropshipzone API
            String token = DropshipzoneAPIClient.authenticate(httpClient);
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

            // 3. Prepare a Map to collect all processed SKU data (SKU -> {quantity, cost, selling_price})
            Map<String, Map<String, String>> finalProcessedSkuData = new HashMap<>();

            // 4. Fetch stock data from Dropshipzone API (fetchStock now populates finalProcessedSkuData directly)
            context.getLogger().log("Starting to fetch and process stock data from Dropshipzone API...");
            // Pass the priceMultiplier to fetchStock
            DropshipzoneAPIClient.fetchStock(httpClient, token, skuList, finalProcessedSkuData, priceMultiplier);
            context.getLogger().log("Finished fetching and processing stock data from Dropshipzone API. Total unique SKUs processed: " + finalProcessedSkuData.size());

            // 5. Update Neto items in parallel
            context.getLogger().log("\n--- Updating Neto Items in Parallel ---");
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Map<String, String> entry : finalProcessedSkuData.values()) {
                String sku = entry.get("sku");
                int quantity = Integer.parseInt(entry.get("quantity"));
                String cost = entry.get("cost");
                String sellingPrice = entry.get("selling_price");

                context.getLogger().log(String.format("Prepared for Neto Update/CSV Output: SKU=%s, Quantity=%d, Cost=%s, SellingPrice=%s", sku, quantity, cost, sellingPrice));

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    DropshipzoneAPIClient.updateNetoItem(httpClient, sku, quantity, sellingPrice);
                }, executorService);

                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            context.getLogger().log("All SKUs processed for update in Neto.");

            // 6. Generate and upload new CSV to S3
            if (s3OutputBucketName != null && !s3OutputBucketName.isEmpty() &&
                s3OutputFilePrefix != null && !s3OutputFilePrefix.isEmpty()) {
                context.getLogger().log("\n--- Generating and Uploading Output CSV to S3 ---");
                String csvContent = generateCsvContent(new ArrayList<>(finalProcessedSkuData.values()));
                String outputS3Key = s3OutputFilePrefix + "_" +
                                      DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneOffset.UTC).format(Instant.now()) +
                                      ".csv";
                uploadCsvToS3(s3Client, s3OutputBucketName, outputS3Key, csvContent);
                context.getLogger().log("Output CSV uploaded to s3://" + s3OutputBucketName + "/" + outputS3Key);

                // 7. Clean up old files in the output bucket
                cleanOldS3Files(s3Client, s3OutputBucketName, s3OutputFilePrefix, s3OutputMaxFiles);
                context.getLogger().log("S3 cleanup complete for bucket " + s3OutputBucketName + " with prefix " + s3OutputFilePrefix);
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
        }
        return null;
    }

    /**
     * Generates CSV content from the processed SKU data.
     * The CSV will have a header: SKU,Quantity,Cost,Selling Price
     * @param data The list of maps containing SKU, quantity, cost, and selling price.
     * @return A String containing the CSV content.
     */
    private String generateCsvContent(List<Map<String, String>> data) {
        StringBuilder csvBuilder = new StringBuilder();
        csvBuilder.append("SKU,Quantity,Cost,Selling Price\n");

        for (Map<String, String> entry : data) {
            String sku = entry.get("sku");
            String quantity = entry.get("quantity");
            String cost = entry.get("cost");
            String sellingPrice = entry.get("selling_price");
            csvBuilder.append(String.format("%s,%s,%s,%s\n", escapeCsv(sku), escapeCsv(quantity), escapeCsv(cost), escapeCsv(sellingPrice)));
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

    /**
     * Cleans up old S3 files in the specified output bucket, retaining only the most recent files
     * based on the provided prefix and max files count.
     *
     * @param s3Client The S3Client instance.
     * @param bucketName The name of the S3 bucket to clean.
     * @param prefix The prefix for the files to consider for cleanup.
     * @param maxFilesToKeep The maximum number of files to retain.
     */
    private void cleanOldS3Files(S3Client s3Client, String bucketName, String prefix, int maxFilesToKeep) {
        System.out.println("Starting S3 cleanup for bucket: " + bucketName + ", prefix: " + prefix + ", max files to keep: " + maxFilesToKeep);

        try {
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .build();

            ListObjectsV2Response listObjectsResponse;
            List<S3Object> allMatchingObjects = new ArrayList<>();

            // Paginate through all objects if more than 1000
            String continuationToken = null;
            do {
                listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest.toBuilder().continuationToken(continuationToken).build());
                allMatchingObjects.addAll(listObjectsResponse.contents());
                continuationToken = listObjectsResponse.nextContinuationToken();
            } while (listObjectsResponse.isTruncated());


            // Filter for CSV files and sort by LastModified timestamp (newest first)
            List<S3Object> csvFiles = new ArrayList<>();
            for(S3Object s3Object : allMatchingObjects) {
                if (s3Object.key().endsWith(".csv")) { // Only consider CSV files
                    csvFiles.add(s3Object);
                }
            }

            csvFiles.sort(Comparator.comparing(S3Object::lastModified).reversed()); // Sort newest to oldest

            System.out.println("Found " + csvFiles.size() + " matching CSV files for cleanup under prefix: " + prefix);

            if (csvFiles.size() > maxFilesToKeep) {
                List<S3Object> filesToDelete = csvFiles.subList(maxFilesToKeep, csvFiles.size());
                System.out.println("Identified " + filesToDelete.size() + " files for deletion.");

                for (S3Object fileToDelete : filesToDelete) {
                    DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(fileToDelete.key())
                            .build();
                    s3Client.deleteObject(deleteObjectRequest);
                    System.out.println("Deleted old S3 file: " + fileToDelete.key());
                }
            } else {
                System.out.println("Number of CSV files (" + csvFiles.size() + ") is less than or equal to max files to keep (" + maxFilesToKeep + "). No files deleted.");
            }

        } catch (Exception e) {
            System.err.println("Error during S3 cleanup: " + e.getMessage());
            e.printStackTrace();
            // Don't re-throw as cleanup is a secondary operation, main flow should not fail because of it.
        }
    }
}
