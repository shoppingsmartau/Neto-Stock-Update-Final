import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
// RE-ADDED: Import ClientOverrideConfiguration
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
// RE-ADDED: Import Duration for timeouts
import java.time.Duration;
// RE-ADDED: Import UrlConnectionHttpClient
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;


import java.util.List;
import java.util.ArrayList;
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
 * including loading SKUs from S3 and updating Neto items in parallel.
 */
public class LambdaHandler implements RequestHandler<ScheduledEvent, Void> {

    private static final int NETO_UPDATE_THREAD_POOL_SIZE = 10;
    private final ExecutorService executorService = Executors.newFixedThreadPool(NETO_UPDATE_THREAD_POOL_SIZE);

    private S3Client s3Client; // S3Client instance

    // S3 client-side timeouts (these variables are now used again)
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

        // --- RE-ADDED EXPLICIT S3Client INITIALIZATION WITH HTTP CLIENT AND TIMEOUTS ---
        // This explicitly tells the SDK to use UrlConnectionHttpClient, resolving the "Multiple HTTP implementations" error.
        if (awsRegion != null && !awsRegion.isEmpty()) {
            this.s3Client = S3Client.builder()
                    .region(Region.of(awsRegion))
                    .overrideConfiguration(clientConfig) // Re-added client configuration
                    .httpClientBuilder(UrlConnectionHttpClient.builder() // Explicitly set HTTP client
                            .connectionTimeout(Duration.ofMillis(S3_CONNECT_TIMEOUT_MS))
                            .socketTimeout(Duration.ofMillis(S3_READ_TIMEOUT_MS)))
                    .build();
            System.out.println("S3Client initialized with region: " + awsRegion);
        } else {
            System.err.println("AWS_REGION environment variable not found. Using default Region.US_EAST_1.");
            this.s3Client = S3Client.builder()
                    .region(Region.US_EAST_1) // Fallback region
                    .overrideConfiguration(clientConfig) // Re-added client configuration
                    .httpClientBuilder(UrlConnectionHttpClient.builder() // Explicitly set HTTP client
                            .connectionTimeout(Duration.ofMillis(S3_CONNECT_TIMEOUT_MS))
                            .socketTimeout(Duration.ofMillis(S3_READ_TIMEOUT_MS)))
                    .build();
            System.out.println("S3Client initialized with fallback region: " + Region.US_EAST_1.id());
        }
        // --- END RE-ADDED EXPLICIT S3Client INITIALIZATION ---
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        // Optional: Reduce log verbosity for production by commenting out some System.out.println
        context.getLogger().log("Lambda function invoked by CloudWatch Event at: " + event.getTime());

        String s3BucketName = System.getenv("S3_BUCKET_NAME");
        String s3FileKey = System.getenv("S3_FILE_KEY");

        if (s3BucketName == null || s3FileKey == null || s3BucketName.isEmpty() || s3FileKey.isEmpty()) {
            context.getLogger().log("Error: S3_BUCKET_NAME or S3_FILE_KEY environment variables not set. Aborting execution.");
            throw new RuntimeException("S3 bucket or file key not configured.");
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
            context.getLogger().log("Attempting to load SKUs from S3 Bucket: " + s3BucketName + ", Key: " + s3FileKey);
            List<String> skuList = DropshipzoneAPIClient.loadSkusFromCSV(s3Client, s3BucketName, s3FileKey);
            if (skuList.isEmpty()) {
                context.getLogger().log("No SKUs found in S3 file: " + s3FileKey + ". Aborting execution.");
                return null;
            }
            context.getLogger().log("Loaded SKUs from S3: " + skuList.size() + " SKUs.");

            // 3. Fetch stock data from Dropshipzone API
            String stockResponse = DropshipzoneAPIClient.fetchStock(token, skuList);
            // context.getLogger().log("\n--- Combined Raw Stock Response from Dropshipzone ---"); // Reduced verbosity
            // if (stockResponse.length() < 5000) {
            //     context.getLogger().log(stockResponse);
            // } else {
            //     context.getLogger().log("Raw stock response is very large, not printing to console.");
            // }

            JSONObject stockJson;
            try {
                // context.getLogger().log("Attempting to parse combined stock response into JSONObject..."); // Reduced verbosity
                stockJson = new JSONObject(stockResponse);
                // context.getLogger().log("Successfully parsed combined response."); // Reduced verbosity
            } catch (org.json.JSONException jsonE) {
                context.getLogger().log("CRITICAL ERROR: Failed to parse combined stock response into JSONObject.");
                context.getLogger().log("Raw response content that failed parsing: " + stockResponse);
                jsonE.printStackTrace();
                throw new RuntimeException("Failed to parse main stock JSON response.", jsonE);
            }

            // 4. Process stock data in memory (applying quantity < 25 rule)
            JSONArray resultArray = new JSONArray();
            if (stockJson.has("result") && stockJson.getJSONArray("result").length() > 0) {
                resultArray = stockJson.getJSONArray("result");
                context.getLogger().log("Total stock items fetched from Dropshipzone API: " + resultArray.length());
            } else {
                context.getLogger().log("No stock data found in the 'result' field of the response.");
            }

            List<String[]> processedSkuData = DropshipzoneAPIClient.processStockData(resultArray, skuList);
            context.getLogger().log("Processed stock data for " + processedSkuData.size() + " SKUs.");


            // 5. Update Neto items in parallel
            context.getLogger().log("\n--- Updating Neto Items in Parallel ---");
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String[] entry : processedSkuData) {
                String sku = entry[0];
                int quantity = Integer.parseInt(entry[1]);

                // Log the SKU and quantity being sent to Neto
                context.getLogger().log(String.format("Prepared to update Neto: SKU=%s, Quantity=%d", sku, quantity));

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    DropshipzoneAPIClient.updateNetoItem(sku, quantity);
                }, executorService);

                futures.add(future);
            }

            // Wait for all parallel Neto update operations to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            context.getLogger().log("All SKUs processed for update in Neto.");

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
            // Close the S3 client when the Lambda invocation is complete
            if (s3Client != null) {
                s3Client.close();
                context.getLogger().log("S3Client closed.");
            }
        }
        return null;
    }
}
