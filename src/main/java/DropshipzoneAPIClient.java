import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder; // Added for URL encoding
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import org.json.JSONObject;
import org.json.JSONArray;

// AWS SDK for S3 imports
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import java.time.Duration; // Import Duration for S3 client timeouts

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration; // For S3 client timeouts
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient; // For S3 client http client


/**
 * A Java client for interacting with the Dropshipzone API to fetch stock data
 * and update item quantities in Neto.
 *
 * This class contains the core logic (API interactions, CSV handling)
 * and is designed to be called by the LambdaHandler class.
 *
 * Requirements:
 * - A 'skus.csv' file placed in an S3 bucket, with the bucket name and key
 * (path) provided via Lambda environment variables.
 * - The 'org.json' library and AWS SDK for S3 must be included in the project dependencies.
 */
public class DropshipzoneAPIClient {

    // Define a batch size for parallel Neto API requests
    private static final int NETO_UPDATE_THREAD_POOL_SIZE = 40; // Can be adjusted (used in LambdaHandler, but relevant context)

    // Define the maximum number of SKUs allowed in the 'skus' parameter for Dropshipzone API /v2/products endpoint
    private static final int DROPSHIPZONE_API_SKU_LIMIT = 50; // As per user's specification "up to 50"

    // Define the number of products to request per API call for pagination (per filtered SKU batch)
    private static final int API_PAGE_SIZE = 100; // Common pagination parameter, should be >= DROPSHIPZONE_API_SKU_LIMIT for efficiency

    // Define timeouts for HTTP connections (in milliseconds)
    private static final int CONNECT_TIMEOUT_MS = 5000; // 5 seconds
    private static final int READ_TIMEOUT_MS = 15000;  // 15 seconds (for Dropshipzone and Neto)
    // Note: Lambda's overall timeout will override individual read timeouts if it's shorter.


    /**
     * Authenticates with the Dropshipzone API using predefined credentials.
     * Credentials are retrieved from environment variables.
     *
     * @return The JWT token string if authentication is successful, otherwise null.
     * @throws IOException If an I/O error occurs during the HTTP request.
     */
    protected static String authenticate() throws IOException {
        String authUrl = "https://api.dropshipzone.com.au/auth";

        // Retrieve credentials from environment variables
        String email = System.getenv("DROPSHIPZONE_EMAIL");
        String password = System.getenv("DROPSHIPZONE_PASSWORD");

        if (email == null || password == null || email.isEmpty() || password.isEmpty()) {
            System.err.println("Error: Dropshipzone credentials (DROPSHIPZONE_EMAIL, DROPSHIPZONE_PASSWORD) not set as environment variables.");
            return null;
        }

        JSONObject jsonInput = new JSONObject()
                .put("email", email)
                .put("password", password);

        HttpURLConnection authConn = (HttpURLConnection) new URL(authUrl).openConnection();
        authConn.setRequestMethod("POST");
        authConn.setRequestProperty("Content-Type", "application/json");
        authConn.setDoOutput(true);
        authConn.setConnectTimeout(CONNECT_TIMEOUT_MS); // Set connect timeout
        authConn.setReadTimeout(READ_TIMEOUT_MS);      // Set read timeout

        try (OutputStream os = authConn.getOutputStream()) {
            os.write(jsonInput.toString().getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = authConn.getResponseCode();
        if (responseCode != 200) {
            System.err.println("Authentication failed with response code: " + responseCode);
            try (BufferedReader errorReader = new BufferedReader(
                    new InputStreamReader(authConn.getErrorStream(), StandardCharsets.UTF_8))) {
                StringBuilder errorResponse = new StringBuilder();
                String line;
                while ((line = errorReader.readLine()) != null) {
                    errorResponse.append(line.trim());
                }
                System.err.println("Authentication Error Details: " + errorResponse.toString());
            }
            return null;
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(authConn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder authResponse = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                authResponse.append(line.trim());
            }
            return extractToken(authResponse.toString());
        }
    }

    /**
     * Fetches product data for a list of SKUs from the Dropshipzone API v2 Products endpoint,
     * using the 'skus' filter and handling pagination.
     *
     * @param token The JWT token obtained from the authentication step.
     * @param allSkus A list of all SKU strings for which to fetch data.
     * @return A single raw JSON string containing aggregated product data from all pages/batches.
     * @throws IOException If an I/O error occurs during any HTTP request.
     */
    protected static String fetchStock(String token, List<String> allSkus) throws IOException {
        String productsBaseUrl = "https://api.dropshipzone.com.au/v2/products";
        JSONArray combinedResultArray = new JSONArray();

        // Iterate through all SKUs in batches of DROPSHIPZONE_API_SKU_LIMIT
        for (int i = 0; i < allSkus.size(); i += DROPSHIPZONE_API_SKU_LIMIT) {
            int endIndex = Math.min(i + DROPSHIPZONE_API_SKU_LIMIT, allSkus.size());
            List<String> currentApiBatchSkus = allSkus.subList(i, endIndex);
            String skuString = String.join(",", currentApiBatchSkus);

            int pageNumber = 1;
            int totalPages = 1; // Initialize to 1 to enter the inner pagination loop at least once

            do {
                // Build query parameters for GET request
                StringBuilder queryParams = new StringBuilder();
                queryParams.append("?");
                queryParams.append("skus=").append(URLEncoder.encode(skuString, StandardCharsets.UTF_8.toString()));
                queryParams.append("&page_size=").append(API_PAGE_SIZE);
                queryParams.append("&page_number=").append(pageNumber);

                String productsUrl = productsBaseUrl + queryParams.toString();

                System.out.println("\nDropshipzone Products Request URL (SKU Batch " + ((i / DROPSHIPZONE_API_SKU_LIMIT) + 1) + ", Page " + pageNumber + "): " + productsUrl);

                HttpURLConnection conn = (HttpURLConnection) new URL(productsUrl).openConnection();
                conn.setRequestMethod("GET"); // Changed from POST to GET
                conn.setRequestProperty("Authorization", "jwt " + token);
                // No Content-Type and DoOutput for GET requests without a body
                conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
                conn.setReadTimeout(READ_TIMEOUT_MS);

                // No OutputStream needed for GET request body

                int responseCode = conn.getResponseCode();
                System.out.println("Dropshipzone Products API Response Code (SKU Batch " + ((i / DROPSHIPZONE_API_SKU_LIMIT) + 1) + ", Page " + pageNumber + "): " + responseCode);

                String responseBody;
                if (responseCode != 200) {
                    try (BufferedReader errorReader = new BufferedReader(
                            new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
                        StringBuilder errorResponse = new StringBuilder();
                        String line;
                        while ((line = errorReader.readLine()) != null) {
                            errorResponse.append(line.trim());
                        }
                        responseBody = errorResponse.toString();
                        System.err.println("Dropshipzone Products API Error Response (SKU Batch " + ((i / DROPSHIPZONE_API_SKU_LIMIT) + 1) + ", Page " + pageNumber + "):\n" + responseBody);
                    }
                    responseBody = "{\"result\":[],\"total\":0,\"total_pages\":0,\"current_page\":0}"; // Default empty
                } else {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                        StringBuilder response = new StringBuilder();
                        String line;
                        while ((line = reader.readLine()) != null) {
                            response.append(line.trim());
                        }
                        responseBody = response.toString();
                    }
                }
                System.out.println("Dropshipzone Products Raw Response (SKU Batch " + ((i / DROPSHIPZONE_API_SKU_LIMIT) + 1) + ", Page " + pageNumber + "): " + responseBody);

                try {
                    JSONObject pageResultJson = new JSONObject(responseBody);
                    if (pageResultJson.has("result") && pageResultJson.getJSONArray("result").length() > 0) {
                        JSONArray pageProductsArray = pageResultJson.getJSONArray("result");
                        for (int j = 0; j < pageProductsArray.length(); j++) {
                            combinedResultArray.put(pageProductsArray.getJSONObject(j));
                        }
                    }

                    // Update totalPages and current_page for pagination loop
                    totalPages = pageResultJson.optInt("total_pages", pageNumber); // Default to current if not found
                    pageNumber = pageResultJson.optInt("current_page", pageNumber) + 1; // Increment for next loop
                    System.out.println("Pagination Info: Current Page=" + (pageNumber - 1) + ", Total Pages=" + totalPages + ", Total products fetched so far: " + combinedResultArray.length());


                } catch (org.json.JSONException jsonE) {
                    System.err.println("ERROR: Failed to parse Dropshipzone Products API response for SKU batch " + ((i / DROPSHIPZONE_API_SKU_LIMIT) + 1) + ", page " + pageNumber + ".");
                    System.err.println("Raw response content that failed parsing: " + responseBody);
                    jsonE.printStackTrace();
                    break; // Exit inner loop on parsing error
                }

            } while (pageNumber <= totalPages); // Continue as long as there are more pages for this SKU batch
        }
        return new JSONObject().put("result", combinedResultArray).toString(); // Return with "result" key for consistency
    }

    /**
     * Extracts the JWT token from a JSON string.
     *
     * @param json The JSON string containing the token.
     * @return The extracted token string, or null if not found.
     */
    protected static String extractToken(String json) {
        JSONObject obj;
        try {
            obj = new JSONObject(json);
        } catch (org.json.JSONException jsonE) {
            System.err.println("ERROR: Failed to parse JSON for token extraction.");
            System.err.println("Raw JSON content that failed parsing: " + json);
            jsonE.printStackTrace();
            return null;
        }
        return obj.optString("token", null);
    }

    /**
     * Loads a list of SKUs from a CSV file located in an S3 bucket.
     * It assumes the SKUs are in the first column and skips the header row.
     *
     * @param s3Client The initialized S3Client.
     * @param bucketName The name of the S3 bucket where the CSV file is stored.
     * @param key The object key (path) to the CSV file within the S3 bucket.
     * @return A list of SKU strings.
     * @throws IOException If an I/O error occurs during file reading or S3 download.
     */
    protected static List<String> loadSkusFromCSV(S3Client s3Client, String bucketName, String key) throws IOException {
        List<String> skus = new ArrayList<>();
        System.out.println("Attempting to load SKUs from S3: Bucket=" + bucketName + ", Key=" + key);

        try (ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object, StandardCharsets.UTF_8))) {

            System.out.println("Successfully opened S3 object stream."); // Added logging here

            String line;
            boolean isFirstLine = true; // Flag to skip the header row
            while ((line = reader.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false; // Skip the first line (header)
                    continue;
                }
                line = line.trim(); // Trim whitespace
                if (!line.isEmpty()) {
                    // Split by comma and take the first part (assuming SKU is the first column)
                    skus.add(line.split(",")[0]);
                }
            }
            System.out.println("Successfully loaded " + skus.size() + " SKUs from S3.");
        } catch (Exception e) {
            System.err.println("Error loading SKUs from S3 bucket '" + bucketName + "' with key '" + key + "': " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to load SKUs from S3.", e);
        }
        return skus;
    }

    /**
     * Processes API data and applies the quantity rule (qty < 25 -> 0).
     * Now also extracts the 'cost' field.
     *
     * @param apiData The JSONArray containing product information returned from the API.
     * @param allOriginalSkus The complete list of SKUs read from the input CSV.
     * @return A list of Maps, where each map contains "sku", "quantity", and "cost".
     */
    protected static List<Map<String, String>> processStockData(JSONArray apiData, List<String> allOriginalSkus) {
        Map<String, JSONObject> apiDataMap = new HashMap<>();
        for (Object obj : apiData) {
            JSONObject item;
            try {
                item = new JSONObject(obj.toString());
            } catch (org.json.JSONException jsonE) {
                System.err.println("ERROR: Failed to parse individual item from Dropshipzone API response JSONArray. Skipping this item.");
                System.err.println("Problematic item string: " + obj.toString());
                jsonE.printStackTrace();
                continue;
            }
            // Assuming 'sku' is still the key for SKU in the v2 products response
            apiDataMap.put(item.optString("sku", "INVALID_SKU"), item);
        }

        List<Map<String, String>> processedSkuData = new ArrayList<>();
        // Only process SKUs that were explicitly in our input CSV
        for (String sku : allOriginalSkus) {
            JSONObject item = apiDataMap.get(sku);
            int stock_qty = 0; // Default to 0
            String cost = "0.00"; // Default cost to 0.00

            if (item != null) {
                // Read from 'stock_qty' field
                String stockQtyStr = item.optString("stock_qty", "0");
                try {
                    stock_qty = (int) Double.parseDouble(stockQtyStr);
                    if (stock_qty < 25) {
                        stock_qty = 0; // Set to 0 if less than 25
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Warning: Invalid number format for 'stock_qty': '" + stockQtyStr + "' for SKU " + sku + ". Defaulting to 0.");
                    stock_qty = 0; // Ensure it's 0 on parse error too
                }

                // Read from 'cost' field
                cost = item.optString("cost", "0.00");
                try {
                    // Optional: Validate cost is a number, if not, keep default
                    Double.parseDouble(cost);
                } catch (NumberFormatException e) {
                    System.err.println("Warning: Invalid number format for 'cost': '" + cost + "' for SKU " + sku + ". Defaulting to 0.00.");
                    cost = "0.00";
                }

            } else {
                System.out.println("Info: SKU " + sku + " from CSV not found in Dropshipzone API /v2/products response. Setting quantity and cost to 0.");
            }

            Map<String, String> skuEntry = new HashMap<>();
            skuEntry.put("sku", sku);
            skuEntry.put("quantity", String.valueOf(stock_qty));
            skuEntry.put("cost", cost);
            processedSkuData.add(skuEntry);
        }
        return processedSkuData;
    }


    /**
     * Updates the quantity of a specific item in Neto using the Neto API.
     * Credentials are retrieved from environment variables.
     *
     * @param sku The SKU of the item to update.
     * @param quantity The new quantity to set for the item.
     */
    protected static void updateNetoItem(String sku, int quantity) {
        String netoUrl = "https://www.shoppingsmart.com.au/do/WS/NetoAPI";

        // Retrieve Neto credentials from environment variables
        String netoUsername = System.getenv("NETOAPI_USERNAME");
        String netoKey = System.getenv("NETOAPI_KEY");

        if (netoUsername == null || netoKey == null || netoUsername.isEmpty() || netoKey.isEmpty()) {
            System.err.println("Error: Neto credentials (NETOAPI_USERNAME, NETOAPI_KEY) not set as environment variables. Skipping Neto update for SKU: " + sku);
            return;
        }

        JSONObject warehouseQuantity = new JSONObject()
                .put("WarehouseID", "2")
                .put("Quantity", String.valueOf(quantity))
                .put("Action", "Set");

        JSONObject item = new JSONObject()
                .put("SKU", sku)
                .put("WarehouseQuantity", warehouseQuantity);

        JSONObject payload = new JSONObject()
                .put("Item", item);

        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(netoUrl).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("NETOAPI_ACTION", "UpdateItem");
            conn.setRequestProperty("NETOAPI_USERNAME", netoUsername);
            conn.setRequestProperty("NETOAPI_KEY", netoKey);
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS); // Set connect timeout
            conn.setReadTimeout(READ_TIMEOUT_MS);      // Set read timeout

            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            String statusMessage = "";
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line.trim());
                }
                String rawResponse = response.toString();
                statusMessage = "Neto API Raw Response for SKU " + sku + " (Code: " + responseCode + "): " + rawResponse;

                try {
                    JSONObject netoResponseJson = new JSONObject(rawResponse);
                    // You might want to parse specific success/error messages from Neto's JSON response here
                    // For example: if (netoResponseJson.has("Ack") && netoResponseJson.getString("Ack").equals("Success")) { ... }
                } catch (org.json.JSONException jsonE) {
                    System.err.println("ERROR: Failed to parse Neto API response JSON for SKU " + sku + ".");
                    System.err.println("Raw Neto response content that failed parsing: " + rawResponse);
                    jsonE.printStackTrace();
                }
            } finally {
                System.out.println(String.format("Neto Update Status for SKU %s (Qty: %d): Response Code: %d. %s",
                    sku, quantity, responseCode, statusMessage));
            }

        } catch (IOException e) {
            System.err.println("Error calling Neto API for SKU " + sku + ": " + e.getMessage());
            e.printStackTrace();
            System.out.println(String.format("Neto Update Status for SKU %s (Qty: %d): Failed due to I/O Error: %s",
                sku, quantity, e.getMessage()));
        }
    }
    /**
     * Main method for standalone testing or running the client directly.
     * This can be used to test API connectivity and logic outside of Lambda.
     */
    public static void main(String[] args) {
        System.out.println("DropshipzoneAPIClient main method started for local testing.");

        // Define placeholder values for environment variables for local testing
        // You MUST replace these with your actual credentials and S3 details for real testing.
        // For security, do NOT hardcode sensitive credentials in production code.
        // These are just for local command-line execution demonstration.
        System.setProperty("DROPSHIPZONE_EMAIL", "your_dropshipzone_email@example.com");
        System.setProperty("DROPSHIPZONE_PASSWORD", "your_dropshipzone_password");
        System.setProperty("NETOAPI_USERNAME", "your_neto_api_username");
        System.setProperty("NETOAPI_KEY", "your_neto_api_key");
        System.setProperty("S3_BUCKET_NAME", "your-dropshipzone-skus-bucket"); // Your actual S3 bucket name
        System.setProperty("S3_FILE_KEY", "skus.csv"); // Your actual S3 file key (e.g., "input/skus.csv")
        System.setProperty("AWS_REGION", "ap-southeast-2"); // Your AWS Region (e.g., "us-east-1", "ap-southeast-2")

        // Initialize S3Client for local testing
        S3Client s3Client = null;
        try {
            String awsRegion = System.getProperty("AWS_REGION"); // Get region from system properties for local test

            // Build ClientOverrideConfiguration for timeouts
            ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                    .apiCallTimeout(Duration.ofMillis(45000)) // 45 seconds total API call timeout
                    .apiCallAttemptTimeout(Duration.ofMillis(30000)) // 30 seconds for each attempt
                    .build();

            s3Client = S3Client.builder()
                    .region(Region.of(awsRegion))
                    .overrideConfiguration(clientConfig)
                    .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder()
                            .connectionTimeout(Duration.ofMillis(5000))
                            .socketTimeout(Duration.ofMillis(30000)))
                    .build();
            System.out.println("Local Test: S3Client initialized with region: " + awsRegion);

            // --- Core Logic Execution ---
            String token = authenticate();
            if (token == null) {
                System.err.println("Local Test: Failed to authenticate with Dropshipzone. Exiting.");
                return;
            }
            System.out.println("Local Test: Dropshipzone token acquired.");

            List<String> skuList = loadSkusFromCSV(s3Client, System.getProperty("S3_BUCKET_NAME"), System.getProperty("S3_FILE_KEY"));
            if (skuList.isEmpty()) {
                System.out.println("Local Test: No SKUs loaded from S3. Exiting.");
                return;
            }
            System.out.println("Local Test: Loaded " + skuList.size() + " SKUs from S3.");

            String stockResponse = fetchStock(token, skuList);
            JSONObject stockJson = new JSONObject(stockResponse);
            JSONArray resultArray = new JSONArray();
            if (stockJson.has("result") && stockJson.getJSONArray("result").length() > 0) { // Changed from "products" to "result"
                resultArray = stockJson.getJSONArray("result");
                System.out.println("Local Test: Total stock items fetched from Dropshipzone API: " + resultArray.length());
            } else {
                System.out.println("Local Test: No stock data found in the 'result' field of the response.");
            }

            // The processedSkuData now holds List<Map<String, String>>
            List<Map<String, String>> processedSkuData = processStockData(resultArray, skuList);
            System.out.println("Local Test: Processed stock data for " + processedSkuData.size() + " SKUs.");

            System.out.println("\n--- Local Test: Updating Neto Items ---");
            for (Map<String, String> entry : processedSkuData) { // Changed loop type
                String sku = entry.get("sku");
                int quantity = Integer.parseInt(entry.get("quantity"));
                String cost = entry.get("cost"); // Get the cost

                System.out.println(String.format("Local Test: Prepared to update Neto: SKU=%s, Quantity=%d, Cost=%s", sku, quantity, cost));
                updateNetoItem(sku, quantity); // Call synchronously for local testing simplicity
            }
            System.out.println("Local Test: Completed Neto updates.");

        } catch (Exception e) {
            System.err.println("Local Test: An error occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (s3Client != null) {
                s3Client.close();
                System.out.println("Local Test: S3Client closed.");
            }
            // Clear system properties after test (optional, but good practice if running multiple tests)
            System.clearProperty("DROPSHIPZONE_EMAIL");
            System.clearProperty("DROPSHIPZONE_PASSWORD");
            System.clearProperty("NETOAPI_USERNAME");
            System.clearProperty("NETOAPI_KEY");
            System.clearProperty("S3_BUCKET_NAME");
            System.clearProperty("S3_FILE_KEY");
            System.clearProperty("AWS_REGION");
        }
    }
}
