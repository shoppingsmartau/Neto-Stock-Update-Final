import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
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

    // Define a batch size for API requests to handle potential API limits
    private static final int SKU_BATCH_SIZE = 40; // Based on observed behavior, can be adjusted

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
            // System.out.println("Dropshipzone Auth Raw Response: " + authResponse.toString()); // Reduced verbosity
            return extractToken(authResponse.toString());
        }
    }

    /**
     * Fetches stock data for a list of SKUs from the Dropshipzone API in batches.
     *
     * @param token The JWT token obtained from the authentication step.
     * @param allSkus A list of all SKU strings for which to fetch stock.
     * @return A single raw JSON response string containing aggregated stock data from all batches.
     * @throws IOException If an I/O error occurs during any HTTP request.
     */
    protected static String fetchStock(String token, List<String> allSkus) throws IOException {
        String stockUrl = "https://api.dropshipzone.com.au/stock";
        JSONArray combinedResultArray = new JSONArray();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime = endTime.minusDays(7);

        for (int i = 0; i < allSkus.size(); i += SKU_BATCH_SIZE) {
            int endIndex = Math.min(i + SKU_BATCH_SIZE, allSkus.size());
            List<String> currentBatchSkus = allSkus.subList(i, endIndex);
            String skuString = String.join(",", currentBatchSkus);

            JSONObject requestPayload = new JSONObject()
                    .put("start_time", startTime.format(formatter))
                    .put("end_time", endTime.format(formatter))
                    .put("skus", skuString);

            // System.out.println("\nDropshipzone Stock Request JSON Payload (Batch " + ((i / SKU_BATCH_SIZE) + 1) + "):"); // Reduced verbosity
            // if (requestPayload.toString().length() < 2000) {
            //     System.out.println(requestPayload.toString(2));
            // } else {
            //     System.out.println("Request payload for batch is very large, not printing to console.");
            // }

            HttpURLConnection conn = (HttpURLConnection) new URL(stockUrl).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization", "jwt " + token);
            conn.setDoOutput(true);
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS); // Set connect timeout
            conn.setReadTimeout(READ_TIMEOUT_MS);      // Set read timeout

            try (OutputStream os = conn.getOutputStream()) {
                os.write(requestPayload.toString().getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            System.out.println("Dropshipzone Stock API Response Code (Batch " + ((i / SKU_BATCH_SIZE) + 1) + "): " + responseCode);

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
                    System.err.println("Dropshipzone Stock API Error Response (Batch " + ((i / SKU_BATCH_SIZE) + 1) + "):\n" + responseBody);
                }
                responseBody = "{\"result\":[]}"; // Return empty array on error for parsing
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
            // System.out.println("Dropshipzone Stock Raw Response (Batch " + ((i / SKU_BATCH_SIZE) + 1) + "): " + responseBody); // Reduced verbosity
            try {
                JSONObject batchResultJson = new JSONObject(responseBody);
                if (batchResultJson.has("result") && batchResultJson.getJSONArray("result").length() > 0) {
                    JSONArray batchResultArray = batchResultJson.getJSONArray("result");
                    for (int j = 0; j < batchResultArray.length(); j++) {
                        combinedResultArray.put(batchResultArray.getJSONObject(j));
                    }
                }
            } catch (org.json.JSONException jsonE) {
                System.err.println("ERROR: Failed to parse Dropshipzone Stock API response for batch " + ((i / SKU_BATCH_SIZE) + 1) + ".");
                System.err.println("Raw response content that failed parsing: " + responseBody);
                jsonE.printStackTrace();
            }
        }
        return new JSONObject().put("result", combinedResultArray).toString();
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
            // System.out.println("Attempting to extract token from JSON: " + json); // Reduced verbosity
            obj = new JSONObject(json);
            // System.out.println("Successfully extracted token JSON."); // Reduced verbosity
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
     * Returns the processed SKU and quantity as a List of String arrays.
     *
     * @param apiData The JSONArray containing stock information returned from the API.
     * @param allOriginalSkus The complete list of SKUs read from the input CSV.
     * @return A list of String arrays, where each inner array is [sku, processed_quantity_string].
     */
    protected static List<String[]> processStockData(JSONArray apiData, List<String> allOriginalSkus) {
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
            apiDataMap.put(item.optString("sku", "INVALID_SKU"), item);
        }

        List<String[]> processedSkuData = new ArrayList<>();
        for (String sku : allOriginalSkus) {
            JSONObject item = apiDataMap.get(sku);
            int new_qty = 0; // Default to 0

            if (item != null) {
                String newQtyStr = item.optString("new_qty", "0");
                try {
                    new_qty = (int) Double.parseDouble(newQtyStr);
                    if (new_qty < 25) {
                        new_qty = 0; // Set to 0 if less than 25
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Warning: Invalid number format for 'new_qty': '" + newQtyStr + "' for SKU " + sku + ". Defaulting to 0.");
                    new_qty = 0; // Ensure it's 0 on parse error too
                }
            }
            processedSkuData.add(new String[]{sku, String.valueOf(new_qty)});
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
            System.out.println("Neto API Response Code for SKU " + sku + ": " + responseCode);

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line.trim());
                }
                // System.out.println("Neto API Raw Response for SKU " + sku + ": " + response.toString()); // Reduced verbosity

                try {
                    JSONObject netoResponseJson = new JSONObject(response.toString());
                    // System.out.println("Neto API Parsed Response for SKU " + sku + ": " + netoResponseJson.toString(2)); // Reduced verbosity
                } catch (org.json.JSONException jsonE) {
                    System.err.println("ERROR: Failed to parse Neto API response for SKU " + sku + ".");
                    System.err.println("Raw Neto response content that failed parsing: " + response.toString());
                    jsonE.printStackTrace();
                }

            }

        } catch (IOException e) {
            System.err.println("Error calling Neto API for SKU " + sku + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}
