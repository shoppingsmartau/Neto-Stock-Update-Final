import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONTokener;

// AWS SDK for S3 imports (unchanged)
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import java.time.Duration;

// Imports for JAVA.NET.HTTP CLIENT
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;


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

    // Define the maximum number of SKUs allowed in the 'skus' parameter for Dropshipzone API /v2/products endpoint
    private static final int DROPSHIPZONE_API_SKU_LIMIT = 50; // As per user's specification "up to 50"

    // Define the number of products to request per API call for pagination (per filtered SKU batch)
    private static final int API_PAGE_SIZE = 100; // Common pagination parameter

    // Define timeouts for HTTP connections (in milliseconds)
    public static final int CONNECT_TIMEOUT_MS = 5000; // 5 seconds
    public static final int READ_TIMEOUT_MS = 15000;  // 15 seconds (for Dropshipzone and Neto)


    /**
     * Authenticates with the Dropshipzone API using predefined credentials.
     * Credentials are retrieved from environment variables.
     *
     * @param httpClient The shared HttpClient instance to use for the request.
     * @return The JWT token string if authentication is successful, otherwise null.
     * @throws IOException If an I/O error occurs during the HTTP request.
     */
    protected static String authenticate(HttpClient httpClient) throws IOException, InterruptedException {
        String authUrl = "https://api.dropshipzone.com.au/auth";

        String email = System.getenv("DROPSHIPZONE_EMAIL");
        String password = System.getenv("DROPSHIPZONE_PASSWORD");

        if (email == null || password == null || email.isEmpty() || password.isEmpty()) {
            System.err.println("Error: Dropshipzone credentials (DROPSHIPZONE_EMAIL, DROPSHIPZONE_PASSWORD) not set as environment variables.");
            return null;
        }

        JSONObject jsonInput = new JSONObject()
                .put("email", email)
                .put("password", password);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(java.net.URI.create(authUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonInput.toString()))
                .timeout(Duration.ofMillis(READ_TIMEOUT_MS))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        int responseCode = response.statusCode();
        if (responseCode != 200) {
            System.err.println("Authentication failed with response code: " + responseCode);
            System.err.println("Authentication Error Details: " + response.body());
            return null;
        }

        return extractToken(response.body());
    }

    /**
     * Fetches product data for a list of SKUs from the Dropshipzone API v2 Products endpoint,
     * using the 'skus' filter and handling pagination. It now processes the response in a streaming manner
     * and populates the provided `processedSkuDataMap` directly, avoiding large in-memory JSON arrays.
     *
     * @param httpClient The shared HttpClient instance to use for the request.
     * @param token The JWT token obtained from the authentication step.
     * @param allSkus A list of all SKU strings for which to fetch data.
     * @param processedSkuDataMap A Map to be populated with processed SKU data (SKU -> {quantity, cost, selling_price}).
     * @throws IOException If an I/O error occurs during any HTTP request.
     */
    protected static void fetchStock(HttpClient httpClient, String token, List<String> allSkus, Map<String, Map<String, String>> processedSkuDataMap) throws IOException, InterruptedException {
        String productsBaseUrl = "https://api.dropshipzone.com.au/v2/products";

        // Iterate through all SKUs in batches of DROPSHIPZONE_API_SKU_LIMIT
        for (int i = 0; i < allSkus.size(); i += DROPSHIPZONE_API_SKU_LIMIT) {
            int endIndex = Math.min(i + DROPSHIPZONE_API_SKU_LIMIT, allSkus.size());
            List<String> currentApiBatchSkus = allSkus.subList(i, endIndex);
            String skuString = String.join(",", currentApiBatchSkus);

            int requestedPageNumber = 1; // Our internal counter for the page number to request
            int totalPages = 1;         // Initialize to 1 to enter the inner pagination loop at least once

            do {
                StringBuilder queryParams = new StringBuilder();
                queryParams.append("?");
                queryParams.append("skus=").append(URLEncoder.encode(skuString, StandardCharsets.UTF_8.toString()));
                queryParams.append("&page_size=").append(API_PAGE_SIZE);
                queryParams.append("&page_number=").append(requestedPageNumber); // Use our internal requested page number

                String productsUrl = productsBaseUrl + queryParams.toString();

                System.out.println("\nDropshipzone Products Request URL (SKU Batch " + ((i / DROPSHIPZONE_API_SKU_LIMIT) + 1) + ", Page " + requestedPageNumber + "): " + productsUrl);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(java.net.URI.create(productsUrl))
                        .header("Authorization", "jwt " + token)
                        .GET()
                        .timeout(Duration.ofMillis(READ_TIMEOUT_MS))
                        .build();

                HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

                int responseCode = response.statusCode();
                System.out.println("Dropshipzone Products API Response Code (SKU Batch " + ((i / DROPSHIPZONE_API_SKU_LIMIT) + 1) + ", Page " + requestedPageNumber + "): " + responseCode);

                JSONObject pageResultJson;
                try (InputStream is = response.body();
                     InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
                     BufferedReader reader = new BufferedReader(isr)) {

                    if (responseCode != 200) {
                        String errorBody;
                        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                            byte[] buffer = new byte[4096];
                            int len;
                            while ((len = is.read(buffer)) != -1) {
                                os.write(buffer, 0, len);
                            }
                            errorBody = os.toString(StandardCharsets.UTF_8.toString());
                        }
                        System.err.println("Dropshipzone Products API Error Response (SKU Batch " + ((i / DROPSHIPZONE_API_SKU_LIMIT) + 1) + ", Page " + requestedPageNumber + "):\n" + errorBody);
                        pageResultJson = new JSONObject("{\"result\":[],\"total\":0,\"total_pages\":0,\"current_page\":0}");
                    } else {
                        pageResultJson = new JSONObject(new JSONTokener(reader));
                    }

                } catch (org.json.JSONException jsonE) {
                    System.err.println("ERROR: Failed to parse Dropshipzone Products API response for SKU batch " + ((i / DROPSHIPZONE_API_SKU_LIMIT) + 1) + ", page " + requestedPageNumber + ".");
                    System.err.println("Problem reading or parsing response: " + jsonE.getMessage());
                    jsonE.printStackTrace();
                    pageResultJson = new JSONObject("{\"result\":[],\"total\":0,\"total_pages\":0,\"current_page\":0}");
                    break;
                }

                if (pageResultJson.has("result") && pageResultJson.getJSONArray("result").length() > 0) {
                    JSONArray pageProductsArray = pageResultJson.getJSONArray("result");
                    processAndAddSkuData(pageProductsArray, processedSkuDataMap);
                }

                // Always get total_pages from the API response
                totalPages = pageResultJson.optInt("total_pages", requestedPageNumber);
                // Increment our internal counter for the NEXT request
                requestedPageNumber++;
                // Log the current state based on what was *just* processed and what will be requested next
                System.out.println("Pagination Info: Processed Page=" + (requestedPageNumber - 1) + ", Reported Total Pages=" + totalPages + ", Total unique products collected so far: " + processedSkuDataMap.size());


            } while (requestedPageNumber <= totalPages); // Loop continues as long as there are more pages to request
        }
    }

    /**
     * Processes a JSONArray of product data and adds/updates entries in the provided map.
     * This replaces the previous `processStockData` which returned a new list.
     *
     * @param apiData The JSONArray of products from one API page/batch.
     * @param processedSkuDataMap The map to update with processed SKU data.
     */
    private static void processAndAddSkuData(JSONArray apiData, Map<String, Map<String, String>> processedSkuDataMap) {
        for (Object obj : apiData) {
            JSONObject item;
            try {
                item = new JSONObject(obj.toString()); // obj.toString() creates string for current item only
            } catch (org.json.JSONException jsonE) {
                System.err.println("ERROR: Failed to parse individual item from Dropshipzone API response JSONArray. Skipping this item.");
                System.err.println("Problematic item string: " + obj.toString());
                jsonE.printStackTrace();
                continue;
            }

            String sku = item.optString("sku", "INVALID_SKU");
            int stock_qty = 0;
            String cost = "0.00";
            String sellingPrice = "0"; // Initialize selling price as a whole number string

            if (!sku.equals("INVALID_SKU")) { // Only process if SKU is valid
                String stockQtyStr = item.optString("stock_qty", "0");
                try {
                    stock_qty = (int) Double.parseDouble(stockQtyStr);
                    if (stock_qty < 25) {
                        stock_qty = 0;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Warning: Invalid number format for 'stock_qty': '" + stockQtyStr + "' for SKU " + sku + ". Defaulting to 0.");
                    stock_qty = 0;
                }

                cost = item.optString("cost", "0.00");
                try {
                    Double.parseDouble(cost);
                } catch (NumberFormatException e) {
                    System.err.println("Warning: Invalid number format for 'cost': '" + cost + "' for SKU " + sku + ". Defaulting to 0.00.");
                    cost = "0.00";
                }

                // Calculate Selling Price: price * 1.4 rounded to nearest whole number
                String priceStr = item.optString("price", "0.00");
                try {
                    double price = Double.parseDouble(priceStr);
                    double calculatedSellingPrice = price * 1.4;
                    // Round to nearest whole number and convert to integer string
                    sellingPrice = String.valueOf(Math.round(calculatedSellingPrice));
                } catch (NumberFormatException e) {
                    System.err.println("Warning: Invalid number format for 'price': '" + priceStr + "' for SKU " + sku + ". Defaulting selling price to 0.");
                    sellingPrice = "0";
                }


                Map<String, String> skuEntry = new HashMap<>();
                skuEntry.put("sku", sku);
                skuEntry.put("quantity", String.valueOf(stock_qty));
                skuEntry.put("cost", cost);
                skuEntry.put("selling_price", sellingPrice); // Add selling price
                processedSkuDataMap.put(sku, skuEntry); // Add/update entry in the main map
            } else {
                System.err.println("Error: Skipping item due to invalid SKU in API response: " + item.toString());
            }
        }
    }


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

    protected static List<String> loadSkusFromCSV(S3Client s3Client, String bucketName, String key) throws IOException {
        List<String> skus = new ArrayList<>();
        System.out.println("Attempting to load SKUs from S3: Bucket=" + bucketName + ", Key=" + key);

        try (ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object, StandardCharsets.UTF_8))) {

            System.out.println("Successfully opened S3 object stream.");

            String line;
            boolean isFirstLine = true;
            while ((line = reader.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }
                line = line.trim();
                if (!line.isEmpty()) {
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
     * Updates the quantity of a specific item in Neto using the Neto API.
     * Credentials are retrieved from environment variables.
     *
     * @param httpClient The shared HttpClient instance to use for the request.
     * @param sku The SKU of the item to update.
     * @param quantity The new quantity to set for the item.
     */
    protected static void updateNetoItem(HttpClient httpClient, String sku, int quantity) {
        String netoUrl = "https://www.shoppingsmart.com.au/do/WS/NetoAPI";

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
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(java.net.URI.create(netoUrl))
                    .header("NETOAPI_ACTION", "UpdateItem")
                    .header("NETOAPI_USERNAME", netoUsername)
                    .header("NETOAPI_KEY", netoKey)
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
                    .timeout(Duration.ofMillis(READ_TIMEOUT_MS))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            int responseCode = response.statusCode();
            String rawResponse = response.body();
            String statusMessage = "Neto API Raw Response for SKU " + sku + " (Code: " + responseCode + "): " + rawResponse;

            System.out.println(String.format("Neto Update Status for SKU %s (Qty: %d): Response Code: %d. %s",
                sku, quantity, responseCode, statusMessage));

            try {
                JSONObject netoResponseJson = new JSONObject(rawResponse);
            } catch (org.json.JSONException jsonE) {
                System.err.println("ERROR: Failed to parse Neto API response JSON for SKU " + sku + ".");
                System.err.println("Raw Neto response content that failed parsing: " + rawResponse);
                jsonE.printStackTrace();
            }

        } catch (IOException | InterruptedException e) {
            System.err.println("Error calling Neto API for SKU " + sku + ": " + e.getMessage());
            e.printStackTrace();
            System.out.println(String.format("Neto Update Status for SKU %s (Qty: %d): Failed due to I/O Error or Interruption: %s",
                sku, quantity, e.getMessage()));
        }
    }
}
