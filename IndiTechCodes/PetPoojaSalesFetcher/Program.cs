using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Threading;
using System.Text.Json.Serialization;

namespace PetPoojaSalesFetcher
{
    class Program
    {
        // SQLite file name (will be created in working directory)
        private const string SqliteFile = "sales_data.db";

        static async Task<int> Main(string[] args)
        {
            Console.WriteLine("=== PetPooja Sales Fetcher ===");

            try
            {
                var config = LoadConfigFromEnv();
                var url = BuildUrl(config);
                Console.WriteLine("Request URL built.");

                string json = await FetchWithRetryAsync(url, maxRetries: 3, initialDelayMs: 1000);
                if (string.IsNullOrWhiteSpace(json))
                {
                    Console.WriteLine("No response or empty response received. Exiting.");
                    return 1;
                }



                // Ensure DB and table exist
                EnsureDatabaseAndTable();

                // Parse and insert
                int inserted = await ParseAndInsertAsync(json);
                Console.WriteLine($"Completed. Rows inserted/updated: {inserted}");

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Fatal error: " + ex.Message);
                Console.WriteLine(ex.ToString());
                return 2;
            }
        }

        // Configuration holder
        class Config
        {
            public string BaseUrl { get; set; }
            public string AppKey { get; set; }
            public string AppSecret { get; set; }
            public string AccessToken { get; set; }
            public string RestID { get; set; }
            public string FromDate { get; set; }
            public string ToDate { get; set; }
        }

        static Config LoadConfigFromEnv()
        {
            var cfg = new Config
            {
                BaseUrl = Environment.GetEnvironmentVariable("PETPOOJA_BASE_URL")
                          ?? "http://api.petpooja.com/V1/orders/get_sales_data/",
                AppKey = Environment.GetEnvironmentVariable("PETPOOJA_APP_KEY") ?? throw new Exception("PETPOOJA_APP_KEY not set"),
                AppSecret = Environment.GetEnvironmentVariable("PETPOOJA_APP_SECRET") ?? throw new Exception("PETPOOJA_APP_SECRET not set"),
                AccessToken = Environment.GetEnvironmentVariable("PETPOOJA_ACCESS_TOKEN") ?? throw new Exception("PETPOOJA_ACCESS_TOKEN not set"),
                RestID = Environment.GetEnvironmentVariable("PETPOOJA_REST_ID") ?? throw new Exception("PETPOOJA_REST_ID not set"),
                FromDate = Environment.GetEnvironmentVariable("PETPOOJA_FROM_DATE") ?? throw new Exception("PETPOOJA_FROM_DATE not set"),
                ToDate = Environment.GetEnvironmentVariable("PETPOOJA_TO_DATE") ?? throw new Exception("PETPOOJA_TO_DATE not set")
            };

            return cfg;
        }

        static string BuildUrl(Config cfg)
        {
            // Build querystring safely (URL encode parameters)
            string q = $"?app_key={Uri.EscapeDataString(cfg.AppKey)}" +
                       $"&app_secret={Uri.EscapeDataString(cfg.AppSecret)}" +
                       $"&access_token={Uri.EscapeDataString(cfg.AccessToken)}" +
                       $"&restID={Uri.EscapeDataString(cfg.RestID)}" +
                       $"&from_date={Uri.EscapeDataString(cfg.FromDate)}" +
                       $"&to_date={Uri.EscapeDataString(cfg.ToDate)}";

            return cfg.BaseUrl.TrimEnd('/') + "/" + q; // keep compatible with provided format
        }

        static async Task<string?> FetchWithRetryAsync(string url, int maxRetries = 3, int initialDelayMs = 1000)
        {
            using var client = new HttpClient();
            int attempt = 0;
            int delay = initialDelayMs;

            while (attempt <= maxRetries)
            {
                attempt++;
                try
                {
                    Console.WriteLine($"Fetching (attempt {attempt})...");
                    using var resp = await client.GetAsync(url);
                    if (resp.IsSuccessStatusCode)
                    {
                        var content = await resp.Content.ReadAsStringAsync();
                        Console.WriteLine("HTTP 200 OK received.");
                        
                        
                        // cHECKING JSON DATA
                        Console.WriteLine("RAW JSON RESPONSE:");
                        Console.WriteLine(content);
                        return content;
                    }
                    else
                    {
                        Console.WriteLine($"HTTP {(int)resp.StatusCode} - {resp.ReasonPhrase}");
                        // treat 4xx as non-retriable except maybe 429
                        if ((int)resp.StatusCode >= 400 && (int)resp.StatusCode < 500 && resp.StatusCode != System.Net.HttpStatusCode.TooManyRequests)
                        {
                            Console.WriteLine("Client error, will not retry.");
                            return null;
                        }
                    }
                }
                catch (HttpRequestException ex)
                {
                    Console.WriteLine($"Request failed: {ex.Message}");
                }
                catch (TaskCanceledException tce)
                {
                    Console.WriteLine($"Request cancelled/timeout: {tce.Message}");
                }

                if (attempt > maxRetries) break;
                Console.WriteLine($"Waiting {delay}ms before next attempt...");
                await Task.Delay(delay);
                delay *= 2; // exponential backoff
            }

            Console.WriteLine("Max retries reached, giving up.");
            return null;
        }

        static void EnsureDatabaseAndTable()
        {
            var csb = new SqliteConnectionStringBuilder { DataSource = SqliteFile };
            using var conn = new SqliteConnection(csb.ToString());
            conn.Open();

            string create = @"
        CREATE TABLE IF NOT EXISTS sales_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            receipt_number TEXT,
            sale_date TEXT,
            transaction_time TEXT,
            sale_amount REAL,
            tax_amount REAL,
            discount_amount REAL,
            round_off REAL,
            net_sale REAL,
            payment_mode TEXT,
            order_type TEXT,
            transaction_status TEXT
        );";
            using var cmd = conn.CreateCommand();
            cmd.CommandText = create;
            cmd.ExecuteNonQuery();
            conn.Close();

            Console.WriteLine($"Database ensured at '{Path.GetFullPath(SqliteFile)}'");
        }

        // Parse JSON and insert rows. Returns count of inserted rows
        static async Task<int> ParseAndInsertAsync(string json)
        {
            int inserted = 0;
            using var doc = JsonDocument.Parse(json);
            JsonElement root = doc.RootElement;

            // The exact JSON structure is unknown; attempt to find the array of orders/sales.
            
            JsonElement? arrayElement = null;

            if (root.ValueKind == JsonValueKind.Array)
            {
                arrayElement = root;
            }
            else
            {
                // try common fields
                if (root.TryGetProperty("data", out var dataProp) && dataProp.ValueKind == JsonValueKind.Array)
                    arrayElement = dataProp;
                else if (root.TryGetProperty("orders", out var ordersProp) && ordersProp.ValueKind == JsonValueKind.Array)
                    arrayElement = ordersProp;
                else if (root.TryGetProperty("sales", out var salesProp) && salesProp.ValueKind == JsonValueKind.Array)
                    arrayElement = salesProp;
                else
                {
                    // search for first array somewhere in the tree
                    foreach (var prop in root.EnumerateObject())
                    {
                        if (prop.Value.ValueKind == JsonValueKind.Array)
                        {
                            arrayElement = prop.Value;
                            break;
                        }
                    }
                }
            }

            if (arrayElement == null)
            {
                Console.WriteLine("Could not find an array of sales/orders in returned JSON. Trying to parse single object as one record.");
                // if root is an object representing one order, treat it as single
                if (root.ValueKind == JsonValueKind.Object)
                {
                    var record = ParseRecord(root);
                    if (record != null)
                    {
                        inserted += InsertRecord(record);
                    }
                }
                return inserted;
            }

            foreach (var item in arrayElement.Value.ValueKind == JsonValueKind.Array? arrayElement.Value.EnumerateArray(): Enumerable.Empty<JsonElement>())
            {
                var record = ParseRecord(item);
                if (record != null)
                {
                    inserted += InsertRecord(record);
                }
            }

            return inserted;
        }

        // record holder
        class SalesRecord
        {
            [JsonPropertyName("Receipt number")]
            public string ReceiptNumber { get; set; } = "";

            [JsonPropertyName("Receipt Date")]
            public string SaleDate { get; set; } = "";

            [JsonPropertyName("Transaction Time")]
            public string TransactionTime { get; set; } = "";

            [JsonPropertyName("Invoice amount")]
            public double SaleAmount { get; set; } = 0.0;

            [JsonPropertyName("Tax amount")]
            public double TaxAmount { get; set; } = 0.0;

            [JsonPropertyName("Discount amount")]
            public double DiscountAmount { get; set; } = 0.0;

            [JsonPropertyName("Round Off")]
            public double RoundOff { get; set; } = 0.0;

            [JsonPropertyName("Net sale")]
            public double NetSale { get; set; } = 0.0;

            [JsonPropertyName("Payment Mode")]
            public string PaymentMode { get; set; } = "";

            [JsonPropertyName("Order Type")]
            public string OrderType { get; set; } = "";

            [JsonPropertyName("Transaction status")]
            public string TransactionStatus { get; set; } = ""; 

        }

        static SalesRecord? ParseRecord(JsonElement el)
        {
            try
            {
                // Helper to read string or return empty
                string GetString(params string[] possibleNames)
                {
                    foreach (var n in possibleNames)
                    {
                        if (el.TryGetProperty(n, out var prop) && prop.ValueKind != JsonValueKind.Null)
                            return prop.ToString();
                    }
                    return "";
                }

                double GetDouble(params string[] possibleNames)
                {
                    foreach (var n in possibleNames)
                    {
                        if (el.TryGetProperty(n, out var prop) && prop.ValueKind != JsonValueKind.Null)
                        {
                            if (prop.ValueKind == JsonValueKind.Number && prop.TryGetDouble(out var d)) return d;
                            if (prop.ValueKind == JsonValueKind.String)
                            {
                                var s = prop.GetString();
                                if (double.TryParse(s, out var d2)) return d2;
                            }
                        }
                    }
                    return 0.0;
                }

                // Map likely names. The prompt suggested:
                // Receipt Date → Sale Date
                // Receipt number → Transaction ID
                // Invoice amount → Sale Amount
                // Discount amount → Discount Amount
                // Tax amount → Tax Amount
                // Net sale → Final Sale Amount
                // Transaction status → Sale Type (SALE/RETURN)

                var recNo = GetString("Receipt number");
                var receiptDateRaw = GetString( "Receipt Date");
                var transactionTime = GetString("Transaction Time");
                var saleAmount = GetDouble("Invoice amount");
                var discount = GetDouble("Discount amount");
                var tax = GetDouble("Tax amount");
                var netSale = GetDouble( "Net sale");

                var paymentMode = GetString("Payment Mode");
                var orderType = GetString("Order Type");
                var transactionStatus = GetString("Transaction status");

                // Try to split receiptDateRaw into date and time
                /*string saleDate = "";
                string transactionTime = "";
                if (!string.IsNullOrWhiteSpace(receiptDateRaw))
                {
                    // many formats are possible; try parse to DateTime
                    if (DateTime.TryParse(receiptDateRaw, out var dt))
                    {
                        saleDate = dt.ToString("yyyy-MM-dd");
                        transactionTime = dt.ToString("HH:mm:ss");
                    }
                    else
                    {
                        // if it's space-separated 'YYYY-MM-DD HH:MM:SS', split
                        var parts = receiptDateRaw.Split(' ');
                        if (parts.Length >= 1) saleDate = parts[0];
                        if (parts.Length >= 2) transactionTime = parts[1];
                    }
                }*/

                // round_off logic: if JSON provides "round_off" or "roundoff" use it, else compute safely
                double roundOff = GetDouble("round_off", "roundoff", "roundOff");
                if (roundOff == 0.0)
                {
                    // compute approximate round off: net - (sale - tax - discount)
                    // Only compute if netSale and saleAmount present (non-zero)
                    if (netSale != 0.0 || saleAmount != 0.0)
                    {
                        roundOff = Math.Round(netSale - (saleAmount - tax - discount), 2);
                    }
                }

                return new SalesRecord
                {
                    ReceiptNumber = recNo,
                    SaleDate = receiptDateRaw,
                    TransactionTime = transactionTime,
                    SaleAmount = saleAmount,
                    TaxAmount = tax,
                    DiscountAmount = discount,
                    RoundOff = roundOff,
                    NetSale = netSale,
                    PaymentMode = paymentMode,
                    OrderType = orderType,
                    TransactionStatus = transactionStatus
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed parsing a record: " + ex.Message);
                return null;
            }
        }

        static int InsertRecord(SalesRecord rec)
        {
            try
            {
                var csb = new SqliteConnectionStringBuilder { DataSource = SqliteFile };
                using var conn = new SqliteConnection(csb.ToString());
                conn.Open();

                
                // For simplicity I insert rows; modify as needed to update duplicates.

                string insert = @"
                INSERT INTO sales_data 
                (receipt_number, sale_date, transaction_time, sale_amount, tax_amount, discount_amount, round_off, net_sale, payment_mode, order_type, transaction_status)
                VALUES
                (@receipt_number, @sale_date, @transaction_time, @sale_amount, @tax_amount, @discount_amount, @round_off, @net_sale, @payment_mode, @order_type, @transaction_status);
            ";

                using var cmd = conn.CreateCommand();
                cmd.CommandText = insert;
                cmd.Parameters.AddWithValue("@receipt_number", string.IsNullOrEmpty(rec.ReceiptNumber) ? DBNull.Value : (object)rec.ReceiptNumber);
                cmd.Parameters.AddWithValue("@sale_date", string.IsNullOrEmpty(rec.SaleDate) ? DBNull.Value : (object)rec.SaleDate);
                cmd.Parameters.AddWithValue("@transaction_time", string.IsNullOrEmpty(rec.TransactionTime) ? DBNull.Value : (object)rec.TransactionTime);
                cmd.Parameters.AddWithValue("@sale_amount", rec.SaleAmount);
                cmd.Parameters.AddWithValue("@tax_amount", rec.TaxAmount);
                cmd.Parameters.AddWithValue("@discount_amount", rec.DiscountAmount);
                cmd.Parameters.AddWithValue("@round_off", rec.RoundOff);
                cmd.Parameters.AddWithValue("@net_sale", rec.NetSale);
                cmd.Parameters.AddWithValue("@payment_mode", string.IsNullOrEmpty(rec.PaymentMode) ? DBNull.Value : (object)rec.PaymentMode);
                cmd.Parameters.AddWithValue("@order_type", string.IsNullOrEmpty(rec.OrderType) ? DBNull.Value : (object)rec.OrderType);
                cmd.Parameters.AddWithValue("@transaction_status", string.IsNullOrEmpty(rec.TransactionStatus) ? DBNull.Value : (object)rec.TransactionStatus);

                int changed = cmd.ExecuteNonQuery();
                conn.Close();

                return changed > 0 ? 1 : 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("DB insert error: " + ex.Message);
                return 0;
            }
        }
    }
}
