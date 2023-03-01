using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Net.Http;
using System.Reflection;
using System.Text.Json;

namespace Parser_Test
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // INITIALIZATION -----------------------------------------------
            string sqlConnectionString = "Server=.;Database=Test;Trusted_Connection=True;";
            string headerUserAgent = "Mozilla/5.0";
            string uriRequestLink = "https://www.lesegais.ru/open-area/graphql";

            // Http client
            HttpClient httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(headerUserAgent);

            // Request
            Uri uri = new Uri(uriRequestLink);
            StringContent stringContent;
            Root root;
            HttpResponseMessage response;
            string resultContent;

            // SQL
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            SqlDataReader reader = null; // Read sql request

            try
            {
                sqlConnection.Open();
                Console.WriteLine("Connection Opened");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            SqlCommand sqlCommand = sqlConnection.CreateCommand();

            // OTHER
            List<Content> contents; // List response content
            FieldInfo[] fields; // Fields of class Content

            // LOOP -----------------------------------------------
            int count = 0;
            do
            {
                if (sqlConnection.State != System.Data.ConnectionState.Open)
                    break;
                // GETTING DATA -----------------------------------------------
                stringContent = new StringContent(GetRequestString(count, 20));
                response = httpClient.PostAsync(uri, stringContent).Result;
                resultContent = response.Content.ReadAsStringAsync().Result;
                root = JsonSerializer.Deserialize<Root>(resultContent);
                httpClient.CancelPendingRequests();

                Console.WriteLine(response.StatusCode.ToString());
                Console.WriteLine(response.Headers);

                contents = root.data.searchReportWoodDeal.content;
                fields = typeof(Content).GetFields(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public);

                foreach (Content item in contents)
                {
                    reader?.Close();
                    // Search for a row from the database by dealNumber
                    sqlCommand.CommandText = GetSelectString(item.dealNumber);
                    reader = sqlCommand.ExecuteReader();

                    if (reader.Read()) // If found
                    {
                        sqlCommand.CommandText = "";
                        // Сompare each column
                        for (int i = 0; i < fields.Length - 1; i++)
                        {
                            //format field to string name and value
                            string name = fields[i].Name;
                            name = name.Substring(1, name.IndexOf('>') - 1);
                            string value = fields[i].GetValue(item).ToString();

                            if (value != reader[i].ToString()) // If it does not match, replace
                            {
                                Console.WriteLine("UPDATE");
                                Console.WriteLine(item.dealNumber);
                                Console.WriteLine(value + " -> " + reader[i].ToString());
                                sqlCommand.CommandText += GetUpdateString(name, value, item.dealNumber);
                            }
                        }
                        reader.Close();
                        if (sqlCommand.CommandText != "")
                            sqlCommand.ExecuteNonQuery();
                    }
                    else
                    {
                        Console.WriteLine("INSERT");
                        reader.Close();
                        sqlCommand.CommandText = GetInsertString(item);
                        sqlCommand.ExecuteNonQuery();
                    }
                }
                System.Threading.Thread.Sleep(10 * 1000);
                Console.WriteLine("Next");
                count++;
            }
            while (!Console.KeyAvailable);

            // CLOSE -----------------------------------------------

            Console.WriteLine("END");
            Console.ReadLine();

            string GetRequestString(int pageNumber, int size)
            {
                string query = "{\"query\":\"query SearchReportWoodDeal($size: Int!, $number: Int!, $filter: Filter, $orders: [Order!]) {" +
                    "\\n  searchReportWoodDeal(filter: $filter, pageable: {number: $number, size: $size}, orders: $orders) {" +
                    "\\n    content {" +
                    "\\n      sellerName" +
                    "\\n      sellerInn" +
                    "\\n      buyerName" +
                    "\\n      buyerInn" +
                    "\\n      woodVolumeBuyer" +
                    "\\n      woodVolumeSeller" +
                    "\\n      dealDate" +
                    "\\n      dealNumber" +
                    "\\n      __typename" +
                    "\\n    }" +
                    "\\n    __typename" +
                    "\\n  }" +
                    "\\n}" +
                    "\\n\",";
                string variables = "\"variables\":{\"size\":" + size + ",\"number\":" + pageNumber + ",\"filter\":null,\"orders\":null},";
                string operationName = "\"operationName\":\"SearchReportWoodDeal\"}";

                return query + variables + operationName;
            }
            string GetSelectString(string dealNumber)
            {
                string str = $"SELECT" +
                    $" [dealNumber]" +
                    $",[sellerName]" +
                    $",[sellerInn]" +
                    $",[buyerName]" +
                    $",[buyerInn]" +
                    $",CAST([dealDate] AS nvarchar)" +
                    $",[woodVolumeBuyer]" +
                    $",[woodVolumeSeller]" +
                    $"FROM [Test].[dbo].[Content] WHERE dealNumber = '{dealNumber}'";
                return str;
            }
            string GetInsertString(Content item)
            {
                string str = "INSERT INTO [Content] VALUES(" +
                            $"'{item.dealNumber}'," +
                            $"'{item.sellerName}'," +
                            $"'{item.sellerInn}'," +
                            $"'{item.buyerName}'," +
                            $"'{item.buyerInn}'," +
                            $"CAST('{item.dealDate}' AS date)," +
                            $"'{item.woodVolumeSeller}'," +
                            $"'{item.woodVolumeBuyer}')";
                return str;
            }
            string GetUpdateString(string columName, string value, string dealNumber)
            {
                string str = "UPDATE [Content] " +
                    $"SET {columName} = '{value}' " +
                    $"WHERE dealNumber = '{dealNumber}' ";
                return str;
            }
        }
    }

    #region Content
    public class Content
    {
        public string dealNumber { get; set; }
        public string sellerName { get; set; }
        public string sellerInn { get; set; }
        public string buyerName { get; set; }
        public string buyerInn { get; set; }
        public string dealDate { get; set; }
        public double woodVolumeBuyer { get; set; }
        public double woodVolumeSeller { get; set; }
        public string __typename { get; set; }
    }

    public class Data
    {
        public SearchReportWoodDeal searchReportWoodDeal { get; set; }
    }

    public class Root
    {
        public Data data { get; set; }
    }

    public class SearchReportWoodDeal
    {
        public List<Content> content { get; set; }
        public string __typename { get; set; }
    }
    #endregion
}