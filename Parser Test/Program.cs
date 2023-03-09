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

            // Counters
            int count = 0; // page counter
            int update, insert;
            DateTime temp, totalTimeIteration;

            // LOOP -----------------------------------------------
            do
            {
                totalTimeIteration = DateTime.Now;
                Console.WriteLine("========================");
                update = insert = 0;
                if (sqlConnection.State != System.Data.ConnectionState.Open)
                    break;
                // GETTING DATA -----------------------------------------------
                Console.WriteLine("------REQUEST-------");
                temp = DateTime.Now;

                stringContent = new StringContent(GetRequestString(count, 700));
                response = httpClient.PostAsync(uri, stringContent).Result;
                resultContent = response.Content.ReadAsStringAsync().Result;

                Console.WriteLine(response.StatusCode.ToString());
                Console.WriteLine(response.Headers);
                Console.WriteLine("Time request: " + (DateTime.Now - temp).TotalSeconds + " sec");

                contents = JsonSerializer.Deserialize<Root>(resultContent).data.searchReportWoodDeal.content;
                httpClient.CancelPendingRequests();

                if (contents.Count == 0)
                {
                    Console.WriteLine("Pause 10 minutes");
                    count = 0;
                    System.Threading.Thread.Sleep(10 * 60 * 1000); // 10 minutes
                    continue;
                }

                // PUT TO DATABASE -------------------------------------------
                Console.WriteLine("------SQL-------");
                fields = typeof(Content).GetFields(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public);

                temp = DateTime.Now;
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
                            string name, value;
                            name = fields[i].Name;
                            name = name.Substring(1, name.IndexOf('>') - 1);

                            if (fields[i].GetValue(item) == null)
                                value = "";
                            else
                                value = fields[i].GetValue(item).ToString();

                            if (value.Length > reader[i].ToString().Length) // If it does not match, replace
                            {
                                //Console.WriteLine("UPDATE");
                                Console.WriteLine(item.dealNumber);
                                Console.WriteLine(value + " -> " + reader[i].ToString());
                                sqlCommand.CommandText += GetUpdateString(name, value, item.dealNumber);
                                update++;
                            }
                        }
                        reader.Close();
                        if (sqlCommand.CommandText != "")
                            sqlCommand.ExecuteNonQuery();
                    }
                    else
                    {
                        //Console.WriteLine("INSERT");
                        reader.Close();
                        sqlCommand.CommandText = GetInsertString(item);
                        sqlCommand.ExecuteNonQuery();
                        insert++;
                    }
                }
                Console.WriteLine("Time sql: " + (DateTime.Now - temp).TotalSeconds + " sec");
                Console.WriteLine($"Updated: {update} \nInserted: {insert}");

                Console.WriteLine("Total time iteration: " + (DateTime.Now - totalTimeIteration).TotalSeconds + " sec");
                Console.WriteLine($"Current page: {count}");
                count++;
            }
            while (!Console.KeyAvailable);

            // CLOSE -----------------------------------------------

            Console.WriteLine("END");
            Console.ReadLine();

            string GetRequestString(int pageNumber, int size)
            {
                string query = "{\"query\":\"query SearchReportWoodDeal($size: Int!, $number: Int!) {" +
                    " searchReportWoodDeal(pageable: {number: $number, size: $size}) {" +
                    " content {" +
                    " sellerName" +
                    " sellerInn" +
                    " buyerName" +
                    " buyerInn" +
                    " woodVolumeBuyer" +
                    " woodVolumeSeller" +
                    " dealDate" +
                    " dealNumber" +
                    "}}}\",";
                string variables = "\"variables\":{\"size\":" + size + ",\"number\":" + pageNumber + "}}";

                return query + variables;
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
                if (item.sellerName != null)
                    item.sellerName = item.sellerName.Replace("'", "''");
                if (item.buyerName != null)
                    item.buyerName = item.buyerName.Replace("'", "''");

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
                if (value != null)
                    value = value.Replace("'", "''");
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
    }
    #endregion
}
