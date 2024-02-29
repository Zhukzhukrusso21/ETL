using System;
using System.Configuration;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Net.Http.Headers;
using System.Net;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using ETLBox;
using ETLBox.Connection;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using WireMock.Server;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Settings;

namespace ETLTest
{
    internal class Etlload
    {
        private SqlConnectionManager sqlConnection;
        private string name;

        //private DataFlowStreamSource<CountryInstituteModel> source;
        //private RowTransformation<CountryInstituteModel> transform;
        //private LookupTransformation<CountryInstituteModel, ExpandoObject> lookupTransfrom;
        //private DbDestination<CountryInstituteModel> dbDest;
        //private DbSource<ExpandoObject> lookupSource;
        //private Distinct<ExpandoObject> duplicateCheck;

        //private Multicast<CountryInstituteModel> multicast;
        //private TextDestination<CountryInstituteModel> text;
        //private JsonDestination<CountryInstituteModel> json;
        public CancellationToken cancellationToken;

        private int port
        {
            get
            {
                return ConfigurationManager.AppSettings["port"] == null ? 61456 : int.Parse(ConfigurationManager.AppSettings["port"]);
            }
        }

        private string[] urls
        {
            get
            {
                return ConfigurationManager.AppSettings["urls"] == null ? new string[] { "localhost" } : ConfigurationManager.AppSettings["urls"].Split(',').ToArray<string>();
            }
        }

        public WireMockServer Server { get; set; }


        public Etlload() { }

        public Etlload(string name, SqlConnectionManager connectionManager, CancellationToken cancellationToken)
        {
            this.name = name;
            this.cancellationToken = cancellationToken;
            this.sqlConnection = connectionManager;
            Initializer();
        }

        public void Initializer()
        {
            StartWebServer();
            //dbDest = new DbDestination<CountryInstituteModel>(sqlConnection, name);


        }

        public async Task TaskCreate(KeyValuePair<string, string> keyValuePair)
        {
            Task.Factory.StartNew(async () => await ComponentCreate(keyValuePair));
        }

        private async Task ComponentCreate(KeyValuePair<string, string> keyValuePair)
        {
            DataFlowStreamSource<CountryInstituteModel> source = new JsonSource<CountryInstituteModel>(keyValuePair.Value, ResourceType.Http);
            RowTransformation<CountryInstituteModel> transform = new RowTransformation<CountryInstituteModel>();
            transform.TransformationFunc = x =>
            {
                //x.Normalize();
                x.pages = string.Join(',', x.web_pages);
                return x;
            };

            //var errorTarget = new JsonDestination<CountryInstituteModel>("errors.json");
            //lookupTransfrom = new LookupTransformation<CountryInstituteModel, ExpandoObject>();
            Multicast<CountryInstituteModel> multicast = new Multicast<CountryInstituteModel>();

#if true
            TextDestination<CountryInstituteModel> text = new TextDestination<CountryInstituteModel>($"files/{name}_{keyValuePair.Key}.log");
            text.WriteLineFunc = row => { return $"{row.country}\t{row.name}\t{row.pages}"; };
#endif

            DbDestination<CountryInstituteModel> dbDest = new DbDestination<CountryInstituteModel>(sqlConnection, name);
            //lookupSource = new DbSource<ExpandoObject>(sqlConnection, name);
            //lookupTransfrom.Source = lookupSource;
            JsonDestination<CountryInstituteModel> json = new JsonDestination<CountryInstituteModel>(/*$"{name}.json"*/);
            json.ResourceType = ResourceType.Http;
            json.HttpClient = CreateDefaultHttpClient();
            json.HttpRequestMessage.Method = HttpMethod.Post;
            json.HasNextUri = (streamMetaData, row) => true;
            json.GetNextUri = (streamMetaData, row) =>
            {
                //streamMetaData.HttpRequestMessage.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "Some token");
                return $"http://{urls[0]}:{port}/countriesApi/{row.country}/{row.name}";
            };

            source.LinkTo(transform);

            transform.LinkTo(multicast);
            //transform.LinkTo(lookupTransfrom, row => row.IsValid());

            //lookupTransfrom.LinkTo(multicast);
            multicast.LinkTo(dbDest);
            multicast.LinkTo(json);
            multicast.LinkTo(text);

            await Network.ExecuteAsync(source).WaitAsync(TimeSpan.FromMinutes(10), cancellationToken);
            //Network.ExecuteAsync(source).Wait();

            Console.WriteLine($"ComponentCreate");

            WriteServerLog();
        }

        private void StartWebServer()
        {
            Server = WireMockServer.Start(port);
            Server.Given(Request.Create().WithPath("/countriesApi/*").UsingPost())
                .RespondWith(Response.Create().WithStatusCode(200));
        }

        private HttpClient CreateDefaultHttpClient()
        {
            var httpClient = new HttpClient(new HttpClientHandler()
            {
                AutomaticDecompression = DecompressionMethods.All
            });
            httpClient.Timeout = TimeSpan.FromSeconds(1000);
            httpClient.DefaultRequestHeaders.Connection.Add("keep-alive");
            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("*/*"));
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("br"));
            httpClient.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("MyImporter", "1.1"));
            httpClient.DefaultRequestHeaders.CacheControl = new CacheControlHeaderValue() { NoCache = true };
            return httpClient;
        }

        private void WriteServerLog()
        {
            var requests = Server.FindLogEntries(
                Request.Create().WithPath("/countriesApi/*").UsingAnyMethod()
            );
            foreach (var req in requests)
            {
                Console.WriteLine("Url: " + req.RequestMessage.Path);
                foreach (var header in req.RequestMessage.Headers)
                    Console.WriteLine("Key:" + header.Key + ", Value:" + header.Value);
                Console.WriteLine(req.RequestMessage.Body);
                Console.WriteLine("------------------------------");
            }
        }

        public void Dispose()
        {
            Server.Stop();
            Server.Dispose();
        }
    }
}
