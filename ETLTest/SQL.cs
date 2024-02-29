using System;
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml;
using System.Diagnostics.Metrics;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using System.Data.SqlClient;


namespace ETLTest
{
    internal class SQL : IDisposable
    {
        public string NameTable { get; set; }

        public SqlConnectionString sqlConnectionString = "Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=master;";
        //public SqlConnectionString sqlConnectionString = ConfigurationManager.ConnectionStrings["ConnectString"].ConnectionString;

        public SqlConnectionManager sqlConnection;

        public SQL(string nameTable)
        {
            NameTable = nameTable;
            sqlConnection = new SqlConnectionManager(sqlConnectionString);
        }

        public void CreateTableTask()
        {
            try {
                CreateDatabaseTask.Create(sqlConnection, NameTable);
                Console.WriteLine($"Create table {NameTable}");
            }
            catch (Exception ex) 
            {
                Console.WriteLine(ex.ToString() + $"{sqlConnectionString.Builder.ToString()}");
                DropDatabaseTask.DropIfExists(sqlConnection, NameTable);
                sqlConnection.Close();
            }
        }

        public void Prepare()
        {
            Console.WriteLine("Starting DataFlow example - preparing database");

            TableDefinition OrderDataTableDef = new TableDefinition(NameTable,
                new List<TableColumn>() {
                    new TableColumn() {Name = "Id", DataType = "INT", AllowNulls = false, DefaultValue = "0"},
                    new TableColumn() {Name = "CountryName", DataType = "nvarchar(100)", AllowNulls = false},
                    new TableColumn() {Name = "InstituteName", DataType = "nvarchar(100)", AllowNulls = false},
                    new TableColumn() {Name = "Pages", DataType = "nvarchar(1000)", AllowNulls = true},
            });

            //Create demo tables & fill with demo data
            OrderDataTableDef.CreateTable(sqlConnection);
#if DEBUG && false
            SqlTask.ExecuteNonQuery(sqlConnection, $"INSERT INTO countries (Id, CountryName, InstituteName, Pages) values (13, 'Russia', 'ChuvashState', 'www.chgu.ru')");
            SqlTask.ExecuteNonQuery(sqlConnection, $"INSERT INTO countries (Id, CountryName, InstituteName, Pages) values (2, 'Russia2', 'MoscowState', 'www.mgu.ru')");
#endif
        }

        public void Dispose()
        {
            sqlConnection.Close();
        }

        public void DropTableTask()
        {
            try
            {
                if (true)
                {
                    DropDatabaseTask.DropIfExists(sqlConnection, NameTable);
                }
                else
                {
                    using(SqlConnection connection = new SqlConnection(sqlConnectionString.ToString()))
                    {
                        String sql = $"DROP TABLE dbo.{NameTable}";

                        using (SqlCommand command = new SqlCommand(sql, connection))
                        {
                        }
                    }
                }
                Console.WriteLine($"Drop table {NameTable}");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}
