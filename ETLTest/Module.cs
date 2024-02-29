using ETLBox.DataFlow;
using Microsoft.IdentityModel.Protocols;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace ETLTest
{
    public class Module : IDisposable
    {
        Dictionary<string, string> dictionary = new Dictionary<string, string>()
        {
            //{ "Russia", "http://universities.hipolabs.com/search?country=Russian+Federation" }
        };
        CancellationToken cancellationToken = new CancellationToken();
        private string filePath 
        { 
            get 
            {
                return ConfigurationManager.AppSettings["key"] == null ? "files/listApi.csv" : ConfigurationManager.AppSettings["key"];
            } 
        }
        private string name
        {
            get
            {
                return ConfigurationManager.AppSettings["name"] == null ? "countries" : ConfigurationManager.AppSettings["name"];
            }
        }
        private int cntTask
        {
            get
            {
                return ConfigurationManager.AppSettings["cnttask"] == null ? 10 : int.Parse(ConfigurationManager.AppSettings["cntTask"]);
            }
        }

        LinkedList<Task> tasks = new LinkedList<Task>();

        public Module()
        {
            if (ReadCsv(filePath))
            {
                ThreadPool.SetMaxThreads(cntTask, cntTask);
                //tasks = new Task[cntTask];
                Exec();
            }
            else
                Console.WriteLine($"Not run programm");
            
            while (!cancellationToken.IsCancellationRequested)
            { }
            //Console.WriteLine($"Click for exit");

            //ConsoleKeyInfo consoleKey = Console.ReadKey();
        }

        public async void Exec()
        {

            SQL sQL = new SQL(name);
            sQL.DropTableTask();
            sQL.CreateTableTask();
            sQL.Prepare();

            //Etlload etl = new Etlload("countries", sQL?.sqlConnection);
            Etlload etl = new Etlload(name, sQL?.sqlConnection, cancellationToken);
            Task.Factory.StartNew(() => { 
            foreach (var item in dictionary)
            {
#if DEBUG && false
                if (item.Key != "Japan"/* && item.Key != "Finland"*/)
                {
                    continue;
                }
#endif
                try
                {
                    if (tasks.Count != 0)
                        checkQueue();
                    if (tasks.Count < cntTask)
                        tasks.AddLast(etl.TaskCreate(item));
                    Console.WriteLine($"Create task with {item.Key}");
                }
                catch (Exception ex)
                {
                    sQL.DropTableTask();

                    Console.WriteLine(ex.Message);
                }

            }
            }).Wait();

            //sQL.Dispose();
        }

        private void checkQueue()
        {
            LinkedListNode<Task> tFirst = tasks.First;
            do
            {
                if (tFirst.Value.IsCompleted || 
                    tFirst.Value.IsCanceled || 
                    tFirst.Value.IsFaulted)
                {
                    tasks.Remove(tFirst);
                }
                tFirst = tFirst.Next;
            } while (tFirst != null);
        }

        private bool ReadCsv(string filepath)
        {
            try
            {
                if (filepath == string.Empty)
                    return false;

                FileInfo fileInfo = new FileInfo(filepath);

                if (!fileInfo.Exists)
                    return false;

                using (StreamReader sr = new StreamReader(filepath))
                {
                    string line;

                    while ((line = sr.ReadLine()) != null)
                    {
                        var splitStr = line.Split(',');
                        if (splitStr.Length >= 2)
                        {
                            dictionary.Add(splitStr[0], splitStr[1]);
                        }
                        else { Console.WriteLine($"File {filepath} is not correct"); }
                    }
                }
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }

        public void Dispose()
        {
            foreach (Task item in tasks)
            {
                item.Dispose();
            }
            dictionary.Clear();
        }
    }
}
