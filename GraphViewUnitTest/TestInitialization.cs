﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    public static class TestInitialization
    {
        public static string ConnectionString { get { return ConnStr; } }

        private static readonly string ConnStr =
            System.Configuration.ConfigurationManager
                .ConnectionStrings["GraphViewDbConnectionString"].ConnectionString;

        /// <summary>
        /// Executes a sql script to clear the database
        /// </summary>
        public static void ClearDatabase()
        {
            using (var conn = new SqlConnection(ConnStr))
            {
                var sr = new StreamReader("ClearDatabase.sql");

                conn.Open();
                var command = conn.CreateCommand();
                var transaction = conn.BeginTransaction("ClearDB");
                var clearQuery = sr.ReadToEnd().Split(new string[] { "GO" }, StringSplitOptions.None);

                command.Connection = conn;
                command.Transaction = transaction;

                foreach (var query in clearQuery)
                {
                    if (query == "") continue;
                    command.CommandText = query;
                    command.ExecuteNonQuery();
                }
                transaction.Commit();
            }
        }

        public static void CreateGraphTable()
        {
            using (var graph = new GraphViewConnection(ConnStr))
            {
                graph.Open();

                const string createEmployeeStr = @"
                CREATE TABLE [EmployeeNode] (
                    [ColumnRole: ""NodeId""]
                    [WorkId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""EmployeeNode""]
                    [Colleagues] [varchar](max),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {credit: ""int"", aaa: ""double"", hhh: ""string""} ]
                    --[ColumnRole: ""Edge"", Reference: ""ClientNode""]
                    [Clients] [varchar](max),
                    [ColumnRole: ""Edge"", Reference: ""EmployeeNode""]
                    [Manager] [varchar](max),
                )";
                graph.CreateNodeTable(createEmployeeStr);

                const string createClientStr = @"
                CREATE TABLE [ClientNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode""]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createClientStr);
                const string createUserStr = @"
                CREATE TABLE [UserNode] (
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Property""]
                    [income] [int],
                )";
                graph.CreateNodeTable(createUserStr);
            }
        }

        public static void GenerateRandomData()
        {
            using (var graph = new GraphViewConnection(ConnStr))
            {
                graph.Open();
                DataGenerator.InsertDataEmployNode(graph.Conn);
                DataGenerator.InsertDataClientNode(graph.Conn);
                foreach (var table in graph.GetNodeTables())
                    graph.UpdateTableStatistics(table.Item1, table.Item2);
            }
        }

        /// <summary>
        /// Create employeenode & clientnode tables along with node/edge insert store procedures
        /// </summary>
        public static void CreateTableAndProc()
        {
            using (var graph = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                graph.Open();
                const string createEmployeeStr = @"
                CREATE TABLE [EmployeeNode] (
                    [ColumnRole: ""NodeId""]
                    [WorkId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a: ""int"", b: ""double"", d:""int""}]
                    [Clients] [varchar](max),
                    [ColumnRole: ""Edge"", Reference: ""EmployeeNode"", Attributes: {a:""int"", c:""string"", d:""int"", e:""double""}]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createEmployeeStr);
                const string createEmployeeStr2 = @"
                CREATE TABLE [ClientNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a:""int"", c:""string"", d:""int"", e:""double""}]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createEmployeeStr2);

                graph.CreateProcedure(@"
                    CREATE PROCEDURE InsertNode @id varchar(32), @name varchar(32)
                    as
                    BEGIN
                    INSERT NODE INTO ClientNode (ClientId, name) VALUES (@id,@name);
                    INSERT NODE INTO EmployeeNode (WorkId, name) VALUES (@id,@name);
                    END
                    ");

                graph.CreateProcedure(@"
                    CREATE PROCEDURE InsertEmployeeNodeClients @src varchar(32),@sink varchar(32),@a int, @b float, @d int
                    as
                    BEGIN
                    INSERT EDGE INTO EmployeeNode.Clients
                    SELECT En, Cn, @a, @b, @d
                    FROM EmployeeNode En, ClientNode Cn
                    WHERE En.Workid = @src AND Cn.ClientId = @sink;
                    END
                    ");

                graph.CreateProcedure(@"
                    CREATE PROCEDURE InsertEmployeeNodeColleagues @src varchar(32),@sink varchar(32),@a int, @c nvarchar(4000), @d int, @e float
                    as
                    BEGIN
                    INSERT EDGE INTO EmployeeNode.Colleagues
                    SELECT En, Cn, @a, @c, @d, @e
                    FROM EmployeeNode En, EmployeeNode Cn
                    WHERE En.Workid = @src AND Cn.Workid = @sink;
                    END
                    ");

                graph.CreateProcedure(@"
                    CREATE PROCEDURE InsertClientNodeColleagues @src varchar(32),@sink varchar(32),@a int, @c nvarchar(4000), @d int, @e float
                    as
                    BEGIN
                    INSERT EDGE INTO ClientNode.Colleagues
                    SELECT En, Cn, @a, @c, @d, @e
                    FROM ClientNode En, ClientNode Cn
                    WHERE En.ClientId = @src AND Cn.ClientId = @sink;
                    END
                    ");
            }
        }

        /// <summary>
        /// Insert data by calling the node/edge store procedures.
        /// Only inserts one outgoning edge for each node pointing to the node with id = curid+1
        /// </summary>
        /// <param name="NodeNum"></param>
        public static void InsertDataByProc(int NodeNum)
        {
            using (var graph = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                graph.Open();
                using (var command = graph.CreateCommand())
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "InsertNode";
                    command.Parameters.Add("@id", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@name", SqlDbType.VarChar, 32);
                    for (int i = 0; i < NodeNum; i++)
                    {
                        command.Parameters["@id"].Value = i;
                        command.Parameters["@name"].Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8);
                        command.ExecuteNonQuery();
                    }

                    command.CommandText = "InsertEmployeeNodeClients";
                    command.Parameters.Clear();
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@sink", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@a", SqlDbType.Int);
                    command.Parameters.Add("@b", SqlDbType.Float);
                    command.Parameters.Add("@d", SqlDbType.Int);
                    var rnd = new Random();
                    for (int i = 0; i < NodeNum; i++)
                    {
                        command.Parameters["@src"].Value = i;
                        command.Parameters["@sink"].Value = (i + 1) % NodeNum;
                        command.Parameters["@a"].Value = rnd.Next();
                        command.Parameters["@b"].Value = rnd.NextDouble();
                        command.Parameters["@d"].Value = rnd.Next();
                        command.ExecuteNonQuery();
                    }

                    command.CommandText = "InsertEmployeeNodeColleagues";
                    command.Parameters.Clear();
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@sink", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@a", SqlDbType.Int);
                    command.Parameters.Add("@c", SqlDbType.NVarChar, 4000);
                    command.Parameters.Add("@d", SqlDbType.Int);
                    command.Parameters.Add("@e", SqlDbType.Float);

                    rnd = new Random();
                    for (int i = 0; i < NodeNum; i++)
                    {
                        command.Parameters["@src"].Value = i;

                        command.Parameters["@sink"].Value = (i + 1) % NodeNum;
                        command.Parameters["@a"].Value = rnd.Next();
                        command.Parameters["@e"].Value = rnd.NextDouble();
                        command.Parameters["@d"].Value = rnd.Next();
                        command.Parameters["@c"].Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8);
                        command.ExecuteNonQuery();

                    }

                    command.CommandText = "InsertClientNodeColleagues";
                    command.Parameters.Clear();
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@sink", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@a", SqlDbType.Int);
                    command.Parameters.Add("@c", SqlDbType.NVarChar, 4000);
                    command.Parameters.Add("@d", SqlDbType.Int);
                    command.Parameters.Add("@e", SqlDbType.Float);

                    rnd = new Random();
                    for (int i = 0; i < NodeNum; i++)
                    {
                        command.Parameters["@src"].Value = i;
                        command.Parameters["@sink"].Value = (i + 1) % NodeNum;
                        command.Parameters["@a"].Value = rnd.Next();
                        command.Parameters["@e"].Value = rnd.NextDouble();
                        command.Parameters["@d"].Value = rnd.Next();
                        command.Parameters["@c"].Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8);
                        command.ExecuteNonQuery();

                    }
                }
                graph.UpdateTableStatistics("dbo", "employeenode");
                graph.UpdateTableStatistics("dbo", "clientnode");
            }
        }

        /// <summary>
        /// Insert data by calling the node/edge store procedures
        /// </summary>
        /// <param name="NodeNum">Number of nodes in each node table</param>
        /// <param name="NodeDegree">Outgoing edge degree of each node</param>
        public static void InsertDataByProc(int NodeNum = 50, int NodeDegree = 20)
        {
            using (var graph = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                graph.Open();
                using (var command = graph.CreateCommand())
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "InsertNode";
                    command.Parameters.Add("@id", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@name", SqlDbType.VarChar, 32);
                    for (int i = 0; i < NodeNum; i++)
                    {
                        command.Parameters["@id"].Value = i;
                        command.Parameters["@name"].Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8);
                        command.ExecuteNonQuery();
                    }

                    command.CommandText = "InsertEmployeeNodeClients";
                    command.Parameters.Clear();
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@sink", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@a", SqlDbType.Int);
                    command.Parameters.Add("@b", SqlDbType.Float);
                    command.Parameters.Add("@d", SqlDbType.Int);
                    var rnd = new Random();
                    for (int i = 0; i < NodeNum; i++)
                    {
                        command.Parameters["@src"].Value = i;
                        for (int j = 0; j < NodeDegree; j++)
                        {
                            command.Parameters["@sink"].Value = rnd.Next(0, NodeNum - 1);
                            command.Parameters["@a"].Value = rnd.Next();
                            command.Parameters["@b"].Value = rnd.NextDouble();
                            command.Parameters["@d"].Value = rnd.Next();
                            command.ExecuteNonQuery();
                        }
                    }

                    command.CommandText = "InsertEmployeeNodeColleagues";
                    command.Parameters.Clear();
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@sink", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@a", SqlDbType.Int);
                    command.Parameters.Add("@c", SqlDbType.NVarChar, 4000);
                    command.Parameters.Add("@d", SqlDbType.Int);
                    command.Parameters.Add("@e", SqlDbType.Float);

                    rnd = new Random();
                    for (int i = 0; i < NodeNum; i++)
                    {
                        command.Parameters["@src"].Value = i;
                        for (int j = 0; j < NodeDegree; j++)
                        {
                            command.Parameters["@sink"].Value = rnd.Next(0, NodeNum - 1);
                            command.Parameters["@a"].Value = rnd.Next();
                            command.Parameters["@e"].Value = rnd.NextDouble();
                            command.Parameters["@d"].Value = rnd.Next();
                            command.Parameters["@c"].Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8);
                            command.ExecuteNonQuery();
                        }
                    }

                    command.CommandText = "InsertClientNodeColleagues";
                    command.Parameters.Clear();
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@sink", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@a", SqlDbType.Int);
                    command.Parameters.Add("@c", SqlDbType.NVarChar, 4000);
                    command.Parameters.Add("@d", SqlDbType.Int);
                    command.Parameters.Add("@e", SqlDbType.Float);

                    rnd = new Random();
                    for (int i = 0; i < NodeNum; i++)
                    {
                        command.Parameters["@src"].Value = i;
                        for (int j = 0; j < NodeDegree; j++)
                        {
                            command.Parameters["@sink"].Value = rnd.Next(0, NodeNum - 1);
                            command.Parameters["@a"].Value = rnd.Next();
                            command.Parameters["@e"].Value = rnd.NextDouble();
                            command.Parameters["@d"].Value = rnd.Next();
                            command.Parameters["@c"].Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8);
                            command.ExecuteNonQuery();
                        }
                    }
                }
                graph.UpdateTableStatistics("dbo", "employeenode");
                graph.UpdateTableStatistics("dbo", "clientnode");
            }
        }
        
        /// <summary>
        /// Clear database, create table and generate random data
        /// </summary>
        public static void Init()
        {
            ClearDatabase();
            CreateGraphTable();
            GenerateRandomData();
        }
    }
}