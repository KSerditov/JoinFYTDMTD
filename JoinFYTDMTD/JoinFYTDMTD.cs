using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.IO;

namespace JoinFYTDMTD
{
    class JoinFYTDMTD
    {
        static int Main(string[] args)
        {

            if (args.Length == 0)
            {
                System.Console.WriteLine("Please enter FYTD filename and MTD filename as arguments.");
                Console.ReadLine();
                return 1;
            }

            Console.WriteLine(args[0]);

            string FileName1 = Path.Combine(Directory.GetCurrentDirectory(), args[0]);
            string FileName2 = Path.Combine(Directory.GetCurrentDirectory(), args[1]);

            //string FileName1 = @"C:\Users\kserditov\documents\FYTD_20160706_Manager.csv";
            //string FileName2 = @"C:\Users\kserditov\documents\MTD_20160706_Manager.csv";

            if (!File.Exists(FileName1))
            {
                System.Console.WriteLine("Specified FYTD file does not exist.");
                Console.ReadLine();
                return 1;
            }

            if (!File.Exists(FileName2))
            {
                System.Console.WriteLine("Specified MTD file does not exist.");
                Console.ReadLine();
                return 1;
            }

            DataSet ds1 = CSVToDataset(FileName1, "FYTD");
            DataSet ds2 = CSVToDataset(FileName2, "MTD");

            DataTable dtResult = new DataTable();

            dtResult.Columns.Add("Level", typeof(string));
            dtResult.Columns.Add("Manager Employee Number", typeof(string));
            dtResult.Columns.Add("Activity Resolution Rate (P) FYTD", typeof(string));
            dtResult.Columns.Add("ARR Goal (P) FYTD", typeof(string));
            dtResult.Columns.Add("ARR Delta (P) FYTD", typeof(string));
            dtResult.Columns.Add("Activity Resolution Rate (P) MTD", typeof(string));
            dtResult.Columns.Add("ARR Goal (P) MTD", typeof(string));
            dtResult.Columns.Add("ARR Delta (P) MTD", typeof(string));

            var results = from table1 in ds1.Tables[0].AsEnumerable()
                          join table2 in ds2.Tables[0].AsEnumerable() on (string)table1["Manager Employee Number"] equals (string)table2["Manager Employee Number"]
                          select dtResult.LoadDataRow(new object[]
                             {
                                "Manager",
                                table1.Field<string>("Manager Employee Number"),
                                table1.Field<string>("Activity Resolution Rate (P) FYTD"),
                                table1.Field<string>("ARR Goal (P) FYTD"),
                                table1.Field<string>("ARR Delta (P) FYTD"),
                                table2.Field<string>("Activity Resolution Rate (P) MTD"),
                                table2.Field<string>("ARR Goal (P) MTD"),
                                table2.Field<string>("ARR Delta (P) MTD"),
                              }, false);
            results.CopyToDataTable();

            DatatableToCSV(dtResult, Path.GetDirectoryName(FileName1));

            return 0;

        }

        static DataSet CSVToDataset(string fileName, string setName)
        {
            WriteSchema(fileName, setName);

            OleDbConnection conn = new OleDbConnection("Provider=Microsoft.Jet.OleDb.4.0; Data Source = " + Path.GetDirectoryName(fileName) + @"; Extended Properties = ""Text;""");
            conn.Open();
            OleDbDataAdapter adapter = new OleDbDataAdapter("SELECT * FROM " + Path.GetFileName(fileName), conn);
            DataSet ds = new DataSet(setName);
            adapter.Fill(ds);
            conn.Close();

            ds.Tables[0].Rows[0].Delete();
            ds.Tables[0].Rows[1].Delete();
            var cnt = ds.Tables[0].Rows.Count;
            ds.Tables[0].Rows[cnt - 1].Delete();
            ds.AcceptChanges();

            return ds;
        }

        static void WriteSchema(string fileName, string setName)
        {
            using (FileStream filestr = new FileStream(Path.GetDirectoryName(fileName) + "\\schema.ini", FileMode.Create, FileAccess.Write))
            {
                using (StreamWriter writer = new StreamWriter(filestr))
                {
                    writer.WriteLine("[" + Path.GetFileName(fileName) + "]");
                    writer.WriteLine("ColNameHeader=False");
                    writer.WriteLine("Format=CSVDelimited");
                    writer.WriteLine("MaxScanRows=0");
                    writer.WriteLine("Col1=\"Manager Employee Number\" Text Width 100");
                    writer.WriteLine("Col2=\"Activity Resolution Rate (P) " + setName + "\" Text Width 100");
                    writer.WriteLine("Col3=\"ARR Goal (P) " + setName + "\" Text Width 100");
                    writer.WriteLine("Col4=\"ARR Delta (P) " + setName + "\" Text Width 100");
                    writer.Close();
                    writer.Dispose();
                }
                filestr.Close();
                filestr.Dispose();
            }
        }

        static void DatatableToCSV(DataTable dt, string path)
        {
            StringBuilder sb = new StringBuilder();

            string[] columnNames = dt.Columns.Cast<DataColumn>().
                                              Select(column => column.ColumnName).
                                              ToArray();
            sb.AppendLine(string.Join(",", columnNames));

            foreach (DataRow row in dt.Rows)
            {
                string[] fields = row.ItemArray.Select(field => field.ToString()).
                                                ToArray();
                sb.AppendLine(string.Join(",", fields));
            }

            File.WriteAllText(Path.Combine(path, "FYTD_MTD_Merged.csv"), sb.ToString());
        }

    }
}
