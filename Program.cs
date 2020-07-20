using System;
using System.IO;
using Microsoft.Hadoop.Avro.Container;
using Microsoft.Hadoop.Avro.Schema;

namespace CheckAvro
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args != null && args.Length != 1)
            {
                Console.WriteLine("No file name is given");
                return;
            }
            else
            {
                if (!File.Exists(args[0]))
                {
                    Console.WriteLine("File doesn't exist");
                    return;
                }

                try
                {
                    using (var reader = AvroContainer.CreateGenericReader(new FileStream(args[0], FileMode.Open)))
                    {
                        int i = 1;
                        while (reader.MoveNext())
                        {
                            int j = 1;
                            foreach (dynamic record in reader.Current.Objects)
                            {
                                foreach (RecordField rf in (reader.Schema as RecordSchema).Fields)
                                {
                                    try
                                    {
                                        var v = record[rf.Name];
                                    }
                                    catch
                                    {
                                        Console.WriteLine("Error reading " + rf.Name + " in record # " + j);
                                    }
                                }
                                //Console.WriteLine(j + "th record in the " + i + "th row has been checked-- OK");
                                j++;
                            }
                            i++;
                        }
                    }
                    Console.ReadKey(true);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("An error occured");
                    Console.WriteLine(ex.Message);
                    Console.ReadKey(true);
                }
                Console.ReadKey(true);
            }
        }
    }
}
