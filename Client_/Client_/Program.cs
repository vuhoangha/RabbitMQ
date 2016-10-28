using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Client_
{
    class Program
    {
        static void Main(string[] args)
        {
            Topic_(args);  
        }


        //có thể ackhnowledgment
        public static void WorkingQueue(string[] args)
        {
            var factory = new ConnectionFactory() { Uri = "amqp://agqovahn:s7y2N_zhinStPW5aCtGboMlEFV2DtutD@buck.rmq.cloudamqp.com/agqovahn" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "task_queue",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);



                var properties = channel.CreateBasicProperties();
                properties.SetPersistent(true);

                var count = 0;
                while (true)
                {
                    count++;
                    var message = GetMessage(args, count);
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                     routingKey: "task_queue",
                     basicProperties: properties,
                     body: body);

                    Console.WriteLine(" [x] Sent {0}", message);
                    Thread.Sleep(50);
                }


            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }


        public static void Pub_Sub(string[] args)
        {
            var factory = new ConnectionFactory() { Uri = "amqp://agqovahn:s7y2N_zhinStPW5aCtGboMlEFV2DtutD@buck.rmq.cloudamqp.com/agqovahn" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");
                var count = 0;
                while (true)
                {
                    Thread.Sleep(50);
                    count++;
                    var message = GetMessage(args,count);
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: "logs",
                                         routingKey: "",
                                         basicProperties: null,
                                         body: body);
                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }
        }

        public static void RoutingKey(string[] args)
        {
            var factory = new ConnectionFactory() { Uri = "amqp://agqovahn:s7y2N_zhinStPW5aCtGboMlEFV2DtutD@buck.rmq.cloudamqp.com/agqovahn" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "direct_logs",
                                        type: "direct");

                var severity = "routingkey_1";

                var count = 0;
                while (true)
                {
                    Thread.Sleep(50);
                    count++;
                    var message = GetMessage(args, count);
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: "direct_logs",
                                         routingKey: severity,
                                         basicProperties: null,
                                         body: body);
                    Console.WriteLine(" [x] Sent '{0}':'{1}'", severity, message);
                }
                
            }

        }

        public static void Topic_(string[] args)
        {
            var factory = new ConnectionFactory() { Uri = "amqp://agqovahn:s7y2N_zhinStPW5aCtGboMlEFV2DtutD@buck.rmq.cloudamqp.com/agqovahn" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "topic_logs",
                                        type: "topic");

                var routingKey = "kern.critical.vhh";


                
                    var message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: "topic_logs",
                                         routingKey: routingKey,
                                         basicProperties: null,
                                         body: body);
                    Console.WriteLine(" [x] Sent '{0}':'{1}'", routingKey, message);
					
					Console.ReadLine();
                
            }
        }

        private static string GetMessage(string[] args, int count)
        {
            return ("Message " + count);
        }

        private static string GetMessage(string[] args)
        {
            return ("Exchange Test");
        }

        public static void HelloWorld()
        {
            var factory = new ConnectionFactory() { Uri = "amqp://agqovahn:s7y2N_zhinStPW5aCtGboMlEFV2DtutD@buck.rmq.cloudamqp.com/agqovahn" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "hello",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                string message = "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: "",
                                     routingKey: "hello",
                                     basicProperties: null,
                                     body: body);
                Console.WriteLine(" [x] Sent {0}", message);
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

    }
}
