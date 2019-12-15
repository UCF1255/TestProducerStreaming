using System;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;
using TestProducerStreaming.RequestModel;
using TestProducerStreaming.ViewModel;

namespace TestProducerStreaming
{
    class Program
    {
        static  void Main(string[] args)
        {
            SuperLog superLog = new SuperLog
            {
                agentRequestModel = new AgentRequestModel
                {
                    agencyName = "MGAgency",
                    agentCode = 12345678,
                    agentName = "Mayank Khanna"
                },

                log = new LOG
                {

                    logName = "Elastic Search Log",
                    logDetail = "ES Details Data"
                }

            };

            Task task = new Task
            {
                TaskName = "TOPIC_TESTEL",
                TopicName = "TOPIC_TEST",
                Parameter = JsonConvert.SerializeObject(superLog)
            };
            var options = new KafkaOptions (new Uri("http://localhost:9092"));
            var router = new BrokerRouter(options);

            var client = new Producer(router);
            Console.WriteLine(client);
            var result = client.SendMessageAsync(task.TopicName, new[] { new Message("Hi Hello! Welcome to Kafka!") }).GetAwaiter().GetResult();
            Console.WriteLine("Response: P{0}, O{1} : {2}", result[0].PartitionId, result[0].Offset, result[0].Topic);
            Console.ReadLine();
        }
    }
}
