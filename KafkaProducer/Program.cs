using avro;
using Avro.IO;
using Avro.Specific;
using Confluent.Kafka;

class Program
{
    static void Main(string[] args)
    {
        for (var i = 1; ; i++)
        {
            // Создаем объект User
            var user = new Order
            {
                orderId = i,
                strings = new List<string>(new []{$"dish{i}", $"dish{i+1}"}),
                date_ = (long)DateTime.Now.Subtract(DateTime.Now).TotalSeconds,
                total_cost = i + i + 1
            };

            // Сериализация объекта User в байты с помощью Avro
            byte[] avroData;
            using (var ms = new MemoryStream())
            {
                var writer = new BinaryEncoder(ms);
                var datumWriter = new SpecificDatumWriter<Order>(user.Schema);
                datumWriter.Write(user, writer);
                avroData = ms.ToArray();
            }

            // Конфигурация Kafka Producer
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"  // Укажите адрес Kafka-брокера
            };

            // Отправка сообщения в Kafka
            using (var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build())
            {
                var message = new Message<Null, byte[]>
                {
                    Value = avroData
                };

                var deliveryResult = producer.ProduceAsync("order-topic", message).GetAwaiter().GetResult();
                Console.WriteLine($"Message delivered to {deliveryResult.TopicPartitionOffset}");
            }
            
            Thread.Sleep(5000);
        }
    }
}