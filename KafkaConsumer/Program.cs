using Confluent.Kafka;

class Program
{
    static void Main(string[] args)
    {
        // Конфигурация Consumer
        var consumerConfig = new ConsumerConfig
        {
            GroupId = "order-consumer-group", // Укажите группу потребителей
            BootstrapServers = "localhost:9092", // Адрес вашего Kafka брокера
            AutoOffsetReset = AutoOffsetReset.Earliest, // Начинаем с самого раннего сообщения
            EnableAutoCommit = false // Выключаем автоматическую фиксацию оффсетов
        };

        // Создание Consumer для чтения сообщений
        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        consumer.Subscribe("order-topic"); // Подписка на топик Kafka

        Console.WriteLine("Waiting for messages...");

        var seenOffsets = new HashSet<long>(); // Следим за хешами для гарантии

        // Бесконечный цикл чтения сообщений
        while (true)
        {
            try
            {
                var consumeResult = consumer.Consume(CancellationToken.None);

                if (!seenOffsets.Contains(consumeResult.TopicPartitionOffset.GetHashCode()))
                {
                    // Вывод сообщения и добавление оффсета в seen_offsets
                    Console.WriteLine($"Received message: {consumeResult.Message.Value} at {consumeResult.TopicPartitionOffset}");
                    seenOffsets.Add(consumeResult.TopicPartitionOffset.GetHashCode());
                }
            }
            catch (ConsumeException e)
            {
                // Обработка ошибки потребления сообщения
                Console.WriteLine($"Error occurred: {e.Error.Reason}");
            }
        }
    }
}