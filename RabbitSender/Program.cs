using RabbitMQ.Client;
using System.Text;

ConnectionFactory factory = new ();
// The default username and password for RabbitMQ are "guest" and "guest" respectively.
// fetch the connection string from environment variable/appsettings.
factory.Uri = new Uri(uriString:"amqp://guest:guest@localhost:5672");

factory.ClientProvidedName = "Rabbit Sender App";

using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

string exchangeName = "demo_exchange";
string routingKey = "demo_routing_key";
string queueName = "demo_queue";

await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct);
// settings from tools and options
await channel.QueueDeclareAsync(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments:null);
await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: routingKey, arguments: null);

//byte[] messageBodyBytes = Encoding.UTF8.GetBytes("Hello, RabbitMQ!");
//await channel.BasicPublishAsync(exchangeName, routingKey, body: messageBodyBytes);

// Run 60 times and simulate delays
for (int i = 0; i < 60; i++)
{
    Console.WriteLine($"Sending message: {i}");
    byte[] messageBodyBytes = Encoding.UTF8.GetBytes($"Message #{i}");
    await channel.BasicPublishAsync(exchangeName, routingKey, body: messageBodyBytes);

    Thread.Sleep(1000);
}

await channel.CloseAsync();
await connection.CloseAsync();