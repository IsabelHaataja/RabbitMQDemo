using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

ConnectionFactory factory = new();
// The default username and password for RabbitMQ are "guest" and "guest" respectively.
// Fetch the connection string from environment variable/appsettings.
factory.Uri = new Uri(uriString: "amqp://guest:guest@localhost:5672");

factory.ClientProvidedName = "Rabbit Receiver2 App";

using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

string exchangeName = "demo_exchange";
string routingKey = "demo_routing_key";
string queueName = "demo_queue";

await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct);
// settings from tools and options
await channel.QueueDeclareAsync(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: routingKey, arguments: null);
// prefetchCount : 1 means that the server will deliver only one message to the consumer at a time, and it won't send another message until the consumer has acknowledged the previous one. This is useful for ensuring that messages are processed one at a time and can help with load balancing when multiple consumers are consuming from the same queue.
// global false means that the QoS settings will apply to the current channel only, rather than being applied globally to all channels on the connection. This allows for more granular control over message delivery and processing for individual consumers or channels.
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

// Event listener
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (sender, args) =>
{
    Task.Delay(TimeSpan.FromSeconds(3)).Wait();
    var body = args.Body.ToArray();
    string message = Encoding.UTF8.GetString(body);

    // This is where you'd save the message to db or do some processing
    Console.WriteLine($"Received message: {message}");

    // If you get a failure here, you'd want to make sure the massage doesn't get acknowleged, so that it can be re-delivered to the same or another consumer for processing.
    channel.BasicAckAsync(args.DeliveryTag, multiple: false);

    return Task.CompletedTask;
};

// Gives tag for the comsumer system --> we can cancel this
string consumerTag = await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

Console.ReadLine();

await channel.BasicCancelAsync(consumerTag);

await channel.CloseAsync();
await connection.CloseAsync();