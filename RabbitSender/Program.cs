using RabbitMQ.Client;
using System.Text;

ConnectionFactory factory = new ();
// The default username and password for RabbitMQ are "guest" and "guest" respectively.
// fetch the connection string from environment variable/appsettings.
factory.Uri = new Uri(uriString:"amqp://guest:guest@localhost:5672");
