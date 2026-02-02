using Kafka.Application;
using Kafka.Application.Handlers;
using Kafka.Domain.Services;
using Kafka.Infrastructure.Services;
using Kafka.Presentation;

string propsPath = args.Length > 0 ? args[0] : "/tmp/server.properties";

IMetadataService metadataService = new MetadataService(propsPath);

var handlers = new IRequestHandler[]
{
    new ApiVersionsHandler(),
    new FetchHandler(metadataService),
    new ProduceHandler(metadataService),
    new DescribeTopicPartitionsHandler(metadataService)
};

var dispatcher = new RequestDispatcher(handlers);

var clientHandler = new ClientHandler(dispatcher);

var server = new KafkaBrokerServer(9092, clientHandler.HandleAsync);
await server.StartAsync();
