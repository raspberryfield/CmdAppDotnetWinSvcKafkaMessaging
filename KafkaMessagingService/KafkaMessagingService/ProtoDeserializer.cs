using Confluent.Kafka;
using Google.Protobuf;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaMessagingService
{
    public class ProtoDeserializer<T> : IDeserializer<T>
    where T : Google.Protobuf.IMessage<T>, new()
    {
        private MessageParser<T> _parser;

        public ProtoDeserializer() { _parser = new MessageParser<T>(() => new T()); }

        public IEnumerable<KeyValuePair<string, object>>
            Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
                => config;

        public void Dispose() { }

        public T Deserialize(string topic, byte[] data) //Deprecated?
            => _parser.ParseFrom(data);

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            => _parser.ParseFrom(data.ToArray());
    }
}
