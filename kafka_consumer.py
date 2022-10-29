import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer



API_KEY = 'MIDAS34RMTMYXIPX'
ENDPOINT_SCHEMA_URL  = 'https://psrc-8kz20.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'rDGc31rh4gD+pFuKju1hLZrly3+a1/XyIHvVSEvfqWjH7T+W9O6jewo4+Oo68NMM'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '7TI4AUDWOTONCRUK'
SCHEMA_REGISTRY_API_SECRET = 'CYwxvZC4Cy3iX5jsTccYdxesFJCnDowkTaKiS7bPrtWKTKHsL0yM+GsA8TrwjBY/'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class order:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return order(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Car.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if car is not None:
                print("User record {}: order: {}\n"
                      .format(msg.key(), order))
        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurant_orderstopic")
