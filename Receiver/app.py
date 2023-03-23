import connexion
from connexion import NoContent
import datetime
import json
import logging
import logging.config
import pykafka
from pykafka import KafkaClient
import requests
import uuid
import yaml 

def process_event(event, endpoint):
    trace_id = str(uuid.uuid4())
    event['trace_id'] = trace_id

    logger.debug(f'Received {endpoint} event with trace id {trace_id}')

    # TODO: create KafkaClient object assigning hostname and port from app_config to named parameter "hosts"
    # and store it in a variable named 'client'
    hosts = f"{app_config['events']['hostname']}:{str(app_config['events']['port'])}"  
    client = KafkaClient(
        hosts=f"{str(app_config['events']['hostname'])}:{str(app_config['events']['port'])}", socket_timeout_ms=100000
        )

    # TODO: index into the client.topics array using topic from app_config
    # and store it in a variable named topic
    topic = client.topics[app_config['events']['topic']]

    # TODO: call get_sync_producer() on your topic variable
    # and store the return value in variable named producer
    producer = topic.get_sync_producer()

    # TODO: create a dictionary with three properties:
    # type (equal to the event type, i.e. endpoint param)
    # datetime (equal to the current time, formatted as per our usual format)
    # payload (equal to the entire event passed into the function)
    pro_dict = {
        "type": endpoint,
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "payload": event
    }

    # TODO: convert the dictionary to a json string
    pro_dict_json = json.dumps(pro_dict)

    # TODO: call the produce() method of your producer variable and pass in your json string
    # note: encode the json string as utf-8
    producer.produce(pro_dict_json.encode('utf-8'))
    
    # TODO: log "PRODUCER::producing x event" where x is the actual event type
    # TODO: log the json string
    logger.debug(f'PRODUCER::producing {event} event')
    logger.debug(f'The json string is {pro_dict_json}')


    return NoContent, 201

# Endpoints
def buy(body):
    process_event(body, 'buy')
    return NoContent, 201

def sell(body):
    process_event(body, 'sell')
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')

app.add_api("openapi.yml", base_path="/processing", strict_validation=True, validate_responses=True)
with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8080)
