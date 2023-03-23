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
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from stats import Stats
from flask_cors import CORS


DB_ENGINE = create_engine("sqlite:///stats.sqlite")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)


def get_latest_stats():
    # TODO create a session
    # TODO query the session for the first Stats record, ordered by Stats.last_updated
    # TODO if result is not empty, convert it to a dict and return it (with status code 200)
    # TODO if result is empty, return NoContent, 201
    session = DB_SESSION()

    result = session.query(Stats).order_by(Stats.last_updated.desc()).first()
    data =result.to_dict()

    return data

def process_event(event, endpoint):
    trace_id = str(uuid.uuid4())
    event['trace_id'] = trace_id

    logger.debug(f'Received {endpoint} event with trace id {trace_id}')

    # TODO: create KafkaClient object assigning hostname and port from app_config to named parameter "hosts"
    # and store it in a variable named 'client'
    hosts = f"{app_config.hostname}:{app_config.port}"  
    client = KafkaClient(bootstrap_servers=hosts)

    # TODO: index into the client.topics array using topic from app_config
    # and store it in a variable named topic
    topic = client.topics[app_config.topic]

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
    producer(pro_dict_json.encode('utf-8'))
    
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
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8080)