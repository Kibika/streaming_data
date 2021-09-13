from kafka import KafkaConsumer, KafkaProducer
import os
import numpy as np
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from scipy.io.wavfile import read, write
from pprint import pprint



if __name__ == "__main__":
    TOPIC_NAME = "AUDIO"

    KAFKA_SERVER = "localhost:9092"

 
    consumer = KafkaConsumer(TOPIC_NAME,
        bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)) 
    # consumer.subscribe(['AUDIO'])              
        
    def decodeBytes(data):
        a=bytearray(data)
        pprint(a)
        b = numpy.array(a, dtype=numpy.int16)
        pprint(b)
        write(r"/home/stella/Week_8/stream/Audios/newfile.wav", 16000, b)
        return 

    for audio in consumer:      
	    audios_data = audio.value()	
	    decodeBytes(audios_data)
        print("Audio = {}".format(decodeBytes(audios_data)))
	
