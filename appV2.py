import sys
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import faulthandler
from ecgdetectors import Detectors
import numpy as np
import tensorflow.keras as keras
from pyspark.sql.types import StructType, StructField, StringType
import time
from datetime import datetime
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

def getSignalInterval(data_signal,data_peaks):
  #a = np.zeros(shape=(len(data_peaks),319)) many signals
  r=[]
  for i in range(len(data_peaks)):
    index = data_peaks[i] 
    value = data_signal[index]
    r.append(value)
    

  #get the highest peak
  rp = np.argmax(r)
  rp = data_peaks[rp]
  print("peak at :", rp)
  #rp : highest point index
  start = rp-165
  end = rp+154
  print(start,end)

  if(start>=0 and end<=len(data_signal)):
    data_signal = data_signal[start:end]

  return data_signal
#--------------------------------------------------------------------------------
def conf_r_peak_Detector(input):
    BASIC_SRATE = 319
    signal_pad_samples = 1
    signal_pad = np.zeros(signal_pad_samples) # pad one sec to detect initial peaks properly
    signalf = input
    detectors = Detectors(BASIC_SRATE)

    detectors = {
                #'pan_tompkins_detector':[detectors.pan_tompkins_detector, []],
                #'hamilton_detector':[detectors.hamilton_detector, []],
                #'christov_detector':[detectors.christov_detector, []]#,
                #'engzee_detector':[detectors.engzee_detector, []],
                'swt_detector':[detectors.swt_detector, []],
                #'two_average_detector':[detectors.two_average_detector, []],
                }
    for kd in detectors.keys():
        vd = detectors[kd]
        r_peaks = np.array(vd[0](np.hstack((signal_pad,signalf)))) - signal_pad_samples
        vd[1] = r_peaks

    data_peaks = detectors['swt_detector'][1]
    print(data_peaks)
    data_signal = getSignalInterval(signalf,data_peaks)
    return data_signal
#----------------------------------------------------------------------------------
def normalize(input):
  maxE = np.amax(input)
  minE = np.amin(input)
  input = (input + minE*(-1))/(maxE-minE)

  return input
#----------------------------------------------------------------------------------
def preprocess(ecg):
    #get signal interval (close to r_peak)
    my_data = conf_r_peak_Detector(ecg)
    #normilaze data
    my_data= normalize(my_data)
    my_data = my_data.reshape(1,my_data.shape[0],1)
    return my_data
#----------------------------------------------------------------------------
def build_model(input_shape):
  """Generates RNN-LSTM model
  :param input_shape (tuple): Shape of input set
  :return model: RNN-LSTM model
  """

  # build network topology
  model = keras.Sequential()

  # 2 LSTM layers
  model.add(keras.layers.LSTM(128, input_shape=input_shape, return_sequences=True))
  model.add(keras.layers.LSTM(64))

  #dropout
  model.add(keras.layers.Dropout(0.5)) # avoid overfitting 

  # dense layer
  model.add(keras.layers.Dense(64, activation='relu'))

  # output layer
  model.add(keras.layers.Dense(6, activation='softmax'))

  return model
#-----------------------------------------------------------
def ecg_type(typeC):
    switcher = {
        0: "(0-N) Normal ",
        1: "(1-L) Left bundle branch block",
        2: "(2-R) Right bundle branch block",
        3: "(3-A) Atrial premature",
        4: "(4-V) Premature ventricular contraction",
        5: "(5-/) Paced",
    }
    return switcher.get(typeC, "")
#-------------------------------------------------------------------
def load_model():
    input_shape = (319,1)
    model = build_model(input_shape) 
    model.load_weights('best_model.h5')
    return model
#---------------------------------------------------------------------------------
def classify(model, my_data):
    classification = model.predict(my_data)*100
    purcentC = "{:.2f}".format(np.amax(classification))
    typeC = np.argmax(classification)
    typeC = ecg_type(typeC)
    prediction = [typeC,purcentC]
    return prediction
#--------------------------------------------------------------------

def set_influx():
    # You can generate an API token from the "API Tokens Tab" in the UI
     token = "sckl3vA--7KYKuYH-DHY2omq1LTiX8q-Y1svgtJ-hI85ozl2YRbvjOxdLP1WTr8gJafvpbjj08Ljg4C0H9ONRA=="
     org = "esi-sba"
     url = "http://localhost:8086"

     client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
     return client

def loadTo_influx(values):
    client = set_influx()
    #Write Data-------------------------------------------------------------------
    bucket="ecg"

    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    point = (
      Point("ecg")
      .tag("host", "host1")
      .field("value", values)
    )
    write_api.write(bucket=bucket, org="esi-sba", record=point)
    client.close()
# def loadTo_influx(values):
#     #tokens-----------------------------------------------------------------------
#     #INFLUXDB_TOKEN="9-TCZXMTYwEYhWnMMMhXwdKJREpM5rKScx4VXKtlW6gVlyuHc5rsPZNGWahm4SffE2HLZ1O4zpgYajicmCMOhA=="
#     #Init client------------------------------------------------------------------
#     token = "lfKYykGn6ZCmaGhG-Y3FOkjSJi5wMXRWK7F5vV-YLegKPXu3G9HtPJqYhAI-rpBvCAHEYIJRcEEqwee3OaWGPw=="
#     org = "esi-sna"
#     bucket = "ecg"
#     write_api = client.write_api(write_options=SYNCHRONOUS)



#     client = influxdb_client.InfluxDBClient(url="http://localhost:8086", token=token, org=org) 
#     return client
      

#     point = (
#     Point("ecg")
#     .tag("host", "host1")
#     .field("value", values)
#     .time(datetime.utcnow(), WritePrecision.NS)
#     )
#     write_api.write(bucket=bucket, org=org, point=point)


    
 

if __name__ == "__main__":
    faulthandler.enable()
    sc = SparkContext(appName="kafka")
    spark = SparkSession.builder.getOrCreate()
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 2) # 2 second window
    
    #kstream = KafkaUtils.createDirectStream(ssc, topics = ['ecg'], kafkaParams = {"metadata.broker.list":"localhost:9092"})
    kafka_topic_name = "ecg"
    kafka_bootstrap_servers = 'localhost:9092'
    zk_bootstrap_servers = 'localhost:2181'
    schema = StructType([StructField("timestamp", StringType(), True), StructField("value", StringType(), True),])

    #kvs = KafkaUtils.createStream(ssc, zk_bootstrap_servers, 'spark-streaming-consumer', {kafka_topic_name:1}) 
    kvs = KafkaUtils.createDirectStream(ssc, [kafka_topic_name], {'bootstrap.servers':kafka_bootstrap_servers})
    #kvs = KafkaUtils.createDirectStream(ssc, [kafka_topic_name], {'bootstrap.servers':kafka_bootstrap_servers,'group.id':'test-group','auto.offset.reset':'largest'})

    
    #preprocess
    
    
    #words = sc.textFile("C:\Spark\spark-2.4.8-bin-hadoop2.7\kafka.txt").
    #wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
    
    #vals = kvs.flatMap(lambda line: line[1].split(","))
    # save lines to file
    #df = spark.createDataFrame(rdd, schema=schema)
    #load model----------------------------------------
    model = load_model()
    
    kvs.foreachRDD(lambda x: getrdds(x))

    def getrdds(rdd):
        print('in rdds')
        if not rdd.isEmpty():
            start_time = datetime.now() #-------------------------------
            v = rdd.values().cache().first()
            second_time = datetime.now() #-------------------------------
            loadTo_influx(v)
            quarter_time = datetime.now()#-----------------------------------

            #remove brackets
            a = v[1:-1]
            #convert a to array of strings
            x = a.split(',')
            #convert to array of floats
            y = np.array(x, dtype=np.float32)
            third_time = datetime.now() #-------------------------------
            
            
            
            my_data = preprocess(y)
            
            fourth_time = datetime.now()#-------------------------------
            #model prediction ------------------------------------------------
            prediction = classify(model, my_data)
            end_time = datetime.now()#-----------------------------------

            print(prediction,'-------------------------------------------------') 

            print('Duration collect: {}'.format(second_time - start_time))
            print('Duration load to influx: {}'.format(quarter_time - second_time))
            print('Duration transform: {}'.format(third_time - quarter_time))
            
            print('Duration preprocessing: {}'.format(fourth_time - quarter_time))
            print('Duration classification: {}'.format(end_time - fourth_time))
            print('Duration total: {}'.format(end_time - start_time))
            #my_data.saveAsTextFiles("C:\Spark\spark-2.4.8-bin-hadoop2.7\output\")'''

        return rdd
    
    
    
    
    ssc.start()
    # stream will run for 50 sec
    ssc.awaitTermination()
    #ssc.awaitTermination()
    ssc.stop()
    sc.stop()