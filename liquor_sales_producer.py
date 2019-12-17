"""
Simulate daily liquor orders and send the order messages to kafka
"""
from kafka import KafkaProducer
import datetime, time, random, threading
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import pandas as pd


def send_order(topic, inputs):
  producer = KafkaProducer(bootstrap_servers=['199.60.17.210:9092', '199.60.17.193:9092'])

  liquor_df = pd.read_csv(inputs, sep=',')
  liquor_df = liquor_df.dropna(axis=0, subset=['City'])
  liquor_df[['Pack']] = liquor_df[['Pack']].astype('int64')
  liquor_size = liquor_df.shape[0] - 1
  
  while True:
    interval = random.randint(0, 15) # send each message with random interval(0s - 15s)
    liquor_index = random.randint(0, liquor_size) # select a random order of 2019 liquor sales csv file as a current order

    # generate an unique item number of the order
    liquor_df.loc[liquor_index, 'Invoice/Item Number'] = 'INV-' + str(int(time.time()))
    # revise the date to today's date 
    liquor_df.loc[liquor_index, 'Date'] = time.strftime("%m/%d/%Y", time.localtime())

    msg = liquor_df.iloc[liquor_index].to_json()
    producer.send(topic, msg.encode('ascii'))
    time.sleep(interval)

def main(topic, inputs):
  # create several threads to simulate various stores selling liquors at the same time
  for i in range(6):
    server_thread = threading.Thread(target=send_order, args=(topic, inputs))
    server_thread.setDaemon(True)
    server_thread.start()

  while 1:
    time.sleep(1)


if __name__ == "__main__":
  topic = sys.argv[1]
  inputs = sys.argv[2]
  main(topic, inputs)

