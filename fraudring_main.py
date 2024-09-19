from fraudring import Fraudring


import pandas as pd
import xlsxwriter
import numpy as np
import datetime
from datetime import date, timedelta
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine
from datetime import date, timedelta
import json
import warnings
import sys
import re
import logging
import time

warnings.filterwarnings("ignore")
logging.basicConfig(filename='Fraudring_refactored.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('%s' %(str(datetime.now())+': Fraud ring has started'))

yr, month, dy, hr, mint = map(int, time.strftime("%Y %m %d %H %M").split())
                    #################################################################


# Function to load configuration from a JSON file
def load_config(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

if __name__ == '__main__':
    # Load the configuration from the JSON file
    config = load_config('Fraud/refactor/config.json')

    # Access the configuration elements
    table_names = config['table_names']
    fraud_table_names = config['fraud_table_names']
    query_thresholds = config['query_thresholds']
    frequency_fraud_rules = config['frequency_fraud_rules']
    db_name = config['db_name']

    # Initialize and run the class with the loaded configuration
    Fraud_ring = Fraudring(table_names, fraud_table_names, query_thresholds, frequency_fraud_rules, db_name)
    
    Fraud_ring.run()