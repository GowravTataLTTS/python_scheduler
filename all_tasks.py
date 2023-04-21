#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 19:45:04 2023

@author: gowrav
"""

import time
from database_connections import Customers
from prefect import task, flow
from subprocess import PIPE, Popen
import schedule
from multiprocessing import Process
from datetime import datetime
from sqlalchemy import update



from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, select

metadata = MetaData()
columns = {"name": 0, "age": 1, "country": 2}


#@task
def keepalived_status():
    state = 'FAULT STATE'
    terminal = Popen(['systemctl', 'status', 'keepalived.service'],
                     stdout=PIPE,
                     stderr=PIPE)
    stdout, stderr = terminal.communicate()
    if stdout.decode().lower().split()[-2] == 'master':
        state = 'MASTER STATE'
    return state


def transaction():
    hostname = "192.168.1.19"
    database_name = "customer"
    user = "postgres"
    password = "mysecretpassword"
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{hostname}:6432/{database_name}')
    sessionfactory = sessionmaker(bind=engine)
    session = sessionfactory()
    print('made the connection with database')
    return session

#@task
def retrive_data():
    with transaction() as session:
        print('started fetching data at', datetime.now().strftime("%H:%M:%S"))
        return session.execute(select(Customers)).scalars().all()

#@task
def transformation_one(data):
    country_map = {"U": "United States", "I": "India",
                   "E": "England",
                   "A": "Australia", "G": "Germany","F":"France"}
    new_data = []
    for i in data:
        if i.country in country_map.keys():
            i.country = country_map[i.country]
            i.name = '1' + ' ' + i.name.lower()
        all = {'name': i.name, 'age': i.age, 'country': i.country, 'number': i.number,'status':i.status}
        new_data.append(all)
    return new_data

#@task
def transformation_two(data):
    new_data = []
    for i in data:
        if isinstance(i['age'], int):
            i['age'] = 60 if i['age'] > 40 else 18
            i['name'] = i['name'].replace('1', '2')
        all = {'name': i['name'], 'age': i['age'], 'country': i['country'], 'number': i['number'],'status':i['status']}
        new_data.append(all)
    return new_data


#@task
def transformation_three(data):
    country_map = {"United States": "American", "India": "Indian",
                   "England": "English",
                   "Australia": "Aussie", "Germany": "German", "France": "French"}
    new_data = []
    for i in data:
        if i['country'] in country_map.keys():
            i['country'] = country_map[i['country']]
            i['name'] = i['name'].replace('2', '3')
            i['status']="Done"
        all = {'name': i['name'], 'age': i['age'], 'country': i['country'], 'number': i['number'],'status':i['status']}
        new_data.append(all)
    return new_data


def insert_data(data):
    for row in data:
        with transaction() as session:
            print('updating row', row)
            session.execute(update(Customers).where(Customers.number == row['number']).values(row))
            session.commit()
            print('updated row', row)
            time.sleep(10)
