#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 17:47:18 2023

@author: gowrav
"""

from prefect import task, flow
from all_tasks import keepalived_status, retrive_data, transformation_one, transformation_two, transformation_three, \
    insert_data
import schedule
from multiprocessing import Process
from datetime import datetime



#@flow
def prefect_checker():
    status = keepalived_status()
    print('main function started at',datetime.now().strftime("%H:%M:%S"))
    if status == "MASTER STATE":
        print('entered inside the flow function at',datetime.now().strftime("%H:%M:%S"))
        names = retrive_data()
        all_names = transformation_one(names)
        second_names = transformation_two(all_names)
        third_names = transformation_three(second_names)
        insert_data(third_names)
    else:
        print(datetime.now().strftime("%H:%M:%S"), "BACKUP")  
    return



def run_per_time():
    print('entered the scheduler at',datetime.now().strftime("%H:%M:%S"))
    schedule.every(1).minute.do(prefect_checker)
    while True:
        schedule.run_pending()


def run_job():
    print('entered the run job at',datetime.now().strftime("%H:%M:%S"))
    p = Process(target=run_per_time)
    p.start()
    p.join()


if __name__ == '__main__':
    run_job()
