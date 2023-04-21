#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 17:04:04 2023

@author: gowrav
"""

from sqlalchemy import Column, MetaData, Text, Integer
from sqlalchemy.ext.declarative import declarative_base

metadata = MetaData()

Base = declarative_base(metadata=metadata)


class Customers(Base):
    __tablename__ = 'customer_data'
    number = Column(Integer, nullable=False, primary_key=True)
    name = Column(Text, nullable=False)
    age = Column(Integer, nullable=False)
    country = Column(Text, nullable=False)
    status = Column(Text, nullable=False)

    def __init__(self, number, name, age, country,status):
        self.number = number
        self.name = name
        self.age = age
        self.country = country
        self.status = status

    def __repr__(self):
        return f"{self.number},{self.name},{self.age},{self.country},{self.status}"
