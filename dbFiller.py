#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 22:07:36 2020

@author: wietze
"""
from threading import Thread
from binance.client import Client
import time
from DBHelper import DBHelper
import numpy as np

#class will be istantiated everytime an api call is made for a specific coinpair/timeinterval
class DbFiller():
    
    #timeIntervalSeconds should be the time interval per seconds
    def __init__(self,timeInterval, timeIntervalSeconds,updateTime,dbName):
        Thread.__init__(self)
        self.timeIntervalSeconds = timeIntervalSeconds
        self.upDateTime = updateTime
        self.dbName = dbName
        self.timeInterval = timeInterval
        
        #own api key here
        apiKey = ""
        self.client = Client(apiKey)
        self.dbHelper = DBHelper(dbName)
        #self.dbHelper = DBHelper("btc"+timeIntervalSeconds".db")
         
         
    #this function will be run on a different thread
    def run(self):
        dbHelper = DBHelper(self.dbName)
        while True:
            if (int(time.time())*1000) >= self.upDateTime:
                print("current time is : ",int(time.time())*1000)
                print("update time is : ",self.upDateTime)
                f = open("coinpair.txt")
                for line in f:
                    
                    coinPair = line.rstrip()
                    timeToStartFrom = dbHelper.getLatestEntry(coinPair) + 60000 #60000 is 1 minute in ms
        
                    candles = self.client.get_historical_klines(coinPair, self.timeInterval, int(timeToStartFrom), "0 min ago UTC")
                
                
                
                    unixTime = np.asarray(dbHelper.getColumn(candles,0))
                    unixTime = unixTime.astype(np.float)
                
                    #convert to numpy array
                    nOpen = np.asarray(dbHelper.getColumn(candles,1))
                    #convert to floats
                    nOpen = nOpen.astype(np.float)
                    
                    
                    nHigh = np.asarray(dbHelper.getColumn(candles,2)) 
                    nHigh = nHigh.astype(np.float)
                    nLow = np.asarray(dbHelper.getColumn(candles,3))
                    nLow = nLow.astype(np.float)
                    nClose = np.asarray(dbHelper.getColumn(candles,4))
                    nClose = nClose.astype(np.float)
                    
                    nVolume = np.asarray(dbHelper.getColumn(candles,5))
                    nVolume = nVolume.astype(np.float)
                    
                    
                    
                    dbInsert = np.zeros((len(candles),6))
                    dbInsert[:,0] = unixTime
                    dbInsert[:,1] = nOpen
                    dbInsert[:,2] = nClose
                    dbInsert[:,3] = nHigh
                    dbInsert[:,4] = nLow
                    dbInsert[:,5] = nVolume
                    
                    #convert to list of tuples which is what sqlite wants
                    dbInsert = list(map(tuple, dbInsert))
                    
                    test = dbHelper.addValues(dbInsert,coinPair)
                    
                
                self.upDateTime = self.upDateTime + self.timeIntervalSeconds
                
            
            time.sleep(1)
        
    
