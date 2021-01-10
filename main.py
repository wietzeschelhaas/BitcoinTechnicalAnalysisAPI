#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 16:12:58 2020

@author: wietze

"""
from binance.client import Client
import numpy as np
import talib
from DBHelper import DBHelper
from dbFiller import DbFiller
import time
import flask
from flask import request, jsonify
import threading


#fill in own api key here, 
apiKey = ""


validIntervals = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]


#set this to true if you want to reload the entire databse, 
#the database cannnot exist so remove the current if there is one
restart = False
    
client = Client(apiKey)



    

# valid intervals - 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
#################################################
#INIT, do this only ones, and again when the raspberry has been turned of for longer than 1000 minutes
#################################################
if restart:
    dbHelper = DBHelper("btc.db")
    
    f = open("coinpair.txt")
    for line in f:
        coinpair = line.rstrip()
        dbHelper.create_table(coinpair)
        
        print("Creating table for ", coinpair)
        
        candles = client.get_historical_klines(coinpair, Client.KLINE_INTERVAL_1MINUTE, "1000 min ago UTC", "1 min ago UTC") 
        
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
        
        
        
        dbInsert = np.zeros((999,6))
        dbInsert[:,0] = unixTime
        dbInsert[:,1] = nOpen
        dbInsert[:,2] = nClose
        dbInsert[:,3] = nHigh
        dbInsert[:,4] = nLow
        dbInsert[:,5] = nVolume
        
        #convert to list of tuples which is what sqlite wants
        dbInsert = list(map(tuple, dbInsert))
        
        test = dbHelper.addValues(dbInsert,coinpair)



#################################################
#if no restart:

#################################################
else:
    #updateTime = dbHelper.getLatestEntry("BTCUSDT") + 60000 #6000 is 1 minute in ms
    #dbFiller = DbFiller("1m",60*1000, updateTime,"btc.db")
    
    #thread = threading.Thread(target=dbFiller.run, daemon=True)
    #thread.start()
    
    #Flask is already multilthreaded when receving requests, no need to implement this.
    app = flask.Flask(__name__)
    
    app.config["DEBUG"] = True #turn this of when deploying
    
    @app.route('/', methods=['GET'])
    def home():
        return "<h1>Tech analysis raspberry api</h1>"
    
    @app.route('/api/price', methods=['GET'])
    def priceSymbol():
        if 'symbol' not in request.args:
            return "Error: No symbol field provided. Please specify a symbol."
        return client.get_symbol_ticker(symbol=request.args['symbol'])
    
    
    #TODO if not a valid s, ymbolshould return "not valid symbol"
    @app.route('/api/indicators/rsi', methods=['GET'])
    def rsi():
        if 'symbol' not in request.args:
            return "Error: No symbol field provided. Please specify a symbol"
        if 'timeInterval' not in request.args:
            return "Error: No timeInterval field provided. Please specify a timeInterval"
        
        dbHelper = DBHelper("btc.db")
        #later pass this as api parameter?
        timeFrame = 14
        
        candles = dbHelper.getLatestNEntries(request.args['symbol'],timeFrame)
        currentPrice = client.get_symbol_ticker(symbol=request.args['symbol'])
        
        l = dbHelper.getColumn(candles,2)
        l.append(currentPrice["price"])
        nClose = np.asarray(l)
        nClose = nClose.astype(np.float)
        
        rsi = talib.RSI(nClose,timeperiod = timeFrame)
        
        response = {"rsi": rsi[len(rsi)-1]}
        return jsonify(response)
        
    
    
    app.run()

    
    
