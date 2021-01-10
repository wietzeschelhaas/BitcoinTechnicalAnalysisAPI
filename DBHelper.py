#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 20:35:31 2020

@author: wietze
"""



import sqlite3


class DBHelper():

    
    def __init__(self,dbname):
        self.databaseName = dbname
        # create a database connection
        self.conn = sqlite3.connect(self.databaseName)
        
    #cannot pass table as paramter, table is therefore vulnrable to sql injection
    #pass table name through this
    def scrub(self,table_name):
        return ''.join( chr for chr in table_name if chr.isalnum() )
    
    
    def create_table(self,coinPair):
        sql_create_table = """ CREATE TABLE """ +  coinPair + """(
    	time INT(14),
    	open FLOAT,
    	close FLOAT,
    	high FLOAT,
    	low FLOAT,
    	volume FLOAT
        );"""
        
        c = self.conn.cursor()
        c.execute(sql_create_table)
    
    def addValues(self, data,table):
        table = self.scrub(table)
        sql = ''' INSERT INTO ''' + table + '''(time,open,close,high,low,volume)
                  VALUES(?,?,?,?,?,?) '''
        cur = self.conn.cursor()
        cur.executemany(sql, data)
        self.conn.commit()
        return cur.lastrowid
    
    
    def getLatestEntry(self,table):
        table = self.scrub(table)
        sql= '''SELECT * 
                FROM    '''+table+'''
                WHERE   time = (SELECT MAX(time)  FROM '''+ table + ''');'''
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchone()[0]
    
    def getLatestNEntries(self,table,n):
        table = self.scrub(table)
        
        sql = '''SELECT * FROM (SELECT * FROM '''+table+''' ORDER BY time DESC LIMIT '''+str(n)+''') ORDER BY time ASC;'''

        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
        
    def getColumn(self,matrix, i):
        return [row[i] for row in matrix]
            
    

    
    
    
    

    

