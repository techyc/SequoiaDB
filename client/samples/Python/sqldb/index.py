#! /usr/bin/python

import pysqldb
from pysqldb import client
from pysqldb import const
from pysqldb.error import (SDBTypeError,
                               SDBBaseError,
                               SDBEndOfCursor)

from bson.objectid import ObjectId

if __name__ == "__main__":

   try:
      # connect to local db, using default args value.
      # host= '192.168.20.48', port= 11810, user= '', password= ''
      db = client("192.168.20.48", 11810)

      # create a cs
      cs_name = "gymnasium"
      cs = db.create_collection_space(cs_name)

      #create a cl
      cl_name = "sports"
      cl = cs.create_collection(cl_name, {"ReplSize":0})

      # get all indexes before create index
      pysqldb._print("Before create index:")
      cr = cl.get_indexes()
      # print indexes
      while True:
         try:
            record = cr.next()
         except SDBEndOfCursor :
            break
         except SDBBaseError:
            raise
         pysqldb._print(record)

      #create an index
      index = {'Item':1, 'Rank':-1}
      index_name = 'idx'
      cl.create_index(index, index_name, False, False)

      pysqldb._print("After create index:")
      # get all indexes
      cr = cl.get_indexes()

      # print indexes
      while True:
         try:
            record = cr.next()
         except SDBEndOfCursor :
            break
         except SDBBaseError, e:
            raise
         pysqldb._print(record)

      # release all
      cs.drop_collection(cl_name)
      del cl

      db.drop_collection_space(cs_name)
      del cs

      db.disconnect()
      del db

   except (SDBTypeError, SDBBaseError), e:
      pysqldb._print(e)
   except SDBBaseError, e:
      pysqldb._print(e.detail)
