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
      cs_name = "gymnasium"
      cs = db.create_collection_space(cs_name)

      cl_name = "sports"
      cl = cs.create_collection(cl_name, {"ReplSize":0})

      # insert single record
      basketball = {"Item":"basketball", "id":0}
      oid = cl.insert(basketball)

      pysqldb._print("before update")
      cr = cl.query()
      while True:
         try:
            record = cr.next()
         except SDBEndOfCursor :
            break
         except SDBBaseError:
            raise
         pysqldb._print(record)

      # update records
      update = {'$set':{"Item":"football", "Rank":1 }}
      cond = {'id':{'$et':0}}
      cl.update(update, condition = cond)

      pysqldb._print("after update")
      cr = cl.query()
      while True:
         try:
            record = cr.next()
         except SDBEndOfCursor :
            break
         except SDBBaseError, e:
            raise
         pysqldb._print(record)

      # drop collection
      cs.drop_collection( cl_name )
      del cl

      # drop collection space
      db.drop_collection_space(cs_name)
      del cs

      db.disconnect()
      del db

   except (SDBTypeError, SDBBaseError), e:
      pysqldb._print(e)
   except SDBBaseError, e:
      pysqldb._print(e.detail)
