#! /usr/bin/python

import pysqldb
from pysqldb import client
from pysqldb import const
from pysqldb import lob
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

      # insert lob
      bin = "1asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfsdfaasdf"
      obj = cl.create_lob()
      obj.write( bin, 30 )
      oid = obj.get_oid()
      obj.close()

      cr = cl.list_lobs()
      while True:
         try:
            lob_one = cr.next()
         except SDBEndOfCursor :
            break
         except SDBBaseError:
            raise
         pysqldb._print(lob_one)

      lob_two = cl.get_lob(oid)
      pysqldb._print(lob_two.get_size())
      pysqldb._print(lob_two.get_create_time())
      datafrom = lob_two.read(20)
      pysqldb._print(datafrom)
      cl.remove_lob(oid)
      pysqldb._print("remove success")
      # drop collection
      cs.drop_collection( cl_name )
      del cl

      # drop collection space
      db.drop_collection_space(cs_name)
      del cs

      db.disconnect()
      del db

   except SDBBaseError, e:
      pysqldb._print(e)
      pysqldb._print(e.detail)
   except SDBTypeError, e:
      pysqldb._print(e)
