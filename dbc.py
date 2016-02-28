# -*- coding: utf-8 -*-
#!/usr/local/bin/python

import MySQLdb,logging,time
from DBUtils.PooledDB import PooledDB
DB_SERVER,DB_USER,DB_PASSWD,DB_NAME = "","","",""


CONN_POOL = PooledDB(creator=MySQLdb, maxcached=20,host=DB_SERVER,
    user=DB_USER,passwd=DB_PASSWD,db=DB_NAME,charset="utf8")

class DBData(object):
    def __init__(self,table_name,*args,**kw):
        self._table_name = table_name

    def _serializeData(self,kvp_list,*args,**kv):
        data_list = []
        if kvp_list:
            data_list = kvp_list[:]
        for arg in args:
            data_list.append(arg)
        for k,v in kv.items():
            data_list.append("%s=%s"%(k,"'%s'"%v if None!=v else "null"))
        return data_list
            
    def _serializeFilter(self, filter_list,*args,**kv):
        filters = self._serializeData(filter_list,*args,**kv)                  
        sql = " and ".join(filters) if len(filters) else None
        return "where %s"%sql if sql else ""        

    def _serializeValue(self, value_list,*args,**kv):
        values = self._serializeData(value_list,*args,**kw)
        return ",".join(values)            

    def _insert(self, **values):
        if not values:
            return 0
        with DBCursor() as cur:            
            key_list, value_list = [],[]
            for k,v in values.items():
                key_list.append(k)
                value_list.append(("'%s'"%v) if None!=v else "null")
            sql_keys = ",".join(key_list)
            sql_values = ",".join(value_list)
            sql = "insert into %s (%s) values (%s)"%(self._table_name, sql_keys, sql_values)
            return cur.execute(sql)

    def _insertOrUpdate(self, update_key_list,*update_keys,**values):
        if not values:
            return 0
        with DBCursor() as cur:
            key_list, value_list, update_list = [],[],[]
            for k,v in values.items():
                key_list.append(k)
                value_list.append(("'%s'"%v) if None!=v else "null")
                if (k in update_key_list) or (k in update_keys):
                    update_list.append("%s=%s"%(k,"'%s'"%v if None!=v else "null"))
            sql_keys = ",".join(key_list)
            sql_values = ",".join(value_list)
            sql_update = ",".join(update_list)
            sql = "insert into %s (%s) values (%s) on duplicate key update %s"%(
                self._table_name, sql_keys, sql_values, sql_update)
            return cur.execute(sql)
    
    def _select(self, columns, filter=None, group=None, order=None, limit=None):
        with DBCursor() as cur:
            sql_columns = ",".join(columns)
            sql_filter = self._serializeFilter(filter)
            sql_group = "group by %s"%group if group else ""
            sql_order = "order by %s"%order if order else ""
            sql_limit = "limit %d"%limit if limit else ""
            sql = "select %s from %s %s %s %s %s"%(sql_columns, self._table_name,
                sql_filter, sql_group, sql_order, sql_limit)
            return cur.execute(sql),cur.fetchall()

    def _update(self, filter, **values):
        if not values:
            return 0
        with DBCursor() as cur:
            sql_filter = self._serializeFilter(filter)
            sql_values = self._serializeValue(None,**values)
            sql = "update %s set %s %s"%(self._table_name, sql_values, sql_filter)
            return cur.execute(sql)
    
    def _delete(self, filter):
        if not filter:
            return 0
        with DBCursor() as cur:
            sql_filter = self._serializeFilter(filter)
            sql = "delete from %s %s"%(self._table_name, sql_filter)
            return cur.execute(sql)

    def _execute(self, sql):
        with DBCursor() as cur:
            return cur.execute(sql), cur.fetchall()
        

class DBCursor(object):    
    def __enter__(self):
        self.db = CONN_POOL.connection()
        self.cur = self.db.cursor()
        return self.cur
    
    def __exit__(self,type,value,traceback):
        self.db.commit()
        self.cur.close()
        self.db.close()        
