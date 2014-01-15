#coding=utf-8
# Create your views here.
from django.http import HttpResponse
from django.shortcuts import render_to_response,RequestContext
import json
import models
import string
import time
import sys
import DB.db
#from DB.db import DB_MYSQL
#from DB.db import DB_CONFIG
#from DB.db import *
import operation.views
import config.views
import threading
import datetime
from multiprocessing import Process

def day_diff(date1, date2):
    d1 = datetime.datetime(string.atoi(date1[0:4]), string.atoi(date1[5:7]), string.atoi(date1[8:10]))
    d2 = datetime.datetime(string.atoi(date2[0:4]), string.atoi(date2[5:7]), string.atoi(date2[8:10]))
    return (d1-d2).days


def task_hits_insert(platform, hash_id, v_time, v_hits_num):
    if(platform == 'mobile'):
        hash_local = models.mobile_task_hits(hash       = hash_id,      \
                                        time            = v_time,       \
                                        hits_num        = v_hits_num    \
                                        ) 
        hash_local.save()
    elif(platform == 'pc'):
        hash_local = models.pc_task_hits(hash           = hash_id,      \
                                        time            = v_time,       \
                                        hits_num        = v_hits_num    \
                                        ) 
        hash_local.save()
        

def task_temperature_insert(platform, hash_id, v_online_time, v_is_valid, v_filesize, v0, v1, v2, v3, v4, v5, v6, v7):
    if(platform == 'mobile'):
        hash_local = models.mobile_task_temperature(hash= hash_id,          \
                                        online_time     = v_online_time,    \
                                        is_valid        = v_is_valid,       \
                                        filesize        = v_filesize,       \
                                        temperature0    = v0,               \
                                        temperature1    = v1,               \
                                        temperature2    = v2,               \
                                        temperature3    = v3,               \
                                        temperature4    = v4,               \
                                        temperature5    = v5,               \
                                        temperature6    = v6,               \
                                        temperature7    = v7               \
                                        ) 
        hash_local.save()
    elif(platform == 'pc'):
        hash_local = models.pc_task_temperature(hash= hash_id,          \
                                        online_time     = v_online_time,    \
                                        is_valid        = v_is_valid,       \
                                        filesize        = v_filesize,       \
                                        temperature0    = v0,               \
                                        temperature1    = v1,               \
                                        temperature2    = v2,               \
                                        temperature3    = v3,               \
                                        temperature4    = v4,               \
                                        temperature5    = v5,               \
                                        temperature6    = v6,               \
                                        temperature7    = v7               \
                                        ) 
        hash_local.save()
        
    

def get_tasks_local(platform):
    task_list = None
    if(platform == 'mobile'):
        task_list = models.mobile_task_temperature.objects.all()
    elif(platform == 'pc'):
        task_list = models.pc_task_temperature.objects.all()    
    return task_list

def get_tasks_hits(platform):
    task_list = None
    if(platform == 'mobile'):
        task_list = models.mobile_task_hits.objects.all()
    elif(platform == 'pc'):
        task_list = models.pc_task_hits.objects.all()    
    return task_list


def get_cold_tasks_rule1(platform):
    task_list = []
    
    db = DB.db.DB_MYSQL()
    db.connect(DB.db.MS_DB_CONFIG.host, DB.db.MS_DB_CONFIG.port, DB.db.MS_DB_CONFIG.user, DB.db.MS_DB_CONFIG.password, DB.db.MS_DB_CONFIG.db)
  
    sql = "SELECT hash, online_time, is_valid, filesize, hot, cold1, cold2, cold3, last_hit_time, total_hits_num FROM %s_task ORDER BY cold1 ASC, hot ASC" % (platform)         
    print sql
    db.execute(sql)
    
    for row in db.cur.fetchall():
        task1 = {}      
        col_num = 0  
        for r in row:
            if(col_num == 0):
                task1['hash'] = r
            elif(col_num == 1):
                task1['online_time'] = r
            elif(col_num == 2):
                task1['is_valid'] = r
            elif(col_num == 3):
                task1['filesize'] = r
            elif(col_num == 4):
                task1['hot'] = r
            elif(col_num == 5):
                task1['cold1'] = r
            elif(col_num == 6):
                task1['cold2'] = r
            elif(col_num == 7):
                task1['cold3'] = r
            elif(col_num == 8):
                task1['last_hit_time'] = r
            elif(col_num == 9):
                task1['total_hits_num'] = r
            col_num += 1
        task_list.append(task1)
     
    return task_list


def get_cold_tasks_rule2(platform, time_limit):
    task_list = []
    
    db = DB.db.DB_MYSQL()
    db.connect(DB.db.MS_DB_CONFIG.host, DB.db.MS_DB_CONFIG.port, DB.db.MS_DB_CONFIG.user, DB.db.MS_DB_CONFIG.password, DB.db.MS_DB_CONFIG.db)
  
    sql = "SELECT hash, online_time, is_valid, filesize, hot, cold1, cold2, cold3, last_hit_time, total_hits_num FROM %s_task where online_time < '%s' ORDER BY hot ASC" % (platform, time_limit)         
    print sql
    db.execute(sql)
    
    for row in db.cur.fetchall():
        task1 = {}      
        col_num = 0  
        for r in row:
            if(col_num == 0):
                task1['hash'] = r
            elif(col_num == 1):
                task1['online_time'] = r
            elif(col_num == 2):
                task1['is_valid'] = r
            elif(col_num == 3):
                task1['filesize'] = r
            elif(col_num == 4):
                task1['hot'] = r
            elif(col_num == 5):
                task1['cold1'] = r
            elif(col_num == 6):
                task1['cold2'] = r
            elif(col_num == 7):
                task1['cold3'] = r
            elif(col_num == 8):
                task1['last_hit_time'] = r
            elif(col_num == 9):
                task1['total_hits_num'] = r
            col_num += 1
        task_list.append(task1)
     
    return task_list


def get_tasks_by_hash(platform, hash_id):
    task_list = None
    if(platform == 'mobile'):
        task_list = models.mobile_task.objects.filter(hash=hash_id)
    elif(platform == 'pc'):
        task_list = models.pc_task.objects.filter(hash=hash_id)  
    return task_list
        

def get_task_list(request, platform):
    print 'get_task_list'
    print request.REQUEST
            
    start = request.REQUEST['start']
    limit = request.REQUEST['limit']    
    start_index = string.atoi(start)
    limit_num = string.atoi(limit)
    print '%d,%d' % (start_index, limit_num)
    
    kwargs = {}
    
    hash_id = ''
    if(request.REQUEST.has_key('hash') == True):
        hash_id = request.REQUEST['hash']
    if(hash_id != ''):
        kwargs['hash'] = hash_id
    
    sort = ''
    if 'sort' in request.REQUEST:
        sort  = request.REQUEST['sort']
        
    dire = ''
    if 'dir' in request.REQUEST:
        dire   = request.REQUEST['dir']
            
    order_condition = ''
    if(len(dire) > 0):
        if(dire == 'ASC'):
            order_condition += ''
        elif(dire == 'DESC'):
            order_condition += '-'
                
    if(len(sort) > 0):
        order_condition += sort
    
    tasks = get_tasks_local(platform)
    tasks1 = tasks.filter(**kwargs)
    tasks2 = None    
    if(len(order_condition) > 0):
        tasks2 = tasks1.order_by(order_condition)[start_index:start_index+limit_num]     
    else:        
        tasks2 = tasks1[start_index:start_index+limit_num]
            
    return_datas = {'success':True, 'data':[]}    
    return_datas['total_count'] = tasks1.count()
    
    for task in tasks2:        
        return_datas['data'].append(task.todict())
        
    return HttpResponse(json.dumps(return_datas))


def down_hot_tasks(request, platform):
    print 'down_hot_tasks'
    print request.REQUEST    
    task_num = request.REQUEST['task_num']
    
    tasks = get_tasks_local(platform) 
    tasks2 = tasks.order_by('-temperature0')[0:task_num]
    
    output = ''
    for task in tasks2:
        output += '%s,%s,%e\n' % (task.hash, task.online_time, task.temperature0)
            
    response = HttpResponse(output, content_type='text/comma-separated-values')
    response['Content-Disposition'] = 'attachment; filename=hot_tasks_%s_%s.csv' % (platform, task_num)
    return response


def down_cold_tasks(request, platform):
    print 'down_cold_tasks'
    print request.REQUEST  
      
    task_num = request.REQUEST['task_num']
        
    tasks = get_tasks_local(platform)  
    tasks2 = tasks.order_by('temperature0')[0:task_num]
       
    output = ''
    for task in tasks2:
        output += '%s,%s,%e\n' % (task.hash, task.online_time, task.temperature0)
    
    response = HttpResponse(output, content_type='text/comma-separated-values')
    response['Content-Disposition'] = 'attachment; filename=cold_tasks_%s_%s.csv' % (platform, task_num)
    return response


def get_tasks_macross_mobile(begin_date, end_date):
    task_list = []
    sql = ""
    
    #reload(sys)
    #sys.setdefaultencoding('utf8')
    where_condition = ''
    if(len(begin_date) > 0):
        begin_time = '%s-%s-%s 00:00:00' % (begin_date[0:4], begin_date[4:6], begin_date[6:8])
        where_condition += " where create_time >= '%s'" % (begin_time)
    if(len(end_date) > 0):
        end_time = '%s-%s-%s 00:00:00' % (end_date[0:4], end_date[4:6], end_date[6:8])
        where_condition += " and create_time < '%s'" % (end_time)
    
    db = DB.db.DB_MYSQL()
    db.connect(DB.db.DB_CONFIG.host, DB.db.DB_CONFIG.port, DB.db.DB_CONFIG.user, DB.db.DB_CONFIG.password, DB.db.DB_CONFIG.db)
        
    #sql = "select dat_hash, create_time, filesize from fs_mobile_dat where state!='dismissed' " + where_condition           
    sql = "select dat_hash, create_time, filesize from fs_mobile_dat" + where_condition
    print sql
    db.execute(sql)
    
    for row in db.cur.fetchall():
        task1 = {}      
        col_num = 0  
        for r in row:
            if(col_num == 0):
                task1['hash'] = r
            elif(col_num == 1):
                task1['online_time'] = r
            elif(col_num == 2):
                task1['filesize'] = r
            col_num += 1
        task_list.append(task1)
    print 'task_list num: %d' % (len(task_list))
    
    sql = "select video_hash, create_time, filesize from fs_video_dat" + where_condition           
    print sql
    db.execute(sql)
    
    for row in db.cur.fetchall():
        task1 = {}      
        col_num = 0  
        for r in row:
            if(col_num == 0):
                task1['hash'] = r
            elif(col_num == 1):
                task1['online_time'] = r
            elif(col_num == 2):
                task1['filesize'] = r
            col_num += 1
        task_list.append(task1)
    print 'task_list num: %d' % (len(task_list))
    
    db.close()  
     
    return task_list


def get_tasks_macross_pc(begin_date, end_date):
    task_list = []
    sql = ""
    
    where_condition = ''
    if(len(begin_date) > 0):
        begin_time = '%s-%s-%s 00:00:00' % (begin_date[0:4], begin_date[4:6], begin_date[6:8])
        where_condition += " where fs_task.create_time >= '%s'" % (begin_time)
    if(len(end_date) > 0):
        end_time = '%s-%s-%s 00:00:00' % (end_date[0:4], end_date[4:6], end_date[6:8])
        where_condition += " and fs_task.create_time < '%s'" % (end_time)
    
    db = DB.db.DB_MYSQL()
    db.connect(DB.db.DB_CONFIG.host, DB.db.DB_CONFIG.port, DB.db.DB_CONFIG.user, DB.db.DB_CONFIG.password, DB.db.DB_CONFIG.db)
    
    #where_condition = " and fs_task.task_hash='92A20D164F8D5F18F2433E9CB39703E2A381EDDC'"
    #sql = "select task_hash, create_time from fs_task where state!='dismissed' " + where_condition
    #sql = "select t.task_hash, t.create_time, d.file_size from fs_task t, fs_dat_file d where t.task_hash=d.hashid and t.state!='dismissed' " + where_condition
    sql = "select fs_task.task_hash, fs_task.create_time, fs_dat_file.file_size from fs_task LEFT JOIN fs_dat_file ON fs_task.task_hash=fs_dat_file.hashid" + where_condition              
    print sql
    db.execute(sql)
    
    for row in db.cur.fetchall():
        task1 = {}      
        col_num = 0  
        for r in row:
            if(col_num == 0):
                task1['hash'] = r
            elif(col_num == 1):
                task1['online_time'] = r
            elif(col_num == 2):
                task1['filesize'] = r
            col_num += 1
        task_list.append(task1)
    
    db.close()  
     
    return task_list


def add_tasks_local(platform, task_list):    
    table = '%s_task_temperature' % (platform)
    
    db = DB.db.DB_MYSQL()
    db.connect(DB.db.MS2_DB_CONFIG.host, DB.db.MS2_DB_CONFIG.port, DB.db.MS2_DB_CONFIG.user, DB.db.MS2_DB_CONFIG.password, DB.db.MS2_DB_CONFIG.db)

    sql = 'SELECT count(*) FROM %s' % (table)
    print sql
    db.execute(sql)
    num_1 = 0    
    for row in db.cur.fetchall():        
        for r in row:
            num_1 = r
            break
        break
    
    print 'INSERT INTO %s' % (table)
    for task in task_list:        
        sql = 'INSERT INTO %s(hash, online_time, is_valid, filesize, temperature0, \
        temperature1, temperature2, temperature3, temperature4, temperature5, temperature6, temperature7) \
        VALUES("%s", "%s", 2, %s, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0) \
        ON DUPLICATE KEY UPDATE is_valid=2' % (table, task['hash'], task['online_time'], task['filesize'])              
        #print sql
        db.execute(sql)
    
    sql = 'SELECT count(*) FROM %s' % (table)
    print sql
    db.execute(sql)
    num_2 = 0    
    for row in db.cur.fetchall():        
        for r in row:
            num_2 = r
            break
        break
    
    num_insert = num_2 - num_1
    
    sql = 'DELETE FROM %s WHERE is_valid!=2' % (table)
    print sql
    db.execute(sql)
    
    sql = 'SELECT count(*) FROM %s' % (table)
    print sql
    db.execute(sql)
    num_3 = 0    
    for row in db.cur.fetchall():        
        for r in row:
            num_3 = r
            break
        break
    
    num_delete = num_2 - num_3
    
    sql = 'UPDATE %s SET is_valid=1' % (table)
    print sql
    db.execute(sql)
        
    db.close()  
     
    return (num_1, num_insert, 0, num_delete)


def get_tasks_macross(platform, begin_date, end_date):
    task_list = None
    if(platform == 'mobile'):
        task_list = get_tasks_macross_mobile(begin_date, end_date)
    elif(platform == 'pc'):        
        task_list = get_tasks_macross_pc(begin_date, end_date)
        
    return task_list


def task_list_find(task_list, hashid):
    for task in task_list:
        if(task.hash == hashid):
            return task
    return None



def show_task_list(request, platform):  
    output = ''
    hashs = request.REQUEST['hashs']    
    hash_list = hashs.split(',')
    
    #reload(sys)
    #sys.setdefaultencoding('utf8')
    
    db = DB.db.DB_MYSQL()
    #db.connect("192.168.8.101", 3317, "public", "funshion", "macross")
    db.connect(DB.db.DB_CONFIG.host, DB.db.DB_CONFIG.port, DB.db.DB_CONFIG.user, DB.db.DB_CONFIG.password, DB.db.DB_CONFIG.db)
    if(platform == 'mobile'):
        sql = 'select dat_hash, cid, serialid, media_id, dat_name from fs_mobile_dat where dat_hash='
    elif(platform == 'pc'):
        sql = 'select a.hashid, a.dat_id, a.serialid, b.media_id, a.dat_name from fs_dat_file a, fs_media_serials b where a.serialid=b.serialid and a.hashid='    
        
    for hashid in hash_list:
        title = '<h1>hash: %s</h1>' % (hashid)
        output += title          
        sql_where = sql + '"%s"'%(hashid)
        db.execute(sql_where)
        
        task_list = []
        for row in db.cur.fetchall():
            task = {}      
            col_num = 0  
            for r in row:
                if(col_num == 0):
                    task['hash'] = r
                elif(col_num == 1):
                    task['cid'] = r
                elif(col_num == 2):
                    task['serialid'] = r
                elif(col_num == 3):
                    task['media_id'] = r
                elif(col_num == 4):
                    task['dat_name'] = r
                col_num += 1
            task_list.append(task)   
        
        for task in task_list:
            output += '%s,%s,%s,%s,%s\n' % (task['hash'], task['cid'], task['serialid'], task['media_id'], task['dat_name']) 
      
    db.close()  
      
    return HttpResponse(output)


def upload_sub_hits_num(platform, previous_day):
    num_insert2 = 0
    num_update2 = 0
    # check if uploaded
    #     true: sub it,
    #     false: do nothing.
    operation_list = operation.views.get_operation_by_type_name(platform, 'upload_hits_num', previous_day)
    if(len(operation_list) <= 0):
        print 'sub_hits_num %s not uploaded' % (previous_day)
        return (True, num_insert2, num_update2)
        
    upload_file = ""
    if(platform == 'mobile'):
        upload_file = DB.db.HITS_FILE.template_mobile % (previous_day)
    elif(platform == 'pc'):
        upload_file = DB.db.HITS_FILE.template_pc % (previous_day)
    print 'sub_hits_num %s' % (upload_file)
       
    line_num = 0
    try:
        hits_file = open(upload_file, "r")
    except:            
        return (False, num_insert2, num_update2)
        
    hash_list_local = get_tasks_local(platform)  
        
    content = hits_file.readlines()
    for line in content:
        items = line.split(' ')
        if(len(items) >= 2):
            hits_num = items[0].strip()
            hash_id = items[1].strip()
            #print '%s, %s' % (hits_num, hash_id)
            task_list = hash_list_local.filter(hash=hash_id)
            if(len(task_list) > 0):
                #print '%s, %s [update -]' % (hits_num, hash_id)
                hash_local = task_list[0]
                if(hash_local.hot > string.atoi(hits_num)): 
                    hash_local.hot -= string.atoi(hits_num)
                else:
                    hash_local.hot = 0                    
                hash_local.save()
                num_update2 += 1
            else:
                print '%s, %s [insert -]' % (hits_num, hash_id)
                num_insert2 += 1
        line_num += 1
        if(line_num % 100 == 0):
            print 'file=%s, line_num=%d' % (upload_file, line_num)
    hits_file.close()
    print 'sub_hits_num line_num=%d, num_insert2=%d, num_update2=%d' % (line_num, num_insert2, num_update2)        
    return (True, num_insert2, num_update2)
    

def upload_add_hits_num_sql(platform, hits_date):
    num_insert = 0
    num_update = 0
    
    upload_file = ""
    if(platform == 'mobile'):        
        upload_file = DB.db.HITS_FILE.template_mobile % (hits_date)
    elif(platform == 'pc'):
        upload_file = DB.db.HITS_FILE.template_pc % (hits_date)
    print 'add_hits_num %s' % (upload_file)
       
    hits_time = '%s-%s-%s 12:00:00' % (hits_date[0:4], hits_date[4:6], hits_date[6:8])  
    
    try:
        hits_file = open(upload_file, "r")
    except:            
        return (False, num_insert, num_update)
    
    table = '%s_task_hits' % (platform)
    
    db = DB.db.DB_MYSQL()
    db.connect(DB.db.MS2_DB_CONFIG.host, DB.db.MS2_DB_CONFIG.port, DB.db.MS2_DB_CONFIG.user, DB.db.MS2_DB_CONFIG.password, DB.db.MS2_DB_CONFIG.db)
    
    line_num = 0 
    while(True): 
        line = hits_file.readline()
        if(line == ''):
            break           
        items = line.split(' ')
        if(len(items) >= 2):
            hits_num = items[0].strip()
            hash_id = items[1].strip()
            v_hits_num = string.atoi(hits_num)
            #sql = 'INSERT ignore INTO %s(hash, time, hits_num) VALUES("%s", "%s", %d)' % (table, hash_id, hits_time, v_hits_num)  
            sql = 'INSERT INTO %s(hash, time, hits_num) VALUES("%s", "%s", %d) ON DUPLICATE KEY UPDATE hits_num=hits_num+%d' % (table, hash_id, hits_time, v_hits_num, v_hits_num) 
            print sql
            db.execute(sql)
            num_insert += 1
        line_num += 1
        if(line_num % 100 == 0):
            print 'file=%s, line_num=%d' % (upload_file, line_num)
        
    hits_file.close() 
    
    print 'add_hits_num line_num=%d, num_insert=%d, num_update=%d' % (line_num, num_insert, num_update)            
    return (True, num_insert, num_update)

    
def upload_add_hits_num_django(platform, hits_date):
    num_insert = 0
    num_update = 0
    
    upload_file = ""
    if(platform == 'mobile'):        
        upload_file = DB.db.HITS_FILE.template_mobile % (hits_date)
    elif(platform == 'pc'):
        upload_file = DB.db.HITS_FILE.template_pc % (hits_date)
    print 'add_hits_num %s' % (upload_file)
       
    hits_time = '%s-%s-%s 12:00:00' % (hits_date[0:4], hits_date[4:6], hits_date[6:8])  
    
    try:
        hits_file = open(upload_file, "r")
    except:            
        return (False, num_insert, num_update)
    
    # method 1    
    hash_list_local = get_tasks_local(platform)
    
    line_num = 0 
    while(True): 
        line = ''        
        try:      
            line = hits_file.readline()
        except:            
            break 
        if(line == ''):
            break           
        items = line.split(' ')
        if(len(items) >= 2):
            hits_num = items[0].strip()
            hash_id = items[1].strip()            
            #print '%s, %s [insert +]' % (hits_num, hash_id)
            v_hits_num = string.atoi(hits_num)            
            task_hits_insert(platform, hash_id, hits_time, v_hits_num)
            num_insert += 1
        line_num += 1
        if(line_num % 100 == 0):
            print 'file=%s, line_num=%d' % (upload_file, line_num)
        
    hits_file.close() 
    
    print 'add_hits_num line_num=%d, num_insert=%d, num_update=%d' % (line_num, num_insert, num_update)            
    return (True, num_insert, num_update)


def do_calc_temperature_normal(platform, record):
    now_time = time.localtime(time.time())        
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'begin@ %s' % (begin_time)
    record.begin_time = begin_time
    record.status = 1
    record.save()
    
    str_time = time.strftime("%Y-%m-%d 00:00:00", now_time)   
    
    day_delta = 1
    previous_date = datetime.datetime(string.atoi(str_time[0:4]), string.atoi(str_time[5:7]), string.atoi(str_time[8:10]), 0, 0, 0) - datetime.timedelta(days=day_delta)
    previous_day = '%04d-%02d-%02d 00:00:00' % (previous_date.year, previous_date.month, previous_date.day)
    week_day = previous_date.weekday()    
    previous_week_day = week_day - 1
    if(previous_week_day < 0):
        previous_week_day = 6
    print 'weekday: %d, previous_weekday: %d' % (week_day, previous_week_day)
    
    hash_list_local = get_tasks_local(platform)  
    tasks_hits_list = get_tasks_hits(platform)

    v_config = config.views.get_config(platform)
    ALPHA = v_config.alpha
    MEAN_HITS_NUM = v_config.mean_hits
    
    num_calc = 0 
    print 'hash_list_local count %d' % (hash_list_local.count())
    for task in hash_list_local:
        hits_list = tasks_hits_list.filter(hash=task.hash, time__gte=previous_date)
        task_temperature = 0.0
        if(hits_list.count() > 0):
            for hits in hits_list:
                days_num = day_diff(previous_day, str(hits.time))
                if(days_num < 0):
                    days_num = 0
                task_temperature = task_temperature + hits.hits_num*(ALPHA**days_num)
        else:
            online_time = task.online_time.strftime("%Y-%m-%d %H:%M:%S")
            days_num = day_diff(previous_day, online_time)
            if(days_num < 0):
                days_num = 0
            if(days_num == 0):
                task_temperature = MEAN_HITS_NUM
            else:
                task_temperature = 0.0
        if(week_day == 0):
            task.temperature1 = task.temperature7*ALPHA + task_temperature     
            task.temperature0 = task.temperature1
        elif(week_day == 1):
            task.temperature2 = task.temperature1*ALPHA + task_temperature  
            task.temperature0 = task.temperature2
        elif(week_day == 2):
            task.temperature3 = task.temperature2*ALPHA + task_temperature
            task.temperature0 = task.temperature3
        elif(week_day == 3):
            task.temperature4 = task.temperature3*ALPHA + task_temperature
            task.temperature0 = task.temperature4
        elif(week_day == 4):
            task.temperature5 = task.temperature4*ALPHA + task_temperature
            task.temperature0 = task.temperature5
        elif(week_day == 5):
            task.temperature6 = task.temperature5*ALPHA + task_temperature
            task.temperature0 = task.temperature6 
        elif(week_day == 6):
            task.temperature7 = task.temperature6*ALPHA + task_temperature
            task.temperature0 = task.temperature7  
        print '%s, %e' % (task.hash, task.temperature0)
        task.save()
        num_calc += 1
    
    #hash_list_local.update(temperature0=temperature1)
        
    now_time = time.localtime(time.time())        
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'end@ %s' % (end_time)
    output = 'now: %s, ' % (end_time)
    output += 'num_calc: %d, ' % (num_calc)
    print output
    record.end_time = end_time
    record.status = 2                
    record.memo = output
    record.save()
    return True 


def do_calc_temperature_renew(platform, record):
    now_time = time.localtime(time.time())        
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'begin@ %s' % (begin_time)
    record.begin_time = begin_time
    record.status = 1
    record.save()
    
    str_time = time.strftime("%Y-%m-%d 00:00:00", now_time)   
    
    day_delta = 1
    previous_date = datetime.datetime(string.atoi(str_time[0:4]), string.atoi(str_time[5:7]), string.atoi(str_time[8:10]), 0, 0, 0) - datetime.timedelta(days=day_delta)
    previous_day = '%04d-%02d-%02d 00:00:00' % (previous_date.year, previous_date.month, previous_date.day)
    week_day = previous_date.weekday()    
    previous_week_day = week_day - 1
    if(previous_week_day < 0):
        previous_week_day = 6
    print 'weekday: %d, previous_weekday: %d' % (week_day, previous_week_day)
    
    hash_list_local = get_tasks_local(platform)  
    tasks_hits_list = get_tasks_hits(platform)

    v_config = config.views.get_config(platform)
    ALPHA = v_config.alpha
    MEAN_HITS_NUM = v_config.mean_hits
    
    num_calc = 0 
    print 'hash_list_local count %d' % (hash_list_local.count())
    for task in hash_list_local:
        hits_list = tasks_hits_list.filter(hash=task.hash)
        task_temperature = 0.0
        if(hits_list.count() > 0):
            for hits in hits_list:
                days_num = day_diff(previous_day, str(hits.time))
                if(days_num < 0):
                    days_num = 0
                task_temperature = task_temperature + hits.hits_num*(ALPHA**days_num)
        else:
            online_time = task.online_time.strftime("%Y-%m-%d %H:%M:%S")
            days_num = day_diff(previous_day, online_time)
            if(days_num < 0):
                days_num = 0
            task_temperature = task_temperature + MEAN_HITS_NUM*(ALPHA**days_num)            
        if(week_day == 0):
            task.temperature1 = task.temperature7*ALPHA + task_temperature     
            task.temperature0 = task.temperature1
        elif(week_day == 1):
            task.temperature2 = task.temperature1*ALPHA + task_temperature  
            task.temperature0 = task.temperature2
        elif(week_day == 2):
            task.temperature3 = task.temperature2*ALPHA + task_temperature
            task.temperature0 = task.temperature3
        elif(week_day == 3):
            task.temperature4 = task.temperature3*ALPHA + task_temperature
            task.temperature0 = task.temperature4
        elif(week_day == 4):
            task.temperature5 = task.temperature4*ALPHA + task_temperature
            task.temperature0 = task.temperature5
        elif(week_day == 5):
            task.temperature6 = task.temperature5*ALPHA + task_temperature
            task.temperature0 = task.temperature6 
        elif(week_day == 6):
            task.temperature7 = task.temperature6*ALPHA + task_temperature
            task.temperature0 = task.temperature7  
        print '%s, %e' % (task.hash, task.temperature0)
        task.save()
        num_calc += 1
        
    now_time = time.localtime(time.time())        
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'end@ %s' % (end_time)
    output = 'now: %s, ' % (end_time)
    output += 'num_calc: %d, ' % (num_calc)
    print output
    record.end_time = end_time
    record.status = 2                
    record.memo = output
    record.save()
    return True 


def do_calc_temperature(platform, record):
    result = False
    memo = record.memo
    if(memo == ''):
        result = do_calc_temperature_normal(platform, record)
    else:
        result = do_calc_temperature_renew(platform, record)
    return result


def do_calc_hot_mean_hits_num(platform, record):
    now_time = time.localtime(time.time())        
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'begin@ %s' % (begin_time)
    record.begin_time = begin_time
    record.status = 1
    record.save()
    
    str_date = record.name   
    str_day_begin = '%s-%s-%s 00:00:00' % (str_date[0:4], str_date[4:6], str_date[6:8]) 
    str_day_end   = '%s-%s-%s 23:59:59' % (str_date[0:4], str_date[4:6], str_date[6:8]) 
    print str_day_begin
    print str_day_end        
    
    tasks_hits_list = get_tasks_hits(platform)
    hot_list = tasks_hits_list.filter(time__gte=str_day_begin, time__lte=str_day_end).order_by('-hits_num')
    total_list_num = hot_list.count()
    hot_list_num = total_list_num/5
    
    total_hits_num = 0
    hot_list_index = 0
    for hot_task in hot_list:
        print 'hot_list_index: %d, hits_num:%d, hash: %s' % (hot_list_index, hot_task.hits_num, hot_task.hash)
        total_hits_num = total_hits_num + hot_task.hits_num
        hot_list_index = hot_list_index + 1
        if(hot_list_index>=hot_list_num):
            break
    
    mean_hits_num = 0
    if(hot_list_num > 0):
        mean_hits_num = total_hits_num/hot_list_num
    
    now_time = time.localtime(time.time())        
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'end@ %s' % (end_time)
    output = 'now: %s, ' % (end_time)
    output += 'hot_list_num: %d[%d], ' % (hot_list_num, total_list_num)
    output += 'total_hits_num: %d, mean_hits_num: %d' % (total_hits_num, mean_hits_num)
    print output
    record.end_time = end_time
    record.status = 2                
    record.memo = output
    record.save()
    return True 

            
def do_upload(platform, record):
    now_time = time.localtime(time.time())        
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'begin@ %s' % (begin_time)
    record.begin_time = begin_time
    record.status = 1
    record.save()
    
    hits_date = record.name
    (result, num_insert, num_update) = upload_add_hits_num_sql(platform, hits_date)
    if(result == False):
        now_time = time.localtime(time.time())        
        end_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
        output = 'now: %s, ' % (end_time)
        output += 'error@add_hits_num %s' % (hits_date)            
        print output            
        record.end_time = end_time
        record.status = 2
        record.memo = output
        record.save()
        return False
    
    now_time = time.localtime(time.time())        
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'end@ %s' % (end_time)
    output = 'now: %s, ' % (end_time)
    output += 'num_insert: %d, ' % (num_insert)
    output += 'num_update: %d, ' % (num_update)
    print output
    record.end_time = end_time
    record.status = 2 
    record.memo = output
    record.save()
    return True
        

def do_sync_all_sql(platform, record):    
    now_time = time.localtime(time.time())        
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'begin@%s' % (begin_time)
    record.begin_time = begin_time
    record.status = 1
    record.save()
    
    begin_date = ''
    end_date = ''
        
    hash_list_macross = get_tasks_macross(platform, begin_date, end_date)
    num_macross = hash_list_macross.__len__()
    print 'num_macross=%d' % (num_macross)
    
    (num_local, num_insert, num_update, num_delete) = add_tasks_local(platform, hash_list_macross)    
    
    now_time = time.localtime(time.time())        
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    output = 'now: %s, ' % (end_time)
    output += 'num_macross: %d, ' % (num_macross)
    output += 'num_local: %d, ' % (num_local)
    output += 'num_insert: %d, ' % (num_insert)
    output += 'num_update: %d, ' % (num_update)
    output += 'num_delete: %d' % (num_delete)
    print output
    record.end_time = end_time
    record.status = 2
    record.memo = output
    record.save()
    return True

        
def do_sync_all_django(platform, record):    
    now_time = time.localtime(time.time())        
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'begin@%s' % (begin_time)
    record.begin_time = begin_time
    record.status = 1
    record.save()
    
    begin_date = ''
    end_date = ''
        
    hash_list_macross = get_tasks_macross(platform, begin_date, end_date)
    num_macross = hash_list_macross.__len__()
    print 'num_macross=%d' % (num_macross)
    
    hash_list_local = get_tasks_local(platform)
    num_local = hash_list_local.count()
    print 'num_local=%d' % (num_local)
    
    num_insert = 0
    num_update = 0
    num_delete = 0
    
    #if(len(date_range) <= 0):
    #    for hash_local in hash_list_local:
    #        hash_local.is_valid = 0
    
    for hash_macross in hash_list_macross:   
        hash_id = hash_macross['hash']     
        online_time = hash_macross['online_time']        
        filesize = hash_macross['filesize']
        print '%s, %s' % (hash_macross['hash'], online_time)
        hash_list = hash_list_local.filter(hash=hash_id)
        if(len(hash_list) <= 0):
            print 'insert'
            is_valid = 2
            task_temperature_insert(platform, hash_id, online_time, is_valid, filesize, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)  
            num_insert += 1    
        else:      
            print 'update' 
            hash_list[0].online_time = online_time
            hash_list[0].is_valid = 2
            hash_list[0].filesize = filesize            
            hash_list[0].save()       
            num_update += 1
    
    hash_list_delete = hash_list_local.filter(is_valid=1)
    num_delete = hash_list_delete.count()
    hash_list_delete.delete()  
    
    get_tasks_local(platform).update(is_valid=1)
    
    now_time = time.localtime(time.time())        
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    output = 'now: %s, ' % (end_time)
    output += 'num_macross: %d, ' % (num_macross)
    output += 'num_local: %d, ' % (num_local)
    output += 'num_insert: %d, ' % (num_insert)
    output += 'num_update: %d, ' % (num_update)
    output += 'num_delete: %d' % (num_delete)
    print output
    record.end_time = end_time
    record.status = 2
    record.memo = output
    record.save()
    return True
    
            
def do_sync_partial(platform, record):    
    now_time = time.localtime(time.time())        
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    print 'begin@%s' % (begin_time)
    record.begin_time = begin_time
    record.status = 1
    record.save()
    
    begin_date = ''
    end_date = ''
    
    date_range = record.memo
    if(len(date_range) > 0):
        parts = date_range.split('~')
        begin_date = parts[0] 
        end_date = parts[1]
    
    hash_list_macross = get_tasks_macross(platform, begin_date, end_date)
    num_macross = hash_list_macross.__len__()
    print 'num_macross=%d' % (num_macross)
    
    hash_list_local = get_tasks_local(platform)
    num_local = hash_list_local.count()
    print 'num_local=%d' % (num_local)
    
    num_insert = 0
    num_update = 0
    num_delete = 0
    
    for hash_macross in hash_list_macross:   
        hash_id = hash_macross['hash']
        online_time = hash_macross['online_time']
        filesize = hash_macross['filesize']
        print '%s, %s' % (hash_id, online_time)
        hash_list = hash_list_local.filter(hash=hash_id)
        if(len(hash_list) <= 0):
            print 'insert'
            is_valid = 1
            task_temperature_insert(platform, hash_id, online_time, is_valid, filesize, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)  
            num_insert += 1 
        else:      
            print 'update'
            hash_list[0].online_time = online_time
            hash_list[0].is_valid = 1
            hash_list[0].filesize = filesize            
            hash_list[0].save()       
            num_update += 1
    
    now_time = time.localtime(time.time())        
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    output = 'now: %s, ' % (end_time)
    output += 'num_macross: %d, ' % (num_macross)
    output += 'num_local: %d, ' % (num_local)
    output += 'num_insert: %d, ' % (num_insert)
    output += 'num_update: %d, ' % (num_update)
    output += 'num_delete: %d' % (num_delete)
    print output
    record.end_time = end_time
    record.status = 2
    record.memo = output
    record.save()
    return True    

            
def do_uploads(platform, record_list):
    for record in record_list:            
        do_upload(platform, record)
           

    
def do_sync(platform, record): 
    result = False       
    if(record.memo == '~'):
        result = do_sync_all_sql(platform, record)
    else:
        result = do_sync_partial(platform, record)
    return result
        

def add_record_sync_hash_db(platform, record_list, operation1):
    records = operation.views.get_operation_undone_by_type(platform, operation1['type'])
    if(len(records) == 0):
        record = operation.views.create_operation_record_by_dict(platform, operation1)
        if(record != None):
            record_list.append(record)
        else:
            return False
    else:
        return False
       
            
def sync_hash_db(request, platform):    
    print 'sync_hash_db'  
    print request.REQUEST    
    #{u'start_now': u'on', u'begin_date': u'20130922', u'end_date': u'20130923'}
    #{u'begin_date': u'', u'end_date': u''}
    
    start_now = False
    if 'start_now' in request.REQUEST:
        if(request.REQUEST['start_now'] == 'on'):
            start_now = True
    
    now_time = time.localtime(time.time())
    today = time.strftime("%Y-%m-%d", now_time)
    dispatch_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
       
    operation = {}
    operation['type'] = 'sync_hash_db'
    operation['name'] = today
    operation['user'] = request.user.username
    operation['dispatch_time'] = dispatch_time
    operation['memo'] = '%s~%s' % (request.REQUEST['begin_date'], request.REQUEST['end_date'])
    
    return_datas = {}
    
    record_list = []
    result = add_record_sync_hash_db(platform, record_list, operation)
    if(result == False):
        return_datas['success'] = False
        return_datas['data'] = 'sync_hash_db operation add error, maybe exist.'
        return HttpResponse(json.dumps(return_datas))
    
    if(start_now == True):
        # start process
        p = Process(target=do_sync, args=(platform, record_list[0]))
        p.start()
        
    return_datas['success'] = True
    return_datas['data'] = 'sync_hash_db operation add success'
    
    str_datas = json.dumps(return_datas)
    response = HttpResponse(str_datas, mimetype='application/json;charset=UTF-8')
    response['Content-Length'] = len(str_datas)
    return response


def add_record_upload_hits_num(platform, record_list, operation1):
    records = operation.views.get_operation_by_type_name(platform, operation1['type'], operation1['name'])
    if(len(records) == 0):
        record = operation.views.create_operation_record_by_dict(platform, operation1)
        if(record != None):
            record_list.append(record)
        else:
            return False
    else:
        return False    
    
    

def upload_hits_num(request, platform):
    print 'upload_hits_num'  
    print request.REQUEST
    #{u'start_now': u'on', u'begin_date': u'20130922', u'end_date': u'20130923'}
    #{u'begin_date': u'', u'end_date': u''}
    start_now = False
    if 'start_now' in request.REQUEST:
        if(request.REQUEST['start_now'] == 'on'):
            start_now = True
    
    begin_date = request.REQUEST['begin_date']
    print begin_date    
    
    end_date = request.REQUEST['end_date']
    print end_date
        
    now_time = time.localtime(time.time())
    #today = time.strftime("%Y-%m-%d", now_time)
    dispatch_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    
    begin_day = None
    return_datas = {}
    if(len(begin_date) >= 8):
        begin_day = datetime.date(string.atoi(begin_date[0:4]), string.atoi(begin_date[4:6]), string.atoi(begin_date[6:8])) 
    else:
        return_datas['success'] = False
        return_datas['data'] = 'begin_date %s error' % (begin_date)  
        return HttpResponse(json.dumps(return_datas))
    
    end_day = begin_day      
    if(len(end_date) >= 8):
        end_day = datetime.date(string.atoi(end_date[0:4]), string.atoi(end_date[4:6]), string.atoi(end_date[6:8])) 
       
    record_list = []
    
    operation = {}
    operation['type'] = 'upload_hits_num'
    operation['name'] = ''
    operation['user'] = request.user.username
    operation['dispatch_time'] = dispatch_time
    operation['memo'] = ''
    
    day_num = 0
    operation['name'] = '%04d%02d%02d' % (begin_day.year, begin_day.month, begin_day.day)
    result = add_record_upload_hits_num(platform, record_list, operation)
    if(result == False):
        return_datas['success'] = False
        return_datas['data'] = 'date %s error' % (operation['name'])  
        return HttpResponse(json.dumps(return_datas))
    day_num += 1    
    while(True):
        d1 = begin_day
        delta = datetime.timedelta(days=day_num)
        d2 = d1 + delta
        if(d2 >= end_day):
            break
        else:
            operation['name'] = '%04d%02d%02d' % (d2.year, d2.month, d2.day)
            result = add_record_upload_hits_num(platform, record_list, operation)
            if(result == False):
                return_datas['success'] = False
                return_datas['data'] = 'date %s error' % (operation['name'])  
                return HttpResponse(json.dumps(return_datas))
        day_num += 1 
            
    if(start_now == True):
        # start thread.
        #t = Thread_UPLOAD(platform, record_list)            
        #t.start()
        # start process
        p = Process(target=do_uploads, args=(platform, record_list))
        p.start()
        
    return_datas['success'] = True
    return_datas['data'] = 'day_num %d' % (day_num)  
    
    str_datas = json.dumps(return_datas)
    response = HttpResponse(str_datas, mimetype='application/json;charset=UTF-8')
    response['Content-Length'] = len(str_datas)
    return response
        
        
def add_record_calc_temperature(platform, record_list, operation1):
    records = operation.views.get_operation_undone_by_type(platform, operation1['type'])
    if(len(records) == 0):
        record = operation.views.create_operation_record_by_dict(platform, operation1)
        if(record != None):
            record_list.append(record)
        else:
            return False
    else:
        return False
    
def add_record_calc_hot_mean_hits_num(platform, record_list, operation1):
    records = operation.views.get_operation_undone_by_type(platform, operation1['type'])
    if(len(records) == 0):
        record = operation.views.create_operation_record_by_dict(platform, operation1)
        if(record != None):
            record_list.append(record)
        else:
            return False
    else:
        return False
    
    
def calc_temperature(request, platform):
    print 'calc_temperature'
    print request.REQUEST
    #{u'start_now': u'on', u'begin_date': u'20130922', u'end_date': u'20130923'}
    #{u'begin_date': u'', u'end_date': u''}
    calc_renew = False
    start_now = False
    if 'calc_renew' in request.REQUEST:
        if(request.REQUEST['calc_renew'] == 'on'):
            calc_renew = True
    if 'start_now' in request.REQUEST:
        if(request.REQUEST['start_now'] == 'on'):
            start_now = True
    
    now_time = time.localtime(time.time())
    today = time.strftime("%Y-%m-%d", now_time)
    dispatch_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    
    memo = ''
    if(calc_renew == True):
        memo = 'renew'
        
    operation = {}
    operation['type'] = 'calc_temperature'
    operation['name'] = today
    operation['user'] = request.user.username
    operation['dispatch_time'] = dispatch_time    
    operation['memo'] = memo
    
    return_datas = {}
    
    record_list = []
    result = add_record_calc_temperature(platform, record_list, operation)
    if(result == False):
        return_datas['success'] = False
        return_datas['data'] = 'calc_temperature operation add error, maybe exist.'
        return HttpResponse(json.dumps(return_datas))
    
    if(start_now == True):
        # start thread.
        #t = Thread_COLD(platform, record_list[0])            
        #t.start()
        # start process
        p = Process(target=do_calc_temperature, args=(platform, record_list[0]))
        p.start()
        
    return_datas['success'] = True
    return_datas['data'] = 'calc_temperature operation add success'
    
    str_datas = json.dumps(return_datas)
    response = HttpResponse(str_datas, mimetype='application/json;charset=UTF-8')
    response['Content-Length'] = len(str_datas)
    return response


def get_parameters(request, platform):
    print 'get_parameters'
    print request.REQUEST
    
    v_config = config.views.get_config(platform)
    
    return_datas = {}
    return_datas['success'] = True
    return_datas['alpha'] = '%f' % (v_config.alpha)
    return_datas['mean_hits'] = '%d' % (v_config.mean_hits)
    
    str_datas = json.dumps(return_datas)
    response = HttpResponse(str_datas, mimetype='application/json;charset=UTF-8')
    response['Content-Length'] = len(str_datas)
    return response


def set_parameters(request, platform):
    print 'set_parameters'
    print request.REQUEST
    
    v_config = config.views.get_config(platform)
    v_config.alpha = string.atof(request.REQUEST['alpha'])
    v_config.mean_hits = string.atoi(request.REQUEST['mean_hits'])
    v_config.save()
    
    return_datas = {}
    return_datas['success'] = True
    return_datas['data'] = 'set_parameters success'
    
    str_datas = json.dumps(return_datas)
    response = HttpResponse(str_datas, mimetype='application/json;charset=UTF-8')
    response['Content-Length'] = len(str_datas)
    return response
    

def calc_hot_mean_hits_num(request, platform):
    print 'calc_hot_mean_hits_num'  
    print request.REQUEST
    #{u'start_now': u'on', u'date': u'20130922'}
    start_now = False
    if 'start_now' in request.REQUEST:
        if(request.REQUEST['start_now'] == 'on'):
            start_now = True
    
    begin_date = request.REQUEST['date']
    print begin_date    
        
    now_time = time.localtime(time.time())
    #today = time.strftime("%Y-%m-%d", now_time)
    dispatch_time = time.strftime("%Y-%m-%d %H:%M:%S", now_time)
    
    begin_day = None
    return_datas = {}
    if(len(begin_date) >= 8):
        begin_day = datetime.date(string.atoi(begin_date[0:4]), string.atoi(begin_date[4:6]), string.atoi(begin_date[6:8])) 
    else:
        return_datas['success'] = False
        return_datas['data'] = 'date %s error' % (begin_date)  
        return HttpResponse(json.dumps(return_datas))
        
    record_list = []
    
    operation = {}
    operation['type'] = 'calc_hot_mean_hits_num'
    operation['name'] = ''
    operation['user'] = request.user.username
    operation['dispatch_time'] = dispatch_time
    operation['memo'] = ''
        
    operation['name'] = '%04d%02d%02d' % (begin_day.year, begin_day.month, begin_day.day)
    result = add_record_calc_hot_mean_hits_num(platform, record_list, operation)
    if(result == False):
        return_datas['success'] = False
        return_datas['data'] = 'date %s error' % (operation['name'])  
        return HttpResponse(json.dumps(return_datas))    
            
    if(start_now == True):
        # start thread.
        #t = Thread_UPLOAD(platform, record_list)            
        #t.start()
        # start process
        p = Process(target=do_calc_hot_mean_hits_num, args=(platform, record_list[0]))
        p.start()
        
    return_datas['success'] = True
    return_datas['data'] = 'day_num %d' % (1)  
    
    str_datas = json.dumps(return_datas)
    response = HttpResponse(str_datas, mimetype='application/json;charset=UTF-8')
    response['Content-Length'] = len(str_datas)
    return response
        
   
def test_insert(request, platform):
    print 'test_insert'
    #2014-01-07 16:00:00+00:00
    print datetime.datetime.now()
    record = models.mobile_task_hits(hash='1234', time='2014-01-08 00:00:00', hits_num=789)
    record.save()
    response = HttpResponse('test_insert done!')
    return response

def test_select(request, platform):
    print 'test_select'
    records = models.mobile_task_hits.objects.all()
    for record in records:
        print record
    response = HttpResponse('test_select done!')
    return response