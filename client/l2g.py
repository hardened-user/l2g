#!/usr/bin/env python2
# -*- coding: utf-8 -*-
# 06.02.2018
#--------------------------------------------------------------------------------------------------
import os
import sys
import re
import datetime
from time import sleep
#
import argparse
import pickle
import subprocess
import tempfile

#--------------------------------------------------------------------------------------------------
# Настройки по-умалчанию
#--------------------------------------------------------------------------------------------------
CNF_GRAPH_WIDHT  = 18   # Размер графика по горизонтали x100 px
CNF_GRAPH_HEIGHT = 9    # Размер графика по вертикали   x100 px
CNF_GRAPH_X_GRID = 40   # Число вспомагательных линий по оси X
CNF_GRAPH_Y_MAX  = 0    # Максимальное значение по оси Y

CNF_TEMP_DIR     = tempfile.gettempdir()
CNF_SAVE_DIR     = "/tmp"                       # Для автоматического сохранения PNG
CNF_UPLOAD_URL   = "http://l2g.keyforce.ru/"    # !!! Обязательно закрывающий слеш

# Форматы
CNF_DT_VARIANTS  = {
    'a': {
            'example': "[28/Apr/2016:16:42:20 +0300], [28/Apr/2016:16:42:20]",
            'regexp': re.compile("(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})"),
            'dt_format': "%d/%b/%Y:%H:%M:%S",
            },
    'b': {
            'example': "[Mon Apr 04 13:10:00.050620 2016]",
            'regexp': re.compile("(\w{3} \w{3} \d{2} \d{2}:\d{2}:\d{2}\.\d{6} \d{4})"),
            'dt_format': "%a %b %d %H:%M:%S.%f %Y",
            },
    'c': {
            'example': "/home/prod_aflcab_s/afl_cabinet/log/errors/2016-04-02/060610-SiebelXMLResponseException",
            'regexp': re.compile("(\d{4}-\d{2}-\d{2}/\d{6})"),
            'dt_format': "%Y-%m-%d/%H%M%S",
            },
    }
#--------------------------------------------------------------------------------------------------


def main():
    #______________________________________________________
    # Входящие аргументы
    try:
        parser = argparse.ArgumentParser(description='log2graph client',
                                         usage="cat data.log | {} <DATE&TIME VARIANT>".format(os.path.basename(sys.argv[0])))
        parser.add_argument('dt_format', action='store', type=str,
                            metavar='<DATE&TIME VARIANT>', help="input data format variant")
        parser.add_argument("-t", action='store', type=str, default="Untitled", dest="title",
                            metavar='<title>', help="graph title")
        parser.add_argument("-n", action='store_true', default=False, dest="n",
                            help="don't upload pickle file")
        parser.add_argument("-d", action='store_true', default=False, dest="d",
                            help="download png file")
        args = parser.parse_args()
    except SystemExit:
        print >> sys.stderr, "="*80
        print >> sys.stderr, "={0} {1} {0}=".format(" "*29, "Available variants")
        print >> sys.stderr, "="*80
        for x in sorted(CNF_DT_VARIANTS):
            print >> sys.stderr, "'{}': {}".format(x, CNF_DT_VARIANTS[x]['example'])
        return False
    #______________________________________________________
    # Проверка dt_format
    if args.dt_format not in CNF_DT_VARIANTS:
        print >> sys.stderr, "[EE] Variant not found: '{}'".format(args.dt_format)
        return False
    #----------------------------------------------------------------------------------------------
    # Парсинг данный STDIN
    #----------------------------------------------------------------------------------------------
    _POINTS = {} # Содержит сырые данные по точкам
    # _POINTS = {
    #   <'datetime'>: <'int'>,
    #   ...
    #   }
    if sys.stdin.isatty():
        print >> sys.stderr, "[EE] Expected input on stdin"
        return False
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        log_row = LogRow(line, CNF_DT_VARIANTS[args.dt_format])
        if not log_row.is_valid:
            continue
        #__________________________________________________
        if log_row.dt in _POINTS:
            _POINTS[log_row.dt] += 1
        else:
            _POINTS[log_row.dt] = 1
    #______________________________________________________
    # Обработка данных
    if _POINTS:
        GRAPH_DATA = mk_graph_data(args, _POINTS)
        if not GRAPH_DATA:
            return False
    else:
        print >> sys.stderr, "[EE] Points not found"
        return True
    #______________________________________________________
    # Сохранение pickle файла
    picke_file_path = os.path.join(CNF_TEMP_DIR, "l2g_{}.pickle".format(datetime.datetime.now().strftime("%Y%m%d%H%M%S")))
    if not save_pickle_data(picke_file_path, GRAPH_DATA):
        return False
    #______________________________________________________
    # Отправка pickle файла на сервер
    if not args.n:
        rc, rd = upload_pickle_file(picke_file_path, CNF_UPLOAD_URL)
        os.remove(picke_file_path)
        if rc != 0:
            print >> sys.stderr, rd
            print >> sys.stderr, "[EE] Failed upload pickle file: '{}'".format(picke_file_path)
            return False
        location = None
        for x in rd.split('\n'):
            if x.find("Location:") > -1:
                location = str(x).split(':', 1)[-1].strip()
                break
        if not location:
            print >> sys.stderr, rd
            print >> sys.stderr, "[EE] Location redirect not found"
            return False
        print "[OK] Uploaded pickle file '{0}'".format(picke_file_path)
        print "[..] Download URL: {}".format(location)
        #__________________________________________________
        if args.d:
            count = 0
            sleep(1)
            while (count < 3):
                sleep(1)
                rc, rd = get_png_status(location.replace("/view", "/status"))
                if rd.strip() == "1":
                    rc, rd = download_png_file(CNF_SAVE_DIR, location.replace("/view", "/download"))
                    if rc != 0:
                        print >> sys.stderr, rd
                        print >> sys.stderr, "[EE] Failed download .png file"
                        return False
                    print "[OK] Downloaded .png file"
                    break
                elif rd.strip() == "0":
                    print "[..] Wait ..."
                    count += 1
                else:
                    print >> sys.stderr, "[EE] Failed create .png file"
                    break
    else:
        print "[..] Upload the file manually to: {}".format(CNF_UPLOAD_URL)
    #______________________________________________________
    return True


#==================================================================================================
# Functions
#==================================================================================================
def mk_graph_data(args, points):
    #______________________________________________________
    GRAPH_DATA = {}
    GRAPH_DATA['points'] = {}                   # Точки для построения графика
    GRAPH_DATA['title']  = args.title           # Имя графика
    GRAPH_DATA['freq']   = None                 # Частота дискретизации в сек.
    GRAPH_DATA['count']  = None                 # Общее число обработыннх записей
    GRAPH_DATA['xfmt']   = "%d.%m.%y  %H:%M"    # Формат даты для подписи по оси X
    GRAPH_DATA['wight']  = CNF_GRAPH_WIDHT
    GRAPH_DATA['height'] = CNF_GRAPH_HEIGHT
    GRAPH_DATA['xgrid']  = CNF_GRAPH_X_GRID
    GRAPH_DATA['ymax']   = CNF_GRAPH_Y_MAX
    #______________________________________________________
    min_time = min(points)
    max_time = max(points)
    interval = int((max_time - min_time).total_seconds())
    print "[..] X axis Min time: {}".format(min_time)
    print "[..] X axis Max time: {}".format(max_time)
    print "[..] X axis Interval: {}".format(datetime.timedelta(seconds=interval))
    # Определим частоту дискретизации
    avg_seconds = interval/CNF_GRAPH_X_GRID
    if avg_seconds >= 3600:
        GRAPH_DATA['freq'] = 3600
    elif avg_seconds >= 600:
        GRAPH_DATA['freq'] = 600
    elif avg_seconds >= 60:
        GRAPH_DATA['freq'] = 60
    elif avg_seconds >= 10:
        GRAPH_DATA['freq'] = 10
    elif avg_seconds >= 1:
        GRAPH_DATA['freq'] = 1
    else:
        raise Exception("Internal Error")
        return False
    print "[..] Frequency: {}s ({})".format(GRAPH_DATA['freq'], datetime.timedelta(seconds=GRAPH_DATA['freq']))
    count = 0
    # Дискретизация значений
    for dt in points.keys():
        if GRAPH_DATA['freq'] == 3600:
            smapling_dt = dt.replace(minute=0, second=0, microsecond=0) # 1h
        elif GRAPH_DATA['freq'] == 600:
            smapling_dt = dt.replace(minute=(dt.minute/10)*10, second=0, microsecond=0) # 10m
        elif GRAPH_DATA['freq'] == 60:
            smapling_dt = dt.replace(second=0, microsecond=0) # 1m
        elif GRAPH_DATA['freq'] == 10:
            smapling_dt = dt.replace(microsecond=(dt.minute/10)*10) # 10s
        elif GRAPH_DATA['freq'] == 1:
            smapling_dt = dt.replace(microsecond=0) # 1s
        else:
            print >> sys.stderr, "[EE] Internal Error 2"
            return False
        # Заполним таблицу
        if smapling_dt in GRAPH_DATA['points']:
            GRAPH_DATA['points'][smapling_dt] += points[dt]
        else:
            GRAPH_DATA['points'][smapling_dt]  = points[dt]
        count += points[dt]
        #__________________________________________________
        # Освобождаем память
        points.pop(dt)
    #______________________________________________________
    # Подсчитаем count
    GRAPH_DATA['count'] = count
    print "[..] Read log lines: {}".format(GRAPH_DATA['count'])
    #______________________________________________________
    # Заполним промежутки времени нулями
    tmp = min(GRAPH_DATA['points'])
    while tmp < max(GRAPH_DATA['points']):
        tmp = tmp + datetime.timedelta(seconds=GRAPH_DATA['freq'])
        if tmp not in GRAPH_DATA['points']:
            GRAPH_DATA['points'][tmp] = 0
    #______________________________________________________
    return GRAPH_DATA


def save_pickle_data(path, data):
    try:
        f = open(path, 'w')
        pickle.dump(data, f)
        f.close()
    except IOError, e:
        print >> sys.stderr, "[EE] {0}".format(e)
        print >> sys.stderr, "[EE] Failed save pickle file"
        return False
    except Exception, e:
        print >> sys.stderr, "[EE] Exception Err: {0}".format(e)
        print >> sys.stderr, "[EE] Exception Inf: {0}".format(sys.exc_info())
        print >> sys.stderr, "[EE] Failed save pickle file"
        return False
    print "[OK] Saved pickle file '{0}'".format(path)
    #______________________________________________________
    return True


def upload_pickle_file(path, url):
    cmd = '''export LC_ALL=""; export LANG="en_US.UTF-8"; curl -is -f -X POST "{}" -F file=@"{}"'''.format(url, path)
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, executable='/bin/bash')
    rd = child.communicate()[0]
    rc = child.returncode
    #______________________________________________________
    return (rc, rd)


def get_png_status(url):
    cmd = '''export LC_ALL=""; export LANG="en_US.UTF-8"; curl -s "{}"'''.format(url)
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    rd = child.communicate()[0]
    rc = child.returncode
    #______________________________________________________
    return (rc, rd)


def download_png_file(path, url):
    cmd = '''export LC_ALL=""; export LANG="en_US.UTF-8"; cd {} && wget -T60 -nv --content-disposition "{}"'''.format(path, url)
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    rd = child.communicate()[0]
    rc = child.returncode
    #______________________________________________________
    return (rc, rd)


#==================================================================================================
# Classes
#==================================================================================================
class LogRow(object):
    def __init__(self, row, dt_format):
        self.raw = row
        self.dt  = None
        self.is_valid = False
        #__________________________________________________
        _tmp = dt_format['regexp'].search(row)
        if _tmp:
            try:
                self.dt = datetime.datetime.strptime(_tmp.group(1), dt_format['dt_format'])
            except ValueError, e:
                print >> sys.stderr, "[EE] Exception Err: {0}".format(e)
                return
            except Exception, e:
                print >> sys.stderr, "[EE] Exception Err: {0}".format(e)
                print >> sys.stderr, "[EE] Exception Inf: {0}".format(sys.exc_info())
                return
        else:
            #print row #### TEST
            return
        #__________________________________________________
        self.is_valid = True

    def __str__(self):
        return self.raw

    def __repr__(self):
        return "<class LogRow '{}'>".format(self.raw)


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    sys.exit(not main()) # BASH compatible
