#!/usr/bin/env python3
# Экспортер для передачи метрик из ftp в grafana
# import requests
import csv
import logging
import shutil
import urllib.request as request
from contextlib import closing
from os import environ, makedirs
from os.path import join, exists, split
import sys

from dotenv import load_dotenv
from prometheus_client import CollectorRegistry, Gauge, Info, push_to_gateway
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.exposition import basic_auth_handler

def update_metrics(exporter_name, labels, data):
    global registry
    g = Gauge(
        "ibn_processes_count",
        "IBN processes count by user",
        ['account'],
        registry=registry
    )
    for record in data:
        # print(record)
        lbl = []
        key = record['account']
        value = int(record['proc_count'])
        description = "{}_{}".format(exporter_name, key).replace('-', 'hyphen').replace('.', 'dot')
        lbl.extend(labels)
        lbl.append(key)
        # Gauge - тип параметра - число, которое может увеличиваться или уменьшаться
        # Оборачиваем в try-except, чтобы не добавлять одни и те же метрики в колекцию, если они уже там есть

        try:
            g.labels(account=key).set(value)
            # exit()
        # FIXME: не игнорить!
        except ValueError as e:

            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.warning('{}. {} {} {}'.format(e, exc_type, fname, exc_tb.tb_lineno))
    # Если значение параметра - None, то устанавливаем значение метрики в NaN
    push_to_gateway('127.0.0.1:9091', job=exporter_name, registry=registry, handler=my_auth_handler)


# скачивает файл и сохраняет во временную директорию
def get_file(host, login, passw, file_name, temp_dir):
    if not exists(temp_dir):
        makedirs(temp_dir)
    new_file = join(temp_dir, file_name)

    # TODO: проверить, что файл существует
    url = 'ftp://{}:{}@{}/{}'.format(login, passw, host, file_name)
    with closing(request.urlopen(url)) as r:
        with open(new_file, 'wb') as f:
            shutil.copyfileobj(r, f)

    # TODO: убедиться, что файл скачан, асинхр
    return new_file


# проверяет не изменилась ли хеш-сумма
def check_hash(file_name, cache_file):
    pass


# преобразовывает скачанный файл в csv
def convert_to_csv(source_file, dest_file):
    # TODO: error handling
    pc = []
    with open(join(dest_file), mode='w') as out_file:
        out_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        out_writer.writerow(['Аккаунт', 'Количество процессов'])
        with open(source_file) as f:
            for line in f:
                line = line.strip()
                if line != '':
                    if ' ' in line:
                        account, proc_count = line.split(' ')
                        out_writer.writerow([account, proc_count])
                        pc.append({"account": account, "proc_count": proc_count})
                    else:
                        logger.info("Can't split line \"{}\" into key-value".format(line))

    return pc

def my_auth_handler(url, method, timeout, headers, data):
    global prom_username
    global prom_passw

    if prom_username is None:
        return basic_auth_handler(url, method, timeout, headers, data, prom_username, prom_passw)
    else:
        return basic_auth_handler(url, method, timeout, headers, data)

if __name__ == '__main__':
    # инициализируем лог
    # FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('ibn_exporter')

    # забираем настройки из файла .env
    env_path = join('.', '.env')
    load_dotenv(dotenv_path=env_path, verbose=True)

    login = environ['login']
    password = environ['password']
    max_try = int(environ['max_try'])
    timeout = float(environ['timeout'])
    host = environ['host']
    exporter_name = environ['exporter_name']
    ibn_labels = environ['ibn_labels'].split(',')
    ibn_file = environ['ibn_process_file']
    write_file_path = environ['write_file_path']
    temp_dir = environ["temp_dir"]
    try:
        prom_username = environ["prom_username"]
        prom_passw = environ["prom_passw"]
    except KeyError as e:
        prom_username = None
        prom_passw = None
    # скачиваем файл с ftp во временную папку
    # TODO: переделать - проверять хеши, если данные не изменились - ничего дальше не делать
    source_file = get_file(host, login, password, ibn_file, temp_dir)

    # определяем коллекцию метрик promeheus
    registry = CollectorRegistry()

    # преобразовываем файл в формат csv
    processes_count = convert_to_csv(source_file, write_file_path)

    # обновляем метрики
    update_metrics(exporter_name, ibn_labels, processes_count)
