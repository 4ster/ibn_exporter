#!/usr/bin/env python3
# Экспортер для передачи метрик из ftp в прометей через pushgateway
# import requests
import logging
import shutil
import urllib.request as request
from contextlib import closing
from os import environ
from os import makedirs
from os.path import join, exists

from dotenv import load_dotenv
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


# скачивает файл и сохраняет во временную директорию
def get_files(host, login, passw, file_name, temp_dir):
    if not exists(temp_dir):
        makedirs(temp_dir)
    new_file = join(temp_dir, file_name)

    # проверить, что файл существует
    url = 'ftp://{}:{}@{}/{}'.format(login, passw, host, file_name)
    with closing(request.urlopen(url)) as r:
        with open(new_file, 'wb') as f:
            shutil.copyfileobj(r, f)

    # убедиться, что файл скачан, асинхр
    return new_file


# проверяет не изменилась ли хеш-сумма
def check_hash(file_name, cache_file):
    pass


# разбирает файл с таймаутами сервисов и возвращается список записей
def get_service_times(filename):
    lst = []
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if line != '':
                service, time_out = line.split(' ')
                lst.append({'service': service, 'timeout': time_out})

    return lst


# разбирает файл с ibn-аккаунтами и числом процессов и возвращает список записей
def get_process_numbers(filename):
    lst = []
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if line != '':
                if ' ' in line:
                    account, proc_count = line.split(' ')
                    lst.append({'account': account, 'proc_count': proc_count})
                else:
                    logger.warning("Can't split line \"{}\" into key-value".format(line))

    return lst


# обновляет метрики
def update_metrics(exporter_name, data):
    global registry
    for key, value in data.items():
        # Gauge - тип параметра - число, которое может увеличиваться или уменьшаться
        # Оборачиваем в try-except, чтобы не добавлять одни и те же метрики в колекцию, если они уже там есть
        try:
            g = Gauge(
                "{}_{}".format(
                    exporter_name,
                    key).replace('-', '_').replace('.', '_'),
                # коллекция параметров
                registry=registry
            )
            # Если значение параметра - None, то устанавливаем значение метрики в NaN
            if value == None:
                g.set("NaN")
            else:
                g.set(value)
            # g.set_to_current_time()

        except ValueError as e:
            pass

        push_to_gateway('127.0.0.1:9091', job=exporter_name, registry=registry)


if __name__ == '__main__':
    # инициализируем лог
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
    service_exporter_name = environ['service_exporter_name']
    ibn_exporter_name = environ['ibn_exporter_name']

    service_file = environ['service_timeout_file']
    ibn_file = environ['ibn_process_file']

    # скачиваем файлы с ftp во временную папку
    # TODO: переделать - проверять хеши, если данные не изменились - ничего дальше не делать
    st = get_files(host, login, password, service_file, "./temp")
    pc = get_files(host, login, password, ibn_file, "./temp")

    # получаем информацию из скачанных файлов
    service_timeouts = get_service_times(st)
    processes_count = get_process_numbers(pc)

    # определяем коллекцию метрик promeheus
    registry = CollectorRegistry()

    # обновляем метрики
    update_metrics(service_exporter_name, service_timeouts)
    update_metrics(ibn_exporter_name, processes_count)

    # logger.info(service_timeouts)
    # logger.info(processes_count)
