#!/usr/bin/env python3
# Экспортер для передачи метрик из ftp в grafana
# import requests
import csv
import logging
import shutil
import urllib.request as request
from contextlib import closing
from os import environ
from os import makedirs
from os.path import join, exists

from dotenv import load_dotenv


# скачивает файл и сохраняет во временную директорию
def get_file(host, login, passw, file_name, temp_dir):
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


# преобразовывает скачанный файл в csv
def convert_to_csv(source_file, dest_file):
    # TODO: error handling
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
                    else:
                        logger.warning("Can't split line \"{}\" into key-value".format(line))


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
    exporter_name = environ['exporter_name']
    ibn_labels = environ['ibn_labels'].split(',')
    ibn_file = environ['ibn_process_file']
    write_file_path = environ['write_file_path']
    temp_dir = environ["temp_dir"]
    # скачиваем файл с ftp во временную папку
    # TODO: переделать - проверять хеши, если данные не изменились - ничего дальше не делать
    source_file = get_file(host, login, password, ibn_file, temp_dir)

    # преобразовываем файл в формат csv
    convert_to_csv(source_file, write_file_path)
