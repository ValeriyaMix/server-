import re
import socket
import uuid
import datetime
import argparse
import sys

add_regexp = re.compile(r'ADD (?P<queue>\w*) (?P<length>\d*) (?P<data>(\w|\W)*)')
get_regexp = re.compile(r'GET (?P<queue>\w*)')
ack_regexp = re.compile(r'ACK (?P<queue>\w*) (?P<id>[\w\-]{,128})')
in_regexp = re.compile(r'IN (?P<queue>\w*) (?P<id>[\w\-]{,128})')

actions = {
    'ADD': add_regexp,
    'GET': get_regexp,
    'ACK': ack_regexp,
    'IN': in_regexp
}


class Task:
    def __init__(self, complete_time):
        self.complete_time = complete_time
        self.queue = {}

    def perform_action(self, action, formatted_data):
        if action == 'ADD':
            return self.add(**formatted_data)
        elif action == 'ACK':
            return self.ack(**formatted_data)
        elif action == 'GET':
            return self.get(**formatted_data)
        elif action == 'IN':
            return self.check_in(**formatted_data)
        raise ValueError

    def add(self, queue, length, data):
        recieve_time = datetime.datetime.now().timestamp()
        task_file = open('task.txt', 'w')
        # TODO: отвалидировать данные no more than 106 and tha same like length
        if queue not in self.queue:
            self.queue[queue] = {}
            self.queue[queue]["tasks"] = []
            self.queue[queue]["inprogress_tasks"] = []
            self.queue[queue]["counter"] = str(uuid.uuid4())

        id = self.queue[queue]["counter"]
        task = {"id": id, "length": length, "data": data, "time": recieve_time}

        # TODO: сохранять задачи в файл
        # TODO: пройти по списку выполняющихся и ожидающих выполнения и сохранить их в файл
        task_file.write(str(task))
        task_file.close()

        self.queue[queue]["tasks"].append(task)
        return id

    def check_overtime(self, queue):
        for task in self.queue[queue]["inprogress_tasks"]:
            if datetime.datetime.now().timestamp() - task["time"] > self.complete_time:
                self.queue[queue]["tasks"].append(task)
        self.queue[queue]["task"].sort(key=lambda x: x["time"])

    def get(self, queue):
        task_file = open('task.txt', 'w')
        # TODO: проверка просроченности задания done
        self.check_overtime(queue)
        item = self.queue[queue]["tasks"].pop()
        # TODO: item нужно положить в список выполняющихся и удалить из списка ожидающих done
        self.queue[queue]["inprogress_tasks"].append(item)
        task_file.write(str(item))
        task_file.close()
        # TODO: сохранять задачи в файл
        return item

    def ack(self, queue, id):
        remove_indices = []
        for index, task in enumerate(self.queue[queue]["inprogress_tasks"]):
            if id in task["id"]:
                remove_indices.append(index)
        for index in remove_indices:
            self.queue[queue]["inprogress_tasks"].pop(index)
        if not remove_indices:
            return "NO"
        else:
            return "YES"




    def check_in(self, queue, id):
        for task in self.queue[queue]["tasks"]:
            current_id = task["id"]
            if current_id == id:
                return "YES"
        return "NO"

    def save(self):
        pass
    # TODO: сохранять задачи в файл написать отдельную функцию в формате csv


def parse_args(args):
    parser = argparse.ArgumentParser(description='This is parser for console')
    parser.add_argument('--port', '-p', default=8080, type=int, dest='port')
    parser.add_argument('--complete_time', '-ctime', type=int, default=300, dest='complete_time')
    return parser.parse_args(args)


def get_formatted_data(data):
    action = data.split()[0]
    action_regexp = actions[action]
    print(action_regexp)

    match_result = action_regexp.search(data)
    if not match_result:
        raise ValueError

    formatted_data = match_result.groupdict()
    return action, formatted_data


def run(parse_data):
    sock = socket.socket()  # Создаем сокет; по умолчанию он работает по TCP и использует IPv4
    # TODO: Принимать значения порта из аргументов командной строки
    complete_time = parse_data.complete_time
    port = parse_data.port
    # Мы создаем объект класса Task, чтобы потом им пользоваться в функции run
    task_manager = Task(complete_time)
    sock.bind(('127.0.0.1', port))  # Слушаем на 5555 порту на адресе 127.0.0.1
    print('Listen on %s:%s' % ('127.0.0.1', port))
    sock.listen(1)  # Выступаем в качестве сервера, который слушает только одно соединение одномоментно

    while True:
        conn, addr = sock.accept()
        try:
            data = conn.recv(1000000)  # Получаем информацию по 1024 байт за раз
            if not data:
                break
            data = data.replace(b'\r\n', b'').decode()
            action, formatted_data = get_formatted_data(data)
            result = task_manager.perform_action(action, formatted_data)
            print(result)
            conn.send(result.encode())  # Отправляем ответ клиенту в верхнем регистре

        finally:
            conn.close()  # Закрываем соединение


if __name__ == '__main__':
    parse_data = parse_args(sys.argv[1:])
    run(parse_data)
    # tasks = Tasks()
    # tasks.add("bla1", "data: data for bla1")
    # tasks.add("bla1", "data2")
    # tasks.add("bla1", "dat3")
    # tasks.add("bla2", "dat3")
    # tasks.add("bla2", "dat3")
    # tasks.add("bla2", "dat3")
    # print(tasks.queue)

# 0) Парсить аргументы командной строки (ArgParse)
# 1) Принимать сообщения от клиентов в сыром виде (запуск сервера)
# 2) Приведение сообщения к внутреннему формату (очистка данных)
# 3) Валидация сообщения
# 4) Выполнение логики программы
# 5) Формирование ответного сообщения
# 6) Отправка сообщения клиенту
