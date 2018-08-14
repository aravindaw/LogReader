import os
import shutil
from datetime import datetime

from sql import sql

log_file_path = "../logs/"
read_log_file_path = "../read_logs/"
data_array = []
commit_data_array = []


class FDNS:

    @staticmethod
    def run_script(path):
        tmp = os.popen("ps -Af").read()
        process_count = tmp.count("FDNS.py")
        if process_count == 0:
            os.system("echo 'Already running.. " + datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S') + "' >> ../system_logs/system.log")
        else:
            FDNS.find_files(path)

    @staticmethod
    def find_files(path):
        os.system("echo 'Executing the script " + datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S') + "' >> ../system_logs/system.log")
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            if os.path.isdir(file_path):
                FDNS.find_files(file_path)
            else:
                FDNS.read_log_file(file_name, file_path)

    @staticmethod
    def read_log_file(file_name, file_path):
        sql.connect_database()
        print("Reading " + file_name)
        os.system("echo 'Reading " + file_name + "' >> ../system_logs/system.log")
        FDNS.check_valid_logs(file_path)
        shutil.move(log_file_path + file_name, read_log_file_path + file_name)
        sql.close_db_connection()

    @staticmethod
    def check_valid_logs(path):
        with open(path) as file:
            global count
            count = 0
            for line in file:
                count = count + 1
                FDNS.write_file(line)
        file.close()
        sql.write_logs_into_database(commit_data_array, count)
        sql.inserting_data()

    @staticmethod
    def write_file(log):
        global date, client_ip, domain, data_array, commit_data_array
        part = log.split(" ")
        if part[2] == "client" and len(part) == 12:
            client_ip = part[3].split("#")
            domain = part[7].replace("www.", "")
            date = datetime.strptime(part[0] + " " + part[1], "%d-%b-%Y  %H:%M:%S.%f")
            dns = part[11]
            dns = dns.replace("(", "")
            dns = dns.replace(")", "")
            data_array.append((date, client_ip[0], domain, dns))
            commit_data_array = list(data_array)
            if len(commit_data_array) == 10000:
                del data_array[:]
                sql.write_logs_into_database(commit_data_array, count)
                sql.inserting_data()
                del commit_data_array[:]


if __name__ == '__main__':
    FDNS.run_script(log_file_path)
