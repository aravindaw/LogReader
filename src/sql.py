import pymysql.cursors


class sql:
    @staticmethod
    def connect_database():
        global cursor
        global connection
        connection = pymysql.connect(host='rnd.vqbn.com',
                                     user='root',
                                     password='19900206',
                                     db='fdns_logs',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        cursor = connection.cursor()

        cursor.execute("USE fdns_logs")
        cursor.execute("TRUNCATE TABLE tmp_logs")
        # for row in cursor.fetchall():
        #     print(row)

    @staticmethod
    def write_logs_into_database(data, count):
        print("Log Count:", count)
        # cursor.execute("INSERT INTO tmp_logs VALUES ('" + date + "', '" + ip + "', '" + domain + "')")
        query = "INSERT INTO tmp_logs (date, client_ip, domain, dns) VALUES (%s, %s, %s, %s)"
        cursor.executemany(query, data)
        connection.commit()

    @staticmethod
    def inserting_data():
        print("########## Verifying with the domain list table ##########")
        cursor.execute(
            """INSERT IGNORE INTO logs SELECT '',a.date,a.client_ip,domain_list.site,domain_list.domain,domain_list.region,a.dns 
            FROM (SELECT * FROM tmp_logs WHERE domain IN (SELECT Domain FROM domain_list)) AS a LEFT OUTER JOIN 
            domain_list ON a.domain = domain_list.domain""")
        connection.commit()
        cursor.execute("TRUNCATE TABLE tmp_logs")
        connection.commit()

    @staticmethod
    def close_db_connection():
        print("DONE")
        cursor.close()
