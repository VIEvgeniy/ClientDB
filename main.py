import psycopg2

class ClientDB:
    def __init__(self, database, user, password):
        self.conn = psycopg2.connect(database=database, user=user, password=password)
    def __del__(self):
        self.conn.commit()
        self.conn.close()
    def drop_table(self):
        with self.conn.cursor() as cur:
            # удаление таблиц
            cur.execute("""
            DROP TABLE phone_and_email_person;
            DROP TABLE email;
            DROP TABLE phone;
            DROP TABLE person;
            """)
            self.conn.commit()


    def create_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE person(
                id SERIAL PRIMARY KEY,
                firstname  VARCHAR(40) NOT NULL,
                lastname  VARCHAR(40) NOT NULL
            );
            CREATE TABLE email(
                id SERIAL PRIMARY KEY,
                address  VARCHAR(40) UNIQUE NOT NULL
            );
            CREATE TABLE phone(
                id SERIAL PRIMARY KEY,
                phone_number  VARCHAR(40) UNIQUE NOT NULL
            );
            CREATE TABLE phone_and_email_person (
                person_id INTEGER REFERENCES person(id),
                email_id INTEGER REFERENCES email(id),
                phone_id INTEGER REFERENCES phone(id),
                CONSTRAINT pk_phone_and_email_person PRIMARY KEY (person_id, email_id, phone_id)
            );
            """)
            self.conn.commit()

    def add_client(self, firstname, lastname, email, phone):
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO  email(address) values(%s) RETURNING id;", (email, ))
            email_id = cur.fetchone()[0]
            cur.execute("INSERT INTO  phone(phone_number) values(%s) RETURNING id;", (phone, ))
            phone_id = cur.fetchone()[0]
            cur.execute("INSERT INTO  person(firstname, lastname) values(%s,%s) RETURNING id;", (firstname, lastname))
            person_id = cur.fetchone()[0]
            cur.execute("""
            INSERT INTO  phone_and_email_person(person_id, email_id, phone_id) values(%s,%s,%s);
            """, (person_id, email_id, phone_id))
            self.conn.commit()
    def find(self, firstname='%', lastname='%', email='%', phone='%'):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT p.id, p.firstname, p.lastname, p2.phone_number, e.address FROM person p, email e, phone p2, phone_and_email_person paep 
            WHERE paep.person_id = p.id and paep.email_id = e.id and paep.phone_id = p2.id AND
            p.firstname LIKE %s AND p.lastname LIKE %s AND p2.phone_number LIKE %s AND e.address LIKE %s;
            """, (firstname, lastname, phone, email))
            res = cur.fetchall()
        return res
    def add_client_phone(self, id, phone):
        pass

# Создать объект для работы БД клиентов
cdb = ClientDB("client", "postgres", "postgres")
# удалить все таблицы
cdb.drop_table()
# создать отношения(таблицы)
cdb.create_table()
# добавление клиентов
cdb.add_client('Иван', 'Иванов', 'ivan@yandex.ru', '+79538125587')
cdb.add_client('Петр', 'Петров', 'petr@gmail.com', '+79208325587')
cdb.add_client('Сидор', 'Сидоров', 'sidor@mail.ru', '+79058125787')
# поиск клиентов с телефоном с телевонным кодом 963
print(cdb.find(phone='%953%'))
# поиск клиентов по фамилии "Иванов"
print(cdb.find(lastname='Иванов'))
# поиск клиентов с электронными адресами на домене mail.ru
print(cdb.find(email='%mail.ru'))
