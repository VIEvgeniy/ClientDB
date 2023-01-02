import psycopg2


class ClientDB:
    def __init__(self, database, user, password):
        self.conn = psycopg2.connect(database=database, user=user, password=password)

    def __del__(self):
        self.conn.close()

    # 1. Функция, создающая структуру БД (таблицы)
    def create_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE person(
                id SERIAL PRIMARY KEY,
                firstname  VARCHAR(40) NOT NULL,
                lastname  VARCHAR(40) NOT NULL,
                email  VARCHAR(40) UNIQUE NOT NULL
            );
            CREATE TABLE phone(
                id SERIAL PRIMARY KEY,
                phone_number  VARCHAR(40) UNIQUE NOT NULL
            );
            CREATE TABLE phone_of_person (
                person_id INTEGER REFERENCES person(id) ON DELETE CASCADE,
                phone_id INTEGER REFERENCES phone(id) ON DELETE CASCADE,
                CONSTRAINT pk_phone_of_person PRIMARY KEY (person_id, phone_id)
            );
            """)
            self.conn.commit()

    # удаление таблиц
    def drop_table(self):
        with self.conn.cursor() as cur:
            # удаление таблиц
            cur.execute("""
            DROP TABLE IF EXISTS phone_of_person;
            DROP TABLE IF EXISTS phone;
            DROP TABLE IF EXISTS person;
            """)
            self.conn.commit()

    # 2. Функция, позволяющая добавить нового клиента
    def add(self, firstname, lastname, email, phone=None):
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO  person(firstname, lastname, email) values(%s,%s,%s) RETURNING id;",
                        (firstname, lastname, email))
            person_id = cur.fetchone()[0]
            if phone is not None:
                self.add_phone(person_id, phone)

    # 3. Функция, позволяющая добавить телефон для существующего клиента
    def add_phone(self, person_id, phone):
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM phone WHERE phone_number = %s", (phone,))
            if cur.fetchone() is None:
                cur.execute("INSERT INTO  phone(phone_number) values(%s) RETURNING id;", (phone,))
                phone_id = cur.fetchone()[0]
                cur.execute("INSERT INTO  phone_of_person(person_id, phone_id) values(%s,%s);",
                            (person_id, phone_id))
                self.conn.commit()
            else:
                print('Ошибка добавления телефона, телефон уже сть в базе')

    # 4. Функция, позволяющая изменить данные о клиенте
    def set(self, client_id, firstname=None, lastname=None, email=None):
        update_string = 'UPDATE person SET '
        update_data = []
        if firstname is None and lastname is None and email is None:
            print('Ошибка: пустой запрос на изменение')
            return
        if firstname:
            update_string += 'firstname=%s'
            update_data.append(firstname)
        if lastname:
            update_string += 'lastname=%s'
            update_data.append(lastname)
        if email:
            update_string += 'email=%s'
            update_data.append(email)
        with self.conn.cursor() as cur:
            if firstname and lastname:
                cur.execute('''
                UPDATE person SET firstname=%s, lastname=%s WHERE id=%s
                ''', (firstname, lastname, client_id))
            elif firstname:
                cur.execute('''
                UPDATE person SET firstname=%s WHERE id=%s
                ''', (firstname, client_id))
            else:
                cur.execute('''
                UPDATE person SET lastname=%s WHERE id=%s
                ''', (lastname, client_id))
            self.conn.commit()

    # 5. Функция, позволяющая удалить телефон для существующего клиента
    def del_phone(self, phone):
        with self.conn.cursor() as cur:
            # cur.execute("SELECT id FROM phone WHERE phone_number = %s", (phone,))
            # phone_id = cur.fetchone()
            # if phone_id is None:
            #     print(f'Ошибка: телефон {phone} не найден')
            # else:
            #     cur.execute("DELETE FROM phone_of_person WHERE phone_id = %s",
            #                 phone_id)
            #     cur.execute("DELETE FROM phone WHERE id = %s", phone_id)
            #     self.conn.commit()
            cur.execute("DELETE FROM phone WHERE phone_number = %s", (phone,))
            self.conn.commit()
    # 6. Функция, позволяющая удалить существующего клиента
    def delete(self, person_id):
        with self.conn.cursor() as cur:
            # удалить все телефоны клиента
            cur.execute("""
            DELETE FROM phone WHERE id IN (
            SELECT phone_id FROM phone_of_person WHERE person_id = %s
            )
            """, (person_id,))
            cur.execute("DELETE FROM person WHERE id = %s", (person_id,))
            self.conn.commit()

    # 7. Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
    def find(self, firstname='%', lastname='%', email='%', phone='%'):
        val = [firstname, lastname, email]
        if not phone:
            phone_where = 'p2.phone_number is NULL'
        else:
            phone_where = 'p2.phone_number LIKE %s'
            val.append(phone)
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT p.*, p2.phone_number FROM person p 
            LEFT JOIN phone_of_person pop ON pop.person_id = p.id
            LEFT JOIN phone p2 ON pop.phone_id = p2.id
            WHERE p.firstname LIKE %s AND p.lastname LIKE %s AND p.email LIKE %s AND
            """ + phone_where + ';', val)
            res = cur.fetchall()
        return res

#     def set_client_phone(self, phone):
#         pass
#
# # Создать объект для работы БД клиентов
client_data = ClientDB("client", "postgres", "postgres")
# удалить все таблицы
client_data.drop_table()
# создать отношения(таблицы)
client_data.create_table()
# # добавление клиентов
client_data.add('Иван', 'Иванов', 'ivan@yandex.ru', '+79538125587')
client_data.add('Петр', 'Петров', 'petr@gmail.com', '+79208325587')
client_data.add('Сидор', 'Сидоров', 'sidor@mail.ru', '+79058125787')
client_data.add('Абрам', 'Абрамов', 'abram@mail.ru')

# поиск клиентов с телефоном с телефонным кодом 963
print('поиск клиентов с телефоном с телефонным кодом 963\n', client_data.find(phone='%953%'))
# поиск клиентов по фамилии "Иванов"
print('поиск клиентов по фамилии "Иванов"\n', client_data.find(lastname='Иванов'))
# поиск клиентов с электронными адресами на домене mail.ru
print(' поиск клиентов с электронными адресами на домене mail.ru\n', client_data.find(email='%mail.ru'))
# Поиск клиентов без телефонов
res = client_data.find(phone='')
print('Поиск клиентов без телефонов\n', res)
# добавоение телефона найденному клиенту
client_data.add_phone(res[0][0],'84862573521')
print('добавоение телефона найденному клиенту\n', client_data.find(phone='84862573521'))
# добавление еще одного телефона
client_data.add_phone(res[0][0],'+79095876354')
print('добавление еще одного телефона\n', client_data.find(email=res[0][3]))
# удаление телефона '84862573521'
client_data.del_phone('84862573521')
print('удаление телефона 84862573521\n', client_data.find(email=res[0][3]))
# удаление клиента целиком
print('удаление клиента целиком')
print('содержание до удаления\n', client_data.find())
client_data.delete(res[0][0])
print('после\n', client_data.find())
# client_data.set(res[0][0]) # тест пустого запроса на изменение
# Изменить Имя клиента с email sidor@mail.ru
print('Изменить Имя клиента с email sidor@mail.ru')
client = client_data.find(email='sidor@mail.ru')
print('Поиск по почте sidor@mail.ru')
print(client)
print('Клиент с почтой sidor@mail.ru найден, замена имени на "Simon"')
client_data.set(client[0][0], firstname='Simon')
print(client_data.find(email='sidor@mail.ru'))

