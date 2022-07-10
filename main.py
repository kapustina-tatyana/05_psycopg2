import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

con = psycopg2.connect(user='postgres', password='postgres', port=5432)
dbname = 'psycopg_055'

def create_db_if_not_exists(con, dbname):
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    try:
        cur.execute(f'CREATE DATABASE {dbname};')
        print(f"created DB {dbname}")
    except psycopg2.ProgrammingError as e:
        print(f"DB {dbname} Already exists")



def create_tables(conn):
    '''создающая структуру таблиц в БД'''
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT exists clients(
           id SERIAL PRIMARY KEY,
           first_name VARCHAR(100),
           last_name VARCHAR(100),
           email VARCHAR(100) unique
           );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phone_number(
            id SERIAL PRIMARY KEY,
            phone_number VARCHAR(12),
            id_client integer REFERENCES clients(id),
            UNIQUE (phone_number, id_client)
            );
    """)
    conn.commit()


def create_new_client(conn, first_name, last_name, email, phone_number=None):
    '''позволяющая добавить нового клиента'''
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO clients(first_name, last_name, email) VALUES( %s, %s, %s) RETURNING id;
        """, (first_name, last_name, email))
    id_client = cur.fetchone()[0]
    conn.commit()
    if phone_number == None:
        print('none value, not update')
    else:
        add_phone_number(conn, id_client, phone_number)



def add_phone_number(conn, id_client, phone_number):
    '''добавить телефон для существующего клиента'''
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO phone_number (phone_number, id_client) VALUES( %s, %s);
            """, (phone_number, id_client))
        print(f"phone number {phone_number} added")
    except psycopg2.errors.UniqueViolation as e:
        print(f"phone number {phone_number} Already exists")

    conn.commit()

def edit_client(conn, id_client, first_name=None, last_name=None, email=None, phone_number=None):
    '''изменить данные о клиенте'''
    cur = conn.cursor()
    if first_name == None:
        print('none value, not update')
    else:
        sql_string = """
        UPDATE clients SET first_name = %s where id = %s;
        """
        cur.execute(sql_string, (first_name, id_client))
    conn.commit()

    if last_name == None:
        print('none value, not update')
    else:
        sql_string = """
        UPDATE clients SET last_name = %s where id = %s;
        """
        cur.execute(sql_string, (last_name, id_client))

    if email == None:
        print('none value, not update')
    else:
        sql_string = """
        UPDATE clients SET email = %s where id = %s;
        """
        cur.execute(sql_string, (email, id_client))

    if phone_number == None:
        print('none value, not update')
    else:
        add_phone_number(conn, id_client, phone_number)

    conn.commit()

def remove_phone_client(conn, id_client, phone_number):
    '''удалить телефон для существующего клиента'''
    cur = conn.cursor()
    cur.execute("""
           DELETE FROM phone_number WHERE id = %s and phone_number = %s ;
           """, (id_client, phone_number))
    conn.commit()
def remove_client(conn, id_client):
    '''удалить существующего клиента'''
    cur = conn.cursor()
    cur.execute("""
    DELETE FROM phone_number WHERE id_client = %s;
    """,(id_client,))
    cur.execute("""
    DELETE FROM clients WHERE id = %s;
    """, (id_client,))
    conn.commit()
def find_client(conn, first_name=None, last_name=None, email=None, phone_number=None):
    '''найти клиента по его данным (имени, фамилии, email-у или телефону)'''
    cur = conn.cursor()

    if first_name == None:
        print('Не найдено')
    else:
        cur.execute("""
        select c.id, c.first_name, c.last_name, c.email, pn.phone_number from clients c
         LEFT JOIN phone_number pn on c.id = pn.id_client
         where first_name = %s;
                """, (first_name,))
        cursor_string = cur.fetchall()
    if last_name == None:
        print('Не найдено')
    else:
        cur.execute("""
        select c.id, c.first_name, c.last_name, c.email, pn.phone_number from clients c
        LEFT JOIN phone_number pn on c.id = pn.id_client
        where last_name = %s;
                """, (last_name,))
        for fetch_all in cur.fetchall():
            cursor_string.append(fetch_all)
    if email == None:
        print('Не найдено')
    else:
        cur.execute("""
        select c.id, c.first_name, c.last_name, c.email, pn.phone_number from clients c
        LEFT JOIN phone_number pn on c.id = pn.id_client
        where email = %s;
                """, (email,))
        for fetch_all in cur.fetchall():
            cursor_string.append(fetch_all)


    if phone_number == None:
        print('Не найдено')
    else:
        cur.execute("""
        select c.id, c.first_name, c.last_name, c.email, pn.phone_number  from phone_number pn
        LEFT JOIN clients c on pn.id_client = c.id
        where phone_number = %s;
                """, (phone_number,))

        for fetch_all in cur.fetchall():
            cursor_string.append(fetch_all)
        return cursor_string

    conn.commit()
create_db_if_not_exists(con, dbname)

with psycopg2.connect(database=dbname, user="postgres", password="postgres") as conn:
    create_tables(conn)
    create_new_client(conn, 'Petr', 'Kuznetcov', 'kuzn_p@email.ru', '+79898847769')
    create_new_client(conn, 'Vasiliy', 'Dudkin', 'dudka@email.ru', '+79031151325')
    create_new_client(conn, 'Ivan', 'Vasin', 'vas@email.ru', '+79855201436')
    create_new_client(conn, 'Ivan', 'Gusev', 'gusev@email.ru')
    add_phone_number(conn, 2, '+77778887774')
    add_phone_number(conn, 4, '+79851124578')
    add_phone_number(conn, 3, '+79150681028')
    edit_client(conn, 1, last_name='petya', first_name= 'Ivanov', phone_number='89261405028')
    remove_phone_client(conn, 3, '+79855201436')
    remove_client(conn, 2)
    find_string = find_client(conn, first_name='Petr', last_name='Vasin', phone_number='+79851124578')
    print(find_string)
    add_phone_number(conn, 3, '+79150681028')
conn.close()