#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Самостоятельно изучите работу с пакетом python-psycopg2
# для работы с базами данных PostgreSQL. 
# Для своего варианта лабораторной работы 2.17 необходимо реализовать
# возможность хранения данных в базе данных СУБД PostgreSQL. 
# Информация в базе данных должна храниться 
# не менее чем в двух таблицах.
# Вариант 11 


import argparse
import typing as t

import psycopg2


def display_people(people: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить информацию о людях
    """
    if people:
        line = "+-{}-+-{}-+-{}-+-{}-+".format(
            "-" * 4, "-" * 30, "-" * 20, "-" * 15
        )
        print(line)
        print(
            "| {:^4} | {:^30} | {:^20} | {:^15} |".format(
                "№", "Имя", "Дата рождения", "Номер телефона"
            )
        )
        print(line)

        for idx, person in enumerate(people, 1):
            print(
                "| {:^4} | {:^30} | {:^20} | {:^15} |".format(
                    idx,
                    person.get("full_name", ""),
                    person.get("birth_date", ""),
                    person.get("phone_number", ""),
                )
            )
            print(line)
    else:
        print("Список людей пуст.")


def create_db() -> None:
    """
    Создать базу данных для хранения информации о людях.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="adminadmin",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()
    # Создать таблицу с информацией о именах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS names (
            name_id serial primary key,
            full_name TEXT NOT NULL
        )
        """
    )
    # Создать таблицу с информацией о людяъ.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS person (
            person_id serial primary key,
            name_id INTEGER NOT NULL,
            birth_date INTEGER NOT NULL,
            phone_number INTEGER NOT NULL,
            FOREIGN KEY(name_id) REFERENCES names(name_id)
        )
        """
    )
    conn.commit()
    conn.close()


def add_person(full_name: str, birth_date: str, phone_number: str) -> None:
    """
    Добавить информацию о человеке в базу данных.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="adminadmin",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT name_id FROM names WHERE full_name = %s
        """,
        (full_name,),
    )
    name_id = cursor.lastrowid
    row = cursor.fetchone()

    if row is None:
        cursor.execute(
            """
            INSERT INTO names (full_name) VALUES (%s)
            """,
            (full_name,),
        )
        name_id = cursor.lastrowid
    else:
        name_id = row[0]
    conn.commit()
    cursor.execute(
        """
        INSERT INTO person (name_id, birth_date, phone_number)
        VALUES (%s, %s, %s)
        """,
        (name_id, birth_date, phone_number),
    )
    conn.commit()
    conn.close()


def select_all() -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех людей.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="adminadmin",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()
    cursor.execute(
        # """
        # SELECT name, birth_date, phone_number
        # FROM people
        # """
        """
        SELECT
        names.full_name,
        person.birth_date,
        person.phone_number
        FROM person
        INNER JOIN names ON names.name_id = person.name_id
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "full_name": row[0],
            "birth_date": row[1],
            "phone_number": row[2],
        }
        for row in rows
    ]


def select_person(find_name):
    """
    Выбрать человека с заданным именем.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="adminadmin",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT names.full_name, person.birth_date, person.phone_number
        FROM person
        INNER JOIN names ON names.name_id = person.name_id
        WHERE person.phone_number = %s
        """,
        (find_name,),
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "full_name": row[0],
            "birth_date": row[1],
            "phone_number": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    parser = argparse.ArgumentParser("people")
    parser.add_argument(
        "--version", action="version", version="%(prog)s 0.1.0"
    )

    subparsers = parser.add_subparsers(dest="command")

    add = subparsers.add_parser("add", help="Add a new person")
    add.add_argument(
        "-n",
        "--name",
        action="store",
        required=True,
        help="The person's name",
    )
    add.add_argument(
        "-b",
        "--birth_date",
        action="store",
        required=True,
        help="The person's birth date",
    )
    add.add_argument(
        "-p",
        "--phone_number",
        action="store",
        required=True,
        help="The person's phone number",
    )

    _ = subparsers.add_parser("display", help="Display all people")

    select = subparsers.add_parser("select", help="Select a person")
    select.add_argument(
        "--sp",
        action="store",
        required=True,
        help="The required name of the person",
    )
    args = parser.parse_args(command_line)

    create_db()
    if args.command == "add":
        add_person(args.name, args.birth_date, args.phone_number)

    elif args.command == "display":
        display_people(select_all())

    elif args.command == "select":
        display_people(select_person(args.sp))
        pass


if __name__ == "__main__":
    main()
