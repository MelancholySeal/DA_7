#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Для своего варианта лабораторной работы 2.17 
# необходимо реализовать хранение данных в
# базе данных SQLite3. Информация в базе данных 
# должна храниться не менее чем в двух
# таблицах.
# Вариант 11 


import argparse
import sqlite3
import typing as t
from pathlib import Path


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


def create_db(database_path: Path) -> None:
    """
    Создать базу данных для хранения информации о людях.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Создать таблицу с информацией о именах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS names (
            name_id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL
        )
        """
    )
    # Создать таблицу с информацией о людяъ.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS person (
            person_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_id TEXT NOT NULL,
            birth_date INTEGER NOT NULL,
            phone_number INTEGER NOT NULL,
            FOREIGN KEY(name_id) REFERENCES names(name_id)
        )
        """
    )
    conn.close()


def add_person(
    database_path: Path, full_name: str, birth_date: str, phone_number: str
) -> None:
    """
    Добавить информацию о человеке в базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT name_id FROM names WHERE full_name = ?
        """,
        (full_name,),
    )

    row = cursor.fetchone()

    if row is None:
        cursor.execute(
            """
            INSERT INTO names (full_name) VALUES (?)
            """,
            (full_name,),
        )
        name_id = cursor.lastrowid
    else:
        name_id = row[0]

    cursor.execute(
        """
        INSERT INTO person (name_id, birth_date, phone_number)
        VALUES (?, ?, ?)
        """,
        (name_id, birth_date, phone_number),
    )
    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех людей.
    """

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT names.full_name, person.birth_date, person.phone_number
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


def select_person(database_path: Path, find_name):
    """
    Выбрать человека с заданным именем.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT names.full_name, person.birth_date, person.phone_number
        FROM person
        INNER JOIN names ON names.name_id = person.name_id
        WHERE person.phone_number = ?
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
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.home() / "people.db"),
        help="The data file name",
    )

    parser = argparse.ArgumentParser("people")
    parser.add_argument(
        "--version", action="version", version="%(prog)s 0.1.0"
    )

    subparsers = parser.add_subparsers(dest="command")

    add = subparsers.add_parser(
        "add", parents=[file_parser], help="Add a new person"
    )
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

    _ = subparsers.add_parser(
        "display", parents=[file_parser], help="Display all people"
    )

    select = subparsers.add_parser(
        "select", parents=[file_parser], help="Select a person"
    )
    select.add_argument(
        "--sp",
        action="store",
        required=True,
        help="The required name of the person",
    )
    args = parser.parse_args(command_line)
    print(args)
    db_path = Path(args.db)
    create_db(db_path)
    if args.command == "add":
        add_person(db_path, args.name, args.birth_date, args.phone_number)

    elif args.command == "display":
        display_people(select_all(db_path))

    elif args.command == "select":
        display_people(select_person(db_path, args.sp))
        pass


if __name__ == "__main__":
    main()
