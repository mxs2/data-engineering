import sqlite3


class Load:
    def __init__(self):
        pass

    def create_sqlite_table(
        self, universities_list: list[dict], db_name: str, table_name: str
    ) -> None:
        """
        Método responsável por criar um banco SQLite e adicionar tabelas nele.

        Args:
            universities_list: list[dict]
            db_name: str
            table_name: str
        """

        con = sqlite3.connect(f"{db_name}.db")
        c = con.cursor()

        c.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
        id INTERGER PRIMARY KEY,
        name TEXT,
        country TEXT,
        state_province TEXT,
        web_pages TEXT,
        domains TEXT
        );
        """)

        for university in universities_list:
            c.execute(
                f"""INSERT INTO {table_name} (name, country, state_province,
            web_pages, domains) VALUES (?,?,?,?,?);""",
                (
                    university.get("name"),
                    university.get("country"),
                    university.get("state-province"),
                    ", ".join(university.get("web_pages", [])),
                    ", ".join(university.get("domains", [])),
                ),
            )

        con.commit()
        con.close()
