import psycopg2
from config import config


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        DROP TABLE stock_history
        """,
        """
        DROP TABLE markets
        """,
        """
        CREATE TABLE  IF NOT EXISTS markets (
            code CHAR(4) PRIMARY KEY NOT NULL,
            name CHAR(50) NOT NULL
        )
        """,
        """ CREATE TABLE IF NOT EXISTS stock_history (
                history_id SERIAL PRIMARY KEY,
                filename VARCHAR(18),
                code CHAR(4),
                year SMALLINT NOT NULL,
                date DATE NOT NULL,
                open FLOAT8 NOT NULL,
                high FLOAT8 NOT NULL,
                low FLOAT8 NOT NULL,
                close FLOAT8 NOT NULL,
                volume FLOAT8 NOT NULL,
                dividend FLOAT8 NOT NULL,
                split FLOAT8 NOT NULL,
                adj_open FLOAT8 NOT NULL,
                adj_high FLOAT8 NOT NULL,
                adj_low FLOAT8 NOT NULL,
                adj_close FLOAT8 NOT NULL,
                adj_volume FLOAT8 NOT NULL,
                CONSTRAINT fk_customer
                    FOREIGN KEY(code) 
                    REFERENCES markets(code)
                    ON DELETE CASCADE
                )
        """)
    conn = None
    try:
        # Read the connection parameters
        params = config()
        # Connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # Create table one by one
        for command in commands:
            cur.execute(command)
        # Close communication with the PostgreSQL database server
        cur.close()
        # Commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# Run the create table function localy for testing.
if __name__ == '__main__':
    create_tables()
