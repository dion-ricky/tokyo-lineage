import unittest

from openlineage.common.sql import SqlMeta, SqlParser

class TestSqlParser(unittest.TestCase):
    sql_1_in = """INSERT INTO sums (value)
        SELECT SUM(c.value) FROM counts AS c;
    """

    sql_2_in = """INSERT INTO movie_actor (a.actor_id, a.actor_name, m.movie_id, m.movie_name)
        SELECT actor_id, actor_name FROM actors AS a
        LEFT JOIN movies m
        ON m.actor_id = a.actor_id;
    """

    def test_single_input_table(self):
        sql_meta: SqlMeta = SqlParser.parse(self.sql_1_in, 'public')

        qualified_name = [t.qualified_name for t in sql_meta.in_tables]
        self.assertListEqual(qualified_name, ['public.counts'])

    def test_two_input_tables(self):
        sql_meta: SqlMeta = SqlParser.parse(self.sql_2_in, 'public')

        qualified_names = [t.qualified_name for t in sql_meta.in_tables]
        compared_to = ['public.actors','public.movies']

        qualified_names = sorted(qualified_names)
        compared_to = sorted(compared_to)

        self.assertListEqual(qualified_names, compared_to)
    
    def test_different_schema(self):
        sql_different_schema = """INSERT INTO movie_actor (a.actor_id, a.actor_name, m.movie_id, m.movie_name)
            SELECT actor_id, actor_name FROM actors AS a
            LEFT JOIN imdb.movies m
            ON m.actor_id = a.actor_id;
        """

        sql_meta: SqlMeta = SqlParser.parse(sql_different_schema, 'public')

        qualified_names = [t.qualified_name for t in sql_meta.in_tables]
        compared_to = ['public.actors','imdb.movies']

        qualified_names = sorted(qualified_names)
        compared_to = sorted(compared_to)

        self.assertListEqual(qualified_names, compared_to)
    
    def test_output_tables(self):
        sql_meta: SqlMeta = SqlParser.parse(self.sql_2_in, 'public')
        
        qualified_names = [t.qualified_name for t in sql_meta.out_tables]
        compared_to = ['public.movie_actor']

        qualified_names = sorted(qualified_names)
        compared_to = sorted(compared_to)

        self.assertListEqual(qualified_names, compared_to)