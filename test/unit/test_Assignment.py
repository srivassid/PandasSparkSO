import unittest
import pandas as pd
from files.Assignment import StackOverflow

class TestStackOverflow(unittest.TestCase):
    def setUp(self):
        self.so = StackOverflow()

    def test_read_posts_file(self):
        self.df = self.so.read_posts_file().head(10)
        self.assertEqual(10, self.df.shape[0])

    def test_total_posts(self):
        self.df = self.so.read_posts_file()
        self.assertEqual(51950, self.df.shape[0])

    def test_last_post(self):
        self.df_date = (self.so.last_post())
        # self.date = (datetime.datetime(2022,6,4,22,31,58,133000))
        pd.api.types.is_datetime64_dtype(self.df_date)

    def test_last_month_posts(self):
        self.three_month_posts = self.so.last_month_posts()
        self.assertEqual(189 ,self.three_month_posts)

    def test_average_comments(self):
        self.average_num_comments = self.so.average_comments()
        self.assertAlmostEqual(856., self.average_num_comments,places=0)

    def test_badges(self):
        self.df = self.so.badges()
        self.assertEqual(1,self.df[self.df['Name'] == 'Critic'] \
                                            ['is_critic'].iloc[0])
        self.assertEqual(1, self.df[self.df['Name'] == 'Editor']\
                                            ['is_editor'].iloc[0])
if __name__ == '__main__':
    unittest.TestCase()