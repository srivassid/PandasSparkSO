import datetime
import numpy as np
import pandas as pd
import os
from pyunpack import Archive
from lxml.etree import XMLParser, parse

pd.options.display.width = 0

class StackOverflow():

    def __init__(self):
        self.path = os.getcwd() + '/data'

    def extract_files(self):
        '''
        Extract files
        :return:
        '''
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        Archive('german.stackexchange.com.7z').extractall(self.path)

    def read_posts_file(self):
        '''
        read files with performance optimization
        and return dataframe
        :return:
        '''
        self.data = []
        self.p = XMLParser(huge_tree=True)
        self.tree = parse(self.path + '/Posts.xml' , parser=self.p)
        for i in (self.tree.iter()):
            # print(i.attrib)
            self.data.append(dict(i.attrib))

        self.output = pd.DataFrame(self.data)
        self.output = self.output.drop([0])
        return self.output

    def total_posts(self):
        '''
        total number of posts, same as total
        number of rows
        :return:
        '''
        self.df = self.read_posts_file()
        print("Total number of posts are ", self.df.shape[0])

    def last_post(self):
        '''
        read post with comments attached to it,
        filter by date, get latest row
        :return:
        '''
        self.df = self.read_posts_file()
        self.df['CommentCount'] = self.df['CommentCount'].astype(int)
        self.df = self.df.loc[self.df['CommentCount'] >= 1]
        # print(self.df.shape)
        self.df['CreationDate'] = pd.to_datetime(self.df['CreationDate'])

        self.most_recent_comment = self.df.loc[self.df['CreationDate'] ==
                                               self.df['CreationDate'].max()]
        print("Most recent comment is ",self.most_recent_comment['Body'])
        print("Most recent comment's timestamp is ",
              self.most_recent_comment['CreationDate'])
        return self.most_recent_comment['CreationDate']

    def last_month_posts(self):
        '''
        filter dataframe for last month's data, get results
        :return:
        '''
        self.df = self.read_posts_file()
        self.df['CreationDate'] = pd.to_datetime(self.df['CreationDate'])
        self.dtDate = datetime.datetime.now()
        self.df['prev_three_month'] = (self.dtDate - pd.DateOffset(months=3))
        self.df['prev_month'] = (self.dtDate - pd.DateOffset(months=1))

        self.df_last_month = self.df.loc[self.df['CreationDate'] >
                                         self.df['prev_month']]

        print("Number of posts in the last month ",
              self.df_last_month.shape[0])

        self.df_last_3_months = self.df.loc[self.df['CreationDate'] >
                                            self.df['prev_three_month']]

        print("Number of posts in the last 3 months ",
              self.df_last_3_months.shape[0])
        return self.df_last_3_months.shape[0]

    def average_comments(self):
        '''
        average comments per month, get total comments
        every month, take average over all months
        :return:
        '''
        self.df = pd.read_xml(self.path + '/Comments.xml')
        # print(self.df)
        self.df['CreationDate'] = pd.to_datetime(self.df['CreationDate'])
        self.df = self.df.set_index('CreationDate')
        self.avg_comments = self.df.groupby(pd.Grouper(freq="M"))
        self.comments_sum = (self.avg_comments.count())
        print("Average number of comments per month are ",
              self.comments_sum.reset_index()['Id'].mean())
        return self.comments_sum.reset_index()['Id'].mean()

    def badges(self):
        '''
        conditional column, if Name has a value of Critic
        or Editor, create new column and attach a 1, else o
        :return:
        '''
        self.df = pd.read_xml(self.path + '/Badges.xml')
        # print(self.df)
        self.df['is_critic'] = np.where(self.df['Name'] == 'Critic', 1, 0)
        self.df['is_editor'] = np.where(self.df['Name'] == 'Editor', 1, 0)
        print(self.df)
        return self.df

if __name__ == '__main__':
    so = StackOverflow()
    # so.total_posts()
    so.last_post()
    # so.last_month_posts()
    # so.average_comments()
    # so.badges()