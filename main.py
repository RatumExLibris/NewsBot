
import threading
from typing import List
import requests as rq
from abc import ABC
import time
from queue import Queue

import telebot
from telebot import types
from bs4 import BeautifulSoup as bs
import pandas as pd

from bot_settings import API_TOKEN


class SubjectInterface(ABC):
    def subscribe(self, user):
        pass

    def detach(self, user):
        pass

    def notify(self):
        pass


class ThemeSubject(SubjectInterface):
    def __init__(self, theme_name):
        self.theme_name = theme_name
        self.users = []

    def subscribe(self, user):
        print(len(self.users))
        self.users.append(user)

    def detach(self, user):
        print(len(self.users))
        self.users.pop(self.users.index(user))

    def notify(self):
        for user in self.users:
            user.update(self.news, self.theme_name)

    def in_subs(self, user):
        #print(user in users)
        #print(user, users)
        return user in self.users

    def set_News(self, news):
        self.news = news
        self.notify()

    def get_users(self):
        return self.users




class ObserverInterface(ABC):
    def update(self, news: dict, theme: str):
        pass


class TGUserObserver(ObserverInterface):
    def __init__(self, chat_id, bot):
        self.bot = bot
        self.chat_id = chat_id

    def update(self, news: dict, theme: str):
        # тут отправка сообщений тг боту
        self.bot.send_message(self.chat_id, news['title'] + '\n\n' + news['text'] +
                         '\n\n' + news['company'] + '\n\n' + theme)
        pass


class VKUserObserver(ObserverInterface):
    def __init__(self, user_id):
        self.user_id = user_id

    def update(self, news: dict, theme: str):
        # тут отправка сообщений вк боту
        pass


class NewsParser:
    def __init__(self):
        self.d = {'business': 4,
                  'politics': 2,
                  'sport': 8}
        self.i_d = {4: 'business',
                    2: 'politics',
                    8: 'sport'}


    def get_url_rbc(self, query, project, category, dateFrom, dateTo, page):
        url = 'https://www.rbc.ru/search/ajax/?'
        url += 'query=' + str(query)
        url += '&project=' + str(project)
        url += '&category=' + str(category)
        url += '&dateFrom=' + str(dateFrom)
        url += '&dateTo=' + str(dateTo)
        url += '&page=' + str(page)
        return url

    def get_url_lenta(self, theme):
        try:
            d = {'business': 4,
                 'politics': 2,
                 'sport': 8}
            theme = d[theme]
            url = 'https://lenta.ru/search/v2/process?sort=2'
            url += '&bloc=' + str(theme)
            return url
        except:
            return ''

    def get_articles_from_url(self, url, company):
        try:
            r = rq.get(url)
            try:
                if company == 'rbc':
                    page = r.json()['items']
                else:
                    page = r.json()['matches']
            except:
                print('ошибка в выполнении запроса к сайту')

            pagedf = pd.DataFrame(page)

            pagedf['text'] = ''
            texts = []
            for i in range(0, pagedf.shape[0]):
                if company == 'rbc':
                    url = pagedf.loc[i, 'fronturl']
                else:
                    url = pagedf.loc[i, 'url']

                r = rq.get(url)
                soup = bs(r.text, features="lxml")
                p_text = soup.find_all('p')
                if p_text:
                    text = ' '.join(map(lambda x:
                                        x.text.replace('<br />', '\n').strip(),
                                        p_text))
                else:
                    text = None

                if company != 'rbc':
                    text = text.split('/')[-1]
                texts.append(text)
            pagedf['text'] = texts

            return pagedf
        except:
            return pd.DataFrame()

    def get_articles(self, query='', project='', category='', dateFrom='', dateTo='', page='0', company='rbc'):
        if company == 'rbc':
            url = self.get_url_rbc(query, project, category, dateFrom, dateTo, page)
        else:
            url = self.get_url_lenta(category)
        return self.get_articles_from_url(url, company)


class Main:
    def __init__(self):
        self.bot = telebot.TeleBot(API_TOKEN)
        self.users = {}
        try:
            df = pd.read_csv('data.csv')
            for i in df['ids']:
                sub = TGUserObserver(i, self.bot)
                self.users[i] = sub
            del df
        except:
            self.users = {}
        print('len users', len(self.users.keys()))
        # self.chat_id = Queue()
        self.themes_q = Queue()
        self.themes_q_news = Queue()
        self.themes_q_news_l = Queue()
        self.themes_list: List[str] = ['business', 'politics', 'realty', 'sport', 'society']

    def __del__(self):
        df = pd.DataFrame({'ids': self.users.keys()})
        df.to_csv('data.csv')
        del df


    def main(self):
        for theme in self.themes_list:
            themesubject = ThemeSubject(theme)
            self.themes_q.put(themesubject)
            self.themes_q_news.put(themesubject)
            self.themes_q_news_l.put(themesubject)

        self.programm()

    def programm(self):
        self.t_1 = threading.Thread(target=self.TeleBot)
        self.t_2 = threading.Thread(target=self.RBCGetNews)
        self.t_3 = threading.Thread(target=self.LentaGetNews)

        self.t_1.start()
        self.t_2.start()
        self.t_3.start()

        self.t_1.join()
        self.t_2.join()
        self.t_3.join()

    def RBCGetNews(self):
        print('GetRBCNews')
        #bot = self.q.get()
        themes = []

        for theme in self.themes_list:
            themes.append(self.themes_q_news.get())

        last_time = {}
        for theme in self.themes_list:
            last_time[theme] = 0

        newsparser = NewsParser()
        while True:
            for theme in self.themes_list:
                try:
                    articles = newsparser.get_articles(project='rbcnews', category=theme, company='rbc')
                    print(theme, articles.shape[0])
                    if articles.shape[0] != 0:
                        articles = articles.loc[articles['publish_date_t'] > last_time[theme]]
                        print(articles.shape[0])
                        print(articles.loc[0, 'title'])
                        last_time[theme] = articles.loc[0, 'publish_date_t']

                        print(articles.columns)

                        n = 10
                        if articles.shape[0] <= n:
                            n = articles.shape[0]
                        for i in range(0, n):
                            themes[self.themes_list.index(theme)].set_News({'title': articles.loc[i, 'title'],
                                                                             'text': articles.loc[i, 'text'][:200],
                                                                             'company': articles.loc[i, 'project']})
                    del articles
                    time.sleep(10)
                except:
                    print('Ошибка в получении статей')

    def LentaGetNews(self):
        print('GetLentaNews')
        #bot = self.lq.get()
        themes = []

        for theme in self.themes_list:
            themes.append(self.themes_q_news_l.get())

        last_time = {}
        for theme in self.themes_list:
            last_time[theme] = 0

        newsparser = NewsParser()

        '''
        while True:
            try:
                articles = newsparser.get_articles_n(category=themes_list)
                print(articles.shape[0])
                for theme in themes_list:
                    try:
                        senddf = articles.loc[(articles['bloc']==theme) & (articles['lastmodtime']>last_time[theme])]
                        last_time[theme] = articles.loc[0, 'lastmodtime']
                        for i in range(0, articles.shape[0]):
                            themes[themes_list.index(theme)].set_News({'title': senddf.loc[i, 'title'],
                                                                         'text': senddf.loc[i, 'text'][:500],
                                                                         'company': 'Lenta.ru'})
                        del senddf
                    except:
                        print('роблема с получением тематических статей Lenta')
                del articles
            except:
                print('ошибка в получении статей Lenta')
            time.sleep(30)
        '''
        while True:
            for theme in self.themes_list:
                try:
                    articles = newsparser.get_articles(category=theme, company='lenta')
                    print(theme + ' Lenta.ru', articles.shape[0])
                    if articles.shape[0] != 0:
                        articles = articles.loc[articles['lastmodtime'] > last_time[theme]]
                        print(articles.shape[0])
                        print(articles.loc[0, 'title'])
                        last_time[theme] = articles.loc[0, 'lastmodtime']

                        print(articles.columns)

                        n = 10
                        if articles.shape[0] <= n:
                            n = articles.shape[0]
                        for i in range(0, n):
                            themes[self.themes_list.index(theme)].set_News({'title': articles.loc[i, 'title'],
                                                                             'text': articles.loc[i, 'text'][:200],
                                                                             'company': 'Lenta.ru'})
                    del articles
                    time.sleep(10)
                except:
                    print('Ошибка в получении статей Lenta')

    def TeleBot(self):
        print('TeleBot')
        # chat_id = 955797805
        themes = []
        # buttons = []
        keyboard = types.InlineKeyboardMarkup()

        for theme in self.themes_list:
            themes.append(self.themes_q.get())
            # buttons.append()
            keyboard.add(types.InlineKeyboardButton(theme, callback_data=theme))

        @self.bot.message_handler(commands=['start'])
        def start_message(message):
            self.bot.send_message(message.chat.id, "Привет ✌️ ")
            self.bot.send_message(message.chat.id, text='Выберите темы', reply_markup=keyboard)
            # chat_id.put(message.chat.id)
            sub = TGUserObserver(message.chat.id, self.bot)
            self.users[message.chat.id] = sub
            df = pd.DataFrame({'ids': self.users.keys()})
            df.to_csv('data.csv')
            del df

            print(message.chat.id)

        @self.bot.message_handler(commands=['stop'])
        def stop_message(message):
            self.bot.send_message(message.chat.id, "Прощай :(")
            for theme in themes:
                theme.detach(self.users[message.chat.id])
            self.users.pop(message.chat.id)
            df = pd.DataFrame({'ids': self.users.keys()})
            df.to_csv('data.csv')
            del df

            print(message.chat.id)

        @self.bot.callback_query_handler(func=lambda call: True)
        def handle(call):
            # bot.send_message(call.message.chat.id, 'Set theme to: {}'.format(str(call.data)))
            if call.message.chat.id not in self.users.keys():
                sub = TGUserObserver(call.message.chat.id, self.bot)
                self.users[call.message.chat.id] = sub
                self.bot.answer_callback_query(call.id, 'Тема: {} выбрана'.format(str(call.data)))
                print('Тема ' + str(call.data) + ' добавлена')
                themes[self.themes_list.index(str(call.data))].subscribe(self.users[call.message.chat.id])
            else:
                if themes[self.themes_list.index(str(call.data))].in_subs(self.users[call.message.chat.id]):
                    self.bot.answer_callback_query(call.id, 'Тема: {} удалена'.format(str(call.data)))
                    print('Тема ' + str(call.data) + ' удалена')
                    themes[self.themes_list.index(str(call.data))].detach(self.users[call.message.chat.id])
                else:
                    self.bot.answer_callback_query(call.id, 'Тема: {} выбрана'.format(str(call.data)))
                    print('Тема ' + str(call.data) + ' добавлена')
                    themes[self.themes_list.index(str(call.data))].subscribe(self.users[call.message.chat.id])

        @self.bot.message_handler(commands=['темы'])
        def message(message):
            # bot.send_message(message.chat.id, text='Выберите темы', reply_markup=keyboard)
            # chat_id.put(message.chat.id)
            # print(message.chat.id)
            if message.chat.id not in self.users.keys():
                sub = TGUserObserver(message.chat.id, self.bot)
                self.users[message.chat.id] = sub
            res = 'Вот твои темы: '
            for theme in self.themes_list:
                if themes[self.themes_list.index(theme)].in_subs(self.users[message.chat.id]):
                    res += theme + ', '
            if res == 'Вот твои темы: ':
                self.bot.send_message(message.chat.id, 'Как же так, Вы ещё не выбрали тему\nДержите панель:')
                self.bot.send_message(message.chat.id, text='Выберите темы', reply_markup=keyboard)
            else:
                self.bot.send_message(message.chat.id, res[:-2])

        '''@self.bot.message_handler(commands=['пидр'])
        def start_message(message):
            self.bot.send_message(message.chat.id, "Сам")'''

        self.bot.infinity_polling()

if __name__ == '__main__':
    main = Main()
    main.main()










    '''
    @staticmethod
    def get_url_rbc(query, project, category: list, dateFrom, dateTo, page):
        urls = []
        for i in category:
            url = 'https://www.rbc.ru/search/ajax/?'
            url += 'query=' + str(query)
            url += '&project=' + str(project)
            url += '&category=' + str(i)
            url += '&dateFrom=' + str(dateFrom)
            url += '&dateTo=' + str(dateTo)
            url += '&page=' + str(page)
            urls.append(url)
        return urls


    def get_url_lenta(self, theme: list):
        urls = []
        for i in range(0, len(theme)):
            url = 'https://lenta.ru/search/v2/process?sort=2'
            url += '&bloc=' + str(self.d[theme[i]])
            urls.append(url)
        return urls

    def get_articles_from_url(self, urls: list, company):
        try:
            r = [grequests.get(u) for u in urls]
            pages = []
            print(grequests.map(r))
            for i in grequests.map(r):
                try:
                    if company == 'rbc':
                        page = i.json()['items']
                    else:
                        page = i.json()['matches']
                except:
                    print('ошибка в выполнении запроса к сайту')
                pages.append(page)

            pagedf = pd.DataFrame()
            for page in pages:
                pagedf = pd.concat([pagedf, pd.DataFrame(page)[0:5]], axis=0)

            pagedf['text'] = ''

            texts = []
            print(pagedf.shape)
            if company == 'rbc':
                urls = pagedf['fronturl'].to_list()
            else:
                urls = pagedf['url'].to_list()
            r = [grequests.get(u, timeout=30) for u in urls]
            print(grequests.map(r))

            for i in grequests.map(r):
                soup = bs(i.text, features="lxml")
                p_text = soup.find_all('p')
                if p_text:
                    text = ' '.join(map(lambda x:
                                        x.text.replace('<br />', '\n').strip(),
                                        p_text))
                else:
                    text = None

                if company != 'rbc' and str(text) != 'None':
                    text = text.split('/')[-1]
                texts.append(text)
            pagedf['text'] = texts
            pagedf = pagedf.loc[pagedf['text'].notnull()]
            if company != 'rbc':
                pagedf['bloc'] = pagedf['bloc'].astype(int)
                pagedf['bloc'] = pagedf['bloc'].map(self.i_d)

            return pagedf
        except:
            return pd.DataFrame()

    def get_articles_n(self, query='', project='', category=[''], dateFrom='', dateTo='', page='0', company='rbc'):
        if company == 'rbc':
            url = self.get_url_rbc(query, project, category, dateFrom, dateTo, page)
        else:
            url = self.get_url_lenta(category)
        return self.get_articles_from_url(url, company)

    @staticmethod
    def _get_url(query, project, category, dateFrom, dateTo, page):
        url = 'https://www.rbc.ru/search/ajax/?'
        url += 'query=' + str(query)
        url += '&project=' + str(project)
        url += '&category=' + str(category)
        url += '&dateFrom=' + str(dateFrom)
        url += '&dateTo=' + str(dateTo)
        url += '&page=' + str(page)
        return url

    @staticmethod
    def _get_articles_from_url(url):
        try:
            r = rq.get(url)
            page = ''
            try:
                page = r.json()['items']
            except:
                print('ошибка в выполнении запроса к сайту')
            pagedf = pd.DataFrame(page)

            pagedf['text'] = ''
            texts = []
            for i in range(0, pagedf.shape[0]):
                url = pagedf.loc[i, 'fronturl']
                r = rq.get(url)
                soup = bs(r.text, features="lxml")
                p_text = soup.find_all('p')
                if p_text:
                    text = ' '.join(map(lambda x:
                                        x.text.replace('<br />', '\n').strip(),
                                        p_text))
                else:
                    text = None
                texts.append(text)
            pagedf['text'] = texts

            return pagedf
        except:
            return pd.DataFrame()

    def get_articles(self, query='', project='', category='', dateFrom='', dateTo='', page='0'):
        url = self._get_url(query, project, category, dateFrom, dateTo, page)
        return self._get_articles_from_url(url)
    '''