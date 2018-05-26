from twitch import TwitchClient
import datetime
import time
import mysql.connector
import urllib3
import ast
urllib3.disable_warnings()


class Observer:
    """

    This class will do the actual data mining (via API) from twitch on the popular streamers and their viewers.

    The observer uses the TwitchClient module for just reduced duplication of efforts.

    It will take the information from the API and will pass it on to the Chronicler.

    """
    def __init__(self, twitch_api_key, sql_user, sql_pass, sql_host, sql_database):

        self.key = twitch_api_key
        self.client = TwitchClient(client_id=twitch_api_key)
        self.http = urllib3.PoolManager()
        self.sql_connector = mysql.connector.connect(user=sql_user, password=sql_pass,
                                                     host=sql_host,
                                                     database=sql_database)
        runs = 24

        current_minutes = datetime.datetime.now().minute
        if current_minutes != 0:
            sleep_time = (60 - current_minutes) * 60
            print("Sleeping for an flat hour: " + str(sleep_time) + " seconds")
            time.sleep(sleep_time)

        while runs > 0:
            print("This many runs left: " + str(runs))
            start = datetime.datetime.now()
            games = self.client.games.get_top(limit=50)
            for game in games:
                self.run_query(game)
            wait_time = (datetime.datetime.now() - start).seconds
            print("Waiting this long before starting the next run: " + str(3600 - wait_time))
            runs -= 1
            time.sleep(3600 - wait_time)

    def run_query(self, game):
        print("Grabbing streamers playing: " + game["game"]["name"])
        streams = self.client.streams.get_live_streams(game=game["game"]["name"], limit=50)
        for stream in streams:
            chatters_page = self.http.request('GET', "https://tmi.twitch.tv/group/user/" +
                                              stream['channel']['name'] + "/chatters")
            try:
                chatters = ast.literal_eval(str(chatters_page.data, 'utf-8'))['chatters']['viewers']
            except TypeError:
                print(chatters_page.data)
                continue
            self.save_query(stream['channel']['name'], game["game"]["name"], chatters, stream)

    def save_query(self, streamer, game, chatters, stream):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        add_data2 = ("INSERT INTO viewers "
                     "(Name, Streamer, Game, Time) "
                     "VALUES (%s, %s, %s, %s)")

        add_data = ("INSERT INTO streamer_pulls "
                    "(name, game, language, viewers, followers, views, pull_time) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)")

        cursor1 = self.sql_connector.cursor()
        counter = 0
        pre_dump_list = [stream["channel"]["name"], stream["game"], stream["channel"]["language"], stream["viewers"],
                         stream["channel"]["followers"], stream["channel"]["views"]]
        for entry in pre_dump_list:
            if isinstance(entry, str):
                if len(entry) > 50:
                    pre_dump_list[counter] = entry[:49]
            counter += 1

        dump = (stream["channel"]["name"],
                stream["game"],
                stream["channel"]["language"],
                stream["viewers"],
                stream["channel"]["followers"],
                stream["channel"]["views"],
                timestamp)
        cursor1.execute(add_data, dump)

        for chatter in chatters:
            pre_dump_list = [chatter, streamer, game]
            counter = 0
            for entry in pre_dump_list:
                if isinstance(entry, str):
                    if len(entry) > 50:
                        pre_dump_list[counter] = entry[:49]
                counter += 1
            dump = (chatter,
                    streamer,
                    game,
                    timestamp
                    )

            cursor1.execute(add_data2, dump)

        self.sql_connector.commit()


class Analyzer:
    """

    The analyzer's job is more for the finished product once the data has been collected. This class will read the
    information stored by the Chronicler in the SQL database and will track trends of viewer migration.

    """

    def __init__(self):

        pass


def get_configs():

    config_file = open('connection.cfg', 'r')
    config_data = config_file.read()
    config_data_split = config_data.split('\n')
    config_dict = {}
    for line in config_data_split:
        split_line = line.split("=")
        config_dict[split_line[0]] = split_line[1]

    return config_dict


configs = get_configs()
password = input("Enter your password for your sql username: ")
obs = Observer(configs['twitch_api_key'], configs['mysql_username'], password, configs['mysql_host'],
               configs['mysql_database'])
# obs.run_query()
