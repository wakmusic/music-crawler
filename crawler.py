import json
from pymysql import Connection, connect
from pymysql.cursors import Cursor, DictCursor
import time
import gspread
from gspread import Worksheet
import schedule
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from waktube import YouTube
from typing import Dict, TypedDict, List, Union, Optional, Tuple
from threading import Thread
from copy import deepcopy

class Database(TypedDict):
    host: str
    port: int
    username: str
    password: str
    name: str

class Column(TypedDict):
    title: str
    url: str
    reaction: str
    date: str
    remix: str
    artists: Dict[str, str]

class Config(TypedDict):
    database: Database
    column: Column

class SongData(TypedDict):
    song_id: str
    title: str
    artist: str
    remix: Optional[str]
    reaction: Optional[str]
    date: int
    start: int
    end: int

class SelectSongData(TypedDict):
    id: int
    song_id: str
    title: str
    artist: str
    remix: Optional[str]
    reaction: Optional[str]
    date: int
    start: int
    end: int

class InsertSongData(TypedDict):
    song_id: str
    title: str
    artist: str
    remix: Optional[str]
    reaction: Optional[str]
    date: int
    start: int
    end: int

class SelectChartData(TypedDict):
    id: int
    song_id: int
    views: int
    increase: int
    last: int

class CurrentRankInfoData(TypedDict):
    id: int
    song_id: int
    views: int
    current_rank: int
    

with open("config.json", encoding="utf-8-sig") as file:
    config: Config = json.load(file)

def get_artists(conn: Connection) -> Dict[str, List[Union[str, int, None]]]:
    with conn.cursor(cursor=Cursor) as cursor:
        cursor.execute("SELECT * FROM artist")
        rows = cursor.fetchall()

    data: Dict[str, int] = {}

    for row in rows:
        data[row[2]] = row[0]

    return data

def get_sheet_index(nameRow: List[str]) -> Dict[str, int]:
    data: Dict[str, int] = {}
    for idx, value in enumerate(nameRow):
        data[value] = idx
    return data

def get_charts_to_update(time: datetime) -> List[str]:
    charts: List[str] = ["hourly"]
    if time.day == 1 and time.hour == 0:
        charts.append("monthly")
    if time.weekday() == 0 and time.hour == 0:
        charts.append("weekly")
    if time.hour == 0:
        charts.append("daily")
    
    charts.append("total")

    return charts

def get_views(conn: Connection, song_id: str, reaction: str) -> int:
    count = 0
    views = 0
    with conn.cursor(Cursor) as cursor:
        while True:
            try:
                views = YouTube(f"https://youtu.be/{song_id}").views
                if reaction != "":
                    views += YouTube(f"https://youtu.be/{reaction}").views
                break
            except:
                if count > 5:
                    cursor.execute(
                        'SELECT * FROM chart_total WHERE song_id = %s',
                        (id,)
                    )
                    total = cursor.fetchone()
                    if not total:
                        views = 0
                    else:
                        views = int(total[2])
                    print(f"Failed to get {id}.")
                    break
                count += 1
                pass

    return views

def get_chart(conn: Connection, chart: str) -> Tuple[SelectChartData]:
    with conn.cursor(DictCursor) as cursor:
        order = "views" if chart == "total" else "increase"
        cursor.execute(f"SELECT * FROM chart_{chart} ORDER BY {order} DESC")
        chart_datas = cursor.fetchall()
    
    return chart_datas

def get_chart_current_info(chart_datas: Tuple[SelectChartData]) -> Dict[str, CurrentRankInfoData]:
    current_rank_info: Dict[str, CurrentRankInfoData] = {}
    for idx, chart_data in enumerate(chart_datas):
        current_rank_info[chart_data["song_id"]] = {
            "id": chart_data["id"],
            "song_id": chart_data["song_id"],
            "views": chart_data["views"],
            "current_rank": idx + 1
        }
    return current_rank_info

def get_all_songs_views(conn: Connection, songs: Tuple[SelectSongData]) -> Dict[str, int]:
    print("Start getting views from youtube.")
    all_song_views = {}
    for song in songs:
        views = get_views(conn=conn, song_id=song["song_id"], reaction=song["reaction"])
        all_song_views[song["song_id"]] = views
    
    return all_song_views


def check_if_song_exist(conn: Connection, song_id: str) -> bool:
    with conn.cursor(Cursor) as cursor:
        cursor.execute("SELECT * FROM song WHERE song_id = %s", (song_id,))
        song = cursor.fetchone()
    
    return song != None

def create_song(conn: Connection, data: InsertSongData) -> bool:
    with conn.cursor(Cursor) as cursor:
        try:
            cursor.execute(
                "INSERT INTO song (song_id, title, artist, remix, reaction, date, start, end) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", 
                (data['song_id'], data['title'], data["artist"], data["remix"], data["reaction"], data["date"], data["start"], data["end"])
            )
            return True
        except:
            return False
        
def update_lyrics(conn: Connection) -> None:
    print("Start Update Lyrics.")
    with conn.cursor(cursor=Cursor) as cursor:
        pass

def update_songs(conn: Connection, songs: Dict[str, SongData]) -> Tuple[SelectSongData]:
    songs_copy = deepcopy(songs)
    with conn.cursor(Cursor) as cursor:
        cursor.execute("SELECT * FROM song")
        db_songs = cursor.fetchall()

        for db_song in db_songs:
            if db_song[1] in songs_copy:
                song = songs_copy[db_song[1]]
                cursor.execute(
                    "UPDATE song SET title=%s, artist=%s, remix=%s, reaction=%s, date=%s, start=%s, end=%s WHERE song_id=%s",
                    (
                        song["title"], 
                        song["artist"], 
                        song["remix"], 
                        song["reaction"], 
                        song["date"], 
                        song["start"], 
                        song["end"],
                        song["song_id"]
                    )
                )
                del songs_copy[db_song[1]]
            else:
                cursor.execute(
                    "DELETE FROM song WHERE song_id=%s",
                    (db_songs[1])
                )
                del songs_copy[db_song[1]]
        
        insert_songs = [tuple(song.values()) for song in songs_copy.values()]
        cursor.executemany(
            "INSERT INTO song (song_id, title, artist, remix, reaction, date, start, end) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            insert_songs
        )
        conn.commit()
    
    with conn.cursor(DictCursor) as cursor:
        cursor.execute("SELECT * FROM song")
        results: Tuple[SelectSongData] = cursor.fetchall()
    
    return results

def update_charts(conn: Connection, songs: Tuple[SelectSongData], charts: List[str]) -> None:
    print("Start running update_charts.")
    all_song_views = get_all_songs_views(conn=conn, songs=songs)

    with conn.cursor(Cursor) as cursor:
        for chart in charts:
            chart_data = get_chart(conn=conn, chart=chart)
            current_chart_infos: Dict[str, CurrentRankInfoData] = get_chart_current_info(chart_datas=chart_data)
            chart_input_data = []
            for song in songs:
                views = all_song_views[song["song_id"]]
                if song["id"] in current_chart_infos:
                    current_chart_info = current_chart_infos[song["id"]]
                    chart_input_data.append((song["id"], views, (views - current_chart_info["views"]), current_chart_info["current_rank"]))
                else:
                    chart_input_data.append((song["id"], views, (views - 0), 0))
            
            try:
                cursor.execute(f"DELETE FROM chart_{chart}")
                cursor.executemany(f"INSERT INTO chart_{chart} (song_id, views, increase, last) VALUES (%s, %s, %s, %s)", chart_input_data)
                print(f"Successfully inserted all data to chart {chart}")
            except:
                print(f"Failed to insert data to chart {chart}")
        conn.commit()

def update_artists(conn: Connection, songs: Tuple[SelectSongData], artists_songs: Dict[str, List[str]], artists: Dict[str, int]) -> None:
    print("Start running update_artists.")
    song_dict = {}
    for song in songs:
        song_dict[song["song_id"]] = song["id"]

    insert_data = []
    for artist_name, song_ids in artists_songs.items():
        artist_id = artists[artist_name]
        for song_id in song_ids:
            insert_data.append((artist_id, song_dict[song_id]))

    with conn.cursor(Cursor) as cursor:
        cursor.execute("DELETE FROM artist_song")
        cursor.executemany("INSERT INTO artist_song (artist_id, song_id) VALUES (%s, %s)", insert_data)
    conn.commit()

def work() -> None:
    conn = connect(
        host=config["database"]["host"], 
        port=config["database"]["port"], 
        user=config["database"]["username"], 
        password=config["database"]["password"], 
        database=config["database"]["name"]
        )
    now = datetime.now()

    print("Successfully connected to database.")

    if now.hour == 1:
        try:
            lyric_thread = Thread(target=update_lyrics, args=(conn,))
            lyric_thread.start()
        except:
            print("Failed to update lyrics.")

    while True:
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                filename='oauth.json', scopes=["https://www.googleapis.com/auth/spreadsheets"], 
            )
            gc = gspread.authorize(creds)
            spreadsheet = gc.open_by_url(
                url="https://docs.google.com/spreadsheets/d/1n8bRCE_OBUOND4pfhlqwEBMR6qifVLyWk5YrHclRWfY"
            )
            worksheet = spreadsheet.get_worksheet(1)
            break
        except Exception as e:
            print(e)
            print("Error while loading Google Spreadsheet.")
            time.sleep(2)
    print("Successfully retrieved spreadsheet.")

    values = worksheet.get_all_values()
    rows = values[1:]

    columns = get_sheet_index(nameRow=values[0])
    artists = get_artists(conn=conn)
    charts = get_charts_to_update(time=now)

    songs: Dict[str, SongData] = {}
    artists_songs: Dict[str, List[str]] = {}

    for row in rows:
        url: str = row[columns[config["column"]["url"]]]

        if url == "0" or url == "":
            continue
        
        full_title: str = row[columns[config["column"]["title"]]]

        try:
            title: str = full_title.split(" - ")[1]
        except IndexError:
            continue
        
        artist: str = full_title.split(" - ")[0].replace(" x ", ", ")
        id = url.split("/")[-1]

        reaction: str = row[columns[config["column"]["reaction"]]]
        reaction = reaction.replace("https://youtu.be/", "")

        if reaction == "0":
            reaction = ""

        date: int = int(row[columns[config["column"]["date"]]].replace(".", ""))
        remix: str = row[columns[config["column"]["remix"]]]

        songs[id] = {
            'song_id': id,
            'title': title,
            'artist': artist,
            'remix': remix,
            'reaction': reaction,
            'date': date,
            'start': 0,
            'end': 0
        }

        for artist_id, name in config["column"]["artists"].items():
            if artist_id not in artists_songs:
                artists_songs[artist_id] = []

            is_artist = row[columns[name]]
            if is_artist == "":
                continue
            
            artists_songs[artist_id].append(id)

    db_songs = update_songs(conn=conn, songs=songs)

    update_artists(conn=conn, songs=db_songs, artists_songs=artists_songs, artists=artists)
    update_charts(conn=conn, songs=db_songs, charts=charts)
    
    with conn.cursor(Cursor) as cursor:
        cursor.execute("UPDATE chart_updated SET time = %s", (int(time.time()), ))
    conn.commit()

    conn.close()
    print("Successfully updated wakmusic chart data.")
        

def add_work_hourly(scheduler) -> None:
    for j in range(0, 24):
        h: str = str(j)
        if j < 10:
            h = f"0{str(j)}"
        scheduler.every().day.at(f"{h}:00").do(work)

if __name__ == "__main__":
    add_work_hourly(schedule)

    while True:
        schedule.run_pending()
        time.sleep(1)







# with open("config.json", encoding="utf-8-sig") as file:
#     js = json.load(file)


# def get_last(cursor, _id, t):
#     data = cursor.execute(f'SELECT * FROM total WHERE id = "{_id}"').fetchone()
#     if data:
#         field = "views"
#         if t != "total":
#             field = "increase"
#         songs = cursor.execute(f"SELECT * FROM {t} ORDER BY {field} DESC").fetchall()
#         last = 1
#         for song in songs:
#             if song[0] == _id:
#                 break
#             last += 1

#         if last == len(songs) + 1:
#             last = 0

#         if t == "total":
#             last_views = data[6]
#         else:
#             s = cursor.execute(f'SELECT * FROM {t} WHERE id = "{_id}"').fetchone()
#             last_views = 0
#             if s:
#                 try:
#                     last_views = int(s[1])
#                 except:
#                     pass
#     else:
#         last_views = 0
#         last = 0
#     return last, last_views


# def insert(charts, cursor, _id, views, t):
#     last, last_views = get_last(cursor, _id, t)
#     temp = (_id, views, views - last_views, last)
#     charts[t].append(temp)


# def update_lyrics():
#     try:
#         creds = ServiceAccountCredentials.from_json_keyfile_name(
#             "config.json", scopes=["https://www.googleapis.com/auth/spreadsheets"]
#         )
#         gc = gspread.authorize(creds)

#         spreadsheet = gc.open_by_url(
#             url="https://docs.google.com/spreadsheets/d/1cplAy6pfH_X4W-odwuZgaRVuvEfUI88NOAonSkThRbE"
#         )
#         current = spreadsheet.get_worksheet(0)
#         col = current.col_values(9)[2:]
#     except:
#         return

#     last = spreadsheet.get_worksheet(1)
#     names = last.col_values(1)[1:]
#     count = last.col_values(2)[1:]

#     data = {}
#     for i in range(len(names)):
#         data[names[i]] = int(count[i])

#     for c in col:
#         try:
#             data[c] += 1
#         except KeyError:
#             data[c] = 1

#     result = sorted(data.items(), key=lambda x: x[1], reverse=True)
#     result_list = (
#         ", ".join([d[0] for d in result]).replace("니엔, ", "").replace("아트아스, ", "")
#     )
#     pre = "서선유, 김모건, 옹냐, 인턴 이기자, 여비날, 배식, 탈영병, "

#     final = pre + result_list

#     conn = sqlite3.connect(js["database_src"] + "static.db")
#     cursor = conn.cursor()

#     cursor.execute(f'UPDATE teams SET name = "{final}" WHERE team = "special2"')
#     conn.commit()


# def check_sheet():
#     artists = {
#         "woowakgood": [],
#         "ine": [],
#         "jingburger": [],
#         "lilpa": [],
#         "jururu": [],
#         "gosegu": [],
#         "viichan": [],
#         "chunshik": [],
#         "kwonmin": [],
#         "kimchimandu": [],
#         "nosferatuhodd": [],
#         "dandapbug": [],
#         "dopamine": [],
#         "dokkhye": [],
#         "roentgenium": [],
#         "haku": [],
#         "bujungingan": [],
#         "secretto": [],
#         "businesskim": [],
#         "friedshrimp": [],
#         "sophia": [],
#         "wakphago": [],
#         "leedeoksoo": [],
#         "carnarjungtur": [],
#         "callycarly": [],
#         "pungsin": [],
#         "freeter": [],
#         "rusuk": [],
#         "hikiking": [],
#     }
#     charts = {"total": [], "hourly": [], "daily": [], "weekly": [], "monthly": []}
#     complete = ["hourly"]

#     now = datetime.now()
#     if now.day == 1 and now.hour == 0:
#         complete.append("monthly")
#     if now.weekday() == 0 and now.hour == 0:
#         complete.append("weekly")
#     if now.hour == 0:
#         complete.append("daily")
#     if now.hour == 1:
#         try:
#             update_lyrics()
#         except:
#             pass

#     while True:
#         try:
#             creds = ServiceAccountCredentials.from_json_keyfile_name(
#                 "config.json", scopes=["https://www.googleapis.com/auth/spreadsheets"]
#             )
#             gc = gspread.authorize(creds)
#             spreadsheet = gc.open_by_url(
#                 url="https://docs.google.com/spreadsheets/d/1n8bRCE_OBUOND4pfhlqwEBMR6qifVLyWk5YrHclRWfY"
#             )
#             worksheet = spreadsheet.get_worksheet(1)
#             break
#         except:
#             print("Error while loading Google Spreadsheet.")

#     print("Sheet Data Retrieved")

#     conn = sqlite3.connect(js["database_src"] + "charts.db")
#     cursor = conn.cursor()

#     values = worksheet.get_all_values()
#     c = js["column"]

#     for v in values[1:]:
#         url = v[c["url"]]
#         if url == "0" or url == "":
#             continue

#         try:
#             title = v[c["title"]].split(" - ")[1]
#         except IndexError:
#             continue

#         artist = v[c["title"]].split(" - ")[0].replace(" x ", ", ")
#         _id = url.split("/")[-1]

#         reaction = v[c["reaction"]].replace("https://youtu.be/", "")
#         if reaction == "0":
#             reaction = ""

#         count = 0
#         while True:
#             try:
#                 views = YouTube(f"https://youtu.be/{_id}").views
#                 if reaction != "":
#                     views += YouTube(f"https://youtu.be/{reaction}").views
#                 break
#             except:
#                 if count > 5:
#                     query = cursor.execute(
#                         f'SELECT * FROM total WHERE id = "{_id}"'
#                     ).fetchone()
#                     if not query:
#                         views = 0
#                     else:
#                         views = int(query[6])
#                     print(f"Failed to get {_id}.")
#                     break
#                 count += 1
#                 pass

#         date = v[c["date"]].replace(".", "")
#         remix = v[c["remix"]]

#         if now.weekday() == 0 and now.hour == 0:
#             last, last_views = get_last(cursor, _id, "total")
#             charts["total"].append(
#                 [_id, title, artist, remix, reaction, date, views, last]
#             )
#         else:
#             data = cursor.execute(f'SELECT * FROM total WHERE id = "{_id}"').fetchone()
#             l_views = 0
#             if data:
#                 l_views = data[7]
#             charts["total"].append(
#                 [_id, title, artist, remix, reaction, date, views, l_views]
#             )

#         insert(charts, cursor, _id, views, "hourly")

#         now = datetime.now()
#         if now.day == 1 and now.hour == 0:
#             insert(charts, cursor, _id, views, "monthly")
#         if now.weekday() == 0 and now.hour == 0:
#             insert(charts, cursor, _id, views, "weekly")
#         if now.hour == 0:
#             insert(charts, cursor, _id, views, "daily")

#     for v in values[1:]:
#         url = v[c["url"]]
#         if url == "0":
#             continue
#         _id = url.split("/")[-1]
#         artists_rev = {v: k for k, v in js["column"].items()}
#         for i in range(22, 56):
#             if 23 <= i <= 24 or 31 <= i <= 33:
#                 continue

#             if v[i] != "":
#                 try:
#                     artists[artists_rev[i]].append(_id)
#                 except KeyError:
#                     pass

#     cursor.execute("DELETE FROM artists")
#     art = []
#     for a in artists:
#         art.append((a, ",".join(artists[a])))
#     print("Saved Artists Data Successfully.")

#     cursor.executemany("INSERT INTO artists VALUES(?, ?)", art)
#     cursor.execute(f'UPDATE updated SET time = "{int(time.time())}"')

#     cursor.execute("DELETE FROM total")

#     ids = []
#     total_data = []
#     for ct in charts["total"]:
#         if ct[0] not in ids:
#             ids.append(ct[0])
#             total_data.append(ct)

#     cursor.executemany("INSERT INTO total VALUES (?, ?, ?, ?, ?, ?, ?, ?)", total_data)

#     for t in complete:
#         cursor.execute(f"DELETE FROM {t}")
#         try:
#             cursor.executemany(
#                 f"INSERT INTO {t} VALUES (?, ?, ?, ?)", list(set(charts[t]))
#             )
#         except:
#             print(f"Error while inserting chart data {t}")

#     conn.commit()
#     conn.close()

#     print("Saved all data successfully.")


# for j in range(0, 24):
#     h = j
#     if j < 10:
#         h = f"0{j}"
#     schedule.every().day.at(f"{h}:00").do(check_sheet)

# while True:
#     schedule.run_pending()
#     time.sleep(1)