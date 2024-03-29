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
from queue import Queue


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
    start: str
    end: str
    order: str
    keyword: str
    significant: str
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
    order: int


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


class SelectKeywordData(TypedDict):
    id: int
    keyword: str


with open("./configs/config.json", encoding="utf-8-sig") as file:
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


def get_views(song_id: str, reaction: str, queue: Queue) -> None:
    count = 0
    views = 0
    while True:
        try:
            views = YouTube(f"https://youtu.be/{song_id}").views
            if reaction != "":
                views += YouTube(f"https://youtu.be/{reaction}").views
            break
        except:
            if count > 5:
                views = 0
                print(f"Failed to get {song_id}.")
                break
            count += 1
            pass

    queue.put((song_id, views))


def get_views_many(
    conn: Connection, queue: Queue, songs: List[Tuple[str, str, int]]
) -> None:
    for song in songs:
        get_views(song_id=song[0], reaction=song[1], queue=queue)


def get_chart(conn: Connection, chart: str) -> Tuple[SelectChartData]:
    with conn.cursor(DictCursor) as cursor:
        order = "views" if chart == "total" else "increase"
        cursor.execute(f"SELECT * FROM chart_{chart} ORDER BY {order} DESC")
        chart_datas = cursor.fetchall()

    return chart_datas


def get_chart_current_info(
    chart_datas: Tuple[SelectChartData],
) -> Dict[str, CurrentRankInfoData]:
    current_rank_info: Dict[str, CurrentRankInfoData] = {}
    for idx, chart_data in enumerate(chart_datas):
        current_rank_info[chart_data["song_id"]] = {
            "id": chart_data["id"],
            "song_id": chart_data["song_id"],
            "views": chart_data["views"],
            "current_rank": idx + 1,
        }
    return current_rank_info


def get_all_songs_views(
    conn: Connection, songs: Tuple[SelectSongData]
) -> Dict[str, int]:
    print("Start getting views from youtube.")

    queue = Queue(maxsize=len(songs))
    all_song_views = {}
    threads: List[Thread] = []
    temp_songs: List[Tuple[str, str, int]] = []

    for song in songs:
        temp_songs.append((song["song_id"], song["reaction"], song["id"]))

        if len(temp_songs) == 30:
            t = Thread(target=get_views_many, args=(conn, queue, temp_songs))
            t.start()
            threads.append(t)
            temp_songs = []

    if len(temp_songs) != 0:
        t = Thread(target=get_views_many, args=(conn, queue, temp_songs))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    for data in queue.queue:
        all_song_views[data[0]] = data[1]

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
                (
                    data["song_id"],
                    data["title"],
                    data["artist"],
                    data["remix"],
                    data["reaction"],
                    data["date"],
                    data["start"],
                    data["end"],
                ),
            )
            return True
        except:
            return False


def update_lyrics() -> None:
    print("Start Update Lyrics.")

    conn = connect(
        host=config["database"]["host"],
        port=config["database"]["port"],
        user=config["database"]["username"],
        password=config["database"]["password"],
        database=config["database"]["name"],
    )
    print("update_lyrics: Successfully connected to database.")

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "configs/oauth.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc = gspread.authorize(creds)

        spreadsheet = gc.open_by_url(
            url="https://docs.google.com/spreadsheets/d/1cplAy6pfH_X4W-odwuZgaRVuvEfUI88NOAonSkThRbE"
        )
        current = spreadsheet.get_worksheet(0)
        old = spreadsheet.get_worksheet(1)
    except:
        print("update_lyrics: Failed to update lyrics workers.")
        return

    current_song_workers = current.col_values(9)[2:]
    old_workers_songs_rows = old.get_all_values()[1:]
    workers: Dict[str, int] = {}

    for row in old_workers_songs_rows:
        if row[4] == "team":
            continue
        if row[0] not in workers:
            workers[row[0]] = 0
        workers[row[0]] += int(row[3])

    for worker in current_song_workers:
        if worker not in workers:
            workers[worker] = 0
        workers[worker] += 1

    sorted_workers = sorted(workers.items(), key=lambda x: x[1], reverse=True)
    worker_names = list(map(lambda x: x[0], sorted_workers))

    result = ", ".join([worker[0] for worker in sorted_workers])
    special = "서선유, 김모건, 옹냐, 인턴 이기자, 여비날, 배식, 탈영병, "
    result = special + result

    
    pc_lyrics_const = ['감람스톤', '활짝핀웃음꽃', '샤비라비', '아기독수리이', 'Lami']

    team_pc_lyrics = []
    for idx, value in enumerate(pc_lyrics_const + worker_names):
        team_pc_lyrics.append(("가사", value, "lyrics", "member", idx + 1))

    try:
        try:
            with conn.cursor(cursor=Cursor) as cursor:
                cursor.execute(
                    "UPDATE team SET name=%s WHERE team=%s", (result, "special2")
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("update_lyrics: query 1 failed.")
            print(e)

        try:
            with conn.cursor(Cursor) as cursor:
                cursor.execute("DELETE FROM team_pc WHERE `type` = %s", ("lyrics",))
                cursor.executemany(
                    "INSERT INTO team_pc (`team`, `member`, `type`, `role`, `order`) VALUES (%s, %s, %s, %s, %s)",
                    team_pc_lyrics,
                )

            conn.commit()
        except Exception as e:
            conn.rollback()
            print("update_lyrics: query 2 failed.")
            print(e)
    finally:
        conn.close()

    print("update_lyrics: Successfully updated lyrics workers.")


def update_songs(conn: Connection, songs: Dict[str, SongData]) -> Tuple[SelectSongData]:
    songs_copy = deepcopy(songs)

    try:
        with conn.cursor(Cursor) as cursor:
            cursor.execute("SELECT * FROM song")
            db_songs = cursor.fetchall()

            for db_song in db_songs:
                if db_song[1] in songs_copy:
                    song = songs_copy[db_song[1]]
                    if db_song[12] != None:
                        cursor.execute(
                            "UPDATE song SET title=%s, artist=%s, remix=%s, reaction=%s, date=%s, start=%s, end=%s, `order`=%s, `deleted_at` = NULL WHERE song_id=%s",
                            (
                                song["title"],
                                song["artist"],
                                song["remix"],
                                song["reaction"],
                                song["date"],
                                song["start"],
                                song["end"],
                                song["order"],
                                song["song_id"],
                            ),
                        )
                    else:
                        cursor.execute(
                            "UPDATE song SET title=%s, artist=%s, remix=%s, reaction=%s, date=%s, start=%s, end=%s, `order`=%s WHERE song_id=%s",
                            (
                                song["title"],
                                song["artist"],
                                song["remix"],
                                song["reaction"],
                                song["date"],
                                song["start"],
                                song["end"],
                                song["order"],
                                song["song_id"],
                            ),
                        )

                    del songs_copy[db_song[1]]
                else:
                    if db_song[12] != None:
                        continue

                    cursor.execute(
                        "UPDATE song SET `deleted_at` = UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) WHERE song_id=%s",
                        (db_song[1],),
                    )

            insert_songs = [tuple(song.values()) for song in songs_copy.values()]
            cursor.executemany(
                "INSERT INTO song (song_id, title, artist, remix, reaction, date, start, end, `order`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                insert_songs,
            )

        conn.commit()
    except Exception as e:
        conn.rollback()

        print("update_songs: query failed.")
        print(e)

    with conn.cursor(DictCursor) as cursor:
        cursor.execute("SELECT * FROM song")
        results: Tuple[SelectSongData] = cursor.fetchall()

    return results


def update_charts(
    conn: Connection, songs: Tuple[SelectSongData], charts: List[str]
) -> None:
    print("Start running update_charts.")
    all_song_views = get_all_songs_views(conn=conn, songs=songs)
    print("update_charts: successfully fetched all songs views.")

    try:
        with conn.cursor(Cursor) as cursor:
            for chart in charts:
                chart_data = get_chart(conn=conn, chart=chart)
                current_chart_infos: Dict[
                    str, CurrentRankInfoData
                ] = get_chart_current_info(chart_datas=chart_data)
                chart_input_data = []
                for song in songs:
                    views = all_song_views[song["song_id"]]
                    if song["id"] in current_chart_infos:
                        current_chart_info = current_chart_infos[song["id"]]
                        chart_input_data.append(
                            (
                                song["id"],
                                views,
                                (views - current_chart_info["views"]),
                                current_chart_info["current_rank"],
                            )
                        )
                    else:
                        chart_input_data.append((song["id"], views, (views - 0), 0))

                try:
                    cursor.execute(f"DELETE FROM chart_{chart}")
                    cursor.executemany(
                        f"INSERT INTO chart_{chart} (song_id, views, increase, last) VALUES (%s, %s, %s, %s)",
                        chart_input_data,
                    )
                    print(f"Successfully inserted all data to chart {chart}")
                except:
                    print(f"Failed to insert data to chart {chart}")
        conn.commit()
    except Exception as e:
        conn.rollback()

        print("update_charts: query failed.")
        print(e)


def update_artists(
    conn: Connection,
    songs: Tuple[SelectSongData],
    artists_songs: Dict[str, List[str]],
    artists: Dict[str, int],
) -> None:
    print("Start running update_artists.")
    song_dict = {}
    for song in songs:
        song_dict[song["song_id"]] = song["id"]

    insert_data = []
    for artist_name, song_ids in artists_songs.items():
        artist_id = artists[artist_name]
        for song_id in song_ids:
            insert_data.append((artist_id, song_dict[song_id]))
    try:
        with conn.cursor(Cursor) as cursor:
            cursor.execute("DELETE FROM artist_song")
            cursor.executemany(
                "INSERT INTO artist_song (artist_id, song_id) VALUES (%s, %s)",
                insert_data,
            )
        conn.commit()
    except Exception as e:
        conn.rollback()

        print("update_artists: query failed.")
        print(e)


def update_keywords(conn: Connection, keywords: List[str]) -> List[str]:
    print("Start running update_keywords.")
    keywords_copy = deepcopy(keywords)
    try:
        with conn.cursor(Cursor) as cursor:
            cursor.execute("SELECT * FROM `keyword`")
            db_keywords = cursor.fetchall()

            for idx, db_keyword in enumerate(db_keywords):
                if db_keyword[1] not in keywords_copy:
                    cursor.execute(
                        "DELETE FROM `keyword` WHERE `keyword` = %s", (db_keyword[1],)
                    )
                else:
                    del keywords_copy[keywords_copy.index(db_keyword[1])]

            cursor.executemany(
                "INSERT INTO `keyword` (`keyword`) VALUES (%s)", keywords_copy
            )
        conn.commit()
    except Exception as e:
        conn.rollback()

        print("update_keywords: query failed.")
        print(e)

    with conn.cursor(DictCursor) as cursor:
        cursor.execute("SELECT `id`, `keyword` FROM `keyword`")
        results: Tuple[SelectKeywordData] = cursor.fetchall()

    return results


def update_keyword_song(
    conn: Connection,
    keywords: List[SelectKeywordData],
    keyword_song: Dict[str, List[str]],
    songs: Tuple[SelectSongData],
) -> None:
    print("Start running update_keyword_song.")

    song_dict: Dict[str, int] = {}
    for song in songs:
        song_dict[song["song_id"]] = song["id"]

    keyword_dict: Dict[str, SelectKeywordData] = {}
    for keyword in keywords:
        keyword_dict[keyword["keyword"]] = keyword

    insert_data = []
    for keyword, song_ids in keyword_song.items():
        if keyword not in keyword_dict:
            continue
        
        db_keyword = keyword_dict[keyword]
        for song_id in song_ids:
            insert_data.append((db_keyword["id"], song_dict[song_id]))
    try:
        with conn.cursor(Cursor) as cursor:
            cursor.execute("DELETE FROM `keyword_song`")
            cursor.executemany(
                "INSERT INTO `keyword_song` (`keyword_id`, `song_id`) VALUES (%s, %s)",
                insert_data,
            )
        conn.commit()
    except Exception as e:
        conn.rollback()

        print("update_keyword_song: query failed.")
        print(e)


def work() -> None:
    conn = connect(
        host=config["database"]["host"],
        port=config["database"]["port"],
        user=config["database"]["username"],
        password=config["database"]["password"],
        database=config["database"]["name"],
    )
    now = datetime.now()

    print(f"Work {now.hour} started.")
    print("Successfully connected to database.")

    if now.hour == 1:
        try:
            lyric_thread = Thread(target=update_lyrics)
            lyric_thread.start()
        except:
            print("Failed to update lyrics.")

    while True:
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                filename="configs/oauth.json",
                scopes=["https://www.googleapis.com/auth/spreadsheets"],
            )
            gc = gspread.authorize(creds)
            spreadsheet = gc.open_by_url(
                url="https://docs.google.com/spreadsheets/d/1Qm6ARImDRLWI-j30aJ9nwHou5WvSHgCtqPVyIh69l68"
            )
            worksheet = spreadsheet.get_worksheet(1)
            break
        except Exception as e:
            print(e)
            print("Error while loading Google Spreadsheet.")
            time.sleep(2)
    print("Successfully retrieved song spreadsheet.")

    values = worksheet.get_all_values()
    rows = values[1:]

    columns = get_sheet_index(nameRow=values[0])
    artists = get_artists(conn=conn)
    charts = get_charts_to_update(time=now)

    songs: Dict[str, SongData] = {}
    artists_songs: Dict[str, List[str]] = {}
    keywords = []
    keyword_song: Dict[str, List[str]] = {}

    for row in rows:
        significant: str = row[columns[config["column"]["significant"]]]
        significant = significant.strip()
        if significant == "임시삭제":
            continue

        url: str = row[columns[config["column"]["url"]]]

        if url == "0" or url == "":
            continue

        full_title: str = row[columns[config["column"]["title"]]]

        try:
            title: str = full_title.split(" - ")[1]
        except:
            print(f"Failed to get title from full_title: {full_title}")
            continue

        artist: str = full_title.split(" - ")[0].replace(" x ", ", ")
        try:
            id = url.split("/")[-1]
        except:
            print(f"Failed to get song_id from url: {url}")
            continue

        reaction: str = row[columns[config["column"]["reaction"]]]
        reaction = reaction.replace("https://youtu.be/", "")

        if reaction == "0":
            reaction = ""

        date_str: str = row[columns[config["column"]["date"]]].replace(".", "")
        try:
            date: int = int(date_str)
        except:
            print(f"Failed to get date from id : {id}")
            continue
        remix: str = row[columns[config["column"]["remix"]]]

        try:
            start = int(row[columns[config["column"]["start"]]])
        except:
            start = 0

        try:
            end = (
                int(row[columns[config["column"]["end"]]])
                if row[columns[config["column"]["end"]]] != ""
                else 0
            )
        except:
            end = 0

        try:
            rawOrder: str = row[columns[config["column"]["order"]]]
            if rawOrder == "" or rawOrder == None:
                print(f"Failed to get order from id : {id}")
                continue
            rawOrder = rawOrder.replace(",", "")
            floatOrder = float(rawOrder)
            order = int(floatOrder * 100)
        except:
            print(f"Failed to get order from id : {id}")
            continue

        raw_keywords: str = row[columns[config["column"]["keyword"]]]
        if raw_keywords != None and raw_keywords != "":
            keywords = raw_keywords.split(",")
        else:
            keywords = []

        songs[id] = {
            "song_id": id,
            "title": title,
            "artist": artist,
            "remix": remix,
            "reaction": reaction,
            "date": date,
            "start": start,
            "end": end,
            "order": order,
        }

        for artist_id, name in config["column"]["artists"].items():
            if artist_id not in artists_songs:
                artists_songs[artist_id] = []

            is_artist = row[columns[name]]
            if is_artist == "":
                continue

            artists_songs[artist_id].append(id)

        for keyword in keywords:
            processed_keyword = keyword.strip().lower()
            if processed_keyword not in keyword_song:
                keyword_song[processed_keyword] = []

            keyword_song[processed_keyword].append(id)

    db_songs = update_songs(conn=conn, songs=songs)
    db_keywords = update_keywords(conn=conn, keywords=list(keyword_song.keys()))

    update_artists(
        conn=conn, songs=db_songs, artists_songs=artists_songs, artists=artists
    )
    update_keyword_song(
        conn=conn, keywords=db_keywords, keyword_song=keyword_song, songs=db_songs
    )
    update_charts(conn=conn, songs=db_songs, charts=charts)

    with conn.cursor(Cursor) as cursor:
        chart_updated_time = int(time.time())
        for chart in charts:
            cursor.execute(
                "UPDATE chart_updated SET time = %s WHERE type = %s",
                (chart_updated_time, chart),
            )
    conn.commit()
    print("Successfully updated wakmusic chart data.")

    conn.close()


def add_work_hourly(scheduler) -> None:
    for j in range(0, 24):
        h: str = str(j)
        if j < 10:
            h = f"0{str(j)}"
        scheduler.every().day.at(f"{h}:00").do(work)


if __name__ == "__main__":
    add_work_hourly(schedule)
    print("Wakmusic Crawler v2 started.")

    # work()
    while True:
        schedule.run_pending()
        time.sleep(1)
