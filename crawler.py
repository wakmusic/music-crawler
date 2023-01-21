import json
import sqlite3
import time
import gspread
import schedule
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from pytube import YouTube

with open('config.json', encoding='utf-8-sig') as file:
    js = json.load(file)


def get_last(cursor, _id, t):
    data = cursor.execute(f'SELECT * FROM total WHERE id = "{_id}"').fetchone()
    if data:
        field = 'views'
        if t != 'total':
            field = 'increase'
        songs = cursor.execute(f'SELECT * FROM {t} ORDER BY {field} DESC').fetchall()
        last = 1
        for song in songs:
            if song[0] == _id:
                break
            last += 1

        if last == len(songs) + 1:
            last = 0

        if t == "total":
            last_views = data[6]
        else:
            s = cursor.execute(f'SELECT * FROM {t} WHERE id = "{_id}"').fetchone()
            last_views = 0
            if s:
                try:
                    last_views = int(s[1])
                except:
                    pass
    else:
        last_views = 0
        last = 0
    return last, last_views


def insert(charts, cursor, _id, views, t):
    last, last_views = get_last(cursor, _id, t)
    temp = (_id, views, views - last_views, last)
    charts[t].append(temp)


def update_lyrics():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name('config.json', scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)

        spreadsheet = gc.open_by_url(url="https://docs.google.com/spreadsheets/d/1cplAy6pfH_X4W-odwuZgaRVuvEfUI88NOAonSkThRbE")
        current = spreadsheet.get_worksheet(0)
        col = current.col_values(9)[2:]
    except:
        return

    last = spreadsheet.get_worksheet(1)
    names = last.col_values(1)[1:]
    count = last.col_values(2)[1:]

    data = {}
    for i in range(len(names)):
        data[names[i]] = int(count[i])

    for c in col:
        try:
            data[c] += 1
        except KeyError:
            data[c] = 1

    result = sorted(data.items(), key=lambda x: x[1], reverse=True)
    result_list = ", ".join([d[0] for d in result]).replace("니엔, ", "").replace("아트아스, ", "")
    pre = "서선유, 김모건, 옹냐, 인턴 이기자, 여비날, 배식, 탈영병, "

    final = pre + result_list

    conn = sqlite3.connect(js['database_src'] + 'static.db')
    cursor = conn.cursor()

    cursor.execute(f'UPDATE teams SET name = "{final}" WHERE team = "special2"')
    conn.commit()


def check_sheet():
    artists = {
        "woowakgood": [],
        "ine": [],
        "jingburger": [],
        "lilpa": [],
        "jururu": [],
        "gosegu": [],
        "viichan": [],
        "chunshik": [],
        "kwonmin": [],
        "kimchimandu": [],
        "nosferatuhodd": [],
        "dandapbug": [],
        "dopamine": [],
        "dokkhye": [],
        "roentgenium": [],
        "haku": [],
        "bujungingan": [],
        "secretto": [],
        "businesskim": [],
        "friedshrimp": [],
        "sophia": [],
        "wakphago": [],
        "leedeoksoo": [],
        "carnarjungtur": [],
        "callycarly": [],
        "pungsin": [],
        "freeter": [],
        "rusuk": [],
        "hikiking": []
    }
    charts = {
        "total": [],
        "hourly": [],
        "daily": [],
        "weekly": [],
        "monthly": []
    }
    complete = ['hourly']

    now = datetime.now()
    if now.day == 1 and now.hour == 0:
        complete.append('monthly')
    if now.weekday() == 0 and now.hour == 0:
        complete.append('weekly')
    if now.hour == 0:
        complete.append('daily')
    if now.hour == 1:
        try:
            update_lyrics()
        except:
            pass

    while True:
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name('config.json', scopes=["https://www.googleapis.com/auth/spreadsheets"])
            gc = gspread.authorize(creds)
            spreadsheet = gc.open_by_url(url="https://docs.google.com/spreadsheets/d/1n8bRCE_OBUOND4pfhlqwEBMR6qifVLyWk5YrHclRWfY")
            worksheet = spreadsheet.get_worksheet(1)
            break
        except:
            pass

    conn = sqlite3.connect(js['database_src'] + 'charts.db')
    cursor = conn.cursor()

    values = worksheet.get_all_values()
    c = js['column']

    for v in values[1:]:
        url = v[c['url']]
        if url == '0':
            continue

        try:
            title = v[c['title']].split(' - ')[1]
        except IndexError:
            continue

        artist = v[c['title']].split(' - ')[0].replace(' x ', ', ')
        _id = url.split('/')[-1]

        reaction = v[c['reaction']].replace('https://youtu.be/', '')
        if reaction == '0':
            reaction = ''

        while True:
            count = 0
            try:
                views = YouTube(f'https://youtu.be/{_id}').views
                if reaction != '':
                    views += YouTube(f'https://youtu.be/{reaction}').views
                break
            except:
                if count > 5:
                    query = cursor.execute(f'SELECT * FROM total WHERE id = "{_id}"').fetchone()
                    if not query:
                        views = 0
                    else:
                        views = int(query[6])
                    break
                count += 1
                pass

        date = v[c['date']].replace('.', '')
        remix = v[c['remix']]

        if now.day == 1 and now.hour == 0:
            last, last_views = get_last(cursor, _id, 'total')
            charts["total"].append([_id, title, artist, remix, reaction, date, views, last])
        else:
            data = cursor.execute(f'SELECT * FROM total WHERE id = "{_id}"').fetchone()
            l_views = 0
            if data:
                l_views = data[7]
            charts["total"].append([_id, title, artist, remix, reaction, date, views, l_views])

        insert(charts, cursor, _id, views, 'hourly')

        now = datetime.now()
        if now.day == 1 and now.hour == 0:
            insert(charts, cursor, _id, views, 'monthly')
        if now.weekday() == 0 and now.hour == 0:
            insert(charts, cursor, _id, views, 'weekly')
        if now.hour == 0:
            insert(charts, cursor, _id, views, 'daily')

    for v in values[1:]:
        url = v[c['url']]
        if url == '0':
            continue
        _id = url.split('/')[-1]
        artists_rev = {v: k for k, v in js['column'].items()}
        for i in range(22, 56):
            if 23 <= i <= 24 or 31 <= i <= 33:
                continue

            if v[i] != '':
                try:
                    artists[artists_rev[i]].append(_id)
                except KeyError:
                    pass

    cursor.execute('DELETE FROM artists')
    art = []
    for a in artists:
        art.append((a, ','.join(artists[a])))
    cursor.executemany('INSERT INTO artists VALUES(?, ?)', art)
    cursor.execute(f'UPDATE updated SET time = "{int(time.time())}"')

    cursor.execute('DELETE FROM total')
    cursor.executemany('INSERT INTO total VALUES (?, ?, ?, ?, ?, ?, ?, ?)', charts['total'])

    for t in complete:
        cursor.execute(f'DELETE FROM {t}')
        try:
            cursor.executemany(f'INSERT INTO {t} VALUES (?, ?, ?, ?)', list(set(charts[t])))
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()


for j in range(0, 24):
    h = j
    if j < 10:
        h = f"0{j}"
    schedule.every().day.at(f"{h}:00").do(check_sheet)

while True:
    schedule.run_pending()
    time.sleep(1)
