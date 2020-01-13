import sqlite3, time
from datetime import datetime, timedelta
from helpers import qsl_clusters

conn = sqlite3.connect('contact_record.db')
c = conn.cursor()

try:
    c.execute('''CREATE TABLE IF NOT EXISTS QSO
                 (Datetime DATE, statesice_callsign TEXT, dx_callsign TEXT, dx_country TEXT, subregion TEXT,distnace real, frequency real)''')
except:
    pass

def load_to_sql():
    its_happening = []
    for qsl in qsl_clusters.run():
        its_happening.append([qsl.frequency, qsl.dx_country, qsl.subregion])
        try:
            c.execute("INSERT INTO QSO (Datetime, statesice_callsign, dx_callsign, dx_country, subregion, distnace, frequency) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (qsl.time, qsl.stateside, qsl.dx, qsl.dx_country, qsl.subregion, qsl.distance_miles, qsl.frequency))
        except sqlite3.IntegrityError as e:
            print(e, "couldn't add twice (Main line 21)")
    conn.commit()
    print('\nLocal observations in the last 5 minutes: ')
    for i in (sorted(its_happening, key=lambda f: f[0])):
        print('\t', i)

def most_frequent(l):
    try:
        return max(set(l), key=l.count)
    except:
        return None

def most_frequent_this_hour(l):
    try:
        return sorted(set(l))
    except:
        return None

freq_to_band = {(1.8, 2) : 160,
                (3.525, 4) : 80,
                (5.33, 5.4035) : 60,
                (7.025, 7.3) : 40,
                (10.1, 10.15) : 30,
                (14.025, 14.35) : 20,
                (18.068, 18.168) : 17,
                (21.025, 21.45) : 15,
                (24.89, 24.99) : 12,
                (28, 29.7) : 10,
                (50, 54) : 6,
               }

def banded(y):
    a = [freq_to_band.get(x) for x in freq_to_band if x[0] <= y <= x[1]]
    if a:
        return a[0]

def expected_subregions(x, y):
    ml = []
    try:
        k = [k for k in freq_to_band if freq_to_band.get(k) == y][0]
        [ml.append(a[1]) for a in x if k[0] <= a[2] <= k[1] and a[1] not in ml and len(a[1]) > 1]
        return sorted(ml)
    except:
        return 'Not yet enough data'

bands_hour = {}
bands_day = {}
while True:
    load_to_sql()
    future_projection = []
    for row in c.execute('SELECT * FROM QSO ORDER BY Datetime'):
        if datetime.now() - timedelta(minutes=15) <= datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S') <= datetime.now() + timedelta(minutes=60):
            future_projection.append([row[3], row[4], row[6]])
    sqlite_select_query = """SELECT * from QSO"""
    c.execute(sqlite_select_query)
    print("Captured QSLs: ", len(c.fetchall()))
    print('Current time:', datetime.now().strftime('%b %d, %H:%M'))
    if future_projection:
        recommended_frequency = most_frequent(list(banded(a[2]) for a in future_projection))
        print('Begin moving to', recommended_frequency, 'meters')
        print('All recorded subregions now and the next hour on', recommended_frequency, 'meters:\n\t',  expected_subregions(future_projection, recommended_frequency))
        print('Expect to see', most_frequent(list(a[0] for a in future_projection)), 'along with', most_frequent(list(a[1] for a in future_projection)), 'throughout the active bands')
    else:
        print('No projections at this time')
    for row in c.execute('SELECT * FROM QSO ORDER BY Datetime'):
        this_hour = datetime.now().hour
        d = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S').hour
        if d == this_hour:
            if d not in bands_hour:
                bands_hour[d] = [freq_to_band.get(x) for x in freq_to_band if x[0] <= row[6] <= x[1]]
            else:
                bands_hour[d] += [freq_to_band.get(x) for x in freq_to_band if x[0] <= row[6] <= x[1]]
        if d not in bands_day:
            bands_day[d] = [freq_to_band.get(x) for x in freq_to_band if x[0] <= row[6] <= x[1]]
        else:
            bands_day[d] += [freq_to_band.get(x) for x in freq_to_band if x[0] <= row[6] <= x[1]]
    print('Historically all DX bands on the', datetime.now().hour, 'hour:\n\t', most_frequent_this_hour(bands_hour.get(datetime.now().hour)))
    #prints most active band for every hour
    #[print(i, most_frequent(bands_day.get(i))) for i in range(1, 25)]
    time.sleep(5*60)
