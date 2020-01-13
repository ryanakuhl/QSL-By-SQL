import requests, re, time
from bs4 import BeautifulSoup
from xml.etree import ElementTree
from geopy.distance import distance
from datetime import datetime, timedelta
from countryinfo import CountryInfo

notifications = []

class CallSignObj:
    def __init__(self, tds):
        self.hams = [sorted(tds[0].text.split('/'), key=lambda a: len(a), reverse=True)[0], sorted(tds[2].text.split('/'), key=lambda a: len(a), reverse=True)[0]]
        self.frequency = round(float(tds[1].text) * .001, 2)
        self.time = datetime.strptime(tds[3].text, '%H%Mz %d %b').replace(datetime.now().year) - timedelta(hours=5)
        self.dx = False
        self.dx_country = ''
        self.stateside = False
        self.distance_miles = 0
        self.subregion = ''
        re_pattern = r'^A[A-L].+|^[KNW].+'
        us = [re.search(re_pattern, b, flags=re.IGNORECASE).group() for b in self.hams if re.search(re_pattern, b, flags=re.IGNORECASE)]
        if len(us) == 1:
            self.hams.remove(us[0])
            canada_re_pattern = r'^C[F-KY-Z]].+|^V[A-GOX-Y].+|^X[J-O].+'
            if len([re.search(canada_re_pattern, b, flags=re.IGNORECASE).group() for b in self.hams if re.search(canada_re_pattern, b, flags=re.IGNORECASE)]) != 1:
                self.dx = self.hams[0]
                self.stateside = us[0]

def run():
    page = requests.get('https://www.qrz.com/dxcluster', verify=False).content
    soup = BeautifulSoup(page, 'html.parser')
    table = soup.find_all('table')[1]
    rows = table.find_all('tr')
    for row in rows[1:]:
        notifications.append(CallSignObj(row.find_all('td')))
    [notifications.remove(n) for n in notifications if n.frequency > 60]
    recent_contacts = [n for n in [n for n in notifications if (datetime.now() - n.time).total_seconds()/60 < 5] if bool(n.dx)]

    your_qth = """longitude, latitude"""
    for a in recent_contacts:
        try:
            page = requests.get('http://api.hamdb.org/v1/' + a.stateside + '/xml').content.decode()
            root = ElementTree.fromstring(page)
            stateside_coordinates = root[0][4].text, root[0][5].text
            d = round(distance(your_qth, stateside_coordinates).miles, 1)
            a.__setattr__('distance_miles', d)
            time.sleep(4)
        except Exception as e:
            print('ERROR qsl_clusters line 47', e, type(e), a.stateside)
            recent_contacts.remove(a)

    close_contacts = [n for n in recent_contacts if n.distance_miles < 500]
    for c in close_contacts:
        dx_country = ''
        try:
            page = requests.get('http://api.hamdb.org/v1/' + c.dx + '/xml').content.decode()
            root = ElementTree.fromstring(page)
            dx_country = root[0][15].text
            if dx_country == 'NOT_FOUND':
                page = requests.get('https://www.qrz.com/lookup/' + c.dx, verify=False).content.decode()
                soup = BeautifulSoup(page, 'html.parser')
                dx_country = soup.find(id='flg')['alt'].replace(' flag', '')
            else:
                dx_country = root[0][15].text
            time.sleep(2)
        except ValueError:
            print('2https://www.qrz.com/lookup/' + c.dx)
            page = requests.get('https://www.qrz.com/lookup/' + c.dx, verify=False).content.decode()
            soup = BeautifulSoup(page, 'html.parser')
            dx_country = soup.find(id='flg')['alt'].replace(' flag', '')

        except Exception as e:
            print('ERROR qsl_clusters line 64', e, c.dx, root)
            time.sleep(2)
            close_contacts.remove(c)
        c.__setattr__('dx_country', dx_country.replace('&', 'and').replace(' Island', '').replace('St ', 'Saint ').rstrip().lstrip())
        try:
            country = CountryInfo(c.dx_country)
            c.__setattr__('subregion', country.subregion())
        except:
            pass
    return close_contacts
