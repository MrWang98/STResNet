import h5py
import pymysql
import datetime
import time
import numpy as np

def main():
    connect = pymysql.connect(
        host='host.tanhuiri.cn',
        port=3306,
        user='root',
        password='19991119+thr',
        db='metro',
        charset='utf8',
        use_unicode=True,
    )
    cur = connect.cursor()

    select_query = """
                SELECT s.station_id
                FROM
                    station s"""
    cur.execute(select_query)
    data_all = cur.fetchall()
    first_line = []
    first_line.append("start_time")
    stations = {}
    for station in data_all:
        first_line.append(station[0])
        stations[station[0]] = 0
    station_location = np.load("station_location.npy")

    startTime = "2019-12-28 18:00:00"
    endTime = "2020-07-17 00:00:00"
    startArray = time.strptime(startTime,"%Y-%m-%d %H:%M:%S")
    startStamp = int(time.mktime(startArray))
    endArray = time.strptime(endTime,"%Y-%m-%d %H:%M:%S")
    endStamp = int(time.mktime(endArray))
    startDateTime = datetime.datetime.fromtimestamp(startStamp)
    sixHourLater = startDateTime + datetime.timedelta(hours=6)
    endDateTime = datetime.datetime.fromtimestamp(endStamp)
    differ = endDateTime-startDateTime
    hours = differ.days*24+differ.seconds/3600
    l = int(hours/6)

    date = []
    str1 = startTime
    str2 = sixHourLater.strftime("%Y-%m-%d %H:%M:%S")
    t = datetime.datetime.strftime(startDateTime,"%Y%m%d")
    str_date = t
    date.append(bytes((t+'0'+str(1)).encode('ascii')))
    count = 2

    d1,d2 = station_location.shape

    in_flow = []

    for hour in range(l):
        in_select_query = """
            SELECT t.station_in,COUNT( t.id ) AS cnt
            FROM
                trips t
            WHERE
                t.time_in BETWEEN "{}" AND "{}" 
            GROUP BY
                t.station_in
            ORDER BY
                t.station_in""".format(str1,str2)
        str_data = []
        cur.execute(in_select_query)
        data_all = cur.fetchall()
        str_data.append(str1)
        for data in data_all:
            if data[0] not in stations:
                print(data[0])

            stations[data[0]]=data[1]
        in_flow_temp = []
        for i in range(d1):
            line = []
            for j in range(d2):
                station = station_location[i][j]
                if station == 'Sta0':
                    line.append(0)
                else:
                    if station not in stations:
                        line.append(0)
                    else:
                        line.append(stations[station])
            in_flow_temp.append(line)
        in_flow.append(in_flow_temp)
        str1 = str2
        t = datetime.datetime.strftime(sixHourLater, "%Y%m%d")
        if t != str_date:
            count = 1
        if count < 10:
            date.append(bytes((t + '0' + str(count)).encode('ascii')))
        else:
            date.append(bytes((t + str(count)).encode('ascii')))
        str_date = t
        count += 1
        x = (hour+2)*6
        sixHourLater = startDateTime + datetime.timedelta(hours=x)
        str2 = sixHourLater.strftime("%Y-%m-%d %H:%M:%S")

    sixHourLater = startDateTime + datetime.timedelta(hours=6)
    str1 = startTime
    str2 = sixHourLater.strftime("%Y-%m-%d %H:%M:%S")
    out_flow = []
    for hour in range(l):
        temp = stations
        in_select_query = """
            SELECT t.station_out,COUNT( t.id ) AS cnt
            FROM
                trips t
            WHERE
                t.time_out BETWEEN "{}" AND "{}" 
            GROUP BY
                t.station_out
            ORDER BY
                t.station_out""".format(str1, str2)
        str_data = []
        cur.execute(in_select_query)
        data_all = cur.fetchall()
        str_data.append(str1)
        for data in data_all:
            if data[0] not in temp:
                print(data[0])

            temp[data[0]] = data[1]

        out_flow_temp = []
        for i in range(d1):
            line = []
            for j in range(d2):
                station = station_location[i][j]
                if station == 'Sta0':
                    line.append(0)
                else:
                    if station not in stations:
                        line.append(0)
                    else:
                        line.append(temp[station])
            out_flow_temp.append(line)
        out_flow.append(in_flow_temp)
        str1 = str2
        x = (hour + 2) * 6
        sixHourLater = startDateTime + datetime.timedelta(hours=x)
        str2 = sixHourLater.strftime("%Y-%m-%d %H:%M:%S")

    allData = []
    for idx,t_date in enumerate(date[:-1]):
        allData.append([in_flow[idx],out_flow[idx]])
    with h5py.File("allData.h5") as f:
        f["data"] = allData
        f["date"] = date[:-1]

if __name__ == '__main__':
    main()