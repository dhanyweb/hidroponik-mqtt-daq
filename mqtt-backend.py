import paho.mqtt.client as mqtt
import mysql.connector
import requests
import datetime
import time

mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    passwd='raspi',
    database='smartgh_pemantauan')

class Topic:
    thd ="thd"
    sn = "sn"
    hum = "hum"
    data = "data"
    temp = "temp"
    date = "date"
    time = "time"
    vol = "vol"
    lux = "lux"
    ph = "ph"
    tds = "tds"
    tanaman = "tanaman"
    tgl_tanam = "tgl_tanam"
    suhu_tandon = "suhu_tandon"
            
saved_data = {
    Topic.thd:0,
    Topic.temp:0,
    Topic.hum:0,
    Topic.sn:0,
    Topic.ph:0,
    Topic.tds:0,
    Topic.lux:0,
    Topic.vol:0,
    Topic.date:0,
    Topic.time:0,
    Topic.data:0,
    Topic.tanaman:0,
    Topic.tgl_tanam:0,
    Topic.suhu_tandon:0
    }

def on_connect(client, userdata, flags, rc):
    print("Connected: "+str(rc))
    client.subscribe(Topic.thd)
    client.subscribe(Topic.temp)
    client.subscribe(Topic.hum)
    client.subscribe(Topic.sn)
    client.subscribe(Topic.ph)
    client.subscribe(Topic.tds)
    client.subscribe(Topic.vol)
    client.subscribe(Topic.lux)
    client.subscribe(Topic.date)
    client.subscribe(Topic.time)
    client.subscribe(Topic.data)
    client.subscribe(Topic.tanaman)
    client.subscribe(Topic.tgl_tanam)
    client.subscribe(Topic.suhu_tandon)

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    if msg.topic == Topic.data:
        parse_received_data(msg.topic, msg.payload)
    else:
        saved_data[msg.topic] = float(msg.payload)

def parse_received_data(topic, data):
    # Parsing data (csv)
    splice = data.split(",")
    sn = splice[0]
    date = splice[1]
    time = splice[2]
    temp = splice[3]
    hum = splice[4]
    cahaya = splice[5]
    vol = splice[6]
    tds = splice[7]
    suhu_tandon = splice[8]
    ph = splice[9]
    thd_tds = splice[10]
    thd_ph = splice[11]
#     tanaman = splice[12]
#     tgl_tanam = splice[13]
    
	# delay_gw_server database local
    concat = date + ' ' + time
    dt = datetime.datetime.strptime(concat, '%Y-%m-%d %H:%M:%S')
    diff = (datetime.datetime.now() - dt)
    print(diff)
    
	# send to db local moni
    mycursor = mydb.cursor()
    sql = "INSERT INTO moni (sn, dgw, tgw, delay_gw_server) VALUES (%s, %s, %s, %s)"
    val = (sn, date, time, diff)
    mycursor.execute(sql, val)
    
	# send to db local moni_detail
    sql2 = "INSERT INTO moni_detail (id, sensor, nilai) VALUES (LAST_INSERT_ID(), %s, %s)"
    val2 = [('Cahaya', cahaya), ('Temp', temp), ('Hum', hum), ('Volume', vol), ('TDS_val', tds), ('Water_temp', suhu_tandon), ('pH_val', ph)]
    mycursor.executemany(sql2, val2)
    mydb.commit()
        
    # send to db server moni & moni_detail
    data1 = "Cahaya"
    data2 = "Temp"
    data3 = "Hum"
    data4 = "Volume"
    data5 = "TDS_val"
    data6 = "Water_temp"
    data7 = "pH_val"
    s = "{}x{}x{}x{}x{}x{}x{}"
    sensor = (s.format(data1, data2, data3, data4, data5, data6, data7))

    val1 = cahaya
    val2 = temp
    val3 = hum
    val4 = vol
    val5 = tds
    val6 = suhu_tandon
    val7 = ph
    n = "{}x{}x{}x{}x{}x{}x{}"
    nilai = (n.format(val1, val2, val3, val4, val5, val6, val7))
      
    data = {'sn':sn,
            'dgw':date,
            'tgw':time,
            'sensor':sensor,
            'nilai':nilai}

    post = requests.get('http://smart-gh.com/input.php?sn=2020060002', params=data)
    if post.status_code == 200:
       print('Data Monitoring has been sent to Database Server')
    elif post.status_code == 404:
        print('Not Found.')
    
    # send to dbserver plant_user
    set1 = "ph"
    set2 = "tds"
    sett = "{}x{}"
    sens = (sett.format(set1, set2))
    
    setval1 = ph
    setval2 = tds
    setval = "{}x{}"
    val = (sett.format(set1, set2))
    
    setup = {'tanaman':"Tomat",
             'thd_ph':thd_ph,
             'thd_tds':thd_tds,
             'thd_wl':vol,
             'tgl_tanam':"2020-02-11",
             'sensor':sens,
             'nilai':val}

    sendsetup = requests.get('http://omahiot.com/input.php?sn=2020060002', params=setup)
    if sendsetup.status_code == 200:
       print('Data Setup has been sent to Database Server')
    elif sendsetup.status_code == 404:
        print('Not Found.')
        
        
    # publish data
    for topic in topics:
        print("Publishing %f to topic %s"%(saved_data[topic], topic))
        client.publish(topic, saved_data[topic])
    print(splice)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set("turusasri","turusasri")
client.connect("127.0.0.1", 1883, 60)

client.loop_forever()