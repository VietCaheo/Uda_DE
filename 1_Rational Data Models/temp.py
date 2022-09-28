rawTime = df[['ts']]
rawTime.head()
print()
print("timestamp prior to convert {} \n".format(rawTime.head()))

#first row ts and convert to datetime in pandas
ts_dtime_S = []

#count = 0
for index, row in rawTime.iterrows():
#    count += 1
    ts_dtime = pd.to_datetime(row["ts"])
    ts_dtime_S.append(pd.Series(ts_dtime))

#print("type of ts_dtime {} \n".format(type(ts_dtime)))
#print("to check hour year month {}  {} {} \n".format(ts_dtime.hour, ts_dtime.year, ts_dtime.month))

#print("number of ts_dtime_S \n {} \n".format(len(ts_dtime_S)))
#print("to see whole ts_dtime_S \n {} \n".format(ts_dtime_S))


#debug
#print("to see type dt after convert {}".format(type(ts_dtime)))
#print("to see value after convert {}".format(ts_dtime))
#print("to see \n hour: {} \n day : {} \n weekofyear: {} \n month: {} \n year: {} \n weekday: {} ".format(ts_dtime.hour, ts_dtime.day, ts_dtime.weekofyear, ts_dtime.month, ts_dtime.year, ts_dtime.weekday))
#print("year = ", ts_dtime_S.dt.year[0])

time_data = []
time_data_e = []

for ele in ts_dtime_S:
    time_data_e.append(ele)
    time_data_e.append(ele.dt.hour)
    time_data_e.append(ele.dt.day)
    time_data_e.append(ele.dt.weekofyear)
    time_data_e.append(ele.dt.month)
    time_data_e.append(ele.dt.year)
    time_data_e.append(ele.dt.weekday)

#to see time_data list
print(time_data)

column_labels = ['ts','hour','day','weekofyear','month','year','weekday']