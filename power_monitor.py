import RPi.GPIO as GPIO
from time import sleep  # this lets us have a time delay (see line 12)
import time
import datetime
import paho.mqtt.publish as publish
import power_monitor_influxDB_cloud
import os
from dotenv import load_dotenv

load_dotenv()
# Load credential from .env file
mqtt_user = os.getenv("MQTT_USER")
mqtt_password = os.getenv("MQTT_PASSWORD")

GPIO.setmode(GPIO.BCM)  # set up BCM GPIO numbering
GPIO.setup(24, GPIO.IN)  # set GPIO25 as input (button)
GPIO.setup(25, GPIO.IN)
GPIO.setup(23, GPIO.OUT)  # LED for indicating pulses
gpio25_prev = 0
gpio24_prev = 1
GPIO.output(23, 0)  # Initialise LED to OFF
broker = "192.168.0.251"  # mqtt broker adress (homeassistant)
port = 1883

global day_tariff, night_tariff, standing  # tarrifs
day_tariff = 0.4545  # Day rate per kW in £
night_tariff = 0.15729  # Night rate per kW in £
standing = 0.43755  # Standing charge per day in £
pulsecount = 0  # pulsecount is pulses between reports. Each report t Influx resets the pulse count
# pulse count period is pulses counted during 15min period. Those pulses used to calculate consumed energy.
pulsecount_period = 0
prev_interval = 0
confirm = 0
nopulsemin = 0  # counting minutes without pulses received
mqtt_high = True  # flag to indicate mqtt message sent

# check if DST is in effect
if time.localtime().tm_isdst == 1:
    dst_start = 510
    dst_fin = 90
else:
    dst_start = 450
    dst_fin = 30

now = datetime.datetime.now()
mod = now.hour * 60 + now.minute
timer1 = 0
pulse_count_day = 0  # couning pulses until 800 , then increment meter reading
pulse_count_night = 0
power = 0
pulses_1min = 0
today = now.day
consumed_daily = {}
consumed_daily_cost = {}
consumed_day_night = [
    0.0,
    0.0,
]  # a list to update day/nigh consumtion values (accumulating for 24h)
consumed_day_night_cost = [
    0.0,
    0.0,
]  # a list to update day/nigh consumtion gbp cost (accumulating for 24h)


def publish_mqtt(topic, payload):
    publish.single(topic, payload, hostname=broker, port=port, client_id="power_consumption", qos=1, auth={'username': mqtt_user,
                                                                                                           'password': mqtt_password})


# get consumed_today value from DB
bucket = "vlad_bucket"
query = f"""from(bucket:"{bucket}")\n|> range(start: today())\n|> filter(fn:(r) => r._measurement == "energy")\n|> filter(fn: (r) => r["host"] == "house")\n|> filter(fn:(r) => r._field == "consumed_today" or r._field == "consumed_today_cost" or r._field == "consumed_today_day" or r._field == "consumed_today_night" or r._field == "consumed_today_day_cost" or r._field == "consumed_today_night_cost" or r._field == "meter_day" or r._field == "meter_night")\n|> aggregateWindow(every: 1d, fn: last, createEmpty: false)"""

final_result = {
    "consumed_today_day": 0,
    "consumed_today_night": 0,
    "consumed_today_day_cost": 0,
    "consumed_today_night_cost": 0,
    "meter_day": 40753.9,
    "meter_night": 24998.23,
}
try:
    result = power_monitor_influxDB_cloud.query(query)
    for table in result:
        for record in table.records:
            field = record.get_field()
            value = record.get_value()
            update = {field: value}
            final_result.update(update)

    consumed_day_night = [
        final_result["consumed_today_day"],
        final_result["consumed_today_night"],
    ]
    consumed_day_night_cost = [
        final_result["consumed_today_day_cost"],
        final_result["consumed_today_night_cost"],
    ]
    consumed_daily = {today: consumed_day_night}
    consumed_daily_cost = {today: consumed_day_night_cost}
    meter_day = final_result["meter_day"]
    meter_night = final_result["meter_night"]
    print(
        f"Obtained last InfluxDB points:\nConsumed Daily: {consumed_daily}\nConsumed Daily Cost: {consumed_daily_cost}\nMeter_Day: {meter_day}\nMeter_Night: {meter_night}")

except TimeoutError:
    # print('InfluxDB Cloud - Timeout Error')
    pass
# temp meter reading to update
# meter_day = 41807.6
# meter_night = 25367.88

while True:
    now = datetime.datetime.now()
    theMonth = now.month
    today = now.day
    pulsecount_period = 0
    enterloop = (
        1  # this is needed for entering the loop below while minutes are matching
    )
    # max_it_hour = 0  #to break out of loop every hour
    while (
        now.minute != 14 and now.minute != 29 and now.minute != 44 and now.minute != 59
    ) or enterloop == 1:  # break out of loop every 15min
        tick = time.time()
        enterloop = 0
        pulsecount_new = pulsecount
        pulses_1min = 0
        n = 0
        while n < 300:
            if gpio25_prev < GPIO.input(25):  # day led pulse detected
                interval = time.time() - timer1
                if interval > (prev_interval / 2) or confirm >= 1:
                    pulsecount += 1
                    pulsecount_period += 1
                    pulses_1min += 1
                    prev_interval = interval
                    confirm = 0
                    timer1 = time.time()  # reset inter-pulse timer
                else:  # possibly false pulse, wait for next one to confirm
                    confirm += 1
                    print("Possibly false pulse, waiting for confirmation")

                timer1 = time.time()  # set inter-pulse timer
                nopulsemin = 0
                print("Pulse detected on PIN 25 (day led)")
                GPIO.output(23, 1)  # LED indicator ON

            if gpio24_prev < GPIO.input(24):  # night pulse detected
                interval = time.time() - timer1
                if interval > (prev_interval * 1.5):
                    # interval = prev_interval
                    confirm += 1

                # confirmed missed previous pulse , so adding another pulse
                if confirm >= 1 and max(interval - prev_interval, prev_interval - interval) < prev_interval / 2:
                    pulsecount += confirm
                    pulsecount_period += confirm
                    pulses_1min += 1
                    confirm = 0
                pulsecount += 1
                pulsecount_period += 1
                pulses_1min += 1
                timer1 = time.time()  # reset inter-pulse timer
                nopulsemin = 0
                print("Pulse detected on PIN 24 (night led)")
                GPIO.output(23, 1)  # LED indicator ON

            gpio25_prev = GPIO.input(25)
            gpio24_prev = GPIO.input(24)
            n += 1
            if GPIO.input(25) == 0 or GPIO.input(24) == 0:
                GPIO.output(23, 0)

            sleep(0.2)

        now = datetime.datetime.now()
        mod = now.hour * 60 + now.minute
        if dst_fin < mod < dst_start:
            day_rate = False
            tariff = night_tariff
            pulse_count_night += pulsecount
        else:
            day_rate = True
            tariff = day_tariff
            pulse_count_day += pulsecount

        if 15 > pulses_1min > 5:
            power = pulses_1min * 60 / 100  # power calculated in Watts
            consumed = pulses_1min / 100  # consumed kW in the last minute
            data = f"energy,host=house pulses={pulsecount_period},pulsecount={pulsecount},interval={interval},current_power={power},day_rate={day_rate}"
            print("Pulsecount = ", pulsecount)
            print("Sending: ", data)
            interval = 0.0
            pulsecount = 0
            try:
                power_monitor_influxDB_cloud.main(data)
            except TimeoutError:
                with open("power_monitor.log", "a") as log:
                    log.write(
                        f"{time.asctime()} InfluxDB Cloud - Timeout Error")
                log.close
                pass
            try:
                publish_mqtt("home/power/consumption", str(power))
            except:
                with open("power_monitor.log", "a") as log:
                    log.write(f"{time.asctime()} MQTT Timeout")
                log.close
                pass
        elif (
            pulsecount > pulsecount_new
            and pulsecount != 0
            and timer1 != 0
            and 3600 > interval > 6
        ):
            try:
                power = 3600 / interval / 100
                consumed = pulsecount / 100
                data = f"energy,host=house pulses={pulsecount_period},pulsecount={pulsecount},interval={interval},current_power={power},day_rate={day_rate}"
                print("Interval = ", interval)
                print("Sending: ", data)
                pulsecount = 0
                interval = 0.0
                try:
                    power_monitor_influxDB_cloud.main(data)
                except TimeoutError:
                    with open("power_monitor.log", "a") as log:
                        log.write(
                            f"{time.asctime()} InfluxDB Cloud - Timeout Error")
                    log.close
                    pass
                try:
                    publish_mqtt("home/power/consumption", str(power))
                    if not mqtt_high and power > 0.2:  # publish mqtt high consumption
                        publish_mqtt("home/power/consumptionhigh", "")
                        mqtt_high = True
                except:
                    with open("power_monitor.log", "a") as log:
                        log.write(f"{time.asctime()} MQTT Timeout")
                    log.close
                    pass
            except:
                with open("power_monitor.log", "a") as log:
                    log.write(
                        f"{time.asctime()} Someting went wrong during consumption calculation"
                    )
                log.close
                pass
        # for those minutes when no pulses received, it will log a datapoint
        elif pulsecount == pulsecount_new and (time.time() - timer1) > prev_interval:
            nopulsemin += 1
            if nopulsemin > 1:
                # power calculated based on minutes elapsed without pulses
                power = 3600 / (time.time() - timer1) / 100
                data = f"energy,host=house current_power={power}"
                try:
                    power_monitor_influxDB_cloud.main(data)
                except TimeoutError:
                    with open("power_monitor.log", "a") as log:
                        log.write(
                            f"{time.asctime()} InfluxDB Cloud - Timeout Error")
                    log.close
                    pass
                try:
                    publish_mqtt("home/power/consumption", str(power))
                    if mqtt_high and power < 0.06:  # publish mqtt low consumption
                        publish_mqtt("home/power/consumptionlow", "")
                        mqtt_high = False
                except:
                    with open("power_monitor.log", "a") as log:
                        log.write(f"{time.asctime()} MQTT Timeout")
                    log.close
                    pass

    consumed_hour = pulsecount_period / 100  # consumed kW in 1 hour
    consumed_hour_cost = consumed_hour * tariff
    # we check what day it is
    now = datetime.datetime.now()
    today = now.day
    # the below daily cumulative calculaition can be phased out ( we switching to hourly sum in the Flux query)
    if consumed_daily.get(today) is None:  # it means its a start of a new day
        # new_value = consumed_hour
        if day_rate:
            # updating the day entry of the list
            consumed_day_night[0] = consumed_hour
            consumed_day_night_cost[0] = consumed_hour_cost
            consumed_day_night[1] = 0.0
            consumed_day_night_cost[1] = (
                standing / 2
            )  # adding half of daily standing charge of night cost
        else:
            consumed_day_night[
                1
            ] = consumed_hour  # updating the night entry of the list
            consumed_day_night_cost[1] = consumed_hour_cost
            consumed_day_night[0] = 0.0
            consumed_day_night_cost[0] = (
                standing / 2
            )  # adding half of daily standing charge of day cost
        try:
            # sending new last reset date in ISO format
            publish_mqtt("home/power/lastreset", now.isoformat())
        except:
            with open("power_monitor.log", "a") as log:
                log.write(f"{time.asctime()} MQTT Timeout")
            log.close
            pass

    else:
        # new_value = (consumed_daily.get(today)) + consumed_hour
        if day_rate:
            consumed_day_night[0] = (
                consumed_day_night[0] + consumed_hour
            )  # updating the day entry of the list
            consumed_day_night_cost[0] = consumed_day_night_cost[0] + \
                consumed_hour_cost
        else:
            pass
            consumed_day_night[1] = (
                consumed_day_night[1] + consumed_hour
            )  # updating the night entry of the list
            consumed_day_night_cost[1] = consumed_day_night_cost[1] + \
                consumed_hour_cost

    consumed_daily.update(
        {today: consumed_day_night}
    )  # accumulated value add hourly reading each hour
    consumed_daily_cost.update(
        {today: consumed_day_night_cost}
    )  # accumulated cost of today added hourly

    meter_day = meter_day + pulse_count_day / 800
    meter_night = meter_night + pulse_count_night / 800
    data = f"energy,host=house consumed={consumed},consumed_hour={consumed_hour},consumed_hour_gbp={consumed_hour_cost},consumed_today={sum(consumed_daily.get(today))},consumed_today_day={consumed_daily.get(today)[0]},consumed_today_night={consumed_daily.get(today)[1]},consumed_today_cost={sum(consumed_daily_cost.get(today))},consumed_today_day_cost={consumed_daily_cost.get(today)[0]},consumed_today_night_cost={consumed_daily_cost.get(today)[1]},consumed={consumed},tariff={tariff},day_rate={day_rate},meter_day={meter_day},meter_night={meter_night}"
    try:
        power_monitor_influxDB_cloud.main(data)
    except TimeoutError:
        # print('InfluxDB Cloud - Timeout Error')
        pass
    with open("power_monitor.log", "a") as log:
        log.write(
            f"{time.asctime()} - Consumed 15m: {consumed_hour:.3f}kWh | Cost 15m: £{consumed_hour_cost:.2f} | Consumed today: {(sum(consumed_daily.get(today))):.3f}kWh | Today cost: £{(sum(consumed_daily_cost.get(today))):.2f}\n"
        )
    log.close
    try:
        publish_mqtt("home/power/consumed",
                     f"{(sum(consumed_daily.get(today))):.3f}")
        publish_mqtt("home/power/energytariff",
                     f"{tariff:.4f}")
    except:
        with open("power_monitor.log", "a") as log:
            log.write(f"{time.asctime()} MQTT Timeout")
        log.close
        pass
