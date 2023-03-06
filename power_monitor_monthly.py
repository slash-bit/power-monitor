# This script runs daiy and re-writes the month Consumption and cost. It also updates the current, month based on the rolling month data

import InfluxQL_Cloud_write_string
import power_monitor_influxDB_cloud
from time import sleep
import datetime

# consumed_month = [408.1, 408.2, 408.3, 253.4, 253.5, 294.5, 280.5, 173.95, 1.9, 1.01, 1.11, 1.12]
# cost_month = [93.1, 93.2, 93.3, 83.4, 83.5, 95.6, 95.7, 62.06, 0.9, 1.01, 1.11, 1.12]
consumed_month = []
cost_month = []
final_result = {}
nowmonth = datetime.datetime.now().month


def query_prev_months():
    query_prev_months = f"""from(bucket: "vlad_bucket")
  |> range(start: -48h )
  |> filter(fn: (r) => r["_measurement"] == "energy")
  |> filter(fn: (r) => r["_field"] == "consumed_month" or r["_field"] == "cost_month")
  |> filter(fn: (r) => r["month"] == "monthly")"""
    result = power_monitor_influxDB_cloud.query(query_prev_months)
    for table in result:
        for record in table.records:
            field = record.get_field()
            value = record.get_value()
            if field == 'consumed_month':
                consumed_month.append(value)
            if field == 'cost_month':
                cost_month.append(value)

    return consumed_month, cost_month


def query_last_month():
    query = f"""from(bucket: "vlad_bucket")
  |> range(start: -48h)
  |> filter(fn: (r) => r["_measurement"] == "energy")
  |> filter(fn: (r) => r["month"] == "rolling")
  |> filter(fn: (r) => r["_field"] == "cost_month" or r["_field"] == "consumed_month")"""
    result = power_monitor_influxDB_cloud.query(query)
    for table in result:
        for record in table.records:
            field = record.get_field()
            value = record.get_value()
            update = {field: value}
            final_result.update(update)
    return final_result


consumed_month, cost_month = query_prev_months()
final_result = query_last_month()
consumed_month[nowmonth-1] = final_result['consumed_month']
cost_month[nowmonth-1] = final_result['cost_month']

for m in range(1, 13):

    data = f"energy,month=monthly month={m},consumed_month={consumed_month[m - 1]},cost_month={cost_month[m - 1]}"
    InfluxQL_Cloud_write_string.write(data)
    sleep(10)
