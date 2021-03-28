from datetime import datetime, timedelta


def parse_hours(data, field_name):
    for item in data['data']:
        parsed_time_list = []
        for working_hour in item[field_name]:
            time_parts = working_hour.split('-')
            begin_time = datetime.strptime(time_parts[0], "%H:%M")
            end_time = datetime.strptime(time_parts[1], "%H:%M")
            if begin_time > end_time:
                end_time += timedelta(days=1)
            parsed_time_list.append((begin_time, end_time))
        item['working_hours'] = parsed_time_list
