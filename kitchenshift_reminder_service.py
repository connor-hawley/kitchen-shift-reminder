import yaml
import requests
import io
import numpy as np
import pandas as pd
import datetime
import boto3

from collections import defaultdict

def get_and_send_messages():
    params = None
    with open('config.yaml', 'r') as fh:
        params = yaml.load(fh, Loader=yaml.FullLoader)
    query_url = params['sheet_url'].format(id=params['sheet_id'], tqx=params['tqx'], sheet=params['sheet'])
    resp = requests.get(query_url)

    decoded_csv = resp.content.decode('utf-8')
    df = pd.read_csv(io.StringIO(decoded_csv))

    df.columns = ['Date', 'Kitchen Shift', 'Name', 'Phone Number', 'Notes']
    df['Date'].fillna(method='ffill', inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Phone Number'].fillna(value=0, inplace=True)
    df = df.astype({'Phone Number': 'int64'})

    today = datetime.date.today()
    today = datetime.datetime(today.year, today.month, today.day)
    df = df.loc[df['Date'] == today]

    messages = {}
    if not df.empty:
        day_of_week = ''
        lunch_boys, dinner_boys = {}, defaultdict(list)
        for row in df.iterrows():
            row = row[1]
            if row['Kitchen Shift'] == np.nan:
                continue

            if 'Lunch' in row['Kitchen Shift']:
                day_of_week = row['Kitchen Shift'].split()[0]
                lunch_boys['Name'] = row['Name']
                lunch_boys['Number'] = row['Phone Number']
            elif 'Dinner' in row['Kitchen Shift']:
                dinner_boys['Name'].append(row['Name'])
                dinner_boys['Number'].append(row['Phone Number'])

        if lunch_boys:
            lunch_message = params['message'].format(
                shift_name=day_of_week + ' Lunch',
                shift_time=params['shift_times']['lunch'],
                dinner_option='',
                manager=params['manager'],
                manager_number=params['manager_number'],
                slides=params['slides'],
                docs=params['docs']
            )

            if lunch_boys['Number'] != 0:
                messages[lunch_boys['Number']] = lunch_message

        if dinner_boys:
            boys = dinner_boys['Name']
            partner1 = params['dinner_option'].format(partner_name=boys[0])
            partner2 = params['dinner_option'].format(partner_name=boys[1])
            dinner_message_1 = params['message'].format(
                shift_name=day_of_week + ' Dinner',
                shift_time=params['shift_times']['dinner'],
                dinner_option=partner2,
                manager=params['manager'],
                manager_number=params['manager_number'],
                slides=params['slides'],
                docs=params['docs']
            )
            dinner_message_2 = params['message'].format(
                shift_name=day_of_week + ' Dinner',
                shift_time=params['shift_times']['dinner'],
                dinner_option=partner1,
                manager=params['manager'],
                manager_number=params['manager_number'],
                slides=params['slides'],
                docs=params['docs']
            )

            if dinner_boys['Number'][0] != 0:
                messages[dinner_boys['Number'][0]] = dinner_message_1
            if dinner_boys['Number'][1] != 0:
                messages[dinner_boys['Number'][1]] = dinner_message_2


    for number, message in messages.items():
        sns = boto3.client('sns')
        sns.publish(
            TopicArn=params['sns-arn'],
            Message=message
        )

if __name__ == '__main__':
    get_and_send_messages()