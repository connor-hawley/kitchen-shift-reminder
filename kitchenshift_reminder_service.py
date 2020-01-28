import yaml
import requests
import io
import numpy as np
import pandas as pd
import datetime
import boto3

from collections import defaultdict

def send_one_message(number, message, mailer_num):
    sns = boto3.client('sns')
    ## Create topic
    topic_arn = sns.create_topic(
        Name='kitchen-messenger-temp-{}'.format(mailer_num)
    )['TopicArn']
    ## Add one subscriber
    response = sns.subscribe(
        TopicArn=topic_arn,
        Protocol='sms',
        Endpoint='1' + str(number),
    )
    ## Send message
    sns.publish(
        TopicArn=topic_arn,
        Message=message
    )
    ## Delete topic
    sns.delete_topic(
        TopicArn=topic_arn
    )


def get_and_send_messages():
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
        lunch_boy, dinner_boy1, dinner_boy2 = {}, {}, {}
        for row in df.iterrows():
            row = row[1]
            if row['Kitchen Shift'] == np.nan:
                continue

            if 'Lunch' in row['Kitchen Shift']:
                day_of_week = row['Kitchen Shift'].split()[0]
                lunch_boy['Name'] = row['Name']
                lunch_boy['Number'] = row['Phone Number']
            elif 'Dinner' in row['Kitchen Shift']:
                if not dinner_boy1:
                    dinner_boy1['Name'] = row['Name']
                    dinner_boy1['Number'] = row['Phone Number']
                else:
                    dinner_boy2['Name'] = row['Name']
                    dinner_boy2['Number'] = row['Phone Number']

        if lunch_boy:
            lunch_message = params['message'].format(
                shift_name=day_of_week + ' Lunch',
                shift_time=params['shift_times']['lunch'],
                dinner_option='',
                manager=params['manager'],
                manager_number=params['manager_number'],
                slides=params['slides'],
                docs=params['docs']
            )

            if lunch_boy['Number'] != 0:
                messages[lunch_boy['Number']] = lunch_message

        if dinner_boy1 and dinner_boy2:
            boy1, boy2 = dinner_boy1['Name'], dinner_boy2['Name']
            partner1 = params['dinner_option'].format(partner_name=boy2)
            partner2 = params['dinner_option'].format(partner_name=boy1)

            dinner_message_1 = params['message'].format(
                shift_name=day_of_week + ' Dinner',
                shift_time=params['shift_times']['dinner'],
                dinner_option=partner1,
                manager=params['manager'],
                manager_number=params['manager_number'],
                slides=params['slides'],
                docs=params['docs']
            )
            dinner_message_2 = params['message'].format(
                shift_name=day_of_week + ' Dinner',
                shift_time=params['shift_times']['dinner'],
                dinner_option=partner2,
                manager=params['manager'],
                manager_number=params['manager_number'],
                slides=params['slides'],
                docs=params['docs']
            )

            if dinner_boy1['Number'] != 0:
                messages[dinner_boy1['Number']] = dinner_message_1
            if dinner_boy2['Number'] != 0:
                messages[dinner_boy2['Number']] = dinner_message_2

    i = 0
    for number, message in messages.items():
        send_one_message(number, message, i)
        i += 1

if __name__ == '__main__':
    get_and_send_messages()