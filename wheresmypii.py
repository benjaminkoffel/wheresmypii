from __future__ import print_function # Python 2/3 compatibility
import re
import csv
import boto3
from botocore.client import Config
import sys

s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))

num_files_to_check = 100
max_file_size = 1000
num_props_before_found = 2
num_found_before_continue = 5

street_names = []
last_names = []
first_names = []

def parse_text_for_mobile(text):
    matches = re.findall(r'043[012]{1}[- ]?[0-9]{3}[- ]?[0-9]{3}', text)
    return ''.join(matches)

def parse_text_for_email(text):
    matches = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', text)
    return ''.join(matches)

def parse_text_for_address(text):
    for street_name in street_names:
        if len(street_name) > 4 and street_name in text:
            return street_name
    return ''

def parse_text_for_name(text):
    for last_name in last_names:
        if len(last_name) > 4 and last_name in text:
            for first_name in first_names:
                full_name = first_name + ' ' + last_name
                if full_name in text:
                    return full_name
                full_name =  last_name + ', ' + first_name
                if full_name in text:
                    return full_name
                full_name =  last_name + ',' + first_name
                if full_name in text:
                    return full_name
    return ''

def parse_text_for_pii(text):

    results = {}

    text = text.upper()

    mobile = parse_text_for_mobile(text)
    if len(mobile) > 0:
        results['MOBILE'] = mobile

    email = parse_text_for_email(text)
    if len(email) > 0:
        results['EMAIL'] = email

    address = parse_text_for_address(text)
    if len(address) > 0:
        results['ADDRESS'] = address

    name = parse_text_for_name(text)
    if len(name) > 0:
        results['NAME'] = name

    return results

if __name__ == '__main__':

    print('loading lists...')

    with open('lists/street-names.csv', 'rb') as csvfile:
        for row in csv.reader(csvfile, delimiter=','):
            street_names.append(row[0])
            if row[2] == 'RD':
                street_names.append(row[1] + ' ROAD')
            if row[2] == 'ST':
                street_names.append(row[1] + ' STREET')
            if row[2] == 'AVE':
                street_names.append(row[1] + ' AVENUE')
            if row[2] == 'TER':
                street_names.append(row[1] + ' TERRACE')
            if row[2] == 'CT':
                street_names.append(row[1] + ' COURT')
            if row[2] == 'ALY':
                street_names.append(row[1] + ' ALLEY')
            if row[2] == 'WAY':
                street_names.append(row[1] + ' WAY')
            if row[2] == 'LN':
                street_names.append(row[1] + ' LANE')
            if row[2] == 'LOOP':
                street_names.append(row[1] + ' LOOP')
            if row[2] == 'DR':
                street_names.append(row[1] + ' DRIVE')
            if row[2] == 'AVE':
                street_names.append(row[1] + ' AVENUE')
            if row[2] == 'PL':
                street_names.append(row[1] + ' PLACE')
            if row[2] == 'BLVD':
                street_names.append(row[1] + ' BOULEVARD')
            if row[2] == 'CIR':
                street_names.append(row[1] + ' CIRCUIT')
            if row[2] == 'RAMP':
                street_names.append(row[1] + ' RAMP')
            if row[2] == 'PLZ':
                street_names.append(row[1] + ' PLAZA')
            if row[2] == 'AVE':
                street_names.append(row[1] + ' AVENUE')

    with open('lists/last-names.csv', 'rb') as csvfile:
        for row in csv.reader(csvfile, delimiter=' '):
            last_names.append(row[0])

    with open('lists/first-names.csv', 'rb') as csvfile:
        for row in csv.reader(csvfile, delimiter=' '):
            first_names.append(row[0])

    print('listing s3 buckets...')

    for bucket in s3.buckets.all():
        print('checking bucket: ' + bucket.name + '...')
        found = 0
        for obj in bucket.objects.limit(count=num_files_to_check):
            if obj.size < max_file_size:
                try:
                    content = obj.get()['Body'].read()
                    results = parse_text_for_pii(content)
                    if len(results) >= num_props_before_found:
                        print(obj.key + ':', results)
                        found += 1
                        if (found >= num_found_before_continue):
                            print('moving on...')
                            break
                except Exception as e:
                   print("error:", e)

    print('search process complete.')
