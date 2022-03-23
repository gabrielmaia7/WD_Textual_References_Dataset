import json
import random
import uuid
import numpy as np
import time
import requests
import traceback
import pdb
import math
import ast
import pandas as pd
import pdb
import pickle
from qwikidata.linked_data_interface import get_entity_dict_from_api
from qwikidata.sparql import return_sparql_query_results
import ast
from datetime import datetime

from urllib3.exceptions import MaxRetryError, ConnectionError
from qwikidata.linked_data_interface import LdiResponseNotOk

import hashlib

class CachedWikidataAPI():
    
    def __init__(self, cache_path = 'entity_cache.p', save_every_x_queries=1):
        self.save_every_x_queries = save_every_x_queries
        self.x_queries_passed = 0
        self.languages = ['en','fr','es','pt','pt-br','it','de']
        self.cache_path = cache_path
        try:
            with open(self.cache_path,'rb') as f:
                self.entity_cache = pickle.load(f)
        except FileNotFoundError:
            print('Cache not found, creating a new one once a save is triggered')
            self.entity_cache = {}
            
    def get_unique_id_from_str(self, my_str):
        return hashlib.md5(str.encode(my_str)).hexdigest()
        
    def save_entity_cache(self, force=False):
        if force:
            self.x_queries_passed = self.save_every_x_queries
        self.x_queries_passed = self.x_queries_passed+1
        if self.x_queries_passed >= self.save_every_x_queries:
            with open(self.cache_path,'wb') as f:
                pickle.dump(self.entity_cache,f)
            self.x_queries_passed = 0

    def get_entity(self, item_id, use_cache=True):
        if item_id in self.entity_cache and use_cache:
            return self.entity_cache[item_id]
        while True:
            try:
                entity = get_entity_dict_from_api(item_id)
                self.entity_cache[item_id] = entity
                self.save_entity_cache()
                return entity
            except (ConnectionError, MaxRetryError) as e:
                #traceback.print_exc()
                time.sleep(1)
                continue
            except LdiResponseNotOk:
                #traceback.print_exc()
                self.entity_cache[item_id] = 'deleted'
                self.save_entity_cache()
                return 'deleted'

    def get_label(self, item, non_language_set=False):
        if type(item) == str:        
            entity = self.get_entity(item)
            if entity == 'deleted':
                return (entity, 'none')
            labels = entity['labels' if 'labels' in entity else 'lemmas']
        elif type(item) == dict:
            if 'labels' in item:
                labels = item['labels']
            elif 'lemmas' in item:        
                labels = item['lemmas']
        for l in self.languages:
            if l in labels:
                return (labels[l]['value'], l)
        if non_language_set:
            all_labels = list(labels.keys())
            if len(all_labels)>0:
                # get first language not in the set
                return (labels[all_labels[0]]['value'], all_labels[0])
        return ('no-label', 'none')
    
    def get_desc(self, item, non_language_set=False):
        if type(item) == str:        
            entity = self.get_entity(item)
            if entity == 'deleted':
                return (entity, 'none')
            descriptions = entity['descriptions']
        elif type(item) == dict:
            if 'descriptions' in item:
                descriptions = item['descriptions']
        for l in self.languages:
            if l in descriptions:
                return (descriptions[l]['value'], l)
        if non_language_set:
            all_descriptions = list(descriptions.keys())
            if len(all_descriptions)>0:
                return (descriptions[all_descriptions[0]]['value'], all_descriptions[0])
        return ('no-desc', 'none')
    
    def get_alias(self, item, non_language_set=False):
        if type(item) == str:        
            entity = self.get_entity(item)
            if entity == 'deleted':
                return ([entity], 'none')
            aliases = entity['aliases']
        elif type(item) == dict:
            if 'aliases' in item:
                aliases = item['aliases']
        for l in self.languages:
            if l in aliases:
                return ([alias['value'] for alias in aliases[l]], l)
        if non_language_set:
            all_aliases = list(aliases.keys())
            if len(all_aliases)>0:          
                #return (aliases[all_aliases[0]]['value'], all_aliases[0])
                return ([alias['value'] for alias in aliases[all_aliases[0]]], all_aliases[0])
        return ('no-alias', 'none')

    def get_datatype(self, item):
        try:
            if type(item) == str:
                entity = self.get_entity(item)
                if entity == 'deleted':
                    return entity
                datatype = entity['datatype']
            elif type(item) == dict:
                datatype = item['datatype']
            return datatype
        except KeyError:
            return 'none'

    def get_claim_values_of(self, item, property_id):
        if type(item) == str:
            entity = self.get_entity(item)
            if entity == 'deleted':
                return entity
            claims = entity['claims']
        elif type(item) == dict:
            claims = item['claims']
        if property_id in claims:
            instance_of_claims = claims[property_id]
            return [i['mainsnak']['datavalue']['value']['id'] for i in instance_of_claims]
        else:
            return []

    def query_sparql_endpoint(self, sparql_query, use_cache=True):
        sparql_query_id = self.get_unique_id_from_str(sparql_query)
        headers = {
            'User-Agent': 'Research on Wikidata done by KCL PhD student Gabriel Amaral',
            'From': 'gabriel.amaral@kcl.ac.uk'  # This is another valid field
        }
        if sparql_query_id in self.entity_cache and use_cache:
            return self.entity_cache[sparql_query_id]
        else:
            wikidata_sparql_url = 'https://query.wikidata.org/sparql'
            try:
                while True:
                    res = requests.get(
                        wikidata_sparql_url, headers = headers, params={"query": sparql_query, "format": "json"})
                    if res.status_code in (429,504):
                        pdb.set_trace()
                        time.sleep(1)
                        continue
                    elif res.status_code == 200:
                        res = res.json()
                        self.entity_cache[sparql_query_id] = res
                        self.save_entity_cache()
                        return res
                    else:
                        print(res.status_code)
                        raise Exception
            except json.JSONDecodeError as e:
                #pdb.set_trace()
                print(res, res.__dict__)
                raise e         
                
    def get_object_label_given_datatype(self, datatype, datavalue):
        dt = datatype#row['datatype']
        dv = datavalue#row['datavalue']

        dt_types = ['wikibase-item', 'monolingualtext', 'quantity', 'time', 'string']
        if dv in ['somevalue', 'novalue']:
            return (dv, 'no_lan')
        if dt not in dt_types:
            print(dt)
            raise ValueError
        else:
            try:
                if dt == dt_types[0]:
                    return self.get_label(ast.literal_eval(dv)['value']['id'], True) #get label here
                elif dt == dt_types[1]:
                    dv = ast.literal_eval(dv)
                    return (dv['value']['text'], dv['value']['language'])
                elif dt == dt_types[2]:
                    dv = ast.literal_eval(dv)
                    amount, unit = dv['value']['amount'], dv['value']['unit']
                    if amount[0] == '+':
                        amount = amount[1:]
                    if str(unit) == '1':
                        return (str(amount), 'en')
                    else:
                        unit_entity_id = unit.split('/')[-1]
                        unit = self.get_label(unit_entity_id, True)#get label here
                        return (' '.join([amount, unit[0]]), unit[1])
                elif dt == dt_types[3]:
                    dv = ast.literal_eval(dv)
                    time = dv['value']['time']
                    timezone = dv['value']['timezone']
                    precision = dv['value']['precision']
                    assert dv['value']['after'] == 0 and dv['value']['before'] == 0

                    sufix = 'BC' if time[0] == '-' else ''
                    time = time[1:]

                    if precision == 11: #date
                        return (datetime.strptime(time, '%Y-%m-%dT00:00:%SZ').strftime('%d/%m/%Y') + sufix, 'en')
                    elif precision == 10: #month
                        try:
                            return (datetime.strptime(time, '%Y-%m-00T00:00:%SZ').strftime("%B of %Y") + sufix, 'en')
                        except ValueError:
                            return (datetime.strptime(time, '%Y-%m-%dT00:00:%SZ').strftime("%B of %Y") + sufix, 'en')
                    elif precision == 9: #year
                        try:
                            return (datetime.strptime(time, '%Y-00-00T00:00:%SZ').strftime('%Y') + sufix, 'en')
                        except ValueError:
                            return (datetime.strptime(time, '%Y-%m-%dT00:00:%SZ').strftime('%Y') + sufix, 'en')
                    elif precision == 8: #decade
                        try:
                            return (datetime.strptime(time, '%Y-00-00T00:00:%SZ').strftime('%Y')[:-1] +'0s' + sufix, 'en')
                        except ValueError:
                            return (datetime.strptime(time, '%Y-%m-%dT00:00:%SZ').strftime('%Y')[:-1] +'0s' + sufix, 'en')
                    elif precision == 7: #century
                        try:
                            parsed_time = datetime.strptime(time, '%Y-00-00T00:00:%SZ')
                        except ValueError:
                            parsed_time = datetime.strptime(time, '%Y-%m-%dT00:00:%SZ')
                        finally:                        
                            return (self.__turn_to_century_or_millennium(
                                parsed_time.strftime('%Y'), mode='C'
                            ) + sufix, 'en')
                    elif precision == 6: #millennium
                        try:
                            parsed_time = datetime.strptime(time, '%Y-00-00T00:00:%SZ')
                        except ValueError:
                            parsed_time = datetime.strptime(time, '%Y-%m-%dT00:00:%SZ')
                        finally:                        
                            return (self.__turn_to_century_or_millennium(
                                parsed_time.strftime('%Y'), mode='M'
                            ) + sufix, 'en')
                    elif precision == 4: #hundred thousand years 
                        timeint = int(datetime.strptime(time, '%Y-00-00T00:00:%SZ').strftime('%Y'))
                        timeint = round(timeint/1e5,1)
                        return (str(timeint) + 'hundred thousand years' + sufix, 'en')
                    elif precision == 3: #million years 
                        timeint = int(datetime.strptime(time, '%Y-00-00T00:00:%SZ').strftime('%Y'))
                        timeint = round(timeint/1e6,1)
                        return (str(timeint) + 'million years' + sufix, 'en')
                    elif precision == 0: #billion years 
                        timeint = int(datetime.strptime(time, '%Y-00-00T00:00:%SZ').strftime('%Y'))
                        timeint = round(timeint/1e9,1)
                        return (str(timeint) + 'billion years' +sufix, 'en')
                elif dt == dt_types[4]:
                    return (ast.literal_eval(dv)['value'], 'en')
            except ValueError as e:
                pdb.set_trace()
                raise e
            
    def __turn_to_century_or_millennium(self, y, mode):
        y = str(y)
        if mode == 'C':
            div = 100
            group = int(y.rjust(3, '0')[:-2])
            mode_name = 'century'
        elif mode == 'M':
            div = 1000
            group = int(y.rjust(4, '0')[:-3])
            mode_name = 'millenium'
        else:        
            raise ValueError('Use mode = C for century and M for millennium')

        if int(y)%div != 0:
            group += 1
        group = str(group)

        group_suffix = (
            'st' if group[-1] == '1' else (
                'nd' if group[-1] == '2' else (
                    'rd' if group[-1] == '3' else 'th'
                )
            )
        )

        return ' '.join([group+group_suffix, mode_name])
    
    def get_object_desc_given_datatype(self, datatype, datavalue):
        dt = datatype#row['datatype']
        dv = datavalue#row['datavalue']

        dt_types = ['wikibase-item', 'monolingualtext', 'quantity', 'time', 'string']
        if dv in ['somevalue', 'novalue']:
            return (dv, 'no_lan')
        if dt not in dt_types:
            print(dt)
            raise ValueError
        else:
            try:
                if dt == dt_types[0]:
                    return self.get_desc(ast.literal_eval(dv)['value']['id']) #get label here
                elif dt == dt_types[1]:
                    return ('no-desc', 'none')
                elif dt == dt_types[2]:
                    dv = ast.literal_eval(dv)
                    amount, unit = dv['value']['amount'], dv['value']['unit']
                    if amount[0] == '+':
                        amount = amount[1:]
                    if str(unit) == '1':
                        return ('no-desc', 'none')
                    else:
                        unit_entity_id = unit.split('/')[-1]
                        return self.get_desc(unit_entity_id)
                elif dt == dt_types[3]:
                    return ('no-desc', 'none')
                elif dt == dt_types[4]:
                    return ('no-desc', 'none')
            except ValueError as e:
                #pdb.set_trace()
                raise e
                
    def get_object_alias_given_datatype(self, datatype, datavalue):
        dt = datatype#row['datatype']
        dv = datavalue#row['datavalue']

        dt_types = ['wikibase-item', 'monolingualtext', 'quantity', 'time', 'string']
        if dv in ['somevalue', 'novalue']:
            return (dv, 'no_lan')
        if dt not in dt_types:
            print(dt)
            raise ValueError
        else:
            try:
                if dt == dt_types[0]:
                    return self.get_alias(ast.literal_eval(dv)['value']['id']) #get label here
                elif dt == dt_types[1]:
                    return ('no-alias', 'none')
                elif dt == dt_types[2]:
                    dv = ast.literal_eval(dv)
                    amount, unit = dv['value']['amount'], dv['value']['unit']
                    if amount[0] == '+':
                        amount = amount[1:]
                        
                    if str(unit) == '1':
                        return ('no-alias', 'none')
                    else:
                        unit_entity_id = unit.split('/')[-1]
                        unit_alias = self.get_alias(unit_entity_id)
                        return ([(amount + ' ' + u) for u in unit_alias[0]], unit_alias[1])
                elif dt == dt_types[3]:
                    dv = ast.literal_eval(dv)
                    time = dv['value']['time']
                    timezone = dv['value']['timezone']
                    precision = dv['value']['precision']
                    assert dv['value']['after'] == 0 and dv['value']['before'] == 0

                    sufix = 'BC' if time[0] == '-' else ''
                    time = time[1:]

                    if precision == 11: #date
                        return ([
                            datetime.strptime(time, '%Y-%m-%dT00:00:%SZ').strftime('%-d of %B, %Y') + sufix,
                            datetime.strptime(time, '%Y-%m-%dT00:00:%SZ').strftime('%d/%m/%Y (dd/mm/yyyy)') + sufix,
                            datetime.strptime(time, '%Y-%m-%dT00:00:%SZ').strftime('%b %-d, %Y') + sufix
                        ], 'en')
                    else: #month
                        return ('no-alias', 'none')
                elif dt == dt_types[4]:
                    return ('no-alias', 'none')
            except ValueError as e:
                #pdb.set_trace()
                raise e