#!/usr/bin/python

import sys
import argparse
import json
import elasticsearch

def connect():
   try:
       es = elasticsearch.Elasticsearch() # localhost:9200 by default
   except Exception as e:
       print(str(e))
   return es


def do_simple_query(es, parameters, frequency, frequency_unit):

   result = {
       "query" : {
           "bool": {
               "must": [
                 {
                   "range": {
                     "@timestamp": {
                       "gte": "now-{0}s".format(frequency)
                     }
                   }
                 }
               ]
           }
       }
   }

   for parameter in parameters:
       tmp = { 'match': {} }
       tmp['match'][parameter[0]] = str(parameter[1])
       result['query']['bool']['must'].append(tmp)

   count = es.count(index='logstash-*', body=result)

   return str(float(count['count']) * frequency_unit / frequency)


def do_simple_query_string(query_string, time_range, frequency_unit=1):

   result = {
      "query" : {
         "query_string": {
            "query": query_string + " AND @timestamp: [now-{0}s TO now]".format(time_range),
            "analyze_wildcard": "true"
         },
      }
   }

   count = es.count(index='logstash-*', body=result)

   return float(count['count']) * frequency_unit / time_range


# HTTP

def item_nginx_requests_rate_per_sec(es, frequency, frequency_unit=1):

   parser.add_argument('--time-range', '-t', type=int, required=True, help='(in sec)')
   parser.add_argument('--http-host', type=str)
   args = parser.parse_args()

   query_string = 'type:nginx.access'
   if args.http_host:
      query_string += ' AND nginx_http_host:{0}'.format(args.http_host)

   print "%.4f" % do_simple_query_string(query_string, args.time_range)


def item_nginx_requests_rate_2xx_per_sec(es, parser):

   parser.add_argument('--time-range', '-t', type=int, required=True, help='(in sec)')
   parser.add_argument('--http-host', type=str)
   args = parser.parse_args()

   query_string = 'type:nginx.access AND nginx_response_code:2*'
   if args.http_host:
      query_string += ' AND nginx_http_host:{0}'.format(args.http_host)

   print "%.4f" % do_simple_query_string(query_string, args.time_range)


def item_nginx_requests_rate_3xx_per_sec(es, parser):

   parser.add_argument('--time-range', '-t', type=int, required=True, help='(in sec)')
   parser.add_argument('--http-host', type=str)
   args = parser.parse_args()

   query_string = 'type:nginx.access AND nginx_response_code:3*'
   if args.http_host:
      query_string += ' AND nginx_http_host:{0}'.format(args.http_host)

   print "%.4f" % do_simple_query_string(query_string, args.time_range)


def item_nginx_requests_rate_4xx_per_sec(es, parser):

   parser.add_argument('--time-range', '-t', type=int, required=True, help='(in sec)')
   parser.add_argument('--http-host', type=str)
   args = parser.parse_args()

   query_string = 'type:nginx.access AND nginx_response_code:4*'
   if args.http_host:
      query_string += ' AND nginx_http_host:{0}'.format(args.http_host)

   print "%.4f" % do_simple_query_string(query_string, args.time_range)


def item_nginx_requests_rate_5xx_per_sec(es, parser):

   parser.add_argument('--time-range', '-t', type=int, required=True, help='(in sec)')
   parser.add_argument('--http-host', type=str)
   args = parser.parse_args()

   query_string = 'type:nginx.access AND nginx_response_code:5*'
   if args.http_host:
      query_string += ' AND nginx_http_host:{0}'.format(args.http_host)

   print "%.4f" % do_simple_query_string(query_string, args.time_range)

def item_nginx_requests_avg_latency(es, parser):

  parser.add_argument('--time-range', '-t', type=int, required=True, help='(in sec)')
  parser.add_argument('--http-host', type=str, required=True)
  args = parser.parse_args()

  tmp = {
     "query": {
        "query_string": {
           "query": "type:nginx.access AND nginx_http_host={0} AND @timestamp: [now-{1}s TO now]".format(args.http_host, args.time_range),
           "analyze_wildcard": "true"
        },
     },
     "aggs": {
        "avg_request_time": {
           "avg": {
              "field": "nginx_request_time"
           }
        }
     }
  }

  result = es.search(index='logstash-*', body=tmp)

  value = result['aggregations']['avg_request_time']['value']

  if value is None:
     print ""
  else:
     print value


def item_nginx_discover_hosts(es, parser):

  parser.add_argument('--time-range', '-t', type=int, required=True, help='time range to query (in sec)')
  args = parser.parse_args()

  tmp = {
     "query": {
        "query_string": {
           "query": "type:nginx.access AND @timestamp: [now-{0}s TO now]".format(args.time_range),
           "analyze_wildcard": "true"
        },
     },
     "aggs": {
        "distinct_nginx_http_host": {
           "terms": {
              "field" : "nginx_http_host.raw"
           }
        }
     }
  }

  result = es.search(index='logstash-*', body=tmp)

  tmp = {"data":[]}
  for host in result['aggregations']['distinct_nginx_http_host']['buckets']:
      tmp["data"].append({"{#HOST}":host['key']})
  print json.dumps(tmp)
  sys.exit(0)
       

#
# POSTFIX
#


def item_postfix_mail_sent_per_min(es, frequency=300, frequency_unit=60):

   tmp = {
      "query": {
         "query_string": {
            "query": "postfix_status:sent AND NOT postfix_relay_service:* AND @timestamp: [now-{0}s TO now]".format(frequency),
            "analyze_wildcard": "true"
         }
      }
   }

   count = es.count(index='logstash-*', body=tmp)
   print str(float(count['count']) * frequency_unit / frequency)


def item_postfix_mail_received_per_min(es, frequency=300, frequency_unit=60):
   print do_simple_query(es, [['postfix_status', 'sent'], ['postfix_relay_service', 'deliver']], frequency, frequency_unit)


def item_postfix_mail_deferred_per_min(es, frequency=300, frequency_unit=60):
   print do_simple_query(es, [['postfix_status', 'deferred']], frequency, frequency_unit)


def item_postfix_mail_bounced_per_min(es, frequency=300, frequency_unit=60):
   print do_simple_query(es, [['postfix_status', 'bounced']], frequency, frequency_unit)


def item_postfix_mail_expired_per_min(es, frequency=300, frequency_unit=60):
   print do_simple_query(es, [['postfix_status', 'expired']], frequency, frequency_unit)


# ...

parser = argparse.ArgumentParser(description='Zabbix script to query Elasticsearch')
parser.add_argument('--item', '-i', type=str, required=True, help='item to retrieve')
args, _ = parser.parse_known_args()

es = connect()

# ARG DICT CREATION

dict_args = {}

if args.item == 'nginx.host.discover':
   item_nginx_discover_hosts(es, parser)

elif args.item == 'nginx.requests.latency_avg':
  item_nginx_requests_avg_latency(es, parser)

elif args.item == 'nginx.requests.total_per_sec':
   item_nginx_requests_rate_per_sec(es, parser)

elif args.item == 'nginx.requests.2xx_per_sec':
   item_nginx_requests_rate_2xx_per_sec(es, parser)

elif args.item == 'nginx.requests.3xx_per_sec':
   item_nginx_requests_rate_3xx_per_sec(es, parser)

elif args.item == 'nginx.requests.4xx_per_sec':
   item_nginx_requests_rate_4xx_per_sec(es, parser)

elif args.item == 'nginx.requests.5xx_per_sec':
   item_nginx_requests_rate_5xx_per_sec(es, parser)

elif args.item == 'postfix.count.sent':
  item_postfix_mail_sent_per_min(**dict_args)

elif args.item == 'postfix.count.received':
  item_postfix_mail_received_per_min(**dict_args)

elif args.item == 'postfix.count.deferred':
  item_postfix_mail_deferred_per_min(**dict_args)

elif args.item == 'postfix.count.bounced':
  item_postfix_mail_bounced_per_min(**dict_args)

elif args.item == 'postfix.count.expired':
  item_postfix_mail_expired_per_min(**dict_args)

else:
  print 'ZBX_NOTSUPPORTED'
