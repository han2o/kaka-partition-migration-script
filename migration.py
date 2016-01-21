#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''usage:
file_test:
python v2.py mapi mapidescribe
online:
python v2.py mapi
'''

import re
import sys
import time
import commands


def check_partition_stat():
#online
#    cmd = '/usr/local/kafka_2.10-0.8.2.2/bin/kafka-topics.sh --zookeeper %s:2181 --topic %s --describe' % (zookeeper_host, topic)
#    describe = commands.getoutput(cmd)

#file_test
    testfile_name = sys.argv[2]
    cmd = 'cat %s' % testfile_name
    describe = commands.getoutput(cmd)
    str = describe.split('\n')
    partition_on_broker = {}
    klst = []
    partition_id = []
    broker_id_sur = []
    replicas_id = []
    dict_lst = []
    old_assignment_dict = {}
    for m in range(1, len(str)):
        lst = str[m].split('\t')
        lst.pop(0)
        for n in range(len(lst)):
            lst[n] = "'"+lst[n].split(': ')[0]+"'"+ ": " +"'"+lst[n].split(": ")[1]+"'"
            klst.append(lst[n])
        describe_dict = eval('{'+','.join(klst)+'}')
        dict_lst.append(describe_dict)

    for i in range(len(dict_lst)):
        if dict_lst[i]['Leader'] == '-1':
            partition_id.append(dict_lst[i]['Partition'])
            replicas_id.append(dict_lst[i]['Replicas'])
        else:
            broker_id_sur.append(dict_lst[i]['Replicas'])

        #计算每个broker上partition的数量
        key = '_'.join(("broker", dict_lst[i]['Replicas']))
        if partition_on_broker.has_key(key):
            partition_on_broker[key] += 1
        else:
            partition_on_broker[key] = 1
    #存活的broker去重
    broker_id_sur = list(set(broker_id_sur))
    broker_id_die = list(set(replicas_id))
    print 'broker_id_sur: %s' % broker_id_sur
    print 'partition_id on die broker: %s' % partition_id
    print 'broker not on line: %s' % broker_id_die
    print 'partition_on_broker: %s' % partition_on_broker
    if  len(broker_id_die) != 0:
        for i in range(len(broker_id_die)):
            partition_on_broker.pop('broker_%s' % broker_id_die[i])
    print 'partition_on_broker: %s' % partition_on_broker

    for i in range(len(replicas_id)):
        old_assignment_dict[partition_id[i]] = replicas_id[i]

    if len(partition_id) != 0:
        pass
        #根据partition_id的长度决定需要迁移到的broker的数量
    else:
        print "all partition stat is ok"
        sys.exit(2)

    print 'old_assign_mapping:  %s' % old_assignment_dict

    mapping_(partition_on_broker, partition_id, broker_id_sur)


def mapping_(partition_on_broker, partition_id, broker_id_sur):
    #将broker_id和partition_id映射
    bro_par_map = {}
    broker_ids = []
    lensP = len(partition_id)
    lensB = len(broker_id_sur)
    #对partition_on_broker 排序，根据len(partition_id) 的大小，挑选可以接受迁移的broker
    sorted_tuple = sorted(partition_on_broker.items(), lambda x, y : cmp(x[1], y[1]))
    if lensP <= lensB:
        for i in range(lensP):
            broker_id = sorted_tuple[i][0].split('_')[1]
            broker_ids.append(broker_id)
        for n in range(lensP):
            bro_par_map[partition_id[n]] = broker_ids[n]

        print 'lensP gt lensB broker_ids: %s' % broker_ids
        print 'reassign_bro_par_map: %s' % bro_par_map
    else:
        for i in range(lensB):
            broker_id = sorted_tuple[i][0].split('_')[1]
            broker_ids.append(broker_id)
        for t in range(lensP - lensB):
            broker_id = sorted_tuple[t-(lensP-lensB)][0].split('_')[1]
            broker_ids.append(broker_id)
        for n in range(lensP):
            bro_par_map[partition_id[n]] = broker_ids[n]

        print 'lensP lt lensB broker_ids: %s' % broker_ids
        print 'reassign_bro_par_map: %s' % bro_par_map


    execute_reassign_partition(partition_id, bro_par_map)
        

def execute_reassign_partition(partition_id, bro_par_map):
    
    for i in range(len(partition_replica_assignment['partitions'])):
        if str(partition_replica_assignment['partitions'][i]['partition']) in partition_id:
            partition_replica_assignment['partitions'][i]['replicas'][0] = bro_par_map[str(partition_replica_assignment['partitions'][i]['partition'])]

    write_json_file(partition_replica_assignment)

def write_json_file(partition_replica_assignment):

    print '\n'
    partition_replica_assignment = str(partition_replica_assignment)
    partition_replica_assignment = partition_replica_assignment.replace('\'', '\"')
    print partition_replica_assignment
    print '\n'
    warning = raw_input('continue or not: ')
    if warning in ['y', 'yes', 'ye', 'Y', 'YES']:
        pass
    else:
        sys.exit(2)
    print '\n'
    json_file_name = '%s-reassign-file.json' % topic 
#    with open(json_file_name, 'w') as f:
#        f.write(str(partition_replica_assignment))
    execute_cmd = '/usr/local/kafka_2.10-0.8.2.2/bin/kafka-reassign-partitions.sh --zookeeper %s:2181 --reassignment-json-file %s --execute' % (zookeeper_host, json_file_name)
    print execute_cmd
#    execute_result = commands.getoutput(execute_cmd)
    time.sleep(5)
    verify_cmd = '/usr/local/kafka_2.10-0.8.2.2/bin/kafka-reassign-partitions.sh --zookeeper %s:2181 --reassignment-json-file %s --verify' % (zookeeper_host, json_file_name)
#    verify_result = commands.getoutput(verify_cmd)
    print verify_cmd


if __name__ == "__main__":
    current_broker_list = raw_input('last_broker_list:\n1,6,10,11,12,13,14,15,17\n\nenter current_broker_list: ')
    zookeeper_host = '10.77.121.59'    
    topic = sys.argv[1]

    topic_json_file_name = '/usr/home/yangqi5/topics-to-move-test.json'
    topic_json_file_text  = '{    "topics": [        {            "topic": "%s"        }    ],    "version": 1}' % topic 
    with open(topic_json_file_name, 'w') as f:
        f.write(topic_json_file_text)
    cmd = '/usr/local/kafka_2.10-0.8.2.2/bin/kafka-reassign-partitions.sh --zookeeper %s:2181 --topics-to-move-json-file %s --broker-list %s --generate' % (zookeeper_host, topic_json_file_name, current_broker_list)
    generate = commands.getoutput(cmd)
    generate = generate.split('\n')
    partition_replica_assignment = eval(generate[2])

    check_partition_stat()


