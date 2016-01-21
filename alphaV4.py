#!/usr/bin/env python
#-*- coding:utf-8 -*-

import re
import sys
import time
import commands


def check_partition_stat():
#    cmd = '/usr/local/kafka_2.10-0.8.2.2/bin/kafka-topics.sh --zookeeper %s:2181 --topic mapi --describe' %s zookeeper_host
#    describe = commands.getoutput(cmd)
    testfile_name = sys.argv[2]
    cmd = 'cat %s' % testfile_name
    describe = commands.getoutput(cmd)
    str = describe.split('\n')
    partition_on_broker = {}
    klst = []
    partition_id = []
    replicas_id = []
    dict_lst = []
    old_assignment_dict = {}
    for m in range(1, len(str)):
        lst = str[m].split('\t')
        lst.pop(0)
#        map(lambda x: describe_dict.setdefault(x.split(':')[0], x.split(':')[1]), lst)
        for n in range(len(lst)):
            lst[n] = "'"+lst[n].split(': ')[0]+"'"+ ": " +"'"+lst[n].split(": ")[1]+"'"
            klst.append(lst[n])
        describe_dict = eval('{'+','.join(klst)+'}')
        dict_lst.append(describe_dict)

    for i in range(len(dict_lst)):
        if dict_lst[i]['Leader'] == '-1':
            partition_id.append(dict_lst[i]['Partition'])
            replicas_id.append(dict_lst[i]['Replicas'])

        #计算每个broker上partition的数量
        key = '_'.join(("broker", dict_lst[i]['Replicas']))
        if partition_on_broker.has_key(key):
            partition_on_broker[key] += 1
        else:
            partition_on_broker[key] = 1
    print 'partition_on_broker: %s' % partition_on_broker

    for i in range(len(replicas_id)):
        old_assignment_dict[partition_id[i]] = replicas_id[i]

    if len(partition_id) != 0:
        print 'leader_value_-1_partition_id: %s' % partition_id
        #根据partition_id的长度决定需要迁移到的broker的数量
    else:
        print "all partition is ok"
        sys.exit(2)

    print 'old_assign_mapping:  %s' % old_assignment_dict

    mapping_(partition_on_broker, partition_id)


def mapping_(partition_on_broker, partition_id):
    #将broker_id和partition_id映射
    bro_par_map = {}
    broker_ids = []
    lens = len(partition_id)
    #对partition_on_broker 排序，根据len(partition_id) 的大小，挑选可以接受迁移的broker
    sorted_tuple = sorted(partition_on_broker.items(), lambda x, y : cmp(x[1], y[1]))
    for i in range(lens):
        broker_id = sorted_tuple[i][0].split('_')[1]
        broker_ids.append(broker_id)
#    print 'partitions_remove_to_broker_ids: %s' % broker_ids
    for n in range(lens):
        bro_par_map[partition_id[n]] = broker_ids[n]
#    return bro_par_map
    print 'reassign_bro_par_map: %s' % bro_par_map
    execute_reassign_partition(partition_id, bro_par_map)
        

def execute_reassign_partition(partition_id, bro_par_map):
    
#    print '\n'
#    print 'execute_partition_id: %s' % partition_id
#    print 'execute_bro_par_map: %s' % bro_par_map
    for i in range(len(partition_replica_assignment['partitions'])):
        if str(partition_replica_assignment['partitions'][i]['partition']) in partition_id:
#            print str(partition_replica_assignment['partitions'][i]['partition'])
#            print str(partition_replica_assignment['partitions'][i]['replicas'][0])
#            print bro_par_map[str(partition_replica_assignment['partitions'][i]['partition'])]
#            print '\n'
            partition_replica_assignment['partitions'][i]['replicas'][0] = bro_par_map[str(partition_replica_assignment['partitions'][i]['partition'])]
#            print str(partition_replica_assignment['partitions'][i]['partition'])
#            print str(partition_replica_assignment['partitions'][i]['replicas'][0])

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
    current_broker_list = [1,6,10,11,12,13,14,15,17]
    print 'current broker list: %s' % current_broker_list
    print '\n'
    zookeeper_host = '10.77.121.59'    
#    topic = raw_input('enter topic name: ')
    topic = sys.argv[1]
    if topic == 'mapi':
        partition_replica_assignment = {"version":1,"partitions":[{"topic":"mapi","partition":18,"replicas":[13]},{"topic":"mapi","partition":13,"replicas":[11]},{"topic":"mapi","partition":5,"replicas":[14]},{"topic":"mapi","partition":1,"replicas":[10]},{"topic":"mapi","partition":11,"replicas":[6]},{"topic":"mapi","partition":14,"replicas":[12]},{"topic":"mapi","partition":21,"replicas":[1]},{"topic":"mapi","partition":10,"replicas":[6]},{"topic":"mapi","partition":7,"replicas":[15]},{"topic":"mapi","partition":9,"replicas":[1]},{"topic":"mai","partition":3,"replicas":[12]},{"topic":"mapi","partition":8,"replicas":[17]},{"topic":"mapi","partition":6,"replicas":[15]},{"topic":"mapi","partition":15,"replicas":[13]},{"topic":"mapi","partition":16,"replicas":[14]},{"topic":"mapi","partition":17,"replicas":[15]},{"topic":"mapi","partition":4,"replicas":[13]},{"topic":"mapi","partition":2,"replicas":[11]},{"topic":"mapi","partition":0,"replicas":[6]},{"topic":"mapi","partition":12,"replicas":[10]},{"topic":"mapi","partition":20,"replicas":[1]},{"topic":"mapi","partition":19,"replicas":[17]}]}
    else:
        partition_replica_assignment = {"version":1,"partitions":[{"topic":"accesslog","partition":13,"replicas":[14]},{"topic":"accesslog","partition":4,"replicas":[15]},{"topic":"accesslog","partition":26,"replicas":[17]},{"topic":"accesslog","partition":56,"replicas":[17]},{"topic":"accesslog","partition":33,"replicas":[14]},{"topic":"accesslog","partition":39,"replicas":[10]},{"topic":"accesslog","partition":18,"replicas":[6]},{"topic":"accesslog","partition":2,"replicas":[13]},{"topic":"accesslog","partition":25,"replicas":[10]},{"topic":"accesslog","partition":50,"replicas":[11]},{"topic":"accesslog","partition":16,"replicas":[17]},{"topic":"accesslog","partition":10,"replicas":[11]},{"topic":"accesslog","partition":29,"replicas":[10]},{"topic":"accesslog","partition":32,"replicas":[13]},{"topic":"accesslog","partition":45,"replicas":[12]},{"topic":"accesslog","partition":54,"replicas":[15]},{"topic":"accesslog","partition":58,"replicas":[6]},{"topic":"accesslog","partition":38,"replicas":[6]},{"topic":"accesslog","partition":20,"replicas":[11]},{"topic":"accesslog","partition":36,"replicas":[17]},{"topic":"accesslog","partition":51,"replicas":[12]},{"topic":"accesslog","partition":43,"replicas":[14]},{"topic":"accesslog","partition":6,"replicas":[17]},{"topic":"accesslog","partition":42,"replicas":[13]},{"topic":"accesslog","partition":27,"replicas":[1]},{"topic":"accesslog","partition":8,"replicas":[6]},{"topic":"accesslog","partition":48,"replicas":[6]},{"topic":"accesslog","partition":19,"replicas":[10]},{"topic":"accesslog","partition":5,"replicas":[1]},{"topic":"accesslog","partition":35,"replicas":[11]},{"topic":"accesslog","partition":46,"replicas":[17]},{"topic":"accesslog","partition":23,"replicas":[14]},{"topic":"accesslog","partition":37,"replicas":[1]},{"topic":"accesslog","partition":0,"replicas":[11]},{"topic":"accesslog","partition":3,"replicas":[14]},{"topic":"accesslog","partition":22,"replicas":[13]},{"topic":"accesslog","partition":7,"replicas":[1]},{"topic":"accesslog","partition":52,"replicas":[13]},{"topic":"accesslog","partition":28,"replicas":[6]},{"topic":"accesslog","partition":15,"replicas":[6]},{"topic":"accesslog","partition":41,"replicas":[12]},{"topic":"accesslog","partition":44,"replicas":[15]},{"topic":"accesslog","partition":11,"replicas":[12]},{"topic":"accesslog","partition":57,"replicas":[1]},{"topic":"accesslog","partition":47,"replicas":[1]},{"topic":"accesslog","partition":24,"replicas":[15]},{"topic":"accesslog","partition":12,"replicas":[13]},{"topic":"accesslog","partition":49,"replicas":[10]},{"topic":"accesslog","partition":21,"replicas":[12]},{"topic":"accesslog","partition":34,"replicas":[15]},{"topic":"accesslog","partition":55,"replicas":[13]},{"topic":"accesslog","partition":9,"replicas":[10]},{"topic":"accesslog","partition":31,"replicas":[12]},{"topic":"accesslog","partition":40,"replicas":[11]},{"topic":"accesslog","partition":1,"replicas":[12]},{"topic":"accesslog","partition":59,"replicas":[10]},{"topic":"accesslog","partition":17,"replicas":[1]},{"topic":"accesslog","partition":14,"replicas":[15]},{"topic":"accesslog","partition":53,"replicas":[14]},{"topic":"accesslog","partition":30,"replicas":[11]}]}

    check_partition_stat()


