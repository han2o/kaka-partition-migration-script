#!/usr/bin/env python
#-*- coding:utf-8 -*-


import sys
import time
import commands


def check_partition_stat():
#online
    cmd = '/usr/local/kafka_2.10-0.8.2.2/bin/kafka-topics.sh --zookeeper %s:2181 --topic %s --describe' % (zookeeper_host, topic)
    describe = commands.getoutput(cmd)

#file_test
#    testfile_name = sys.argv[2]
#    cmd = 'cat %s' % testfile_name
#    describe = commands.getoutput(cmd)

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
    print '\n需要重新分配的partition id: %s' % partition_id
    if  len(broker_id_die) != 0:
        for i in range(len(broker_id_die)):
            partition_on_broker.pop('broker_%s' % broker_id_die[i])

    for i in range(len(replicas_id)):
        old_assignment_dict[partition_id[i]] = replicas_id[i]

    if len(partition_id) != 0:
        pass
    else:
        print "all partition stat is ok"
        sys.exit(2)

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

        print 'lensP lt lensB broker_ids:  %s' % broker_ids
        print '重新分配的partition-broker的映射: %s' % bro_par_map
    else:
        for i in range(lensB):
            broker_id = sorted_tuple[i][0].split('_')[1]
            broker_ids.append(broker_id)
        for t in range(lensP - lensB):
            broker_id = sorted_tuple[t-(lensP-lensB)][0].split('_')[1]
            broker_ids.append(broker_id)
        for n in range(lensP):
            bro_par_map[partition_id[n]] = broker_ids[n]

        print 'lensP gt lensB broker_ids:  %s' % broker_ids
        print '重新分配的partition-broker的映射: %s' % bro_par_map

    execute_reassign_partition(partition_id, bro_par_map)
        

def execute_reassign_partition(partition_id, bro_par_map):
    
    for i in range(len(partition_replica_assignment['partitions'])):
        if str(partition_replica_assignment['partitions'][i]['partition']) in partition_id:
            partition_replica_assignment['partitions'][i]['replicas'][0] = int(bro_par_map[str(partition_replica_assignment['partitions'][i]['partition'])])

    write_json_file(partition_replica_assignment)

def write_json_file(partition_replica_assignment):

    partition_replica_assignment = str(partition_replica_assignment)
    partition_replica_assignment = partition_replica_assignment.replace('\'', '\"')
    print '\npartition分配策略:\n%s' % partition_replica_assignment
    warning = raw_input('\n是否执行分配(yes or not): ')
    if warning in ['y', 'yes', 'ye', 'Y', 'YES']:
        pass
    else:
        sys.exit(2)
    json_file_name = '%s-reassign-file.json' % topic 
    with open(reassign-file.json, 'w') as f:
        f.write(str(partition_replica_assignment))
    execute_cmd = '/usr/local/kafka_2.10-0.8.2.2/bin/kafka-reassign-partitions.sh --zookeeper %s:2181 --reassignment-json-file %s --execute' % (zookeeper_host, json_file_name)
    print execute_cmd
#    commands.getoutput(execute_cmd)
    time.sleep(5)
    verify_cmd = '/usr/local/kafka_2.10-0.8.2.2/bin/kafka-reassign-partitions.sh --zookeeper %s:2181 --reassignment-json-file %s --verify' % (zookeeper_host, json_file_name)
#    commands.getoutput(verify_cmd)
    print verify_cmd


if __name__ == "__main__":
    if sys.argv[1] in ['-h', '--help', '-help']:
        print'''usage:
file_test:
    python kafka_partition_migrate.py topic_name mapidescribe
online:
    python kafka_partition_migrate.py topic_name
             '''
        sys.exit(2)
    else:
        current_broker_list = raw_input('\nbroker id的参考列表:\n1,6,10,11,12,13,14,15,17\n\n输入存活的broker id:\n')
        zookeeper_host = '10.77.121.59'    
        topic = sys.argv[1]

        topic_json_file_name = '/tmp/topics-to-move-test.json'
        topic_json_file_text  = '{    "topics": [        {            "topic": "%s"        }    ],    "version": 1}' % topic 
        with open(topic_json_file_name, 'w') as f:
            f.write(topic_json_file_text)
        cmd = '/usr/local/kafka_2.10-0.8.2.2/bin/kafka-reassign-partitions.sh --zookeeper %s:2181 --topics-to-move-json-file %s --broker-list %s --generate' % (zookeeper_host, topic_json_file_name, current_broker_list)
        generate = commands.getoutput(cmd)
        generate = generate.split('\n')
        partition_replica_assignment = eval(generate[2])

        check_partition_stat()

