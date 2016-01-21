# kaka-partition-migration-script
kafka partition迁移策略脚本

usage:

file_test:
>python stable.py mapi mapidescribe

online:
>python stable.py mapi


improve:

>维护一个list，这个list保存存活的broker，将partition迁移到这个list有的broker上；
