# %%
import re
import csv
import glob


def parser_log(filename):

    # 打开日志文件并读取内容
    with open(filename, 'r') as file:
        log = file.read()

    # 使用正则表达式分割日志文件
    sections = re.split(r'-{48}', log)

    data_list = []

    for section in sections:
        if section == '':
            continue
        # 使用正则表达式匹配需要的信息
        workload = re.search(r'workload: (\w+),', section)
        threads = re.search(r'threads: (\d+)', section)
        loaded_throughput = re.search(r'Throughput: load, ([\d\.]+) Kops/s', section)
        run_throughput = re.search(r'Throughput: run, ([\d\.]+) Kops/s', section)
        dram_consumption = re.search(r'DRAM consumption: ([\d\.]+) MB.', section)
        page_size = re.search(r'PAGE= (\d+) Bytes', section)
        write_read_count = re.search(r'Write_count=\s+(\d+) read_count=\s+(\d+)', section)
        filesize = re.search(r'fielsize=\s+(\d+)', section)
        zone_read_written = re.search(r'\[Zone\] Read:\s+(\d+) Units, Written:\s+(\d+) Units', section)
        load_size = re.search(r'Load size: (\d+),', section)
        run_size = re.search(r'Run size: (\d+)', section)

        # 如果所有信息都找到了，就创建一个字典来存储这些信息
        # if workload and threads and loaded_throughput and run_throughput and dram_consumption and page_size and write_read_count:
        try:
            data = {
                'workload': workload.group(1),
                'threads': threads.group(1),
                'loaded_keys': load_size.group(1),
                'running_keys': run_size.group(1),
                'loaded_throughput': loaded_throughput.group(1),
                'run_throughput': run_throughput.group(1),
                'dram_consumption': dram_consumption.group(1),
                'page_size': page_size.group(1),
                'write_count': write_read_count.group(1) ,
                'read_count':write_read_count.group(2),
                'filesize': filesize.group(1),
                'zone_read': float(zone_read_written.group(1))*1000*512/(int(page_size.group(1))),
                'zone_written': float(zone_read_written.group(2))*1000*512/(int(page_size.group(1)))
            }
            data_list.append(data)
        except Exception as e:
            print(e)
            continue
            
    
    output_csv_file=filename.replace('.log','.csv')
    # 将字典列表写入CSV文件
    with open(output_csv_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data_list[0].keys())
        writer.writeheader()
        for data in data_list:
            writer.writerow(data)
        print(f"parser data {output_csv_file} success!")

# %%
file_list = glob.glob('./result/log/*.log')
for file in file_list:
    parser_log(file)
# %%
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# 读取CSV文件
# print(file_list[0].replace('.log','.csv'))
# df = pd.read_csv(file_list[0].replace('.log','.csv'))

def plot_ycsb(file):
    df = pd.read_csv(file)
    print(file)
    plt.figure()
    # 定义柱子的宽度
    bar_width = 0.2

    # 生成柱子的位置
    r1 = np.arange(len(df['workload']))
    r2 = [x + bar_width for x in r1]
    r3 = [x + bar_width for x in r2]
    r4=[x + bar_width for x in r3]
    r5=[x + bar_width for x in r4]

    bars1 = plt.bar(r2, df['write_count'], width=bar_width, label='write_count')
    bars2 = plt.bar(r3, df['read_count'], width=bar_width, label='read_count')
    bars3 = plt.bar(r1, df['filesize'], width=bar_width, label='filesize')
    # bars4 = plt.bar(r3, df['zone_read'], width=bar_width, label='zone_read')
    # bars5 = plt.bar(r4, df['zone_written'], width=bar_width, label='zone_written')
    # 在柱子上添加标签
    i=0
    for bar in bars2:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, round(yval/df['filesize'][i], 0), ha='center', va='bottom')
        i+=1
    i=0
    for bar in bars1:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, round(yval/df['filesize'][i], 0), ha='center', va='bottom')
        i+=1
    i=0
    # for bar in bars4:
    #     yval = bar.get_height()
    #     plt.text(bar.get_x() + bar.get_width()/2, yval, round(yval/(df['filesize'][i]), 0), ha='center', va='bottom')
    #     i+=1
    # i=0
    # for bar in bars5:
    #     yval = bar.get_height()
    #     plt.text(bar.get_x() + bar.get_width()/2, yval, round(yval/(df['filesize'][i]), 0), ha='center', va='bottom')
    #     i+=1



    # 设置x轴的标签
    plt.xticks([r + bar_width for r in range(len(df['workload']))], df['workload'])

    # 设置图像的标题和坐标轴标签
    plt.title(f"PageSize: {file.split('-')[-2]} Load 1M Run 1M Pairs")
    plt.xlabel('Workload')
    plt.ylabel('Relative Value')

    # 添加图例
    plt.legend()

    # 显示图像
    plt.show()
# %%
for file in file_list:
    plot_ycsb(file.replace('.log','.csv'))
# %%
