#/bin/bash

log_dir=./result/log/
exe_dir=./build/

tree_list=("bbtree")
bench_list=(ycsba ycsbb ycsbc ycsbd ycsbe ycsbg)
thread_list=(1)
# maxium ops
threshold_list=(1 2)

test_name="Ycsb"
clean=$1
if [[ "$clean" == "clean" ]]; then
    for h in ${tree_list[@]}; do
        rm -f ${log_dir}${test_name}-${h}.txt
    done
fi

for h in ${tree_list[@]}; do
    rlog=${log_dir}${test_name}-${h}.txt
    for load in ${bench_list[@]}; do
        for tid in ${thread_list[@]}; do
            for threshold in ${threshold_list[@]}; do
                echo "------------------------------------------------" >>${rlog}
                numactl -N 0 ${exe_dir}${h} ${load} ${tid} ${threshold} 2>&1 | tee -a ${rlog}
                echo "----------------------------------------\n\n" >>${rlog}
                echo "------------------------------------------------\n"
            done
        done
    done
done
