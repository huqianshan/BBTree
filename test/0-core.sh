#/bin/bash

log_dir=./test/result/log/
exe_dir=./build/test/

tree_list=("btree-olc")
bench_list=(ycsba ycsbb ycsbc)
thread_list=(1 2 4 8 16 32 48 56)
# maxium ops
threshold_list=(20)
# echo $(pwd)
# echo ${exe_dir}${tree_list[0]}
# exit
test_name="Ycsb-${1}"
clean=$2
if [[ "$clean" == "clean" ]]; then
    for h in ${tree_list[@]}; do
        rm -f ${log_dir}${test_name}-${h}.log
    done
fi

for h in ${tree_list[@]}; do
    rlog=${log_dir}${test_name}-${h}.log
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
