# Run dbsp in a memory constrained environment to measure the performance impact of OS swapping
# the process

cd /sys/fs/cgroup
sudo mkdir dbsp
cd dbsp

sudo chown -R ${USER}:${USER} /sys/fs/cgroup/dbsp

# No limit
sudo sh -c 'echo 128G > /sys/fs/cgroup/dbsp/memory.high'
sudo sh -c 'echo 128G > /sys/fs/cgroup/dbsp/memory.max'

# cat cgroup.controllers
sudo sh -c 'echo 128M > /sys/fs/cgroup/dbsp/memory.high'
sudo sh -c 'echo 128M > /sys/fs/cgroup/dbsp/memory.max'



sudo mkdir tasks
cd tasks


sudo swapon -a

# Run dbsp
#sudo cgexec -g memory:dbsp memhog 2G

sudo cgexec -g memory:dbsp bash

cargo bench --bench nexmark --features "with-nexmark with-serde with-csv persistence" -- --first-event-rate=5000000 --max-events=3000000 --cpu-cores 1 --num-event-generators 6 --source-buffer-size 10000 --input-batch-size 40000 --query q9