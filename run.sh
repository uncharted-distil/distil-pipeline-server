export SOLUTION_SERVER_RESULT_DIR=$PWD/datasets
export SOLUTION_SEND_DELAY=2000
export SOLUTION_NUM_UPDATES=3
export SOLUTION_MAX_SOLUTIONS=5
export SOLUTION_ERR_PERCENT=0.1

witch --cmd="make compile && make fmt && go run main.go" --watch="main.go,pipeline/**/*.go,env/**/*.go" --ignore=""
