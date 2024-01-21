# MapReduce Framework

## Setup:
- Create a virtual environment in Python
- Activate the virtual machine
- Install all the dependencies in the requirements.txt file 
- Install mapreduce as package
- Give execute access to bin/mapreduce script
```
$ python3 -m venv env
$ source ./env/bin/activate
$ pip install -r requirements.txt
$ pip install .
$ chmod +x bin/mapreduce
```

## To Run:
```
./bin/mapreduce start
```

### To submit a MapReduce job to manager:
``` 
(env) $ mapreduce-submit \
	-h <host> \
	-p <port> \
	-i <input directory> \
	-o <output directory> \
	-m <mapper executable> \
	-r <reducer executable> \
	--nmappers <number of mappers> \
	--nreducers <number of reducers>
```
ex: 
```
(env) $ mapreduce-submit \
	-h localhost \
	-p 6000 \
	-i tests/testdata/input \
	-o output \
	-m tests/testdata/exec/wc_map.sh \
	-r tests/testdata/exec/wc_reduce.sh \
	--nmappers 2 \
	--nreducers 2
```