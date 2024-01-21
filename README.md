# MapReduce Framework

## Setup:
- Create a virtual environment in Python
```$ python3 -m venv env```
- Activate the virtual machine
```$ source ./env/bin/activate```
- Install all the dependencies in the requirements.txt file 
```$ pip install -r requirements.txt```
- Install mapreduce as package
```$ pip install .```
- Give execute access to bin/mapreduce script
```$ chmod +x bin/mapreduce```

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