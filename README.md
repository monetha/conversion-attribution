# User journey phases and conversion attribution matrix
Executable file `user-journey-conversion-attribution-matrix.py`  
Creates matrix with :  
* Rows - number of session in chain  
* Columns - user journey phase  
* Values - count of such sessions  

Start params  
```bash
python3 user-journey-conversion-attribution-matrix.py --help
```
DS-instance execution path `/home/ubuntu/conversion-attribution/user-journey-conversion-attribution-matrix.py`


# Source/Medium/Campaign Heatmap 
Executable file `source_medium_heatmap.py`  
Sessions count in proxy chains of customers grouped by first/last sources and mediums.

Start params  
```bash
python3 source_medium_heatmap.py --help
```

*Production params* `--only_conversion=True --proxy_days=0`  
Example
```bash
python3 source_medium_heatmap.py --account_id={ACCOUNT_ID} --start_date={START_DATE} --end_date={END_DATE} --only_conversion=True --proxy_days=0
```

DS-instance execution path `/home/ubuntu/conversion-attribution/source_medium_heatmap.py`