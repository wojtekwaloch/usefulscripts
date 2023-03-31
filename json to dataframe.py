import pandas as pd
import json

matts_json = {
    "column1": "val1",
    "column2": "val2",
    "column3": {
        "column3.1": "val3.1",
        "column3.2": "val3.2"
    },
    "column4": "val4"
}

json_string = json.dumps(matts_json)
df = pd.read_json(json_string, orient="columns")
print(df)

df = pd.json_normalize(json.loads(json_string))
print(df)


###MH solution:
resp = 'column1,column2,column3,column4 \nval1,val2,{"column3.1":"val3.1","column3.2":"val2.2"},val4 \n'
resp = resp.replace('","','";"')
resp = resp.split('\n')
df = pd.DataFrame([sub.split(",") for sub in resp])
df.columns = df.iloc[0]
df = df.tail(-1)
df['column3']=df['column3'].replace('";"','","')

