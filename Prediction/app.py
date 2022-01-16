from flask import Flask, request
import json
from pathlib import Path
from typing import List
from flask_cors import CORS

class TLM(object):
    def __init__(self) -> None:
        super().__init__()
        self.__dict = {}

    def _read(self, filepath: Path) -> None:
        with open(filepath, 'r', encoding='utf-8') as file:
            for line in file:
                line_ = line.strip('\n').split('\t', 2)
                key = line_[0]
                dict_ = json.loads(line_[1])
                sorted_list = [key for key, value in sorted(dict_.items(), key = lambda kv:(kv[1], kv[0]), reverse=True)]
                self.__dict[key] = sorted_list

    def getSuggestList(self, prefix: str) -> List[str]:
        return self.__dict.get(prefix, [])

app = Flask(__name__)
CORS(app, support_credentials=True)
if __name__ == '__main__':
    app.run()

tlm = TLM()
#tlm._read(Path("./output/part-r-00001"))

for i in range(0, 10):
    p = "F:/output/part-r-0000" + str(i)
    tlm._read(Path(p))

"""
for i in range(10, 50):
    p = "F:/output/part-r-000" + str(i)
    tlm._read(Path(p))

for i in range(100, 256):
    p = "F:/output/part-r-00" + str(i)
    tlm._read(Path(p))
"""

@app.route('/predict', methods=['POST'])
def predict():
    data = json.loads(request.data)
    res_list = tlm.getSuggestList(data['prefix'])
    #print(res_list)
    reply = {
        'data': res_list
    }
    return json.dumps(reply, ensure_ascii=False)

"""
while(1):
    s = input("please input: ")
    print(tlm.getSuggestList(s))
"""