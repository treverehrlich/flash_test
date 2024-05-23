""" 
API Model 

"""

import os
from fastapi import FastAPI
from mangum import Mangum
import uvicorn

app = FastAPI()
handler = Mangum(app)

@app.get("/")
def read_root():
   return {"Welcome to root of the pricing model api."}

if __name__ == "__main__":
   uvicorn.run(app, host="0.0.0.0", port=8080,debug=True)