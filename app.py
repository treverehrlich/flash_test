""" 
API Model 

"""

import os
from fastapi import FastAPI
from mangum import Mangum
import uvicorn

from app_secrets import AVRL_API_KEY, AVRL_API_SANDBOX_KEY

app = FastAPI()
handler = Mangum(app)

@app.get("/")
def read_root():
   return {f"Welcome to root of the pricing model api. {AVRL_API_KEY}"}

if __name__ == "__main__":
   uvicorn.run(app, host="0.0.0.0", port=8080,debug=True)